/*-------------------------------------------------------------------------
 *
 * nbtsort.c
 *		Build a btree from sorted input by loading leaf pages sequentially.
 *
 * NOTES
 *
 * We use tuplesort.c to sort the given index tuples into order.
 * Then we scan the index tuples in order and build the btree pages
 * for each level.  We load source tuples into leaf-level pages.
 * Whenever we fill a page at one level, we add a link to it to its
 * parent level (starting a new parent level if necessary).  When
 * done, we write out each final page on each level, adding it to
 * its parent level.  When we have only one page on a level, it must be
 * the root -- it can be attached to the btree metapage and we are done.
 *
 * This code is moderately slow (~10% slower) compared to the regular
 * btree (insertion) build code on sorted or well-clustered data.  On
 * random data, however, the insertion build code is unusable -- the
 * difference on a 60MB heap is a factor of 15 because the random
 * probes into the btree thrash the buffer pool.  (NOTE: the above
 * "10%" estimate is probably obsolete, since it refers to an old and
 * not very good external sort implementation that used to exist in
 * this module.  tuplesort.c is almost certainly faster.)
 *
 * It is not wise to pack the pages entirely full, since then *any*
 * insertion would cause a split (and not only of the leaf page; the need
 * for a split would cascade right up the tree).  The steady-state load
 * factor for btrees is usually estimated at 70%.  We choose to pack leaf
 * pages to the user-controllable fill factor (default 90%) while upper pages
 * are always packed to 70%.  This gives us reasonable density (there aren't
 * many upper pages if the keys are reasonable-size) without risking a lot of
 * cascading splits during early insertions.
 *
 * Formerly the index pages being built were kept in shared buffers, but
 * that is of no value (since other backends have no interest in them yet)
 * and it created locking problems for CHECKPOINT, because the upper-level
 * pages were held exclusive-locked for long periods.  Now we just build
 * the pages in local memory and smgrwrite or smgrextend them as we finish
 * them.  They will need to be re-read into shared buffers on first use after
 * the build finishes.
 *
 * Since the index will never be used unless it is completely built,
 * from a crash-recovery point of view there is no need to WAL-log the
 * steps of the build.  After completing the index build, we can just sync
 * the whole file to disk using smgrimmedsync() before exiting this module.
 * This can be seen to be sufficient for crash recovery by considering that
 * it's effectively equivalent to what would happen if a CHECKPOINT occurred
 * just after the index build.  However, it is clearly not sufficient if the
 * DBA is using the WAL log for PITR or replication purposes, since another
 * machine would not be able to reconstruct the index from WAL.  Therefore,
 * we log the completed index pages to WAL if and only if WAL archiving is
 * active.
 *
 * This code isn't concerned about the FSM at all. The caller is responsible
 * for initializing that.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtsort.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"		/* pgrminclude ignore */
#include "utils/rel.h"
#include "utils/sortsupport.h"
#include "utils/tuplesort.h"

/* Magic numbers for parallel state sharing */
#define PARALLEL_KEY_BTREE_SHARED		UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_TUPLESORT			UINT64CONST(0xA000000000000002)
#define PARALLEL_KEY_TUPLESORT_SPOOL2	UINT64CONST(0xA000000000000003)

/*
 * Status record for spooling/sorting phase.  (Note we may have two of
 * these due to the special requirements for uniqueness-checking with
 * dead tuples.)
 */
typedef struct BTSpool
{
	Tuplesortstate *sortstate;	/* state data for tuplesort.c */
	Relation	heap;
	Relation	index;
	bool		isunique;
} BTSpool;

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.  Note that there is a separate tuplesort TOC
 * entry, private to tuplesort.c but allocated by this module on its behalf.
 */
typedef struct BTShared
{
	/*
	 * These fields are not modified throughout the sort.  They primarily exist
	 * for the benefit of worker processes, that need to create BTSpool state
	 * corresponding to that used by the leader.
	 */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isunique;
	bool		isconcurrent;
	int			scantuplesortstates;

	/*
	 * mutex protects all fields before heapdesc.
	 *
	 * These fields contain status information of interest to B-Tree index
	 * builds, that must work just the same when an index is built in
	 * parallel.
	 */
	slock_t		mutex;

	/*
	 * reltuples is the total number of input heap tuples for an index
	 * build.
	 *
	 * havedead and indtuples aggregate state that is collected by
	 * _bt_build_callback() across all workers.  These are needed because a
	 * unified BTBuildState is only present in leader process.
	 */
	double		reltuples;
	bool		havedead;
	double		indtuples;

	/*
	 * This variable-sized field must come last.
	 *
	 * See _bt_parallel_shared_estimate().
	 */
	ParallelHeapScanDescData heapdesc;
} BTShared;

/*
 * Status for leader in parallel index build.
 */
typedef struct BTLeader
{
	/* parallel context itself */
	ParallelContext	   *pcxt;

	/*
	 * nworkertuplesorts is the exact number of worker processes
	 * successfully launched, plus the leader process if it participates as
	 * a worker.
	 */
	int					nworkertuplesorts;

	/*
	 * Leader-as-worker state.
	 *
	 * workersort is the leader-as-worker tuplesort state.  workersort2 is the
	 * corresponding btspool2 state, used only when building unique indexes.
	 */
	Tuplesortstate	   *workersort;
	Tuplesortstate	   *workersort2;

	/*
	 * Leader process convenience pointers to shared state (leader avoids TOC
	 * lookups).
	 *
	 * btshared is the shared state for entire build.  sharedsort is the
	 * shared, tuplesort-managed state passed to each process tuplesort.
	 * sharedsort2 is the corresponding btspool2 shared state, used only when
	 * building unique indexes.
	 */
	BTShared		   *btshared;
	Sharedsort		   *sharedsort;
	Sharedsort		   *sharedsort2;
} BTLeader;

/* Working state for btbuild and its callback */
typedef struct BTBuildState
{
	bool		isunique;
	bool		havedead;
	Relation	heap;
	BTSpool    *spool;

	/*
	 * spool2 is needed only when the index is a unique index. Dead tuples
	 * are put into spool2 instead of spool in order to avoid uniqueness
	 * check.
	 */
	BTSpool    *spool2;
	double		indtuples;

	/*
	 * btleader is only present when a parallel index build is performed,
	 * and only in leader process (actually, only the leader has a
	 * BTBuildState.  Workers have their own spool and spool2, though.)
	 */
	BTLeader   *btleader;
} BTBuildState;

/*
 * Status record for a btree page being built.  We have one of these
 * for each active tree level.
 *
 * The reason we need to store a copy of the minimum key is that we'll
 * need to propagate it to the parent node when this page is linked
 * into its parent.  However, if the page is not a leaf page, the first
 * entry on the page doesn't need to contain a key, so we will not have
 * stored the key itself on the page.  (You might think we could skip
 * copying the minimum key on leaf pages, but actually we must have a
 * writable copy anyway because we'll poke the page's address into it
 * before passing it up to the parent...)
 */
typedef struct BTPageState
{
	Page		btps_page;		/* workspace for page building */
	BlockNumber btps_blkno;		/* block # to write this page at */
	IndexTuple	btps_minkey;	/* copy of minimum key (first item) on page */
	OffsetNumber btps_lastoff;	/* last item offset loaded */
	uint32		btps_level;		/* tree level (0 = leaf) */
	Size		btps_full;		/* "full" if less than this much free space */
	struct BTPageState *btps_next;		/* link to parent level, if any */
} BTPageState;

/*
 * Overall status record for index writing phase.
 */
typedef struct BTWriteState
{
	Relation	heap;
	Relation	index;
	bool		btws_use_wal;	/* dump pages to WAL? */
	BlockNumber btws_pages_alloced;		/* # pages allocated */
	BlockNumber btws_pages_written;		/* # pages written out */
	Page		btws_zeropage;	/* workspace for filling zeroes */
} BTWriteState;


static double _bt_heapscan(Relation heap, Relation index,
		 BTBuildState *buildstate, IndexInfo *indexInfo);
static void _bt_spooldestroy(BTSpool *btspool);
static void _bt_spool(BTSpool *btspool, ItemPointer self,
		 Datum *values, bool *isnull);
static void _bt_leafbuild(BTBuildState buildstate);
static void _bt_build_callback(Relation index, HeapTuple htup, Datum *values,
		 bool *isnull, bool tupleIsAlive, void *state);
static Page _bt_blnewpage(uint32 level);
static BTPageState *_bt_pagestate(BTWriteState *wstate, uint32 level);
static void _bt_slideleft(Page page);
static void _bt_sortaddtup(Page page, Size itemsize,
			   IndexTuple itup, OffsetNumber itup_off);
static void _bt_buildadd(BTWriteState *wstate, BTPageState *state,
			 IndexTuple itup);
static void _bt_uppershutdown(BTWriteState *wstate, BTPageState *state);
static void _bt_load(BTWriteState *wstate,
		 BTSpool *btspool, BTSpool *btspool2);
static BTLeader *_bt_begin_parallel(BTSpool *btspool, bool isconcurrent,
		int request, bool leaderasworker);
static void _bt_end_parallel(BTLeader *btleader);
static void _bt_leader_sort_as_worker(BTBuildState *buildstate);
static double _bt_leader_wait_for_workers(BTBuildState *buildstate);
static void _bt_worker_main(dsm_segment *seg, shm_toc *toc);
static Size _bt_parallel_shared_estimate(Snapshot snapshot);
static void _bt_parallel_scan_and_sort(BTSpool *btspool, BTSpool *btspool2,
		  BTShared *btshared, Sharedsort *sharedsort,
		  Sharedsort *sharedsort2, int sort_mem);


/*
 * Main interface routine
 */
IndexBuildResult *
_bt_dobuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult   *result;
	BTBuildState		buildstate;
	double				reltuples;

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
		ResetUsage();
#endif   /* BTREE_BUILD_STATS */

	buildstate.isunique = indexInfo->ii_Unique;
	buildstate.havedead = false;
	buildstate.heap = heap;
	buildstate.spool = NULL;
	buildstate.spool2 = NULL;
	buildstate.indtuples = 0;
	buildstate.btleader = NULL;

	/*
	 * We expect to be called exactly once for any index relation. If that's
	 * not the case, big trouble's what we have.
	 */
	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	reltuples = _bt_heapscan(heap, index, &buildstate, indexInfo);

	/*
	 * Finish the build by (1) completing the sort of the spool file, (2)
	 * inserting the sorted tuples into btree pages and (3) building the
	 * upper levels.
	 */
	_bt_leafbuild(buildstate);
	_bt_spooldestroy(buildstate.spool);
	if (buildstate.spool2)
		_bt_spooldestroy(buildstate.spool2);

	/* Shut down leader (and its workers) if required */
	if (buildstate.btleader)
		_bt_end_parallel(buildstate.btleader);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		/*
		 * For serial builds, the entire build is summarized.  Otherwise, just
		 * the leader processing portion of the build is summarized.
		 */
		if (!buildstate.btleader)
			ShowUsage("BTREE BUILD STATS");
		else
			ShowUsage("BTREE LEADER PROCESSING STATS");

		ResetUsage();
	}
#endif   /* BTREE_BUILD_STATS */

	return result;
}


/*
 * Internal routines.
 */


/*
 * Create and initialize one or two spool structures, and save them in
 * caller's buildstate argument (a second spool is required for unique
 * index builds).  Scan the heap, possibly in parallel, filling spools
 * with IndexTuples.
 *
 * Returns the total number of heap tuples scanned.
 */
static double
_bt_heapscan(Relation heap, Relation index, BTBuildState *buildstate,
			 IndexInfo *indexInfo)
{
	BTSpool			   *btspool = (BTSpool *) palloc0(sizeof(BTSpool));
	SortCoordinate		coordinate = NULL;
	double				reltuples = 0;
	bool				force_single_worker = false;

	/*
	 * We size the sort area as maintenance_work_mem rather than work_mem to
	 * speed index creation.  This should be OK since a single backend can't
	 * run multiple index creations in parallel (see also: notes on
	 * parallelism and maintenance_work_mem below).  Note that creation of a
	 * unique index actually requires two BTSpool objects.
	 */
	btspool->heap = heap;
	btspool->index = index;
	btspool->isunique = indexInfo->ii_Unique;

	/*
	 * Determine optimal target number of parallel worker processes for build.
	 *
	 * Builds on catalog relations are unsupported.  This is because of
	 * problems with relcache entries being independently held by worker
	 * processes.  For example, reindex_relation() will call
	 * RelationSetIndexList() to forcibly disable the use of indexes on the
	 * table being reindexed (when REINDEX_REL_SUPPRESS_INDEX_USE is specified
	 * by caller).  ReindexIsProcessingIndex() will therefore fail to work in
	 * parallel workers, since certain state isn't propagated to parallel
	 * workers.  (This is okay for non-catalog builds that must REINDEX
	 * because worker processes perform only a tightly controlled set of
	 * tasks.)
	 *
	 * Perhaps the relevant state could be propagated here instead, avoiding
	 * this restriction, but it seems very brittle to duplicate considerations
	 * made within index.c here.
	 */
	if (!IsCatalogRelation(heap))
	{
		Oid			table = RelationGetRelid(btspool->heap);
		Oid			index = RelationGetRelid(btspool->index);
		int			request;

		/*
		 * Sometimes, a parallel sort with one worker process, and no
		 * leader-as-worker scan and sort is indicated by the planner.  This
		 * testing option ensures that every process involved behaves
		 * deterministically, and that the single worker process is reliably
		 * subject to any relevant limitation of the parallel infrastructure.
		 */
		request = plan_create_index_workers(table, index);
		if (request == -1)
		{
			force_single_worker = true;
			request = 1;
		}

		/*
		 * Attempt to launch parallel workers according to our target number,
		 * and available worker processes.
		 *
		 * Note that the planner will indicate that 0 workers will be
		 * requested in the event of detecting that parallelism is unsafe.
		 * This must be heeded (it isn't just a suggestion).  Note, however,
		 * that the parallel safety of sort comparators is not a concern, per
		 * tuplesort.h contract.
		 */
		if (request > 0)
			buildstate->btleader = _bt_begin_parallel(btspool,
													  indexInfo->ii_Concurrent,
													  request,
													  !force_single_worker);
	}

	/*
	 * If no workers could actually be launched (btleader is NULL), perform
	 * serial index build
	 */
	if (buildstate->btleader)
	{
		/*
		 * 1 or more parallel workers launched by now.  Initialize
		 * coordination state.
		 */
		coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
		coordinate->isWorker = false;
		coordinate->launched = buildstate->btleader->nworkertuplesorts;
		coordinate->sharedsort = buildstate->btleader->sharedsort;
	}

	/*
	 * Begin tuplesort for this process (there may be worker process
	 * tuplesorts already underway).
	 *
	 * In cases where parallelism is involved, the leader always gets a
	 * standard maintenance_work_mem budget here, just like a serial sort.
	 * Parallel worker states receive only a fraction of maintenance_work_mem,
	 * though.  With only limited exception, there is no overlap in memory
	 * allocations among the leader process and worker processes (we rely on
	 * tuplesort.c not allocating the vast majority of memory needed by leader
	 * until output is first consumed, and we rely on most memory being
	 * released by tuplesort.c when workers are put into a quiescent state).
	 *
	 * The overall effect is that maintenance_work_mem always represents an
	 * absolute high watermark on the amount of memory used by a CREATE INDEX
	 * operation, regardless of the use of parallelism or any other factor.
	 */
	btspool->sortstate = tuplesort_begin_index_btree(heap, index,
													 buildstate->isunique,
													 maintenance_work_mem,
													 coordinate, false);

	/* Save as primary spool (any worker processes have their own) */
	buildstate->spool = btspool;

	/*
	 * If building a unique index, put dead tuples in a second spool to keep
	 * them out of the uniqueness check.
	 */
	if (indexInfo->ii_Unique)
	{
		BTSpool		   *btspool2 = (BTSpool *) palloc0(sizeof(BTSpool));
		SortCoordinate	coordinate2 = NULL;

		/* Initialize secondary spool */
		btspool2->heap = heap;
		btspool2->index = index;
		btspool2->isunique = false;

		if (buildstate->btleader)
		{
			/*
			 * Set up non-private state that is passed to
			 * tuplesort_begin_index_btree() about the basic high level
			 * coordination of a parallel sort.
			 */
			coordinate2 = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
			coordinate2->isWorker = false;
			coordinate2->launched = buildstate->btleader->nworkertuplesorts;
			coordinate2->sharedsort = buildstate->btleader->sharedsort2;
		}

		/*
		 * We expect that the second one (for dead tuples) won't get very
		 * full, so we give it only work_mem
		 */
		btspool2->sortstate = tuplesort_begin_index_btree(heap, index,
														  false, work_mem,
														  coordinate2, false);
		/* Save as secondary spool (any worker processes have their own) */
		buildstate->spool2 = btspool2;
	}

	if (!buildstate->btleader)
	{
		/* Spool is to be sorted serially.  Do heap scan. */
		reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
									   _bt_build_callback, (void *) buildstate,
									   NULL);
		/* okay, all heap tuples are indexed */
		if (buildstate->spool2 && !buildstate->havedead)
		{
			/* spool2 turns out to be unnecessary */
			_bt_spooldestroy(buildstate->spool2);
			buildstate->spool2 = NULL;
		}
	}
	else
	{
		/*
		 * Parallel scans are underway by now.
		 *
		 * Leader typically participates as a worker, too.
		 */
		if (!force_single_worker)
			_bt_leader_sort_as_worker(buildstate);

		/*
		 * Wait on worker processes to finish.  They should finish at
		 * approximately the same time as the leader-as-worker operation, and
		 * so the wait is typically only for an instant.
		 */
		reltuples = _bt_leader_wait_for_workers(buildstate);
	}

	return reltuples;
}

/*
 * clean up a spool structure and its substructures.
 */
static void
_bt_spooldestroy(BTSpool *btspool)
{
	tuplesort_end(btspool->sortstate);
	pfree(btspool);
}

/*
 * spool an index entry into the sort file.
 */
static void
_bt_spool(BTSpool *btspool, ItemPointer self, Datum *values, bool *isnull)
{
	tuplesort_putindextuplevalues(btspool->sortstate, btspool->index,
								  self, values, isnull);
}

/*
 * given state with a spool loaded by successive calls to _bt_spool, create
 * an entire btree.
 */
static void
_bt_leafbuild(BTBuildState buildstate)
{
	BTSpool		   *btspool = buildstate.spool;
	BTSpool		   *btspool2 = buildstate.spool2;
	BTWriteState	wstate;

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats && !buildstate.btleader)
	{
		ShowUsage("BTREE BUILD (Spool) STATISTICS");
		ResetUsage();
	}
#endif   /* BTREE_BUILD_STATS */

	tuplesort_performsort(btspool->sortstate);
	if (btspool2)
		tuplesort_performsort(btspool2->sortstate);

	wstate.heap = btspool->heap;
	wstate.index = btspool->index;

	/*
	 * We need to log index creation in WAL iff WAL archiving/streaming is
	 * enabled UNLESS the index isn't WAL-logged anyway.
	 */
	wstate.btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate.index);

	/* reserve the metapage */
	wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
	wstate.btws_pages_written = 0;
	wstate.btws_zeropage = NULL;	/* until needed */

	_bt_load(&wstate, btspool, btspool2);
}

/*
 * Per-tuple callback from IndexBuildHeapScan
 */
static void
_bt_build_callback(Relation index,
				   HeapTuple htup,
				   Datum *values,
				   bool *isnull,
				   bool tupleIsAlive,
				   void *state)
{
	BTBuildState *buildstate = (BTBuildState *) state;

	/*
	 * insert the index tuple into the appropriate spool file for subsequent
	 * processing
	 */
	if (tupleIsAlive || buildstate->spool2 == NULL)
		_bt_spool(buildstate->spool, &htup->t_self, values, isnull);
	else
	{
		/* dead tuples are put into spool2 */
		buildstate->havedead = true;
		_bt_spool(buildstate->spool2, &htup->t_self, values, isnull);
	}

	buildstate->indtuples += 1;
}

/*
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
static Page
_bt_blnewpage(uint32 level)
{
	Page		page;
	BTPageOpaque opaque;

	page = (Page) palloc(BLCKSZ);

	/* Zero the page and set up standard page header info */
	_bt_pageinit(page, BLCKSZ);

	/* Initialize BT opaque state */
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	opaque->btpo_prev = opaque->btpo_next = P_NONE;
	opaque->btpo.level = level;
	opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;
	opaque->btpo_cycleid = 0;

	/* Make the P_HIKEY line pointer appear allocated */
	((PageHeader) page)->pd_lower += sizeof(ItemIdData);

	return page;
}

/*
 * emit a completed btree page, and release the working storage.
 */
static void
_bt_blwritepage(BTWriteState *wstate, Page page, BlockNumber blkno)
{
	/* Ensure rd_smgr is open (could have been closed by relcache flush!) */
	RelationOpenSmgr(wstate->index);

	/* XLOG stuff */
	if (wstate->btws_use_wal)
	{
		/* We use the heap NEWPAGE record type for this */
		log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
	}

	/*
	 * If we have to write pages nonsequentially, fill in the space with
	 * zeroes until we come back and overwrite.  This is not logically
	 * necessary on standard Unix filesystems (unwritten space will read as
	 * zeroes anyway), but it should help to avoid fragmentation. The dummy
	 * pages aren't WAL-logged though.
	 */
	while (blkno > wstate->btws_pages_written)
	{
		if (!wstate->btws_zeropage)
			wstate->btws_zeropage = (Page) palloc0(BLCKSZ);
		/* don't set checksum for all-zero page */
		smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM,
				   wstate->btws_pages_written++,
				   (char *) wstate->btws_zeropage,
				   true);
	}

	PageSetChecksumInplace(page, blkno);

	/*
	 * Now write the page.  There's no need for smgr to schedule an fsync for
	 * this write; we'll do it ourselves before ending the build.
	 */
	if (blkno == wstate->btws_pages_written)
	{
		/* extending the file... */
		smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, blkno,
				   (char *) page, true);
		wstate->btws_pages_written++;
	}
	else
	{
		/* overwriting a block we zero-filled before */
		smgrwrite(wstate->index->rd_smgr, MAIN_FORKNUM, blkno,
				  (char *) page, true);
	}

	pfree(page);
}

/*
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
static BTPageState *
_bt_pagestate(BTWriteState *wstate, uint32 level)
{
	BTPageState *state = (BTPageState *) palloc0(sizeof(BTPageState));

	/* create initial page for level */
	state->btps_page = _bt_blnewpage(level);

	/* and assign it a page position */
	state->btps_blkno = wstate->btws_pages_alloced++;

	state->btps_minkey = NULL;
	/* initialize lastoff so first item goes into P_FIRSTKEY */
	state->btps_lastoff = P_HIKEY;
	state->btps_level = level;
	/* set "full" threshold based on level.  See notes at head of file. */
	if (level > 0)
		state->btps_full = (BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR) / 100);
	else
		state->btps_full = RelationGetTargetPageFreeSpace(wstate->index,
												   BTREE_DEFAULT_FILLFACTOR);
	/* no parent level, yet */
	state->btps_next = NULL;

	return state;
}

/*
 * slide an array of ItemIds back one slot (from P_FIRSTKEY to
 * P_HIKEY, overwriting P_HIKEY).  we need to do this when we discover
 * that we have built an ItemId array in what has turned out to be a
 * P_RIGHTMOST page.
 */
static void
_bt_slideleft(Page page)
{
	OffsetNumber off;
	OffsetNumber maxoff;
	ItemId		previi;
	ItemId		thisii;

	if (!PageIsEmpty(page))
	{
		maxoff = PageGetMaxOffsetNumber(page);
		previi = PageGetItemId(page, P_HIKEY);
		for (off = P_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off))
		{
			thisii = PageGetItemId(page, off);
			*previi = *thisii;
			previi = thisii;
		}
		((PageHeader) page)->pd_lower -= sizeof(ItemIdData);
	}
}

/*
 * Add an item to a page being built.
 *
 * The main difference between this routine and a bare PageAddItem call
 * is that this code knows that the leftmost data item on a non-leaf
 * btree page doesn't need to have a key.  Therefore, it strips such
 * items down to just the item header.
 *
 * This is almost like nbtinsert.c's _bt_pgaddtup(), but we can't use
 * that because it assumes that P_RIGHTMOST() will return the correct
 * answer for the page.  Here, we don't know yet if the page will be
 * rightmost.  Offset P_FIRSTKEY is always the first data key.
 */
static void
_bt_sortaddtup(Page page,
			   Size itemsize,
			   IndexTuple itup,
			   OffsetNumber itup_off)
{
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	IndexTupleData trunctuple;

	if (!P_ISLEAF(opaque) && itup_off == P_FIRSTKEY)
	{
		trunctuple = *itup;
		trunctuple.t_info = sizeof(IndexTupleData);
		itup = &trunctuple;
		itemsize = sizeof(IndexTupleData);
	}

	if (PageAddItem(page, (Item) itup, itemsize, itup_off,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to the index page");
}

/*----------
 * Add an item to a disk page from the sort output.
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *	 stored, we should store only the link.
 *
 * A leaf page being built looks like:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...           |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |	 ^ last										  |
 * |												  |
 * +-------------+------------------------------------+
 * |			 | itemN ...                          |
 * +-------------+------------------+-----------------+
 * |		  ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.
 *
 * 'last' pointer indicates the last offset added to the page.
 *----------
 */
static void
_bt_buildadd(BTWriteState *wstate, BTPageState *state, IndexTuple itup)
{
	Page		npage;
	BlockNumber nblkno;
	OffsetNumber last_off;
	Size		pgspc;
	Size		itupsz;

	/*
	 * This is a handy place to check for cancel interrupts during the btree
	 * load phase of index creation.
	 */
	CHECK_FOR_INTERRUPTS();

	npage = state->btps_page;
	nblkno = state->btps_blkno;
	last_off = state->btps_lastoff;

	pgspc = PageGetFreeSpace(npage);
	itupsz = IndexTupleDSize(*itup);
	itupsz = MAXALIGN(itupsz);

	/*
	 * Check whether the item can fit on a btree page at all. (Eventually, we
	 * ought to try to apply TOAST methods if not.) We actually need to be
	 * able to fit three items on every page, so restrict any one item to 1/3
	 * the per-page available space. Note that at this point, itupsz doesn't
	 * include the ItemId.
	 *
	 * NOTE: similar code appears in _bt_insertonpg() to defend against
	 * oversize items being inserted into an already-existing index. But
	 * during creation of an index, we don't go through there.
	 */
	if (itupsz > BTMaxItemSize(npage))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
				   itupsz, BTMaxItemSize(npage),
				   RelationGetRelationName(wstate->index)),
		errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
				"Consider a function index of an MD5 hash of the value, "
				"or use full text indexing."),
				 errtableconstraint(wstate->heap,
									RelationGetRelationName(wstate->index))));

	/*
	 * Check to see if page is "full".  It's definitely full if the item won't
	 * fit.  Otherwise, compare to the target freespace derived from the
	 * fillfactor.  However, we must put at least two items on each page, so
	 * disregard fillfactor if we don't have that many.
	 */
	if (pgspc < itupsz || (pgspc < state->btps_full && last_off > P_FIRSTKEY))
	{
		/*
		 * Finish off the page and write it out.
		 */
		Page		opage = npage;
		BlockNumber oblkno = nblkno;
		ItemId		ii;
		ItemId		hii;
		IndexTuple	oitup;

		/* Create new page of same level */
		npage = _bt_blnewpage(state->btps_level);

		/* and assign it a page position */
		nblkno = wstate->btws_pages_alloced++;

		/*
		 * We copy the last item on the page into the new page, and then
		 * rearrange the old page so that the 'last item' becomes its high key
		 * rather than a true data item.  There had better be at least two
		 * items on the page already, else the page would be empty of useful
		 * data.
		 */
		Assert(last_off > P_FIRSTKEY);
		ii = PageGetItemId(opage, last_off);
		oitup = (IndexTuple) PageGetItem(opage, ii);
		_bt_sortaddtup(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY);

		/*
		 * Move 'last' into the high key position on opage
		 */
		hii = PageGetItemId(opage, P_HIKEY);
		*hii = *ii;
		ItemIdSetUnused(ii);	/* redundant */
		((PageHeader) opage)->pd_lower -= sizeof(ItemIdData);

		/*
		 * Link the old page into its parent, using its minimum key. If we
		 * don't have a parent, we have to create one; this adds a new btree
		 * level.
		 */
		if (state->btps_next == NULL)
			state->btps_next = _bt_pagestate(wstate, state->btps_level + 1);

		Assert(state->btps_minkey != NULL);
		ItemPointerSet(&(state->btps_minkey->t_tid), oblkno, P_HIKEY);
		_bt_buildadd(wstate, state->btps_next, state->btps_minkey);
		pfree(state->btps_minkey);

		/*
		 * Save a copy of the minimum key for the new page.  We have to copy
		 * it off the old page, not the new one, in case we are not at leaf
		 * level.
		 */
		state->btps_minkey = CopyIndexTuple(oitup);

		/*
		 * Set the sibling links for both pages.
		 */
		{
			BTPageOpaque oopaque = (BTPageOpaque) PageGetSpecialPointer(opage);
			BTPageOpaque nopaque = (BTPageOpaque) PageGetSpecialPointer(npage);

			oopaque->btpo_next = nblkno;
			nopaque->btpo_prev = oblkno;
			nopaque->btpo_next = P_NONE;		/* redundant */
		}

		/*
		 * Write out the old page.  We never need to touch it again, so we can
		 * free the opage workspace too.
		 */
		_bt_blwritepage(wstate, opage, oblkno);

		/*
		 * Reset last_off to point to new page
		 */
		last_off = P_FIRSTKEY;
	}

	/*
	 * If the new item is the first for its page, stash a copy for later. Note
	 * this will only happen for the first item on a level; on later pages,
	 * the first item for a page is copied from the prior page in the code
	 * above.
	 */
	if (last_off == P_HIKEY)
	{
		Assert(state->btps_minkey == NULL);
		state->btps_minkey = CopyIndexTuple(itup);
	}

	/*
	 * Add the new item into the current page.
	 */
	last_off = OffsetNumberNext(last_off);
	_bt_sortaddtup(npage, itupsz, itup, last_off);

	state->btps_page = npage;
	state->btps_blkno = nblkno;
	state->btps_lastoff = last_off;
}

/*
 * Finish writing out the completed btree.
 */
static void
_bt_uppershutdown(BTWriteState *wstate, BTPageState *state)
{
	BTPageState *s;
	BlockNumber rootblkno = P_NONE;
	uint32		rootlevel = 0;
	Page		metapage;

	/*
	 * Each iteration of this loop completes one more level of the tree.
	 */
	for (s = state; s != NULL; s = s->btps_next)
	{
		BlockNumber blkno;
		BTPageOpaque opaque;

		blkno = s->btps_blkno;
		opaque = (BTPageOpaque) PageGetSpecialPointer(s->btps_page);

		/*
		 * We have to link the last page on this level to somewhere.
		 *
		 * If we're at the top, it's the root, so attach it to the metapage.
		 * Otherwise, add an entry for it to its parent using its minimum key.
		 * This may cause the last page of the parent level to split, but
		 * that's not a problem -- we haven't gotten to it yet.
		 */
		if (s->btps_next == NULL)
		{
			opaque->btpo_flags |= BTP_ROOT;
			rootblkno = blkno;
			rootlevel = s->btps_level;
		}
		else
		{
			Assert(s->btps_minkey != NULL);
			ItemPointerSet(&(s->btps_minkey->t_tid), blkno, P_HIKEY);
			_bt_buildadd(wstate, s->btps_next, s->btps_minkey);
			pfree(s->btps_minkey);
			s->btps_minkey = NULL;
		}

		/*
		 * This is the rightmost page, so the ItemId array needs to be slid
		 * back one slot.  Then we can dump out the page.
		 */
		_bt_slideleft(s->btps_page);
		_bt_blwritepage(wstate, s->btps_page, s->btps_blkno);
		s->btps_page = NULL;	/* writepage freed the workspace */
	}

	/*
	 * As the last step in the process, construct the metapage and make it
	 * point to the new root (unless we had no data at all, in which case it's
	 * set to point to "P_NONE").  This changes the index to the "valid" state
	 * by filling in a valid magic number in the metapage.
	 */
	metapage = (Page) palloc(BLCKSZ);
	_bt_initmetapage(metapage, rootblkno, rootlevel);
	_bt_blwritepage(wstate, metapage, BTREE_METAPAGE);
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void
_bt_load(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2)
{
	BTPageState *state = NULL;
	bool		merge = (btspool2 != NULL);
	IndexTuple	itup,
				itup2 = NULL;
	bool		should_free,
				should_free2,
				load1;
	TupleDesc	tupdes = RelationGetDescr(wstate->index);
	int			i,
				keysz = RelationGetNumberOfAttributes(wstate->index);
	ScanKey		indexScanKey = NULL;
	SortSupport sortKeys;

	if (merge)
	{
		/*
		 * Another BTSpool for dead tuples exists. Now we have to merge
		 * btspool and btspool2.
		 */

		/* the preparation of merge */
		itup = tuplesort_getindextuple(btspool->sortstate,
									   true, &should_free);
		itup2 = tuplesort_getindextuple(btspool2->sortstate,
										true, &should_free2);
		indexScanKey = _bt_mkscankey_nodata(wstate->index);

		/* Prepare SortSupport data for each column */
		sortKeys = (SortSupport) palloc0(keysz * sizeof(SortSupportData));

		for (i = 0; i < keysz; i++)
		{
			SortSupport sortKey = sortKeys + i;
			ScanKey		scanKey = indexScanKey + i;
			int16		strategy;

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = scanKey->sk_collation;
			sortKey->ssup_nulls_first =
				(scanKey->sk_flags & SK_BT_NULLS_FIRST) != 0;
			sortKey->ssup_attno = scanKey->sk_attno;
			/* Abbreviation is not supported here */
			sortKey->abbreviate = false;

			AssertState(sortKey->ssup_attno != 0);

			strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ?
				BTGreaterStrategyNumber : BTLessStrategyNumber;

			PrepareSortSupportFromIndexRel(wstate->index, strategy, sortKey);
		}

		_bt_freeskey(indexScanKey);

		for (;;)
		{
			load1 = true;		/* load BTSpool next ? */
			if (itup2 == NULL)
			{
				if (itup == NULL)
					break;
			}
			else if (itup != NULL)
			{
				for (i = 1; i <= keysz; i++)
				{
					SortSupport entry;
					Datum		attrDatum1,
								attrDatum2;
					bool		isNull1,
								isNull2;
					int32		compare;

					entry = sortKeys + i - 1;
					attrDatum1 = index_getattr(itup, i, tupdes, &isNull1);
					attrDatum2 = index_getattr(itup2, i, tupdes, &isNull2);

					compare = ApplySortComparator(attrDatum1, isNull1,
												  attrDatum2, isNull2,
												  entry);
					if (compare > 0)
					{
						load1 = false;
						break;
					}
					else if (compare < 0)
						break;
				}
			}
			else
				load1 = false;

			/* When we see first tuple, create first index page */
			if (state == NULL)
				state = _bt_pagestate(wstate, 0);

			if (load1)
			{
				_bt_buildadd(wstate, state, itup);
				if (should_free)
					pfree(itup);
				itup = tuplesort_getindextuple(btspool->sortstate,
											   true, &should_free);
			}
			else
			{
				_bt_buildadd(wstate, state, itup2);
				if (should_free2)
					pfree(itup2);
				itup2 = tuplesort_getindextuple(btspool2->sortstate,
												true, &should_free2);
			}
		}
		pfree(sortKeys);
	}
	else
	{
		/* merge is unnecessary */
		while ((itup = tuplesort_getindextuple(btspool->sortstate,
											   true, &should_free)) != NULL)
		{
			/* When we see first tuple, create first index page */
			if (state == NULL)
				state = _bt_pagestate(wstate, 0);

			_bt_buildadd(wstate, state, itup);
			if (should_free)
				pfree(itup);
		}
	}

	/* Close down final pages and write the metapage */
	_bt_uppershutdown(wstate, state);

	/*
	 * If the index is WAL-logged, we must fsync it down to disk before it's
	 * safe to commit the transaction.  (For a non-WAL-logged index we don't
	 * care since the index will be uninteresting after a crash anyway.)
	 *
	 * It's obvious that we must do this when not WAL-logging the build. It's
	 * less obvious that we have to do it even if we did WAL-log the index
	 * pages.  The reason is that since we're building outside shared buffers,
	 * a CHECKPOINT occurring during the build has no way to flush the
	 * previously written data to disk (indeed it won't know the index even
	 * exists).  A crash later on would replay WAL from the checkpoint,
	 * therefore it wouldn't replay our earlier WAL entries. If we do not
	 * fsync those pages here, they might still not be on disk when the crash
	 * occurs.
	 */
	if (RelationNeedsWAL(wstate->index))
	{
		RelationOpenSmgr(wstate->index);
		smgrimmedsync(wstate->index->rd_smgr, MAIN_FORKNUM);
	}
}

/*
 * _bt_begin_parallel - Creates parallel context, and launches workers for
 * leader.
 *
 * btspool argument should be initialized (with the exception of the tuplesort
 * state, which may be created using shared state initially created here).
 *
 * isconcurrent indicates if operation is CREATE INDEX CONCURRENTLY, which
 * uses an MVCC snapshot.
 *
 * request is the target number of parallel workers to launch.
 *
 * leaderasworker indicates if leader process participates in scanning the
 * target heap relation and sorting runs (whether or not it participates as a
 * worker).
 *
 * Returns a BTLeader, which contains state sufficient to have leader process
 * coordinate entire parallel sort (or sorts, if a second spool is required).
 * If not even a single worker process can be launched, returns NULL,
 * indicating to caller that parallel sort should not be coordinated.
 */
static BTLeader *
_bt_begin_parallel(BTSpool *btspool, bool isconcurrent, int request,
				   bool leaderasworker)
{
	ParallelContext	   *pcxt;
	int					scantuplesortstates;
	Snapshot			snapshot;
	Size				estspool;
	Size				estsort;
	BTShared		   *btshared;
	Sharedsort		   *sharedsort;
	Sharedsort		   *sharedsort2;
	BTLeader		   *btleader = (BTLeader *) palloc0(sizeof(BTLeader));

	/*
	 * Enter parallel mode, and create context for parallel build of btree
	 * index
	 */
	EnterParallelMode();
	pcxt = CreateParallelContext(_bt_worker_main, request, true);

	/*
	 * Caller typically but not always wants leader process to participate in
	 * scan and sort step (i.e. to behave as a worker does initially).
	 */
	Assert(request > 0);
	scantuplesortstates = leaderasworker ? request + 1 : request;

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we
	 * use SnapshotAny because we must retrieve all tuples and do our own
	 * time qual checks (because we have to index RECENTLY_DEAD tuples). In
	 * a concurrent build, or during bootstrap, we take a regular MVCC
	 * snapshot and index whatever's live according to that.
	 */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * Estimate size for two keys -- nbtsort.c shared workspace, and
	 * tuplesort-private shared workspace
	 */
	estspool = _bt_parallel_shared_estimate(snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estspool);
	estsort = tuplesort_estimate_shared(scantuplesortstates);
	shm_toc_estimate_chunk(&pcxt->estimator, estsort);
	/* Unique case requires a second spool, and so a third shared workspace */
	if (!btspool->isunique)
	{
		shm_toc_estimate_keys(&pcxt->estimator, 2);
	}
	else
	{
		/* Account for PARALLEL_KEY_TUPLESORT_SPOOL2 */
		shm_toc_estimate_chunk(&pcxt->estimator, estsort);
		shm_toc_estimate_keys(&pcxt->estimator, 3);
	}

	/* Everyone's had a chance to ask for space, so now create the DSM */
	InitializeParallelDSM(pcxt);

	/*
	 * Store shared build state, for which we reserved space
	 */
	btshared = (BTShared *) shm_toc_allocate(pcxt->toc, estspool);
	/* Initialize immutable state */
	btshared->heaprelid = RelationGetRelid(btspool->heap);
	btshared->indexrelid = RelationGetRelid(btspool->index);
	btshared->isunique = btspool->isunique;
	btshared->isconcurrent = isconcurrent;
	btshared->scantuplesortstates = scantuplesortstates;
	/* Initialize mutable state */
	SpinLockInit(&btshared->mutex);
	btshared->reltuples = 0.0;
	btshared->havedead = false;
	btshared->indtuples = 0.0;
	heap_parallelscan_initialize(&btshared->heapdesc, btspool->heap, snapshot);

	/*
	 * Store shared tuplesort-private state, for which we reserved space.
	 * Then, initialize opaque state using tuplesort routine.
	 */
	sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
	tuplesort_initialize_shared(sharedsort, scantuplesortstates);

	/* Add TOC entries */
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BTREE_SHARED, btshared);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

	/* Unique case requires a second spool, and associated shared state */
	if (!btspool->isunique)
	{
		sharedsort2 = NULL;
	}
	else
	{
		/*
		 * Store additional shared tuplesort-private state, for which we
		 * reserved space.  Then, initialize opaque state using tuplesort
		 * routine.
		 */
		sharedsort2 = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
		tuplesort_initialize_shared(sharedsort2, scantuplesortstates);

		/* Add additional TOC entry */
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT_SPOOL2, sharedsort2);
	}

	/*
	 * Launch workers to sort runs.
	 *
	 * Save parallel context information within leader state.
	 */
	LaunchParallelWorkers(pcxt);
	btleader->pcxt = pcxt;
	btleader->nworkertuplesorts = pcxt->nworkers_launched;
	if (leaderasworker)
		btleader->nworkertuplesorts++;
	btleader->workersort = NULL;
	btleader->workersort2 = NULL;
	btleader->btshared = btshared;
	btleader->sharedsort = sharedsort;
	btleader->sharedsort2 = sharedsort2;

	/*
	 * Leader process does not directly use this snapshot reference, even
	 * during a leader-as-worker scan and sort operation
	 */
	if (IsMVCCSnapshot(snapshot))
		UnregisterSnapshot(snapshot);

	/* If no worker processes were launched, index build should be serial */
	if (pcxt->nworkers_launched == 0)
	{
		_bt_end_parallel(btleader);
		pfree(btleader);
		return NULL;
	}

	return btleader;
}

/*
 * _bt_end_parallel - end parallel mode
 *
 * Shuts down workers, and destroys parallel context.
 *
 * This has to be called after _bt_load() has consumed any tuples that it
 * requires, because quiesced worker processes need to remain around until
 * leader processing completes, per tuplesort contract.  Though passive, they
 * still manage the lifetime of state that is shared with the leader.
 */
static void
_bt_end_parallel(BTLeader *btleader)
{
	/* Shutdown leader-as-worker tuplesort */
	if (btleader->workersort)
		tuplesort_end(btleader->workersort);
	/* Shutdown leader-as-worker second spool tuplesort */
	if (btleader->workersort2)
		tuplesort_end(btleader->workersort2);
	/* Shutdown worker processes */
	WaitForParallelWorkersToFinish(btleader->pcxt);
	DestroyParallelContext(btleader->pcxt);
	ExitParallelMode();
}

/*
 * _bt_leader_sort_as_worker - within leader, participate as a parallel worker
 *
 * The leader's Tuplesortstate for sorting runs is distinct from the
 * Tuplesortstate it uses for consuming final output.  The leader-as-worker
 * case is minimally divergent from the regular worker case.
 *
 * Consuming even just the first index tuple output by the leader
 * Tuplesortstate is dependent on workers having completely finished already,
 * so there is nothing for the leader process to do when control reaches here.
 * The leader process might as well participate as a worker (actually, it's
 * probably also advisable to have the leader participate as a worker to be
 * consistent with other parallel operations that do the same).
 */
static void
_bt_leader_sort_as_worker(BTBuildState *buildstate)
{
	BTLeader  *btleader = buildstate->btleader;
	BTSpool   *leaderworker;
	BTSpool   *leaderworker2;

	/* Allocate memory for leader-as-worker's own private spool */
	leaderworker = (BTSpool *) palloc0(sizeof(BTSpool));

	/* Initialize leader-as-worker's own spool */
	leaderworker->heap = buildstate->spool->heap;
	leaderworker->index = buildstate->spool->index;
	leaderworker->isunique = buildstate->spool->isunique;

	/* Initialize leader-as-worker's own second spool, if required */
	if (!btleader->btshared->isunique)
	{
		leaderworker2 = NULL;
	}
	else
	{
		/* Allocate memory for worker's own private secondary spool */
		leaderworker2 = (BTSpool *) palloc0(sizeof(BTSpool));

		/* Initialize worker's own secondary spool */
		leaderworker2->heap = leaderworker->heap;
		leaderworker2->index = leaderworker->index;
		leaderworker2->isunique = false;
	}

	/*
	 * Call entry point also called by worker processes, to initialize
	 * sortstates, scan, and perform sort step.
	 *
	 * This will cause parent BTBuildState to store leader-as-worker pg_class
	 * statistics (e.g., BTBuildState.indtuples), since BTBuildState for entire
	 * sort is reused.  This does not matter, because
	 * _bt_parallel_scan_and_sort() also aggregates this in shared memory,
	 * which is what we'll go on to use later.
	 *
	 * Might as well use reliable figure when doling out leader-as-worker
	 * maintenance_work_mem (when requested number of workers were not
	 * launched, this will be somewhat higher than it is for other workers).
	 */
	_bt_parallel_scan_and_sort(leaderworker, leaderworker2, btleader->btshared,
							   btleader->sharedsort, btleader->sharedsort2,
							   maintenance_work_mem /
							   btleader->nworkertuplesorts);
#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Leader with Worker Spool) STATISTICS");
		ResetUsage();
	}
#endif   /* BTREE_BUILD_STATS */

	/* Store tuplesort state for shut down later */
	btleader->workersort = leaderworker->sortstate;
	if (leaderworker2)
		btleader->workersort2 = leaderworker2->sortstate;
}

/*
 * _bt_leader_wait_for_workers - within leader, wait for worker processes to
 * finish
 */
static double
_bt_leader_wait_for_workers(BTBuildState *buildstate)
{
	BTLeader	   *btleader = buildstate->btleader;
	double			reltuples = 0;

	/* Have tuplesort actually wait for leader to finish */
	tuplesort_leader_wait(buildstate->spool->sortstate);
	/* Be tidy; wait on second spool's tuplesort */
	if (buildstate->spool2)
		tuplesort_leader_wait(buildstate->spool2->sortstate);

	/*
	 * Pass caller the sum of each worker's heap and index reltuples, which
	 * are accumulated for all workers.  Caller needs this for ambuild
	 * statistics.  This may overwrite leader-as-worker values already set.
	 */
	SpinLockAcquire(&btleader->btshared->mutex);
	buildstate->havedead = btleader->btshared->havedead;
	reltuples = btleader->btshared->reltuples;
	buildstate->indtuples = btleader->btshared->indtuples;
	SpinLockRelease(&btleader->btshared->mutex);

	return reltuples;
}

/*
 * _bt_worker_main - Perform work within a launched parallel process.
 *
 * Parallel workers sort one or more runs only.  Merging of these runs occurs
 * serially, within the leader process (which also processes some input as a
 * worker).
 */
static void
_bt_worker_main(dsm_segment *seg, shm_toc *toc)
{
	BTSpool		   *btspool;
	BTSpool		   *btspool2;
	BTShared	   *btshared;
	Sharedsort	   *sharedsort;
	Sharedsort	   *sharedsort2;
	Relation		heapRel;
	Relation		indexRel;
	LOCKMODE		heapLockmode;
	LOCKMODE		indexLockmode;

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
		ResetUsage();
#endif   /* BTREE_BUILD_STATS */

	/* Allocate memory for worker's own private spool */
	btspool = (BTSpool *) palloc0(sizeof(BTSpool));

	/* Look up shared state */
	btshared = shm_toc_lookup(toc, PARALLEL_KEY_BTREE_SHARED);

	/*
	 * Open relations using lockmodes known to be obtained by leader's caller
	 * within index.c
	 */
	if (!btshared->isconcurrent)
		heapLockmode = ShareLock;
	else
		heapLockmode = ShareUpdateExclusiveLock;

	/* REINDEX and CREATE INDEX [CONCURRENTLY] use an AccessExclusiveLock */
	indexLockmode = AccessExclusiveLock;

	/* Open relations here, for worker */
	heapRel = heap_open(btshared->heaprelid, heapLockmode);
	indexRel = index_open(btshared->indexrelid, indexLockmode);

	/* Initialize worker's own spool */
	btspool->heap = heapRel;
	btspool->index = indexRel;
	btspool->isunique = btshared->isunique;

	/* Look up shared state private to tuplesort.c */
	sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT);
	if (!btshared->isunique)
	{
		btspool2 = NULL;
		sharedsort2 = NULL;
	}
	else
	{
		/* Allocate memory for worker's own private secondary spool */
		btspool2 = (BTSpool *) palloc0(sizeof(BTSpool));

		/* Initialize worker's own secondary spool */
		btspool2->heap = btspool->heap;
		btspool2->index = btspool->index;
		btspool2->isunique = false;
		/* Look up shared state private to tuplesort.c */
		sharedsort2 = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT_SPOOL2);
	}

	/* Perform sorting of spool, and possibly a spool2 */
	_bt_parallel_scan_and_sort(btspool, btspool2, btshared, sharedsort,
							   sharedsort2,
							   maintenance_work_mem /
							   btshared->scantuplesortstates);

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Worker Spool) STATISTICS");
		ResetUsage();
	}
#endif   /* BTREE_BUILD_STATS */

	/*
	 * End sort operation.  This worker no longer required when released from
	 * the wait that is about to happen here.
	 */
	tuplesort_end(btspool->sortstate);
	/* Do the same for second worker spool, if any */
	if (btspool2)
		tuplesort_end(btspool2->sortstate);

	index_close(indexRel, indexLockmode);
	heap_close(heapRel, heapLockmode);
}

/*
 * _bt_parallel_shared_estimate - returns the size need to store the given
 * shared state for a parallel btree index build. (This does not include the
 * shared tuplesort state or states.)
 *
 * This includes room for the ParallelHeapScanDesc's serialized snapshot, if
 * that's required.
 */
static Size
_bt_parallel_shared_estimate(Snapshot snapshot)
{
	/* Frequently, only a special snapshot (SnapshotAny) is used */
	if (!IsMVCCSnapshot(snapshot))
	{
		Assert(snapshot == SnapshotAny);
		return sizeof(BTShared);
	}
	else
	{
		return add_size(offsetof(BTShared, heapdesc) +
						offsetof(ParallelHeapScanDescData, phs_snapshot_data),
						EstimateSnapshotSpace(snapshot));
	}
}

/*
 * _bt_parallel_scan_and_sort - perform a worker's portion of a parallel sort.
 *
 * This generates a tuplesort for passed btspool, and a second tuplesort state
 * if a second btspool is based (for unique index builds).
 *
 * The second spool sort is not a good target for parallel sort, since
 * typically there is very little to sort, but it's convenient to be
 * consistent.
 */
static void
_bt_parallel_scan_and_sort(BTSpool *btspool, BTSpool *btspool2,
						   BTShared *btshared, Sharedsort *sharedsort,
						   Sharedsort *sharedsort2, int sort_mem)
{
	SortCoordinate	coordinate;
	BTBuildState   *heapcallbackstate;
	HeapScanDesc	scan;
	double			reltuples;
	bool			havedead = false;
	IndexInfo	   *indexInfo;

	/*
	 * Initialize local tuplesort coordination state.
	 */
	coordinate = palloc0(sizeof(SortCoordinateData));
	coordinate->isWorker = true;
	coordinate->launched = -1;
	coordinate->sharedsort = sharedsort;

	/* Begin sort of run(s) */
	btspool->sortstate = tuplesort_begin_index_btree(btspool->heap,
													 btspool->index,
													 btspool->isunique,
													 sort_mem, coordinate,
													 false);
	/*
	 * Initialize heap scan state, in preparation for joining scan.
	 */
	heapcallbackstate = (BTBuildState *) palloc0(sizeof(BTBuildState));
	heapcallbackstate->isunique = btshared->isunique;
	heapcallbackstate->havedead = false;
	heapcallbackstate->heap = btspool->heap;
	heapcallbackstate->spool = btspool;
	heapcallbackstate->spool2 = btspool2;
	heapcallbackstate->indtuples = 0;
	heapcallbackstate->btleader = NULL;

	/*
	 * Just as with serial case, may require a second spool (this one is
	 * worker-private instance, though; there will also be a leader-only
	 * second spool for merging worker's second spools)
	 */
	if (heapcallbackstate->spool2)
	{
		SortCoordinate coordinate2;

		/*
		 * We expect that the second one (for dead tuples) won't get very
		 * full, so we give it only work_mem (unless sort_mem is less for
		 * worker).  Worker processes are generally permitted to allocate
		 * work_mem independently.
		 */
		coordinate2 = palloc0(sizeof(SortCoordinateData));
		coordinate2->isWorker = true;
		coordinate2->launched = -1;
		coordinate2->sharedsort = sharedsort2;
		heapcallbackstate->spool2->sortstate =
			tuplesort_begin_index_btree(btspool->heap, btspool->index, false,
										Min(sort_mem, work_mem), coordinate2,
										false);
	}

	/* Join parallel scan */
	indexInfo = BuildIndexInfo(btspool->index);
	scan = heap_beginscan_parallel(btspool->heap, &btshared->heapdesc);
	reltuples = IndexBuildHeapScan(btspool->heap, btspool->index, indexInfo,
								   true, _bt_build_callback,
								   (void *) heapcallbackstate, scan);

	/* Execute this workers part of the sort */
	tuplesort_performsort(btspool->sortstate);

	if (btspool2)
	{
		tuplesort_performsort(btspool2->sortstate);
		havedead = heapcallbackstate->havedead;
	}

	/*
	 * Done.  Leave a way for leader to determine we're finished.  Record how
	 * many tuples were in this worker's share of the relation.
	 */
	SpinLockAcquire(&btshared->mutex);
	btshared->havedead = btshared->havedead || havedead;
	btshared->reltuples += reltuples;
	btshared->indtuples += heapcallbackstate->indtuples;
	SpinLockRelease(&btshared->mutex);
}
