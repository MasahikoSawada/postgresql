/*-------------------------------------------------------------------------
 *
 * vacuumlazy.c
 *	  Concurrent ("lazy") vacuuming.
 *
 *
 * The major space usage for LAZY VACUUM is storage for the array of dead tuple
 * TIDs.  We want to ensure we can vacuum even the very largest relations with
 * finite memory space usage.  To do that, we set upper bounds on the number of
 * tuples we will keep track of at once.
 *
 * We are willing to use at most maintenance_work_mem (or perhaps
 * autovacuum_work_mem) memory space to keep track of dead tuples.  We
 * initially allocate an array of TIDs of that size, with an upper limit that
 * depends on table size (this limit ensures we don't allocate a huge area
 * uselessly for vacuuming small tables).  If the array threatens to overflow,
 * we suspend the heap scan phase and perform a pass of index cleanup and page
 * compaction, then resume the heap scan with an empty TID array.
 *
 * If we're processing a table with no indexes, we can just vacuum each page
 * as we go; there's no need to save up multiple tuples to minimize the number
 * of index scans performed.  So we don't use maintenance_work_mem memory for
 * the TID array, just enough to hold as many heap tuples as fit on one page.
 *
 * In PostgreSQL 12, we support a parallel option for lazy vacuum. In parallel
 * lazy vacuum, multiple vacuum worker processes get blocks in parallel using
 * parallel heap scan and process them. If a table with indexes the parallel
 * vacuum workers vacuum the heap and indexes in parallel.  Also, since dead
 * tuple TIDs is shared with all vacuum processes including the leader process
 * the parallel vacuum processes have to make two synchronization points in
 * lazy vacuum processing: when before starting vacuum and when before clearing
 * dead tuple TIDs. In these two points the leader treats dead tuple TIDs as
 * an arbiter. The information required by parallel lazy vacuum such as the
 * statistics of table, parallel heap scan description have to be shared with
 * all vacuum processes, and table statistics are funneled by the leader
 * process after finished. Note that dead tuple TIDs need to be shared only
 * when the table has indexes. For table with no indexes, each parallel worker
 * processes blocks and vacuum them independently.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuumlazy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/storage.h"
#include "commands/dbcommands.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"


/*
 * Space/time tradeoff parameters: do these need to be user-tunable?
 *
 * To consider truncating the relation, we want there to be at least
 * REL_TRUNCATE_MINIMUM or (relsize / REL_TRUNCATE_FRACTION) (whichever
 * is less) potentially-freeable pages.
 */
#define REL_TRUNCATE_MINIMUM	1000
#define REL_TRUNCATE_FRACTION	16

/*
 * Timing parameters for truncate locking heuristics.
 *
 * These were not exposed as user tunable GUC values because it didn't seem
 * that the potential for improvement was great enough to merit the cost of
 * supporting them.
 */
#define VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL		20	/* ms */
#define VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL		50	/* ms */
#define VACUUM_TRUNCATE_LOCK_TIMEOUT			5000	/* ms */

/*
 * When a table has no indexes, vacuum the FSM after every 8GB, approximately
 * (it won't be exact because we only vacuum FSM after processing a heap page
 * that has some removable tuples).  When there are indexes, this is ignored,
 * and we vacuum FSM after each index/heap cleaning pass.
 */
#define VACUUM_FSM_EVERY_PAGES \
	((BlockNumber) (((uint64) 8 * 1024 * 1024 * 1024) / BLCKSZ))

/*
 * Guesstimation of number of dead tuples per page.  This is used to
 * provide an upper limit to memory allocated when vacuuming small
 * tables.
 */
#define LAZY_ALLOC_TUPLES		MaxHeapTuplesPerPage

/*
 * Before we consider skipping a page that's marked as clean in
 * visibility map, we must've seen at least this many clean pages.
 */
#define SKIP_PAGES_THRESHOLD	((BlockNumber) 32)

/*
 * Size of the prefetch window for lazy vacuum backwards truncation scan.
 * Needs to be a power of 2.
 */
#define PREFETCH_SIZE			((BlockNumber) 32)

/* DSM key for parallel lazy vacuum */
#define VACUUM_KEY_SHARED			UINT64CONST(0xFFFFFFFFFFF00001)
#define VACUUM_KEY_VACUUM_STATS		UINT64CONST(0xFFFFFFFFFFF00002)
#define VACUUM_KEY_INDEX_STATS	    UINT64CONST(0xFFFFFFFFFFF00003)
#define VACUUM_KEY_DEAD_TUPLES		UINT64CONST(0xFFFFFFFFFFF00004)
#define VACUUM_KEY_WORKERS			UINT64CONST(0xFFFFFFFFFFF00005)
#define VACUUM_KEY_QUERY_TEXT		UINT64CONST(0xFFFFFFFFFFF00006)

/* Check if daed tuple is shared among workers */
#define IsDeadTupleShared(lvstate) \
	(((LVState *)(lvstate))->parallel_mode && \
	 ((LVState *)(lvstate))->vacrelstats->nindexes > 0)

/* see note in  lazy_scan_get_nextpage about forcing scanning of last page */
#define FORCE_CHECK_PAGE(nblocks, blkno, vacrelstats) \
	((blkno) == (nblocks) - 1 && should_attempt_truncation((vacrelstats)))

#define IsInParallelVacuum() (WorkerState != NULL)

#define IsVacuumLeader() !IsParallelWorker()
#define IsVacuumWorker() IsParallelWorker()

/* Vacuum worker state for parallel lazy vacuum */
typedef enum VacWorkerSate
{
	VACSTATE_INVALID = 0,
	VACSTATE_SCAN,
	VACSTATE_SCAN_FINISHED,
	VACSTATE_VACUUM_INDEX,
	VACSTATE_VACUUM_INDEX_FINISHED,
	VACSTATE_VACUUM_HEAP,
	VACSTATE_VACUUM_HEAP_FINISHED,
	VACSTATE_BEFORE_CLEANUP_INDEX,
	VACSTATE_CLEANUP_INDEX
} VacWorkerState;

char *tab[] = {
	"\"INVALID\"",
	"\"SCAN\"",
	"\"SCAN_FINISHED\"",
	"\"VACUUM_INDEX\"",
	"\"INDEX_FINISHED\"",
	"\"VACUUM_HEAP\"",
	"\"HEAP_FINISHED\"",
	"\"BEFORE_CLENAUP\"",
	"\"CLEANUP\""
};
#define TOSTR(state) (tab[(state)])


typedef struct IndexStats
{
	bool		need_update;
	BlockNumber	num_pages;
	BlockNumber	num_tuples;
	bool		done;
} IndexStats;

/* Struct for index statistics that are used for parallel lazy vacuum */
typedef struct LVSharedIndStats
{
	int		nindexes;
	int		nprocessed;

	slock_t		mutex;
	IndexStats stats[FLEXIBLE_ARRAY_MEMBER];
} LVIndStats;
#define SizeOfLVIndStats offsetof(LVIndStats, stats) + sizeof(IndexStats)

/*
 * Shared information among parallel workers.
 */
typedef struct LVShared
{
	Oid		relid;

	/* Options and thresholds used for lazy vacuum */
	VacuumOption		options;
	bool	aggressive;
	int		elevel;
	TransactionId	oldestXmin;
	TransactionId	freezeLimit;
	MultiXactId		multiXactCutoff;

	/* Maximum tuples each worker can have */
	int		max_dead_tuples_per_worker;

	/* Parallel heap scan description */
	ParallelHeapScanDescData	heapdesc;
} LVShared;

typedef struct VacuumWorker
{
	pid_t				pid;
	slock_t				mutex;
	VacWorkerState		state;
} VacuumWorker;

typedef struct LVWorkerState
{
	int		nparticipantvacuum;
	int		nparticipantvacuum_launched;

	ConditionVariable	cv;
	LWLock				vacuumlock;

	VacuumWorker workers[FLEXIBLE_ARRAY_MEMBER];
} LVWorkerState;
#define SizeOfLVWorkerState offsetof(LVWorkerState, workers) + sizeof(VacuumWorker)

/* Struct to control dead tuple TIDs array */
typedef struct LVTidMap
{
	int		max_items;	/* # slots allocated in itemptrs */
	int		num_items;	/* current # of entries */
	int		item_idx;

	bool	shared;
	slock_t	mutex;

	/* List of TIDs of tuples we intend to delete */
	/* NB: this list is ordered by TID address */
	ItemPointerData	itemptrs[FLEXIBLE_ARRAY_MEMBER];
} LVTidMap;
#define SizeOfLVTidMap offsetof(LVTidMap, itemptrs) + sizeof(ItemPointerData)

/*
 * Scan description data for lazy vacuum. In parallel lazy vacuum,
 * we use only heapscan.
 */
typedef struct LVScanDescData
{
	/* Common information for scanning heap */
	Relation	lv_rel;
	bool		disable_page_skipping;
	bool		aggressive;

	/* Use for single lazy vacuum, otherwise NULL */
	HeapScanDesc lv_heapscan;

	/* Use for parallel lazy vacuum othersize invalid values */
	BlockNumber	lv_cblock;
	BlockNumber	lv_next_unskippable_block;
	BlockNumber	lv_nblocks;
} LVScanDescData;
typedef struct LVScanDescData *LVScanDesc;

typedef struct LVRelStats
{
	/* hasindex = true means two-pass strategy; false means one-pass */
	bool		hasindex;
	/* Overall statistics about rel */
	BlockNumber old_rel_pages;	/* previous value of pg_class.relpages */
	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber scanned_pages;	/* number of pages we examined */
	BlockNumber pinskipped_pages;	/* # of pages we skipped due to a pin */
	BlockNumber frozenskipped_pages;	/* # of frozen pages we skipped */
	BlockNumber tupcount_pages; /* pages whose tuples we counted */
	BlockNumber empty_pages;
	double		old_live_tuples;	/* previous value of pg_class.reltuples */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_live_tuples;	/* new estimated total # of live tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	double		unused_tuples;
	double		num_tuples;
	BlockNumber pages_removed;
	double		tuples_deleted;
	BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
	int			num_index_scans;
	TransactionId latestRemovedXid;
	bool		lock_waiter_detected;
} LVRelStats;

/* State of lazy vacuum execution */
typedef struct LVState
{
	Oid			relid;
	Relation	relation;
	Relation	*indRels;
	int			nindexes;
	IndexBulkDeleteResult **indbulkstats;

	/* Lazy vacuum scan options */
	bool	aggressive;
	VacuumOption	options;

	/* Vacuum statistics for the target table */
	LVRelStats	*vacrelstats;

	/* Dead tuple information */
	LVTidMap	*dead_tuples;

	/* Used for parallel lazy vacuum */
	LVShared		*lvshared;
	LVIndStats		*indstats;
	ParallelContext	*pcxt;
} LVState;

/* A few variables that don't seem worth passing around as parameters */
static int	elevel = -1;

static TransactionId OldestXmin;
static TransactionId FreezeLimit;
static MultiXactId MultiXactCutoff;

static BufferAccessStrategy vac_strategy;
static LVWorkerState	*WorkerState = NULL;
static VacuumWorker		*MyVacuumWorker = NULL;

/* non-export function prototypes */
static void lazy_scan_heap(LVState *lvstate);
static void lazy_vacuum_heap(Relation onerel, LVState *lvstate);
static bool lazy_check_needs_freeze(Buffer buf, bool *hastup);
static void lazy_vacuum_all_indexes(LVState *lvstate);
static void lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  LVRelStats *vacrelstats,
				  LVTidMap *dead_tuples);
static void lazy_cleanup_all_indexes(LVState *lvstate);
static void lazy_cleanup_index(Relation indrel,
							   IndexBulkDeleteResult *stats,
							   LVRelStats *vacrelstats,
							   IndexStats *indstas);
static int lazy_vacuum_page(LVState *lvstate, Relation onerel, BlockNumber blkno, Buffer buffer,
				 int tupindex, Buffer *vmbuffer);
static bool should_attempt_truncation(LVRelStats *vacrelstats);
static void lazy_truncate_heap(Relation onerel, LVRelStats *vacrelstats);
static BlockNumber count_nondeletable_pages(Relation onerel,
						 LVRelStats *vacrelstats);
static void lazy_space_alloc(LVState *lvstate, BlockNumber relblocks);
static void lazy_record_dead_tuple(LVTidMap *dead_tuples,
					   ItemPointer itemptr);
static bool lazy_tid_reaped(ItemPointer itemptr, void *dt);
static int	vac_cmp_itemptr(const void *left, const void *right);
static bool heap_page_is_all_visible(Relation rel, Buffer buf,
						 TransactionId *visibility_cutoff_xid, bool *all_frozen);
static BlockNumber lazy_get_next_vacuum_page(LVState *lvstate);

/* function prototype for parallel vacuum */
static void lazy_vacuum_begin_parallel(LVState *lvstate, int request);
static void lazy_vacuum_end_parallel(LVState *lvstate, bool update_stats);
static long lazy_get_max_dead_tuples(LVRelStats *vacrelstats, BlockNumber relblocks);
static LVScanDesc lv_beginscan(Relation relation, LVShared *lvshared,
							   bool aggressive, bool disable_page_skipping);
static void lv_endscan(LVScanDesc lvscan);
static BlockNumber lazy_scan_get_nextpage(LVScanDesc lvscan, LVRelStats *vacrelstats,
										  bool *all_visible_according_to_vm_p,
										  Buffer *vmbuffer_p);
static bool lazy_dead_tuples_is_full(LVTidMap *tidmap);
static int lazy_get_dead_tuple_count(LVTidMap *dead_tuples);
static void lazy_prepare_to_next_state(LVState *lvstate, int next_state);
static void lazy_gather_worker_stats(LVState *lvstate, LVRelStats *vacrelstats);
static void vacuum_worker_onexit(int code, Datum arg);
static void vacuum_worker_detach(void);
static void vacuum_worker_attach(void);
static void lazy_set_my_worker_state(VacWorkerState new_state);
static VacWorkerState lazy_get_my_worker_state(void);
static bool lazy_check_worker_state(void);
static void lazy_set_workers_state(VacWorkerState new_state);
static void lazy_wait_for_vacuum_workers_attach(ParallelContext *pcxt);

/*
 *	lazy_vacuum_rel() -- perform LAZY VACUUM for one heap relation
 *
 *		This routine vacuums a single heap, cleans out its indexes, and
 *		updates its relpages and reltuples statistics.
 *
 *		At entry, we have already established a transaction and opened
 *		and locked the relation.
 */
void
lazy_vacuum_rel(Relation onerel, VacuumOption options, VacuumParams *params,
				BufferAccessStrategy bstrategy)
{
	LVState	   *lvstate;
	LVRelStats *vacrelstats;
	PGRUsage	ru0;
	TimestampTz starttime = 0;
	long		secs;
	int			usecs;
	double		read_rate,
				write_rate;
	bool		aggressive;		/* should we scan all unfrozen pages? */
	bool		scanned_all_unfrozen;	/* actually scanned all such pages? */
	TransactionId xidFullScanLimit;
	MultiXactId mxactFullScanLimit;
	BlockNumber new_rel_pages;
	BlockNumber new_rel_allvisible;
	double		new_live_tuples;
	TransactionId new_frozen_xid;
	MultiXactId new_min_multi;

	Assert(params != NULL);

	/* measure elapsed time iff autovacuum logging requires it */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		pg_rusage_init(&ru0);
		starttime = GetCurrentTimestamp();
	}

	if (options.flags & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  RelationGetRelid(onerel));

	vac_strategy = bstrategy;

	vacuum_set_xid_limits(onerel,
						  params->freeze_min_age,
						  params->freeze_table_age,
						  params->multixact_freeze_min_age,
						  params->multixact_freeze_table_age,
						  &OldestXmin, &FreezeLimit, &xidFullScanLimit,
						  &MultiXactCutoff, &mxactFullScanLimit);

	/*
	 * We request an aggressive scan if the table's frozen Xid is now older
	 * than or equal to the requested Xid full-table scan limit; or if the
	 * table's minimum MultiXactId is older than or equal to the requested
	 * mxid full-table scan limit; or if DISABLE_PAGE_SKIPPING was specified.
	 */
	aggressive = TransactionIdPrecedesOrEquals(onerel->rd_rel->relfrozenxid,
											   xidFullScanLimit);
	aggressive |= MultiXactIdPrecedesOrEquals(onerel->rd_rel->relminmxid,
											  mxactFullScanLimit);
	if (options.flags & VACOPT_DISABLE_PAGE_SKIPPING)
		aggressive = true;

	/* Create lazy vacuum state and statistics */
	lvstate = (LVState *) palloc0(sizeof(LVState));
	lvstate->aggressive = aggressive;
	lvstate->options = options;
	lvstate->relid = RelationGetRelid(onerel);
	lvstate->relation = onerel;
	lvstate->indstats = NULL;
	lvstate->dead_tuples = NULL;
	lvstate->lvshared = NULL;

	vacrelstats = (LVRelStats *) palloc0(sizeof(LVRelStats));
	vacrelstats->old_rel_pages = onerel->rd_rel->relpages;
	vacrelstats->old_live_tuples = onerel->rd_rel->reltuples;
	vacrelstats->num_index_scans = 0;
	vacrelstats->pages_removed = 0;
	vacrelstats->unused_tuples = 0;
	vacrelstats->num_tuples = 0;
	vacrelstats->empty_pages = 0;
	vacrelstats->lock_waiter_detected = false;
	lvstate->vacrelstats = vacrelstats;

	/* Open all indexes of the relation */
	vac_open_indexes(onerel, RowExclusiveLock, &lvstate->nindexes, &lvstate->indRels);
	vacrelstats->hasindex = (lvstate->nindexes > 0);

	lazy_scan_heap(lvstate);

	/* Done with indexes */
	vac_close_indexes(lvstate->nindexes, lvstate->indRels, NoLock);

	/*
	 * Compute whether we actually scanned the all unfrozen pages. If we did,
	 * we can adjust relfrozenxid and relminmxid.
	 *
	 * NB: We need to check this before truncating the relation, because that
	 * will change ->rel_pages.
	 */
	if ((vacrelstats->scanned_pages + vacrelstats->frozenskipped_pages)
		< vacrelstats->rel_pages)
	{
		Assert(!aggressive);
		scanned_all_unfrozen = false;
	}
	else
		scanned_all_unfrozen = true;

	/*
	 * Optionally truncate the relation.
	 */
	if (should_attempt_truncation(vacrelstats))
		lazy_truncate_heap(onerel, vacrelstats);

	/* Report that we are now doing final cleanup */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_FINAL_CLEANUP);

	/*
	 * Update statistics in pg_class.
	 *
	 * A corner case here is that if we scanned no pages at all because every
	 * page is all-visible, we should not update relpages/reltuples, because
	 * we have no new information to contribute.  In particular this keeps us
	 * from replacing relpages=reltuples=0 (which means "unknown tuple
	 * density") with nonzero relpages and reltuples=0 (which means "zero
	 * tuple density") unless there's some actual evidence for the latter.
	 *
	 * It's important that we use tupcount_pages and not scanned_pages for the
	 * check described above; scanned_pages counts pages where we could not
	 * get cleanup lock, and which were processed only for frozenxid purposes.
	 *
	 * We do update relallvisible even in the corner case, since if the table
	 * is all-visible we'd definitely like to know that.  But clamp the value
	 * to be not more than what we're setting relpages to.
	 *
	 * Also, don't change relfrozenxid/relminmxid if we skipped any pages,
	 * since then we don't know for certain that all tuples have a newer xmin.
	 */
	new_rel_pages = vacrelstats->rel_pages;
	new_live_tuples = vacrelstats->new_live_tuples;
	if (vacrelstats->tupcount_pages == 0 && new_rel_pages > 0)
	{
		new_rel_pages = vacrelstats->old_rel_pages;
		new_live_tuples = vacrelstats->old_live_tuples;
	}

	visibilitymap_count(onerel, &new_rel_allvisible, NULL);
	if (new_rel_allvisible > new_rel_pages)
		new_rel_allvisible = new_rel_pages;

	new_frozen_xid = scanned_all_unfrozen ? FreezeLimit : InvalidTransactionId;
	new_min_multi = scanned_all_unfrozen ? MultiXactCutoff : InvalidMultiXactId;

	vac_update_relstats(onerel,
						new_rel_pages,
						new_live_tuples,
						new_rel_allvisible,
						vacrelstats->hasindex,
						new_frozen_xid,
						new_min_multi,
						false);

	/* report results to the stats collector, too */
	pgstat_report_vacuum(RelationGetRelid(onerel),
						 onerel->rd_rel->relisshared,
						 new_live_tuples,
						 vacrelstats->new_dead_tuples);
	pgstat_progress_end_command();

	/* and log the action if appropriate */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		TimestampTz endtime = GetCurrentTimestamp();

		if (params->log_min_duration == 0 ||
			TimestampDifferenceExceeds(starttime, endtime,
									   params->log_min_duration))
		{
			StringInfoData buf;
			char	   *msgfmt;

			TimestampDifference(starttime, endtime, &secs, &usecs);

			read_rate = 0;
			write_rate = 0;
			if ((secs > 0) || (usecs > 0))
			{
				read_rate = (double) BLCKSZ * VacuumPageMiss / (1024 * 1024) /
					(secs + usecs / 1000000.0);
				write_rate = (double) BLCKSZ * VacuumPageDirty / (1024 * 1024) /
					(secs + usecs / 1000000.0);
			}

			/*
			 * This is pretty messy, but we split it up so that we can skip
			 * emitting individual parts of the message when not applicable.
			 */
			initStringInfo(&buf);
			if (lvstate->aggressive)
				msgfmt = _("automatic aggressive vacuum of table \"%s.%s.%s\": index scans: %d\n");
			else
				msgfmt = _("automatic vacuum of table \"%s.%s.%s\": index scans: %d\n");
			appendStringInfo(&buf, msgfmt,
							 get_database_name(MyDatabaseId),
							 get_namespace_name(RelationGetNamespace(onerel)),
							 RelationGetRelationName(onerel),
							 vacrelstats->num_index_scans);
			appendStringInfo(&buf, _("pages: %u removed, %u remain, %u skipped due to pins, %u skipped frozen\n"),
							 vacrelstats->pages_removed,
							 vacrelstats->rel_pages,
							 vacrelstats->pinskipped_pages,
							 vacrelstats->frozenskipped_pages);
			appendStringInfo(&buf,
							 _("tuples: %.0f removed, %.0f remain, %.0f are dead but not yet removable, oldest xmin: %u\n"),
							 vacrelstats->tuples_deleted,
							 vacrelstats->new_rel_tuples,
							 vacrelstats->new_dead_tuples,
							 OldestXmin);
			appendStringInfo(&buf,
							 _("buffer usage: %d hits, %d misses, %d dirtied\n"),
							 VacuumPageHit,
							 VacuumPageMiss,
							 VacuumPageDirty);
			appendStringInfo(&buf, _("avg read rate: %.3f MB/s, avg write rate: %.3f MB/s\n"),
							 read_rate, write_rate);
			appendStringInfo(&buf, _("system usage: %s"), pg_rusage_show(&ru0));

			ereport(LOG,
					(errmsg_internal("%s", buf.data)));
			pfree(buf.data);
		}
	}
}

/*
 * For Hot Standby we need to know the highest transaction id that will
 * be removed by any change. VACUUM proceeds in a number of passes so
 * we need to consider how each pass operates. The first phase runs
 * heap_page_prune(), which can issue XLOG_HEAP2_CLEAN records as it
 * progresses - these will have a latestRemovedXid on each record.
 * In some cases this removes all of the tuples to be removed, though
 * often we have dead tuples with index pointers so we must remember them
 * for removal in phase 3. Index records for those rows are removed
 * in phase 2 and index blocks do not have MVCC information attached.
 * So before we can allow removal of any index tuples we need to issue
 * a WAL record containing the latestRemovedXid of rows that will be
 * removed in phase three. This allows recovery queries to block at the
 * correct place, i.e. before phase two, rather than during phase three
 * which would be after the rows have become inaccessible.
 */
static void
vacuum_log_cleanup_info(Relation rel, LVRelStats *vacrelstats)
{
	/*
	 * Skip this for relations for which no WAL is to be written, or if we're
	 * not trying to support archive recovery.
	 */
	if (!RelationNeedsWAL(rel) || !XLogIsNeeded())
		return;

	/*
	 * No need to write the record at all unless it contains a valid value
	 */
	if (TransactionIdIsValid(vacrelstats->latestRemovedXid))
		(void) log_heap_cleanup_info(rel->rd_node, vacrelstats->latestRemovedXid);
}

/*
 *	do_lazy_scan_heap() -- scan an open heap relation
 *
 *		This routine prunes each page in the heap, which will among other
 *		things truncate dead tuples to dead line pointers, defragment the
 *		page, and set commit status bits (see heap_page_prune).  It also builds
 *		lists of dead tuples and pages with free space, calculates statistics
 *		on the number of live tuples in the heap, and marks pages as
 *		all-visible if appropriate.  When done, or when we run low on space for
 *		dead-tuple TIDs, invoke vacuuming of indexes and call lazy_vacuum_heap
 *		to reclaim dead line pointers.
 *
 *		If there are no indexes then we can reclaim line pointers on the fly;
 *		dead line pointers need only be retained until all index pointers that
 *		reference them have been killed.
 */
static void
do_lazy_scan_heap(LVState *lvstate)
{
	Relation 	onerel = lvstate->relation;
	LVRelStats	*vacrelstats = lvstate->vacrelstats;
	LVScanDesc	lvscan;
	BlockNumber nblocks,
				blkno;
	HeapTupleData tuple;
	char	   *relname;
	TransactionId relfrozenxid = onerel->rd_rel->relfrozenxid;
	TransactionId relminmxid = onerel->rd_rel->relminmxid;
	BlockNumber empty_pages,
				vacuumed_pages,
				next_fsm_block_to_vacuum;
	double		num_tuples,		/* total number of nonremovable tuples */
				live_tuples,	/* live tuples (reltuples estimate) */
				tups_vacuumed,	/* tuples cleaned up by vacuum */
				nkeep,			/* dead-but-not-removable tuples */
				nunused;		/* unused item pointers */
	int			i;
	Buffer		vmbuffer = InvalidBuffer;
	xl_heap_freeze_tuple *frozen;
	bool		all_visible_accroding_to_vm;
	const int	initprog_index[] = {
		PROGRESS_VACUUM_PHASE,
		PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
		PROGRESS_VACUUM_MAX_DEAD_TUPLES
	};
	int64		initprog_val[3];

	relname = RelationGetRelationName(onerel);
	if (IsVacuumLeader())
	{
		if (lvstate->aggressive)
			ereport(elevel,
					(errmsg("aggressively vacuuming \"%s.%s\"",
							get_namespace_name(RelationGetNamespace(onerel)),
							relname)));
		else
			ereport(elevel,
					(errmsg("vacuuming \"%s.%s\"",
							get_namespace_name(RelationGetNamespace(onerel)),
							relname)));
	}

	empty_pages = vacuumed_pages = 0;
	next_fsm_block_to_vacuum = (BlockNumber) 0;
	num_tuples = live_tuples = tups_vacuumed = nkeep = nunused = 0;

	lvstate->indbulkstats = (IndexBulkDeleteResult **)
		palloc0(lvstate->nindexes * sizeof(IndexBulkDeleteResult *));

	nblocks = RelationGetNumberOfBlocks(onerel);
	vacrelstats->rel_pages = nblocks;
	vacrelstats->scanned_pages = 0;
	vacrelstats->tupcount_pages = 0;
	vacrelstats->nonempty_pages = 0;
	vacrelstats->empty_pages = 0;
	vacrelstats->latestRemovedXid = InvalidTransactionId;

	frozen = palloc(sizeof(xl_heap_freeze_tuple) * MaxHeapTuplesPerPage);

	lvscan = lv_beginscan(onerel, lvstate->lvshared, lvstate->aggressive,
						  (lvstate->options.flags & VACOPT_DISABLE_PAGE_SKIPPING) != 0);

	/* Report that we're scanning the heap, advertising total # of blocks */
	initprog_val[0] = PROGRESS_VACUUM_PHASE_SCAN_HEAP;
	initprog_val[1] = nblocks;
	initprog_val[2] = lvstate->dead_tuples->max_items;
	pgstat_progress_update_multi_param(3, initprog_index, initprog_val);

	while ((blkno = lazy_scan_get_nextpage(lvscan, lvstate->vacrelstats,
										   &all_visible_accroding_to_vm, &vmbuffer))
		   != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		bool		tupgone,
					hastup;
		int			prev_dead_count;
		int			nfrozen;
		Size		freespace;
		bool		all_visible_according_to_vm = false;
		bool		all_visible;
		bool		all_frozen = true;	/* provided all_visible is also true */
		bool		has_dead_tuples;
		TransactionId visibility_cutoff_xid = InvalidTransactionId;

		vacuum_delay_point();

		/*
		 * If we are close to overrunning the available space for dead-tuple
		 * TIDs, pause and do a cycle of vacuuming before we tackle this page.
		 */
		if (lazy_dead_tuples_is_full(lvstate->dead_tuples))
		{
			const int	hvp_index[] = {
				PROGRESS_VACUUM_PHASE,
				PROGRESS_VACUUM_NUM_INDEX_VACUUMS
			};
			int64		hvp_val[2];

			/*
			 * Before beginning index vacuuming, we release any pin we may
			 * hold on the visibility map page.  This isn't necessary for
			 * correctness, but we do it anyway to avoid holding the pin
			 * across a lengthy, unrelated operation.
			 */
			if (BufferIsValid(vmbuffer))
			{
				ReleaseBuffer(vmbuffer);
				vmbuffer = InvalidBuffer;
			}

			/* Log cleanup info before we touch indexes */
			if (!IsParallelWorker())
				vacuum_log_cleanup_info(onerel, vacrelstats);

			/*
			 * Prepare for starting heap vacuum, if necessary. Wait for
			 * all workers and sort dead tuple arrays.
			 */
			lazy_set_my_worker_state(VACSTATE_SCAN_FINISHED);
			lazy_prepare_to_next_state(lvstate, VACSTATE_VACUUM_INDEX);

			/* Report that we are now vacuuming indexes */
			pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
										 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);

			/* Remove index entries */
			lazy_vacuum_all_indexes(lvstate);

			/*
			 * Prepare for heap vacuum. Since we must not vacuum any heap tuple
			 * before finished index vacuum, wait for other parallel workers
			 * if launched.
			 */
			lazy_set_my_worker_state(VACSTATE_VACUUM_INDEX_FINISHED);
			lazy_prepare_to_next_state(lvstate, VACSTATE_VACUUM_HEAP);

			/*
			 * Report that we are now vacuuming the heap.  We also increase
			 * the number of index scans here; note that by using
			 * pgstat_progress_update_multi_param we can update both
			 * parameters atomically.
			 */
			hvp_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_HEAP;
			hvp_val[1] = vacrelstats->num_index_scans + 1;
			pgstat_progress_update_multi_param(2, hvp_index, hvp_val);

			/* Remove tuples from heap */
			lazy_vacuum_heap(onerel, lvstate);

			/*
			 * Vacuum the Free Space Map to make newly-freed space visible on
			 * upper-level FSM pages.  Note we have not yet processed blkno.
			 */
			if (!IsVacuumLeader())
			{
				FreeSpaceMapVacuumRange(onerel, next_fsm_block_to_vacuum, blkno);
				next_fsm_block_to_vacuum = blkno;
			}

			/*
			 * Prepare for the next heap scan. Forget the now-vacuumed tuples,
			 * and press on, but be careful not to reset latestRemovedXid since
			 * we want that value to be valid.
			 */
			lazy_set_my_worker_state(VACSTATE_VACUUM_HEAP_FINISHED);
			lazy_prepare_to_next_state(lvstate, VACSTATE_SCAN);
			lvstate->dead_tuples->num_items = 0;
			vacrelstats->num_index_scans++;

			/* Report that we are once again scanning the heap */
			pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
										 PROGRESS_VACUUM_PHASE_SCAN_HEAP);
		}

		/*
		 * Pin the visibility map page in case we need to mark the page
		 * all-visible.  In most cases this will be very cheap, because we'll
		 * already have the correct page pinned anyway.  However, it's
		 * possible that (a) next_unskippable_block is covered by a different
		 * VM page than the current block or (b) we released our pin and did a
		 * cycle of index vacuuming.
		 *
		 */
		visibilitymap_pin(onerel, blkno, &vmbuffer);

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, vac_strategy);

		/* We need buffer cleanup lock so that we can prune HOT chains. */
		if (!ConditionalLockBufferForCleanup(buf))
		{
			/*
			 * If we're not performing an aggressive scan to guard against XID
			 * wraparound, and we don't want to forcibly check the page, then
			 * it's OK to skip vacuuming pages we get a lock conflict on. They
			 * will be dealt with in some future vacuum.
			 */
			if (!lvstate->aggressive &&
				!FORCE_CHECK_PAGE(vacrelstats->rel_pages, blkno, vacrelstats))
			{
				ReleaseBuffer(buf);
				vacrelstats->pinskipped_pages++;
				continue;
			}

			/*
			 * Read the page with share lock to see if any xids on it need to
			 * be frozen.  If not we just skip the page, after updating our
			 * scan statistics.  If there are some, we wait for cleanup lock.
			 *
			 * We could defer the lock request further by remembering the page
			 * and coming back to it later, or we could even register
			 * ourselves for multiple buffers and then service whichever one
			 * is received first.  For now, this seems good enough.
			 *
			 * If we get here with aggressive false, then we're just forcibly
			 * checking the page, and so we don't want to insist on getting
			 * the lock; we only need to know if the page contains tuples, so
			 * that we can update nonempty_pages correctly.  It's convenient
			 * to use lazy_check_needs_freeze() for both situations, though.
			 */
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			if (!lazy_check_needs_freeze(buf, &hastup))
			{
				UnlockReleaseBuffer(buf);
				vacrelstats->scanned_pages++;
				vacrelstats->pinskipped_pages++;
				if (hastup)
					vacrelstats->nonempty_pages = blkno + 1;
				continue;
			}
			if (!lvstate->aggressive)
			{
				/*
				 * Here, we must not advance scanned_pages; that would amount
				 * to claiming that the page contains no freezable tuples.
				 */
				UnlockReleaseBuffer(buf);
				vacrelstats->pinskipped_pages++;
				if (hastup)
					vacrelstats->nonempty_pages = blkno + 1;
				continue;
			}
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBufferForCleanup(buf);
			/* drop through to normal processing */
		}

		vacrelstats->scanned_pages++;
		vacrelstats->tupcount_pages++;

		page = BufferGetPage(buf);

		if (PageIsNew(page))
		{
			/*
			 * An all-zeroes page could be left over if a backend extends the
			 * relation but crashes before initializing the page. Reclaim such
			 * pages for use.
			 *
			 * We have to be careful here because we could be looking at a
			 * page that someone has just added to the relation and not yet
			 * been able to initialize (see RelationGetBufferForTuple). To
			 * protect against that, release the buffer lock, grab the
			 * relation extension lock momentarily, and re-lock the buffer. If
			 * the page is still uninitialized by then, it must be left over
			 * from a crashed backend, and we can initialize it.
			 *
			 * We don't really need the relation lock when this is a new or
			 * temp relation, but it's probably not worth the code space to
			 * check that, since this surely isn't a critical path.
			 *
			 * Note: the comparable code in vacuum.c need not worry because
			 * it's got exclusive lock on the whole relation.
			 */
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockRelationForExtension(onerel, ExclusiveLock);
			UnlockRelationForExtension(onerel, ExclusiveLock);
			LockBufferForCleanup(buf);
			if (PageIsNew(page))
			{
				ereport(WARNING,
						(errmsg("relation \"%s\" page %u is uninitialized --- fixing",
								relname, blkno)));
				PageInit(page, BufferGetPageSize(buf), 0);
				empty_pages++;
			}
			freespace = PageGetHeapFreeSpace(page);
			MarkBufferDirty(buf);
			UnlockReleaseBuffer(buf);

			RecordPageWithFreeSpace(onerel, blkno, freespace);
			continue;
		}

		if (PageIsEmpty(page))
		{
			empty_pages++;
			freespace = PageGetHeapFreeSpace(page);

			/* empty pages are always all-visible and all-frozen */
			if (!PageIsAllVisible(page))
			{
				START_CRIT_SECTION();

				/* mark buffer dirty before writing a WAL record */
				MarkBufferDirty(buf);

				/*
				 * It's possible that another backend has extended the heap,
				 * initialized the page, and then failed to WAL-log the page
				 * due to an ERROR.  Since heap extension is not WAL-logged,
				 * recovery might try to replay our record setting the page
				 * all-visible and find that the page isn't initialized, which
				 * will cause a PANIC.  To prevent that, check whether the
				 * page has been previously WAL-logged, and if not, do that
				 * now.
				 */
				if (RelationNeedsWAL(onerel) &&
					PageGetLSN(page) == InvalidXLogRecPtr)
					log_newpage_buffer(buf, true);

				PageSetAllVisible(page);
				visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
								  vmbuffer, InvalidTransactionId,
								  VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);
				END_CRIT_SECTION();
			}

			UnlockReleaseBuffer(buf);
			RecordPageWithFreeSpace(onerel, blkno, freespace);
			continue;
		}

		/*
		 * Prune all HOT-update chains in this page.
		 *
		 * We count tuples removed by the pruning step as removed by VACUUM.
		 */
		tups_vacuumed += heap_page_prune(onerel, buf, OldestXmin, false,
										 &vacrelstats->latestRemovedXid);

		/*
		 * Now scan the page to collect vacuumable items and check for tuples
		 * requiring freezing.
		 */
		all_visible = true;
		has_dead_tuples = false;
		nfrozen = 0;
		hastup = false;
		prev_dead_count = lazy_get_dead_tuple_count(lvstate->dead_tuples);
		maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * Note: If you change anything in the loop below, also look at
		 * heap_page_is_all_visible to see if that needs to be changed.
		 */
		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;

			itemid = PageGetItemId(page, offnum);

			/* Unused items require no processing, but we count 'em */
			if (!ItemIdIsUsed(itemid))
			{
				nunused += 1;
				continue;
			}

			/* Redirect items mustn't be touched */
			if (ItemIdIsRedirected(itemid))
			{
				hastup = true;	/* this page won't be truncatable */
				continue;
			}

			ItemPointerSet(&(tuple.t_self), blkno, offnum);

			/*
			 * DEAD item pointers are to be vacuumed normally; but we don't
			 * count them in tups_vacuumed, else we'd be double-counting (at
			 * least in the common case where heap_page_prune() just freed up
			 * a non-HOT tuple).
			 */
			if (ItemIdIsDead(itemid))
			{
				lazy_record_dead_tuple(lvstate->dead_tuples, &(tuple.t_self));
				all_visible = false;
				continue;
			}

			Assert(ItemIdIsNormal(itemid));

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(onerel);

			tupgone = false;

			/*
			 * The criteria for counting a tuple as live in this block need to
			 * match what analyze.c's acquire_sample_rows() does, otherwise
			 * VACUUM and ANALYZE may produce wildly different reltuples
			 * values, e.g. when there are many recently-dead tuples.
			 *
			 * The logic here is a bit simpler than acquire_sample_rows(), as
			 * VACUUM can't run inside a transaction block, which makes some
			 * cases impossible (e.g. in-progress insert from the same
			 * transaction).
			 */
			switch (HeapTupleSatisfiesVacuum(&tuple, OldestXmin, buf))
			{
				case HEAPTUPLE_DEAD:

					/*
					 * Ordinarily, DEAD tuples would have been removed by
					 * heap_page_prune(), but it's possible that the tuple
					 * state changed since heap_page_prune() looked.  In
					 * particular an INSERT_IN_PROGRESS tuple could have
					 * changed to DEAD if the inserter aborted.  So this
					 * cannot be considered an error condition.
					 *
					 * If the tuple is HOT-updated then it must only be
					 * removed by a prune operation; so we keep it just as if
					 * it were RECENTLY_DEAD.  Also, if it's a heap-only
					 * tuple, we choose to keep it, because it'll be a lot
					 * cheaper to get rid of it in the next pruning pass than
					 * to treat it like an indexed tuple.
					 *
					 * If this were to happen for a tuple that actually needed
					 * to be deleted, we'd be in trouble, because it'd
					 * possibly leave a tuple below the relation's xmin
					 * horizon alive.  heap_prepare_freeze_tuple() is prepared
					 * to detect that case and abort the transaction,
					 * preventing corruption.
					 */
					if (HeapTupleIsHotUpdated(&tuple) ||
						HeapTupleIsHeapOnly(&tuple))
						nkeep += 1;
					else
						tupgone = true; /* we can delete the tuple */
					all_visible = false;
					break;
				case HEAPTUPLE_LIVE:
					/* Tuple is good --- but let's do some validity checks */
					if (onerel->rd_rel->relhasoids &&
						!OidIsValid(HeapTupleGetOid(&tuple)))
						elog(WARNING, "relation \"%s\" TID %u/%u: OID is invalid",
							 relname, blkno, offnum);

					/*
					 * Count it as live.  Not only is this natural, but it's
					 * also what acquire_sample_rows() does.
					 */
					live_tuples += 1;

					/*
					 * Is the tuple definitely visible to all transactions?
					 *
					 * NB: Like with per-tuple hint bits, we can't set the
					 * PD_ALL_VISIBLE flag if the inserter committed
					 * asynchronously. See SetHintBits for more info. Check
					 * that the tuple is hinted xmin-committed because of
					 * that.
					 */
					if (all_visible)
					{
						TransactionId xmin;

						if (!HeapTupleHeaderXminCommitted(tuple.t_data))
						{
							all_visible = false;
							break;
						}

						/*
						 * The inserter definitely committed. But is it old
						 * enough that everyone sees it as committed?
						 */
						xmin = HeapTupleHeaderGetXmin(tuple.t_data);
						if (!TransactionIdPrecedes(xmin, OldestXmin))
						{
							all_visible = false;
							break;
						}

						/* Track newest xmin on page. */
						if (TransactionIdFollows(xmin, visibility_cutoff_xid))
							visibility_cutoff_xid = xmin;
					}
					break;
				case HEAPTUPLE_RECENTLY_DEAD:

					/*
					 * If tuple is recently deleted then we must not remove it
					 * from relation.
					 */
					nkeep += 1;
					all_visible = false;
					break;
				case HEAPTUPLE_INSERT_IN_PROGRESS:

					/*
					 * This is an expected case during concurrent vacuum.
					 *
					 * We do not count these rows as live, because we expect
					 * the inserting transaction to update the counters at
					 * commit, and we assume that will happen only after we
					 * report our results.  This assumption is a bit shaky,
					 * but it is what acquire_sample_rows() does, so be
					 * consistent.
					 */
					all_visible = false;
					break;
				case HEAPTUPLE_DELETE_IN_PROGRESS:
					/* This is an expected case during concurrent vacuum */
					all_visible = false;

					/*
					 * Count such rows as live.  As above, we assume the
					 * deleting transaction will commit and update the
					 * counters after we report.
					 */
					live_tuples += 1;
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					break;
			}

			if (tupgone)
			{
				lazy_record_dead_tuple(lvstate->dead_tuples, &(tuple.t_self));
				HeapTupleHeaderAdvanceLatestRemovedXid(tuple.t_data,
													   &vacrelstats->latestRemovedXid);
				tups_vacuumed += 1;
				has_dead_tuples = true;
			}
			else
			{
				bool		tuple_totally_frozen;

				num_tuples += 1;
				hastup = true;

				/*
				 * Each non-removable tuple must be checked to see if it needs
				 * freezing.  Note we already have exclusive buffer lock.
				 */
				if (heap_prepare_freeze_tuple(tuple.t_data,
											  relfrozenxid, relminmxid,
											  FreezeLimit, MultiXactCutoff,
											  &frozen[nfrozen],
											  &tuple_totally_frozen))
					frozen[nfrozen++].offset = offnum;

				if (!tuple_totally_frozen)
					all_frozen = false;
			}
		}						/* scan along page */

		/*
		 * If we froze any tuples, mark the buffer dirty, and write a WAL
		 * record recording the changes.  We must log the changes to be
		 * crash-safe against future truncation of CLOG.
		 */
		if (nfrozen > 0)
		{
			START_CRIT_SECTION();

			MarkBufferDirty(buf);

			/* execute collected freezes */
			for (i = 0; i < nfrozen; i++)
			{
				ItemId		itemid;
				HeapTupleHeader htup;

				itemid = PageGetItemId(page, frozen[i].offset);
				htup = (HeapTupleHeader) PageGetItem(page, itemid);

				heap_execute_freeze_tuple(htup, &frozen[i]);
			}

			/* Now WAL-log freezing if necessary */
			if (RelationNeedsWAL(onerel))
			{
				XLogRecPtr	recptr;

				recptr = log_heap_freeze(onerel, buf, FreezeLimit,
										 frozen, nfrozen);
				PageSetLSN(page, recptr);
			}

			END_CRIT_SECTION();
		}

		/*
		 * If there are no indexes then we can vacuum the page right now
		 * instead of doing a second scan.
		 */
		if (lvstate->nindexes == 0 && lvstate->dead_tuples->num_items > 0)
		{
			/* Remove tuples from heap */
			lazy_vacuum_page(lvstate, onerel, blkno, buf, 0, &vmbuffer);
			has_dead_tuples = false;

			/*
			 * Forget the now-vacuumed tuples, and press on, but be careful
			 * not to reset latestRemovedXid since we want that value to be
			 * valid.
			 */
			lvstate->dead_tuples->num_items = 0;
			vacuumed_pages++;

			/*
			 * Periodically do incremental FSM vacuuming to make newly-freed
			 * space visible on upper FSM pages.  Note: although we've cleaned
			 * the current block, we haven't yet updated its FSM entry (that
			 * happens further down), so passing end == blkno is correct.
			 */
			if (blkno - next_fsm_block_to_vacuum >= VACUUM_FSM_EVERY_PAGES &&
				!IsParallelWorker())
			{
				FreeSpaceMapVacuumRange(onerel, next_fsm_block_to_vacuum,
										blkno);
				next_fsm_block_to_vacuum = blkno;
			}
		}

		freespace = PageGetHeapFreeSpace(page);

		/* mark page all-visible, if appropriate */
		if (all_visible && !all_visible_according_to_vm)
		{
			uint8		flags = VISIBILITYMAP_ALL_VISIBLE;

			if (all_frozen)
				flags |= VISIBILITYMAP_ALL_FROZEN;

			/*
			 * It should never be the case that the visibility map page is set
			 * while the page-level bit is clear, but the reverse is allowed
			 * (if checksums are not enabled).  Regardless, set the both bits
			 * so that we get back in sync.
			 *
			 * NB: If the heap page is all-visible but the VM bit is not set,
			 * we don't need to dirty the heap page.  However, if checksums
			 * are enabled, we do need to make sure that the heap page is
			 * dirtied before passing it to visibilitymap_set(), because it
			 * may be logged.  Given that this situation should only happen in
			 * rare cases after a crash, it is not worth optimizing.
			 */
			PageSetAllVisible(page);
			MarkBufferDirty(buf);
			visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
							  vmbuffer, visibility_cutoff_xid, flags);
		}

		/*
		 * As of PostgreSQL 9.2, the visibility map bit should never be set if
		 * the page-level bit is clear.  However, it's possible that the bit
		 * got cleared after we checked it and before we took the buffer
		 * content lock, so we must recheck before jumping to the conclusion
		 * that something bad has happened.
		 */
		else if (all_visible_according_to_vm && !PageIsAllVisible(page)
				 && VM_ALL_VISIBLE(onerel, blkno, &vmbuffer))
		{
			elog(WARNING, "page is not marked all-visible but visibility map bit is set in relation \"%s\" page %u",
				 relname, blkno);
			visibilitymap_clear(onerel, blkno, vmbuffer,
								VISIBILITYMAP_VALID_BITS);
		}

		/*
		 * It's possible for the value returned by GetOldestXmin() to move
		 * backwards, so it's not wrong for us to see tuples that appear to
		 * not be visible to everyone yet, while PD_ALL_VISIBLE is already
		 * set. The real safe xmin value never moves backwards, but
		 * GetOldestXmin() is conservative and sometimes returns a value
		 * that's unnecessarily small, so if we see that contradiction it just
		 * means that the tuples that we think are not visible to everyone yet
		 * actually are, and the PD_ALL_VISIBLE flag is correct.
		 *
		 * There should never be dead tuples on a page with PD_ALL_VISIBLE
		 * set, however.
		 */
		else if (PageIsAllVisible(page) && has_dead_tuples)
		{
			elog(WARNING, "page containing dead tuples is marked as all-visible in relation \"%s\" page %u",
				 relname, blkno);
			PageClearAllVisible(page);
			MarkBufferDirty(buf);
			visibilitymap_clear(onerel, blkno, vmbuffer,
								VISIBILITYMAP_VALID_BITS);
		}

		/*
		 * If the all-visible page is turned out to be all-frozen but not
		 * marked, we should so mark it.  Note that all_frozen is only valid
		 * if all_visible is true, so we must check both.
		 */
		else if (all_visible_according_to_vm && all_visible && all_frozen &&
				 !VM_ALL_FROZEN(onerel, blkno, &vmbuffer))
		{
			/*
			 * We can pass InvalidTransactionId as the cutoff XID here,
			 * because setting the all-frozen bit doesn't cause recovery
			 * conflicts.
			 */
			visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
							  vmbuffer, InvalidTransactionId,
							  VISIBILITYMAP_ALL_FROZEN);
		}

		UnlockReleaseBuffer(buf);

		/* Remember the location of the last page with nonremovable tuples */
		if (hastup)
			vacrelstats->nonempty_pages = blkno + 1;

		/*
		 * If we remembered any tuples for deletion, then the page will be
		 * visited again by lazy_vacuum_heap, which will compute and record
		 * its post-compaction free space.  If not, then we're done with this
		 * page, so remember its free space as-is.  (This path will always be
		 * taken if there are no indexes.)
		 */
		if (lazy_get_dead_tuple_count(lvstate->dead_tuples) == prev_dead_count)
			RecordPageWithFreeSpace(onerel, blkno, freespace);
	}

	/* report that everything is scanned and vacuumed */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno);

	pfree(frozen);

	/* save stats for use later */
	vacrelstats->tuples_deleted = tups_vacuumed;
	vacrelstats->new_dead_tuples = nkeep;
	vacrelstats->empty_pages = empty_pages;
	vacrelstats->unused_tuples = nunused;
	vacrelstats->num_tuples = num_tuples;

	/* now we can compute the new value for pg_class.reltuples */
	vacrelstats->new_live_tuples = vac_estimate_reltuples(onerel,
														  nblocks,
														  vacrelstats->tupcount_pages,
														  live_tuples);

	/* also compute total number of surviving heap entries */
	vacrelstats->new_rel_tuples =
		vacrelstats->new_live_tuples + vacrelstats->new_dead_tuples;

	/*
	 * Release any remaining pin on visibility map page.
	 */
	if (BufferIsValid(vmbuffer))
	{
		ReleaseBuffer(vmbuffer);
		vmbuffer = InvalidBuffer;
	}

	/* If any tuples need to be deleted, perform final vacuum cycle */
	/* XXX put a threshold on min number of tuples here? */
	if (lazy_get_dead_tuple_count(lvstate->dead_tuples) > 0)
	{
		const int	hvp_index[] = {
			PROGRESS_VACUUM_PHASE,
			PROGRESS_VACUUM_NUM_INDEX_VACUUMS
		};
		int64		hvp_val[2];

		/* Log cleanup info before we touch indexes */
		if (IsVacuumLeader())
			vacuum_log_cleanup_info(onerel, vacrelstats);

		lazy_set_my_worker_state(VACSTATE_SCAN_FINISHED);
		lazy_prepare_to_next_state(lvstate, VACSTATE_VACUUM_INDEX);

		/* Report that we are now vacuuming indexes */
		pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
									 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);

		/* Remove index entries */
		lazy_vacuum_all_indexes(lvstate);

		lazy_set_my_worker_state(VACSTATE_VACUUM_INDEX_FINISHED);
		lazy_prepare_to_next_state(lvstate, VACSTATE_VACUUM_HEAP);

		/* Report that we are now vacuuming the heap */
		hvp_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_HEAP;
		hvp_val[1] = vacrelstats->num_index_scans + 1;
		pgstat_progress_update_multi_param(2, hvp_index, hvp_val);

		/* Remove tuples from heap */
		pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
									 PROGRESS_VACUUM_PHASE_VACUUM_HEAP);
		lazy_vacuum_heap(onerel, lvstate);
		vacrelstats->num_index_scans++;
	}

	/*
	 * Vacuum the remainder of the Free Space Map.  We must do this whether or
	 * not there were indexes.
	 */
	if (blkno > next_fsm_block_to_vacuum && !IsParallelWorker())
		FreeSpaceMapVacuumRange(onerel, next_fsm_block_to_vacuum, blkno);

	/* report all blocks vacuumed; and that we're cleaning up */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_INDEX_CLEANUP);

	/*
	 * There is nothing to do before cleanup indexes but this ensures
	 * that all worker rocesses don't finish lazy_scan_heap before
	 * the leader reaches here. This is required some cases, for example,
	 * where all workers has done before the leader enters to do_lazy_scan_heap.
	 */
	lazy_set_my_worker_state(VACSTATE_BEFORE_CLEANUP_INDEX);
	lazy_prepare_to_next_state(lvstate, VACSTATE_CLEANUP_INDEX);

	/* Do post-vacuum cleanup and statistics update for each index */
	lazy_cleanup_all_indexes(lvstate);

	/* End scan */
	lv_endscan(lvscan);

	/* If no indexes, make log report that lazy_vacuum_heap would've made */
	if (vacuumed_pages)
		ereport(elevel,
				(errmsg("\"%s\": removed %.0f row versions in %u pages",
						RelationGetRelationName(onerel),
						tups_vacuumed, vacuumed_pages)));

}


/*
 *	lazy_vacuum_heap() -- second pass over the heap
 *
 *		This routine marks dead tuples as unused and compacts out free
 *		space on their pages.  Pages not having dead tuples recorded from
 *		lazy_scan_heap are not visited at all.
 *
 * Note: the reason for doing this as a second pass is we cannot remove
 * the tuples until we've removed their index entries, and we want to
 * process index entry removal in batches as large as possible.
 */
static void
lazy_vacuum_heap(Relation onerel, LVState *lvstate)
{
	int			tupindex = 0;
	int			npages;
	PGRUsage	ru0;
	Buffer		vmbuffer = InvalidBuffer;
	BlockNumber tblk;

	pg_rusage_init(&ru0);
	npages = 0;

	while ((tblk = lazy_get_next_vacuum_page(lvstate)) != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		Size		freespace;

		vacuum_delay_point();

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, tblk, RBM_NORMAL,
								 vac_strategy);
		if (!ConditionalLockBufferForCleanup(buf))
		{
			ReleaseBuffer(buf);
			++tupindex;
			continue;
		}
		tupindex = lazy_vacuum_page(lvstate, onerel, tblk, buf, tupindex,
									&vmbuffer);

		/* Now that we've compacted the page, record its available space */
		page = BufferGetPage(buf);
		freespace = PageGetHeapFreeSpace(page);

		UnlockReleaseBuffer(buf);
		RecordPageWithFreeSpace(onerel, tblk, freespace);
		npages++;
	}

	if (BufferIsValid(vmbuffer))
	{
		ReleaseBuffer(vmbuffer);
		vmbuffer = InvalidBuffer;
	}

	ereport(elevel,
			(errmsg("\"%s\": removed %d row versions in %d pages",
					RelationGetRelationName(onerel),
					tupindex, npages),
			 errdetail_internal("%s", pg_rusage_show(&ru0))));
}

/*
 *	lazy_vacuum_page() -- free dead tuples on a page
 *					 and repair its fragmentation.
 *
 * Caller must hold pin and buffer cleanup lock on the buffer.
 *
 * tupindex is the index in vacrelstats->dead_tuples of the first dead
 * tuple for this page.  We assume the rest follow sequentially.
 * The return value is the first tupindex after the tuples of this page.
 */
static int
lazy_vacuum_page(LVState *lvstate, Relation onerel, BlockNumber blkno,
				 Buffer buffer, int tupindex, Buffer *vmbuffer)
{
	LVRelStats *vacrelstats = lvstate->vacrelstats;
	LVTidMap   *dead_tuples = lvstate->dead_tuples;
	Page		page = BufferGetPage(buffer);
	OffsetNumber unused[MaxOffsetNumber];
	int			uncnt = 0;
	TransactionId visibility_cutoff_xid;
	bool		all_frozen;

	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);

	START_CRIT_SECTION();

	for (; tupindex < lazy_get_dead_tuple_count(dead_tuples); tupindex++)
	{
		BlockNumber tblk;
		OffsetNumber toff;
		ItemId		itemid;

		tblk = ItemPointerGetBlockNumber(&dead_tuples->itemptrs[tupindex]);
		if (tblk != blkno)
			break;				/* past end of tuples for this block */
		toff = ItemPointerGetOffsetNumber(&dead_tuples->itemptrs[tupindex]);
		itemid = PageGetItemId(page, toff);
		ItemIdSetUnused(itemid);
		unused[uncnt++] = toff;
	}

	PageRepairFragmentation(page);

	/*
	 * Mark buffer dirty before we write WAL.
	 */
	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(onerel))
	{
		XLogRecPtr	recptr;

		recptr = log_heap_clean(onerel, buffer,
								NULL, 0, NULL, 0,
								unused, uncnt,
								vacrelstats->latestRemovedXid);
		PageSetLSN(page, recptr);
	}

	/*
	 * End critical section, so we safely can do visibility tests (which
	 * possibly need to perform IO and allocate memory!). If we crash now the
	 * page (including the corresponding vm bit) might not be marked all
	 * visible, but that's fine. A later vacuum will fix that.
	 */
	END_CRIT_SECTION();

	/*
	 * Now that we have removed the dead tuples from the page, once again
	 * check if the page has become all-visible.  The page is already marked
	 * dirty, exclusively locked, and, if needed, a full page image has been
	 * emitted in the log_heap_clean() above.
	 */
	if (heap_page_is_all_visible(onerel, buffer, &visibility_cutoff_xid,
								 &all_frozen))
		PageSetAllVisible(page);

	/*
	 * All the changes to the heap page have been done. If the all-visible
	 * flag is now set, also set the VM all-visible bit (and, if possible, the
	 * all-frozen bit) unless this has already been done previously.
	 */
	if (PageIsAllVisible(page))
	{
		uint8		vm_status = visibilitymap_get_status(onerel, blkno, vmbuffer);
		uint8		flags = 0;

		/* Set the VM all-frozen bit to flag, if needed */
		if ((vm_status & VISIBILITYMAP_ALL_VISIBLE) == 0)
			flags |= VISIBILITYMAP_ALL_VISIBLE;
		if ((vm_status & VISIBILITYMAP_ALL_FROZEN) == 0 && all_frozen)
			flags |= VISIBILITYMAP_ALL_FROZEN;

		Assert(BufferIsValid(*vmbuffer));
		if (flags != 0)
			visibilitymap_set(onerel, blkno, buffer, InvalidXLogRecPtr,
							  *vmbuffer, visibility_cutoff_xid, flags);
	}

	return tupindex;
}

/*
 *	lazy_check_needs_freeze() -- scan page to see if any tuples
 *					 need to be cleaned to avoid wraparound
 *
 * Returns true if the page needs to be vacuumed using cleanup lock.
 * Also returns a flag indicating whether page contains any tuples at all.
 */
static bool
lazy_check_needs_freeze(Buffer buf, bool *hastup)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber offnum,
				maxoff;
	HeapTupleHeader tupleheader;

	*hastup = false;

	/* If we hit an uninitialized page, we want to force vacuuming it. */
	if (PageIsNew(page))
		return true;

	/* Quick out for ordinary empty page. */
	if (PageIsEmpty(page))
		return false;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		itemid = PageGetItemId(page, offnum);

		/* this should match hastup test in count_nondeletable_pages() */
		if (ItemIdIsUsed(itemid))
			*hastup = true;

		/* dead and redirect items never need freezing */
		if (!ItemIdIsNormal(itemid))
			continue;

		tupleheader = (HeapTupleHeader) PageGetItem(page, itemid);

		if (heap_tuple_needs_freeze(tupleheader, FreezeLimit,
									MultiXactCutoff, buf))
			return true;
	}							/* scan along page */

	return false;
}


/*
 *	lazy_vacuum_index() -- vacuum one index relation.
 *
 *		Delete all the index entries pointing to tuples listed in
 *		vacrelstats->dead_tuples, and update running statistics.
 */
static void
lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  LVRelStats *vacrelstats,
				  LVTidMap	*dead_tuples)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = elevel;
	/* We can only provide an approximate value of num_heap_tuples here */
	ivinfo.num_heap_tuples = vacrelstats->old_live_tuples;
	ivinfo.strategy = vac_strategy;

	/* Do bulk deletion */
	*stats = index_bulk_delete(&ivinfo, *stats,
							   lazy_tid_reaped, (void *) dead_tuples);

	ereport(elevel,
			(errmsg("scanned index \"%s\" to remove %d row versions",
					RelationGetRelationName(indrel),
					lazy_get_dead_tuple_count(dead_tuples)),
			 errdetail_internal("%s", pg_rusage_show(&ru0))));
}

/*
 *	lazy_cleanup_index() -- do post-vacuum cleanup for one index relation.
 */
static void
lazy_cleanup_index(Relation indrel,
				   IndexBulkDeleteResult *stats,
				   LVRelStats *vacrelstats,
				   IndexStats *indstats)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = (vacrelstats->tupcount_pages < vacrelstats->rel_pages);
	ivinfo.message_level = elevel;

	/*
	 * Now we can provide a better estimate of total number of surviving
	 * tuples (we assume indexes are more interested in that than in the
	 * number of nominally live tuples).
	 */
	ivinfo.num_heap_tuples = vacrelstats->new_rel_tuples;
	ivinfo.strategy = vac_strategy;

	stats = index_vacuum_cleanup(&ivinfo, stats);

	if (!stats)
		return;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 */
	if (!stats->estimated_count)
	{
		if (indstats)
		{
			indstats->need_update = true;
			indstats->num_pages = stats->num_pages;
			indstats->num_tuples = stats->num_index_tuples;
		}
		else
			vac_update_relstats(indrel,
								stats->num_pages,
								stats->num_index_tuples,
								0,
								false,
								InvalidTransactionId,
								InvalidMultiXactId,
								false);
	}

	ereport(elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indrel),
					stats->num_index_tuples,
					stats->num_pages),
			 errdetail("%.0f index row versions were removed.\n"
					   "%u index pages have been deleted, %u are currently reusable.\n"
					   "%s.",
					   stats->tuples_removed,
					   stats->pages_deleted, stats->pages_free,
					   pg_rusage_show(&ru0))));

	pfree(stats);
}

/*
 * should_attempt_truncation - should we attempt to truncate the heap?
 *
 * Don't even think about it unless we have a shot at releasing a goodly
 * number of pages.  Otherwise, the time taken isn't worth it.
 *
 * Also don't attempt it if we are doing early pruning/vacuuming, because a
 * scan which cannot find a truncated heap page cannot determine that the
 * snapshot is too old to read that page.  We might be able to get away with
 * truncating all except one of the pages, setting its LSN to (at least) the
 * maximum of the truncated range if we also treated an index leaf tuple
 * pointing to a missing heap page as something to trigger the "snapshot too
 * old" error, but that seems fragile and seems like it deserves its own patch
 * if we consider it.
 *
 * This is split out so that we can test whether truncation is going to be
 * called for before we actually do it.  If you change the logic here, be
 * careful to depend only on fields that lazy_scan_heap updates on-the-fly.
 */
static bool
should_attempt_truncation(LVRelStats *vacrelstats)
{
	BlockNumber possibly_freeable;

	possibly_freeable = vacrelstats->rel_pages - vacrelstats->nonempty_pages;
	if (possibly_freeable > 0 &&
		(possibly_freeable >= REL_TRUNCATE_MINIMUM ||
		 possibly_freeable >= vacrelstats->rel_pages / REL_TRUNCATE_FRACTION) &&
		old_snapshot_threshold < 0)
		return true;
	else
		return false;
}

/*
 * lazy_truncate_heap - try to truncate off any empty pages at the end
 */
static void
lazy_truncate_heap(Relation onerel, LVRelStats *vacrelstats)
{
	BlockNumber old_rel_pages = vacrelstats->rel_pages;
	BlockNumber new_rel_pages;
	PGRUsage	ru0;
	int			lock_retry;

	pg_rusage_init(&ru0);

	/* Report that we are now truncating */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_TRUNCATE);

	/*
	 * Loop until no more truncating can be done.
	 */
	do
	{
		/*
		 * We need full exclusive lock on the relation in order to do
		 * truncation. If we can't get it, give up rather than waiting --- we
		 * don't want to block other backends, and we don't want to deadlock
		 * (which is quite possible considering we already hold a lower-grade
		 * lock).
		 */
		vacrelstats->lock_waiter_detected = false;
		lock_retry = 0;
		while (true)
		{
			if (ConditionalLockRelation(onerel, AccessExclusiveLock))
				break;

			/*
			 * Check for interrupts while trying to (re-)acquire the exclusive
			 * lock.
			 */
			CHECK_FOR_INTERRUPTS();

			if (++lock_retry > (VACUUM_TRUNCATE_LOCK_TIMEOUT /
								VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL))
			{
				/*
				 * We failed to establish the lock in the specified number of
				 * retries. This means we give up truncating.
				 */
				vacrelstats->lock_waiter_detected = true;
				ereport(elevel,
						(errmsg("\"%s\": stopping truncate due to conflicting lock request",
								RelationGetRelationName(onerel))));
				return;
			}

			pg_usleep(VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL * 1000L);
		}

		/*
		 * Now that we have exclusive lock, look to see if the rel has grown
		 * whilst we were vacuuming with non-exclusive lock.  If so, give up;
		 * the newly added pages presumably contain non-deletable tuples.
		 */
		new_rel_pages = RelationGetNumberOfBlocks(onerel);
		if (new_rel_pages != old_rel_pages)
		{
			/*
			 * Note: we intentionally don't update vacrelstats->rel_pages with
			 * the new rel size here.  If we did, it would amount to assuming
			 * that the new pages are empty, which is unlikely. Leaving the
			 * numbers alone amounts to assuming that the new pages have the
			 * same tuple density as existing ones, which is less unlikely.
			 */
			UnlockRelation(onerel, AccessExclusiveLock);
			return;
		}

		/*
		 * Scan backwards from the end to verify that the end pages actually
		 * contain no tuples.  This is *necessary*, not optional, because
		 * other backends could have added tuples to these pages whilst we
		 * were vacuuming.
		 */
		new_rel_pages = count_nondeletable_pages(onerel, vacrelstats);

		if (new_rel_pages >= old_rel_pages)
		{
			/* can't do anything after all */
			UnlockRelation(onerel, AccessExclusiveLock);
			return;
		}

		/*
		 * Okay to truncate.
		 */
		RelationTruncate(onerel, new_rel_pages);

		/*
		 * We can release the exclusive lock as soon as we have truncated.
		 * Other backends can't safely access the relation until they have
		 * processed the smgr invalidation that smgrtruncate sent out ... but
		 * that should happen as part of standard invalidation processing once
		 * they acquire lock on the relation.
		 */
		UnlockRelation(onerel, AccessExclusiveLock);

		/*
		 * Update statistics.  Here, it *is* correct to adjust rel_pages
		 * without also touching reltuples, since the tuple count wasn't
		 * changed by the truncation.
		 */
		vacrelstats->pages_removed += old_rel_pages - new_rel_pages;
		vacrelstats->rel_pages = new_rel_pages;

		ereport(elevel,
				(errmsg("\"%s\": truncated %u to %u pages",
						RelationGetRelationName(onerel),
						old_rel_pages, new_rel_pages),
				 errdetail_internal("%s",
									pg_rusage_show(&ru0))));
		old_rel_pages = new_rel_pages;
	} while (new_rel_pages > vacrelstats->nonempty_pages &&
			 vacrelstats->lock_waiter_detected);
}

/*
 * Rescan end pages to verify that they are (still) empty of tuples.
 *
 * Returns number of nondeletable pages (last nonempty page + 1).
 */
static BlockNumber
count_nondeletable_pages(Relation onerel, LVRelStats *vacrelstats)
{
	BlockNumber blkno;
	BlockNumber prefetchedUntil;
	instr_time	starttime;

	/* Initialize the starttime if we check for conflicting lock requests */
	INSTR_TIME_SET_CURRENT(starttime);

	/*
	 * Start checking blocks at what we believe relation end to be and move
	 * backwards.  (Strange coding of loop control is needed because blkno is
	 * unsigned.)  To make the scan faster, we prefetch a few blocks at a time
	 * in forward direction, so that OS-level readahead can kick in.
	 */
	blkno = vacrelstats->rel_pages;
	StaticAssertStmt((PREFETCH_SIZE & (PREFETCH_SIZE - 1)) == 0,
					 "prefetch size must be power of 2");
	prefetchedUntil = InvalidBlockNumber;
	while (blkno > vacrelstats->nonempty_pages)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		bool		hastup;

		/*
		 * Check if another process requests a lock on our relation. We are
		 * holding an AccessExclusiveLock here, so they will be waiting. We
		 * only do this once per VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL, and we
		 * only check if that interval has elapsed once every 32 blocks to
		 * keep the number of system calls and actual shared lock table
		 * lookups to a minimum.
		 */
		if ((blkno % 32) == 0)
		{
			instr_time	currenttime;
			instr_time	elapsed;

			INSTR_TIME_SET_CURRENT(currenttime);
			elapsed = currenttime;
			INSTR_TIME_SUBTRACT(elapsed, starttime);
			if ((INSTR_TIME_GET_MICROSEC(elapsed) / 1000)
				>= VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL)
			{
				if (LockHasWaitersRelation(onerel, AccessExclusiveLock))
				{
					ereport(elevel,
							(errmsg("\"%s\": suspending truncate due to conflicting lock request",
									RelationGetRelationName(onerel))));

					vacrelstats->lock_waiter_detected = true;
					return blkno;
				}
				starttime = currenttime;
			}
		}

		/*
		 * We don't insert a vacuum delay point here, because we have an
		 * exclusive lock on the table which we want to hold for as short a
		 * time as possible.  We still need to check for interrupts however.
		 */
		CHECK_FOR_INTERRUPTS();

		blkno--;

		/* If we haven't prefetched this lot yet, do so now. */
		if (prefetchedUntil > blkno)
		{
			BlockNumber prefetchStart;
			BlockNumber pblkno;

			prefetchStart = blkno & ~(PREFETCH_SIZE - 1);
			for (pblkno = prefetchStart; pblkno <= blkno; pblkno++)
			{
				PrefetchBuffer(onerel, MAIN_FORKNUM, pblkno);
				CHECK_FOR_INTERRUPTS();
			}
			prefetchedUntil = prefetchStart;
		}

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, vac_strategy);

		/* In this phase we only need shared access to the buffer */
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		if (PageIsNew(page) || PageIsEmpty(page))
		{
			/* PageIsNew probably shouldn't happen... */
			UnlockReleaseBuffer(buf);
			continue;
		}

		hastup = false;
		maxoff = PageGetMaxOffsetNumber(page);
		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;

			itemid = PageGetItemId(page, offnum);

			/*
			 * Note: any non-unused item should be taken as a reason to keep
			 * this page.  We formerly thought that DEAD tuples could be
			 * thrown away, but that's not so, because we'd not have cleaned
			 * out their index entries.
			 */
			if (ItemIdIsUsed(itemid))
			{
				hastup = true;
				break;			/* can stop scanning */
			}
		}						/* scan along page */

		UnlockReleaseBuffer(buf);

		/* Done scanning if we found a tuple here */
		if (hastup)
			return blkno + 1;
	}

	/*
	 * If we fall out of the loop, all the previously-thought-to-be-empty
	 * pages still are; we need not bother to look at the last known-nonempty
	 * page.
	 */
	return vacrelstats->nonempty_pages;
}

/*
 * lazy_space_alloc - space allocation decisions for lazy vacuum
 *
 * See the comments at the head of this file for rationale.
 */
static void
lazy_space_alloc(LVState *lvstate, BlockNumber relblocks)
{
	long		maxtuples;
	LVTidMap	*dead_tuples;

	Assert(lvstate->dead_tuples == NULL);

	maxtuples = lazy_get_max_dead_tuples(lvstate->vacrelstats,
										 relblocks);

	dead_tuples = (LVTidMap *) palloc(SizeOfLVTidMap +
									  sizeof(ItemPointerData) * (int) maxtuples);
	dead_tuples->max_items = maxtuples;
	dead_tuples->num_items = 0;
	dead_tuples->shared = false;
	dead_tuples->item_idx = 0;

	lvstate->dead_tuples = dead_tuples;
}

/*
 * lazy_record_dead_tuple - remember one deletable tuple
 */
static void
lazy_record_dead_tuple(LVTidMap *dead_tuples, ItemPointer itemptr)
{
	if (dead_tuples->shared)
		SpinLockAcquire(&(dead_tuples->mutex));

	/*
	 * The array shouldn't overflow under normal behavior, but perhaps it
	 * could if we are given a really small maintenance_work_mem. In that
	 * case, just forget the last few tuples (we'll get 'em next time).
	 */
	if (dead_tuples->num_items < dead_tuples->max_items)
	{
		dead_tuples->itemptrs[dead_tuples->num_items] = *itemptr;
		(dead_tuples->num_items)++;
		pgstat_progress_update_param(PROGRESS_VACUUM_NUM_DEAD_TUPLES,
									 dead_tuples->num_items);
	}

	if (dead_tuples->shared)
		SpinLockRelease(&(dead_tuples->mutex));
}

/*
 *	lazy_tid_reaped() -- is a particular tid deletable?
 *
 *		This has the right signature to be an IndexBulkDeleteCallback.
 *
 *		Assumes dead_tuples array is in sorted order.
 */
static bool
lazy_tid_reaped(ItemPointer itemptr, void *dt)
{
	LVTidMap	*dead_tuples = (LVTidMap *) dt;
	ItemPointer res;

	res = (ItemPointer) bsearch((void *) itemptr,
								(void *) dead_tuples->itemptrs,
								lazy_get_dead_tuple_count(dead_tuples),
								sizeof(ItemPointerData),
								vac_cmp_itemptr);

	return (res != NULL);
}

/*
 * Comparator routines for use with qsort() and bsearch().
 */
static int
vac_cmp_itemptr(const void *left, const void *right)
{
	BlockNumber lblk,
				rblk;
	OffsetNumber loff,
				roff;

	lblk = ItemPointerGetBlockNumber((ItemPointer) left);
	rblk = ItemPointerGetBlockNumber((ItemPointer) right);

	if (lblk < rblk)
		return -1;
	if (lblk > rblk)
		return 1;

	loff = ItemPointerGetOffsetNumber((ItemPointer) left);
	roff = ItemPointerGetOffsetNumber((ItemPointer) right);

	if (loff < roff)
		return -1;
	if (loff > roff)
		return 1;

	return 0;
}

/*
 * Check if every tuple in the given page is visible to all current and future
 * transactions. Also return the visibility_cutoff_xid which is the highest
 * xmin amongst the visible tuples.  Set *all_frozen to true if every tuple
 * on this page is frozen.
 */
static bool
heap_page_is_all_visible(Relation rel, Buffer buf,
						 TransactionId *visibility_cutoff_xid,
						 bool *all_frozen)
{
	Page		page = BufferGetPage(buf);
	BlockNumber blockno = BufferGetBlockNumber(buf);
	OffsetNumber offnum,
				maxoff;
	bool		all_visible = true;

	*visibility_cutoff_xid = InvalidTransactionId;
	*all_frozen = true;

	/*
	 * This is a stripped down version of the line pointer scan in
	 * lazy_scan_heap(). So if you change anything here, also check that code.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff && all_visible;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;
		HeapTupleData tuple;

		itemid = PageGetItemId(page, offnum);

		/* Unused or redirect line pointers are of no interest */
		if (!ItemIdIsUsed(itemid) || ItemIdIsRedirected(itemid))
			continue;

		ItemPointerSet(&(tuple.t_self), blockno, offnum);

		/*
		 * Dead line pointers can have index pointers pointing to them. So
		 * they can't be treated as visible
		 */
		if (ItemIdIsDead(itemid))
		{
			all_visible = false;
			*all_frozen = false;
			break;
		}

		Assert(ItemIdIsNormal(itemid));

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = RelationGetRelid(rel);

		switch (HeapTupleSatisfiesVacuum(&tuple, OldestXmin, buf))
		{
			case HEAPTUPLE_LIVE:
				{
					TransactionId xmin;

					/* Check comments in lazy_scan_heap. */
					if (!HeapTupleHeaderXminCommitted(tuple.t_data))
					{
						all_visible = false;
						*all_frozen = false;
						break;
					}

					/*
					 * The inserter definitely committed. But is it old enough
					 * that everyone sees it as committed?
					 */
					xmin = HeapTupleHeaderGetXmin(tuple.t_data);
					if (!TransactionIdPrecedes(xmin, OldestXmin))
					{
						all_visible = false;
						*all_frozen = false;
						break;
					}

					/* Track newest xmin on page. */
					if (TransactionIdFollows(xmin, *visibility_cutoff_xid))
						*visibility_cutoff_xid = xmin;

					/* Check whether this tuple is already frozen or not */
					if (all_visible && *all_frozen &&
						heap_tuple_needs_eventual_freeze(tuple.t_data))
						*all_frozen = false;
				}
				break;

			case HEAPTUPLE_DEAD:
			case HEAPTUPLE_RECENTLY_DEAD:
			case HEAPTUPLE_INSERT_IN_PROGRESS:
			case HEAPTUPLE_DELETE_IN_PROGRESS:
				{
					all_visible = false;
					*all_frozen = false;
					break;
				}
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}
	}							/* scan along page */

	return all_visible;
}

/*
 * Perform single or parallel lazy vacuum.
 */
static void
lazy_scan_heap(LVState *lvstate)
{
	int nworkers = 0;
	StringInfoData buf;
	LVRelStats *vacrelstats = lvstate->vacrelstats;
	PGRUsage	ru0;

	if ((lvstate->options.flags & VACOPT_PARALLEL) != 0)
		nworkers = plan_lazy_vacuum_workers(RelationGetRelid(lvstate->relation),
											lvstate->options.nworkers);

	if (nworkers > 0)
	{
		/*
		 * Attempt to launch parallel worker and prepare to participate
		 * parallel vacuum as a worker.
		 */
		lazy_vacuum_begin_parallel(lvstate, nworkers);
	}
	else
	{
		/* Prepare dead tuple space for the single lazy scan heap */
		lazy_space_alloc(lvstate, RelationGetNumberOfBlocks(lvstate->relation));
	}

	pg_rusage_init(&ru0);

	/* Do the actual lazy vacuum */
	do_lazy_scan_heap(lvstate);

	if (nworkers > 0)
	{
		/*
		 * Finish paralell lazy vacuum. Collect lazy vacuum statistics
		 * from workers before end use of parallelism, and update relation
		 * and indexes statistics.
		 */
		lazy_vacuum_end_parallel(lvstate, true);
	}

	/*
	 * This is pretty messy, but we split it up so that we can skip emitting
	 * individual parts of the message when not applicable.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 _("%.0f dead row versions cannot be removed yet, oldest xmin: %u\n"),
					 vacrelstats->new_dead_tuples, OldestXmin);
	appendStringInfo(&buf, _("There were %.0f unused item pointers.\n"),
					 vacrelstats->unused_tuples);
	appendStringInfo(&buf, ngettext("Skipped %u page due to buffer pins, ",
									"Skipped %u pages due to buffer pins, ",
									vacrelstats->pinskipped_pages),
					 vacrelstats->pinskipped_pages);
	appendStringInfo(&buf, ngettext("%u frozen page.\n",
									"%u frozen pages.\n",
									vacrelstats->frozenskipped_pages),
					 vacrelstats->frozenskipped_pages);
	appendStringInfo(&buf, ngettext("%u page is entirely empty.\n",
									"%u pages are entirely empty.\n",
									vacrelstats->empty_pages),
					 vacrelstats->empty_pages);
	appendStringInfo(&buf, _("%s."), pg_rusage_show(&ru0));

	ereport(elevel,
			(errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u out of %u pages",
					RelationGetRelationName(lvstate->relation),
					vacrelstats->tuples_deleted, vacrelstats->num_tuples,
					vacrelstats->scanned_pages, vacrelstats->rel_pages),
			 errdetail_internal("%s", buf.data)));
	pfree(buf.data);
}

/*
 * Create parallel context, and launch workers for lazy vacuum.
 * Also this function contructs the leader's lvstate.
 */
static void
lazy_vacuum_begin_parallel(LVState *lvstate, int request)
{
	ParallelContext *pcxt;
	Size			estshared,
					estvacstats,
					estindstats,
					estdt,
					estworker;
	LVRelStats		*vacrelstats = lvstate->vacrelstats;
	LVShared		*lvshared;
	int			querylen;
	int 		keys = 0;
	char		*sharedquery;
	long	 	maxtuples;
	int			nparticipants = request + 1;
	int i;

	EnterParallelMode();
	pcxt = CreateParallelContext("postgres", "lazy_parallel_vacuum_main",
								 request, true);
	lvstate->pcxt = pcxt;

	/* Calculate maximum dead tuples we store */
	maxtuples = lazy_get_max_dead_tuples(vacrelstats,
										 RelationGetNumberOfBlocks(lvstate->relation));

	/* Estimate size for shared state -- VACUUM_KEY_SHARED */
	estshared = MAXALIGN(sizeof(LVShared));
	shm_toc_estimate_chunk(&pcxt->estimator, estshared);
	keys++;

	/* Estimate size for vacuum statistics for only workers -- VACUUM_KEY_VACUUM_STATS */
	estvacstats = MAXALIGN(mul_size(sizeof(LVRelStats), request));
	shm_toc_estimate_chunk(&pcxt->estimator, estvacstats);
	keys++;

	/* Estimate size for parallel worker status including the leader -- VACUUM_KEY_WORKERS */
	estworker = MAXALIGN(SizeOfLVWorkerState +
						 mul_size(sizeof(VacuumWorker), nparticipants));
	shm_toc_estimate_chunk(&pcxt->estimator, estworker);
	keys++;

	/* We have to dead tuple information only when the table has indexes */
	if (lvstate->nindexes > 0)
	{
		/* Estimate size for index statistics -- VACUUM_KEY_INDEX_STATS */
		estindstats = MAXALIGN(SizeOfLVIndStats +
							   mul_size(sizeof(IndexStats), lvstate->nindexes));
		shm_toc_estimate_chunk(&pcxt->estimator, estindstats);
		keys++;

		/* Estimate size for dead tuple control -- VACUUM_KEY_DEAD_TUPLES */
		estdt = MAXALIGN(SizeOfLVTidMap +
						 mul_size(sizeof(ItemPointerData), maxtuples));
		shm_toc_estimate_chunk(&pcxt->estimator, estdt);
		keys++;
	}

	shm_toc_estimate_keys(&pcxt->estimator, keys);

	/* Finally, estimate VACUUM_KEY_QUERY_TEXT space */
	querylen = strlen(debug_query_string);
	shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	InitializeParallelDSM(pcxt);

	/*
	 * Initialize dynamic shared memory for parallel lazy vacuum. We store
	 * relevant informations of parallel heap scanning, dead tuple array
	 * and vacuum statistics for each worker and some parameters for lazy vacuum.
	 */
	lvshared = shm_toc_allocate(pcxt->toc, estshared);
	lvshared->relid = lvstate->relid;
	lvshared->aggressive = lvstate->aggressive;
	lvshared->options = lvstate->options;
	lvshared->oldestXmin = OldestXmin;
	lvshared->freezeLimit = FreezeLimit;
	lvshared->multiXactCutoff = MultiXactCutoff;
	lvshared->elevel = elevel;
	lvshared->max_dead_tuples_per_worker = maxtuples / nparticipants;
	heap_parallelscan_initialize(&lvshared->heapdesc, lvstate->relation, SnapshotAny);
	shm_toc_insert(pcxt->toc, VACUUM_KEY_SHARED, lvshared);
	lvstate->lvshared = lvshared;

	/* Prepare vacuum relation statistics */
	vacrelstats = (LVRelStats *) shm_toc_allocate(pcxt->toc, estvacstats);
	for (i = 0; i < request; i++)
		memcpy(&vacrelstats[i], lvstate->vacrelstats, sizeof(LVRelStats));
	shm_toc_insert(pcxt->toc, VACUUM_KEY_VACUUM_STATS, vacrelstats);

	/* Prepare worker status */
	WorkerState = (LVWorkerState *) shm_toc_allocate(pcxt->toc, estworker);
	ConditionVariableInit(&WorkerState->cv);
	LWLockInitialize(&WorkerState->vacuumlock, LWTRANCHE_PARALLEL_VACUUM);
	WorkerState->nparticipantvacuum = nparticipants;
	for (i = 0; i < nparticipants; i++)
	{
		VacuumWorker *worker = &(WorkerState->workers[i]);

		worker->pid = InvalidPid;
		worker->state = VACSTATE_INVALID;	/* initial state */
		SpinLockInit(&worker->mutex);
	}
	shm_toc_insert(pcxt->toc, VACUUM_KEY_WORKERS, WorkerState);

	/* Prepare index statistics and deadtuple space if the table has index */
	if (lvstate->nindexes > 0)
	{
		LVIndStats	*indstats;
		LVTidMap	*dead_tuples;

		/* Prepare Index statistics */
		indstats = shm_toc_allocate(pcxt->toc, estindstats);
		indstats->nindexes = lvstate->nindexes;
		indstats->nprocessed = 0;
		MemSet(indstats->stats, 0, sizeof(IndexStats) * indstats->nindexes);
		SpinLockInit(&indstats->mutex);
		shm_toc_insert(pcxt->toc, VACUUM_KEY_INDEX_STATS, indstats);
		lvstate->indstats = indstats;

		/* Prepare shared dead tuples space */
		dead_tuples = (LVTidMap *) shm_toc_allocate(pcxt->toc, estdt);
		dead_tuples->max_items = maxtuples;
		dead_tuples->num_items = 0;
		dead_tuples->item_idx = 0;
		dead_tuples->shared = true;
		SpinLockInit(&dead_tuples->mutex);
		shm_toc_insert(pcxt->toc, VACUUM_KEY_DEAD_TUPLES, dead_tuples);
		lvstate->dead_tuples = dead_tuples;
	}

	/* Store query string for workers */
	sharedquery = shm_toc_allocate(pcxt->toc, querylen + 1);
	memcpy(sharedquery, debug_query_string, querylen + 1);
	shm_toc_insert(pcxt->toc, VACUUM_KEY_QUERY_TEXT, sharedquery);

	/* Set master pid to itself */
	pgstat_report_leader_pid(MyProcPid);

	/* Launch workers */
	LaunchParallelWorkers(pcxt);

	if (pcxt->nworkers_launched == 0)
	{
		lazy_vacuum_end_parallel(lvstate, false);
		return;
	}

	/* Update the number of workers participating */
	WorkerState->nparticipantvacuum_launched = pcxt->nworkers_launched + 1;

	lazy_wait_for_vacuum_workers_attach(pcxt);

	/* Participant as a worker */
	vacuum_worker_attach();

	return;
}

/*
 * Wait for all workers finish and exit parallel vacuum. If update_stats
 * is true, gather vacuum statistics of all parallel workers and
 * update index statistics.
 */
static void
lazy_vacuum_end_parallel(LVState *lvstate, bool update_stats)
{
	IndexStats *indstats = NULL;

	Assert(IsVacuumLeader());

	/* Wait for workers finished vacuum */
	WaitForParallelWorkersToFinish(lvstate->pcxt);

	if (update_stats)
	{
		LVRelStats *vacrelstats = lvstate->vacrelstats;

		/*
		 * Gather vacuum statistics of all workers. Since this function access
		 * to shared information we need do before destroy parallel context.
		 */
		lazy_gather_worker_stats(lvstate, vacrelstats);

		/* Now we can compute the new value for pg_class.reltuples */
		vacrelstats->new_rel_tuples = vac_estimate_reltuples(lvstate->relation,
															 vacrelstats->rel_pages,
															 vacrelstats->tupcount_pages,
															 vacrelstats->new_live_tuples);

		/* Copy new index stats to local memory */
		indstats = (IndexStats *) palloc(sizeof(IndexStats) * lvstate->nindexes);
		memcpy(indstats, lvstate->indstats, sizeof(IndexStats) * lvstate->nindexes);
	}

	/* Unregister worker callback before destroy shmem context */
	if (IsInParallelVacuum())
		cancel_before_shmem_exit(vacuum_worker_onexit, (Datum) 0);

	DestroyParallelContext(lvstate->pcxt);
	ExitParallelMode();

	/* After exit parallel mode, update index statistics */
	if (update_stats)
	{
		int i;

		for (i = 0; i < lvstate->nindexes; i++)
		{
			Relation	ind = lvstate->indRels[i];
			IndexStats *istat = (IndexStats *) &(indstats[i]);

			/* Update index statsistics */
			if (indstats->need_update)
				vac_update_relstats(ind,
									istat->num_pages,
									istat->num_tuples,
									0,
									false,
									InvalidTransactionId,
									InvalidMultiXactId,
									false);
		}
	}

	/* Reset shared fields */
	lvstate->indstats = NULL;
	lvstate->dead_tuples = NULL;
	WorkerState = NULL;
	MyVacuumWorker = NULL;
}

/*
 * lazy_gather_worker_stats() -- Gather vacuum statistics from workers
 */
static void
lazy_gather_worker_stats(LVState *lvstate, LVRelStats *vacrelstats)
{
	int	i;
	LVRelStats *lvstats_list;

	Assert(IsInParallelMode());
	lvstats_list = (LVRelStats *) shm_toc_lookup(lvstate->pcxt->toc,
												 VACUUM_KEY_VACUUM_STATS,
												 false);

	/* Gather each worker stats */
	for (i = 0; i < (WorkerState->nparticipantvacuum_launched - 1); i++)
	{
		LVRelStats *wstats = (LVRelStats*) ((char *) lvstats_list + sizeof(LVRelStats) * i);

		vacrelstats->scanned_pages += wstats->scanned_pages;
		vacrelstats->pinskipped_pages += wstats->pinskipped_pages;
		vacrelstats->frozenskipped_pages += wstats->frozenskipped_pages;
		vacrelstats->tupcount_pages += wstats->tupcount_pages;
		vacrelstats->empty_pages += wstats->empty_pages;
		vacrelstats->new_live_tuples += wstats->new_live_tuples;
		vacrelstats->new_dead_tuples += wstats->new_dead_tuples;
		vacrelstats->pages_removed += wstats->pages_removed;
		vacrelstats->tuples_deleted += wstats->tuples_deleted;
		vacrelstats->nonempty_pages += wstats->nonempty_pages;
		vacrelstats->unused_tuples += wstats->unused_tuples;
		vacrelstats->num_tuples += wstats->num_tuples;
	}

	/* all vacuum workers have same value of rel_pages */
	vacrelstats->rel_pages = lvstats_list->rel_pages;
}

/*
 * Return the number of maximum dead tuples can be stored according
 * to vac_work_mem.
 */
static long
lazy_get_max_dead_tuples(LVRelStats *vacrelstats, BlockNumber relblocks)
{
	long maxtuples;
	int	vac_work_mem = IsAutoVacuumWorkerProcess() &&
		autovacuum_work_mem != -1 ?
		autovacuum_work_mem : maintenance_work_mem;

	if (vacrelstats->hasindex)
	{
		maxtuples = (vac_work_mem * 1024L) / sizeof(ItemPointerData);
		maxtuples = Min(maxtuples, INT_MAX);
		maxtuples = Min(maxtuples, MaxAllocSize / sizeof(ItemPointerData));

		/* curious coding here to ensure the multiplication can't overflow */
		if ((BlockNumber) (maxtuples / LAZY_ALLOC_TUPLES) > relblocks)
			maxtuples = relblocks * LAZY_ALLOC_TUPLES;

		/* stay sane if small maintenance_work_mem */
		maxtuples = Max(maxtuples, MaxHeapTuplesPerPage);
	}
	else
		maxtuples = MaxHeapTuplesPerPage;

	return maxtuples;
}

/*
 * Perform work within a launched parallel process.
 */
void
lazy_parallel_vacuum_main(dsm_segment *seg, shm_toc *toc)
{
	LVState		*lvstate = (LVState *) palloc(sizeof(LVState));
	LVShared	*lvshared;
	LVRelStats	*vacrelstats;
	char		*sharedquery;

	ereport(DEBUG1,
			(errmsg("starting parallel lazy vacuum worker")));

	/* Look up worker state and attach */
	WorkerState = (LVWorkerState *) shm_toc_lookup(toc, VACUUM_KEY_WORKERS, false);
	vacuum_worker_attach();

	/* Set debug_query_string for individual workers first */
	sharedquery = shm_toc_lookup(toc, VACUUM_KEY_QUERY_TEXT, false);
	debug_query_string = sharedquery;
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Set shared state */
	lvshared = (LVShared *) shm_toc_lookup(toc, VACUUM_KEY_SHARED, false);

	/* Set individual vacuum statistics */
	vacrelstats = (LVRelStats *) shm_toc_lookup(toc, VACUUM_KEY_VACUUM_STATS, false);
	lvstate->relid = lvshared->relid;
	lvstate->aggressive = lvshared->aggressive;
	lvstate->options = lvshared->options;
	lvstate->vacrelstats = vacrelstats + ParallelWorkerNumber;
	lvstate->relation = relation_open(lvstate->relid, ShareUpdateExclusiveLock);
	vac_open_indexes(lvstate->relation, RowExclusiveLock, &lvstate->nindexes,
					 &lvstate->indRels);
	lvstate->lvshared = lvshared;

	/* Set the space for both index statistics and dead tuples if table with index */
	if (lvstate->nindexes > 0)
	{
		LVTidMap		*dead_tuples;
		LVIndStats		*indstats;

		/* Attach shared dead tuples */
		dead_tuples = (LVTidMap *) shm_toc_lookup(toc, VACUUM_KEY_DEAD_TUPLES, false);
		lvstate->dead_tuples = dead_tuples;

		/* Attach Shared index stats */
		indstats = (LVIndStats *) shm_toc_lookup(toc, VACUUM_KEY_INDEX_STATS, false);
		lvstate->indstats = indstats;
	}
	else
	{
		/* Dead tuple are stored into the local memory if no indexes */
		lazy_space_alloc(lvstate, RelationGetNumberOfBlocks(lvstate->relation));
		lvstate->indstats = NULL;
	}

	/* Restore parameters being used for lazy vacuum */
	OldestXmin = lvshared->oldestXmin;
	FreezeLimit = lvshared->freezeLimit;
	MultiXactCutoff = lvshared->multiXactCutoff;
	elevel = lvshared->elevel;

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  lvshared->relid);

	/* Do lazy vacuum */
	do_lazy_scan_heap(lvstate);

	pgstat_progress_end_command();

	vac_close_indexes(lvstate->nindexes, lvstate->indRels, RowExclusiveLock);
	heap_close(lvstate->relation, ShareUpdateExclusiveLock);
}

/*
 * Return the block number we need to scan next, or InvalidBlockNumber if scan
 * is done.
 *
 * Except when aggressive is set, we want to skip pages that are
 * all-visible according to the visibility map, but only when we can skip
 * at least SKIP_PAGES_THRESHOLD consecutive pages.	 Since we're reading
 * sequentially, the OS should be doing readahead for us, so there's no
 * gain in skipping a page now and then; that's likely to disable
 * readahead and so be counterproductive. Also, skipping even a single
 * page means that we can't update relfrozenxid, so we only want to do it
 * if we can skip a goodly number of pages.
 *
 * When aggressive is set, we can't skip pages just because they are
 * all-visible, but we can still skip pages that are all-frozen, since
 * such pages do not need freezing and do not affect the value that we can
 * safely set for relfrozenxid or relminmxid.
 *
 * Before entering the main loop, establish the invariant that
 * next_unskippable_block is the next block number >= blkno that we can't
 * skip based on the visibility map, either all-visible for a regular scan
 * or all-frozen for an aggressive scan.  We set it to nblocks if there's
 * no such block.  We also set up the skipping_blocks flag correctly at
 * this stage.
 *
 * In not parallel mode, before entering the main loop, establish the
 * invariant that next_unskippable_block is the next block number >= blkno
 * that's not we can't skip based on the visibility map, either all-visible
 * for a regular scan or all-frozen for an aggressive scan.	 We set it to
 * nblocks if there's no such block.  We also set up the skipping_blocks
 * flag correctly at this stage.
 *
 * In parallel mode, pstate is not NULL. We scan heap pages
 * using parallel heap scan description. Each worker calls heap_parallelscan_nextpage()
 * in order to exclusively get	block number we need to scan at next.
 * If given block is all-visible according to visibility map, we skip to
 * scan this block immediately unlike not parallel lazy scan.
 *
 * Note: The value returned by visibilitymap_get_status could be slightly
 * out-of-date, since we make this test before reading the corresponding
 * heap page or locking the buffer.	 This is OK.  If we mistakenly think
 * that the page is all-visible or all-frozen when in fact the flag's just
 * been cleared, we might fail to vacuum the page.	It's easy to see that
 * skipping a page when aggressive is not set is not a very big deal; we
 * might leave some dead tuples lying around, but the next vacuum will
 * find them.  But even when aggressive *is* set, it's still OK if we miss
 * a page whose all-frozen marking has just been cleared.  Any new XIDs
 * just added to that page are necessarily newer than the GlobalXmin we
 * Computed, so they'll have no effect on the value to which we can safely
 * set relfrozenxid.  A similar argument applies for MXIDs and relminmxid.
 *
 * We will scan the table's last page, at least to the extent of
 * determining whether it has tuples or not, even if it should be skipped
 * according to the above rules; except when we've already determined that
 * it's not worth trying to truncate the table.	 This avoids having
 * lazy_truncate_heap() take access-exclusive lock on the table to attempt
 * a truncation that just fails immediately because there are tuples in
 * the last page.  This is worth avoiding mainly because such a lock must
 * be replayed on any hot standby, where it can be disruptive.
 */
static BlockNumber
lazy_scan_get_nextpage(LVScanDesc lvscan, LVRelStats *vacrelstats,
					   bool *all_visible_according_to_vm_p, Buffer *vmbuffer_p)
{

	BlockNumber blkno;

	if (lvscan->lv_heapscan)
	{
		/*
		 * In parallel lazy vacuum since it's hard to know how many consecutive
		 * all-visible pages exits on table we skip to scan the heap page immediately.
		 * if it is all-visible page.
		 */
		while ((blkno = heap_parallelscan_nextpage(lvscan->lv_heapscan)) != InvalidBlockNumber)
		{
			*all_visible_according_to_vm_p = false;
			vacuum_delay_point();

			/* Consider to skip scan page according visibility map */
			if (!lvscan->disable_page_skipping &&
				!FORCE_CHECK_PAGE(vacrelstats->rel_pages, blkno, vacrelstats))
			{
				uint8		vmstatus;

				vmstatus = visibilitymap_get_status(lvscan->lv_rel, blkno, vmbuffer_p);

				if (lvscan->aggressive)
				{
					if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) != 0)
					{
						vacrelstats->frozenskipped_pages++;
						continue;
					}
					else if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0)
						*all_visible_according_to_vm_p = true;
				}
				else
				{
					if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0)
					{
						if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) != 0)
							vacrelstats->frozenskipped_pages++;
						continue;
					}
				}
			}

			/* We need to scan current blkno, break */
			break;
		}
	}
	else
	{
		bool skipping_blocks = false;

		/* Initialize lv_nextunskippable_page if needed */
		if (lvscan->lv_cblock == 0 && !lvscan->disable_page_skipping)
		{
			while (lvscan->lv_next_unskippable_block < lvscan->lv_nblocks)
			{
				uint8		vmstatus;

				vmstatus = visibilitymap_get_status(lvscan->lv_rel,
													lvscan->lv_next_unskippable_block,
													vmbuffer_p);
				if (lvscan->aggressive)
				{
					if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) == 0)
						break;
				}
				else
				{
					if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) == 0)
						break;
				}
				vacuum_delay_point();
				lvscan->lv_next_unskippable_block++;
			}

			if (lvscan->lv_next_unskippable_block >= SKIP_PAGES_THRESHOLD)
				skipping_blocks = true;
			else
				skipping_blocks = false;
		}

		/* Decide the block number we need to scan */
		for (blkno = lvscan->lv_cblock; blkno < lvscan->lv_nblocks; blkno++)
		{
			if (blkno == lvscan->lv_next_unskippable_block)
			{
				/* Time to advance next_unskippable_block */
				lvscan->lv_next_unskippable_block++;
				if (!lvscan->disable_page_skipping)
				{
					while (lvscan->lv_next_unskippable_block < lvscan->lv_nblocks)
					{
						uint8		vmstatus;

						vmstatus = visibilitymap_get_status(lvscan->lv_rel,
															lvscan->lv_next_unskippable_block,
															vmbuffer_p);
						if (lvscan->aggressive)
						{
							if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) == 0)
								break;
						}
						else
						{
							if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) == 0)
								break;
						}
						vacuum_delay_point();
						lvscan->lv_next_unskippable_block++;
					}
				}

				/*
				 * We know we can't skip the current block.	 But set up
				 * skipping_all_visible_blocks to do the right thing at the
				 * following blocks.
				 */
				if (lvscan->lv_next_unskippable_block - blkno > SKIP_PAGES_THRESHOLD)
					skipping_blocks = true;
				else
					skipping_blocks = false;

				/*
				 * Normally, the fact that we can't skip this block must mean that
				 * it's not all-visible.  But in an aggressive vacuum we know only
				 * that it's not all-frozen, so it might still be all-visible.
				 */
				if (lvscan->aggressive && VM_ALL_VISIBLE(lvscan->lv_rel, blkno, vmbuffer_p))
					*all_visible_according_to_vm_p = true;

				/* Found out that next unskippable block number */
				break;
			}
			else
			{
				/*
				 * The current block is potentially skippable; if we've seen a
				 * long enough run of skippable blocks to justify skipping it, and
				 * we're not forced to check it, then go ahead and skip.
				 * Otherwise, the page must be at least all-visible if not
				 * all-frozen, so we can set *all_visible_according_to_vm_p = true.
				 */
				if (skipping_blocks &&
					!FORCE_CHECK_PAGE(vacrelstats->rel_pages, blkno, vacrelstats))
				{
					/*
					 * Tricky, tricky.	If this is in aggressive vacuum, the page
					 * must have been all-frozen at the time we checked whether it
					 * was skippable, but it might not be any more.	 We must be
					 * careful to count it as a skipped all-frozen page in that
					 * case, or else we'll think we can't update relfrozenxid and
					 * relminmxid.	If it's not an aggressive vacuum, we don't
					 * know whether it was all-frozen, so we have to recheck; but
					 * in this case an approximate answer is OK.
					 */
					if (lvscan->aggressive || VM_ALL_FROZEN(lvscan->lv_rel, blkno, vmbuffer_p))
						vacrelstats->frozenskipped_pages++;
					continue;
				}

				*all_visible_according_to_vm_p = true;

				/* We need to scan current blkno, break */
				break;
			}
		} /* for */

		/* Advance the current block number for the next scan */
		lvscan->lv_cblock = blkno + 1;
	}

	return (blkno == lvscan->lv_nblocks) ? InvalidBlockNumber : blkno;
}

/*
 * lazy_prepare_to_next_state - prepartion to enter the next state
 *
 * For vacuum worker, we wait for its status to be changed by the leader.
 * For vacuum leader, we wait for the state of all workers become the same
 * state of the leader state. After that do the appropriate preparation work
 * for the next state and change thier state to the next_state.
 */
static void
lazy_prepare_to_next_state(LVState *lvstate, int next_state)
{
	/* If not in parallel vacuum, nothing to prepare */
	if (!IsInParallelVacuum())
		return;

	while (true)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Vacuum workers wait until its state has changed by leader process.
		 */
		if (IsVacuumWorker())
		{
			VacWorkerState mystate = lazy_get_my_worker_state();

			if (mystate == next_state)
				break;
			ereport(LOG, (errmsg("[%d] In prepare, check my status but not ready %s. sleep...",
								 ParallelWorkerNumber, TOSTR(next_state))));
		}
		/*
		 * For the vacuum leader, check the all worker state and do the
		 * preparation work.
		 */
		else if (lazy_check_worker_state())
		{
			Assert(IsVacuumLeader());

			/*
			 * Before enter the next state do the prepare work. Note that since all
			 * workers are waiting for ready the leader doesn't need to acquire any
			 * lock to modify shared dead tuples.
			 */
			switch (next_state)
			{
				case VACSTATE_SCAN:
					{
						/* Clear dead tuples */
						MemSet(lvstate->dead_tuples->itemptrs, 0,
							   sizeof(ItemPointerData) * lvstate->dead_tuples->max_items);
						lvstate->dead_tuples->num_items = 0;
						break;
					}
				case VACSTATE_VACUUM_INDEX:
					{
						LVTidMap	*dead_tuples = lvstate->dead_tuples;

						/* Sort dead tuple array */
						qsort((void *) dead_tuples->itemptrs,
							  dead_tuples->num_items,
							  sizeof(ItemPointerData), vac_cmp_itemptr);
						break;
					}
				case VACSTATE_VACUUM_HEAP:
					{
						int i;
						LVIndStats *indstats = lvstate->indstats;

						/*
						 * It can happen that a parallel worker failed to vacuum
						 * an assigned index and exits. Even in the case the leader
						 * doesn't end with an error. Therefore we must not skip to
						 * vacuum the index. All garbage on indexes must be reclaim
						 * before vacuuming the heap.
						 */
						for (i = 0; i < indstats->nindexes; i++)
						{
							if (!indstats->stats[i].done)
							{
								ereport(NOTICE,
										(errmsg("vacuum index \"%s\" again due to failed at the previous time",
												RelationGetRelationName(lvstate->indRels[i])),
										 errdetail("parallel worker failed to vacuum.")));

								/*
								 * Found a index that parallel worker failed to vacuum,
								 * remove index entries.
								 */
								lazy_vacuum_index(lvstate->indRels[i], &lvstate->indbulkstats[i],
												  lvstate->vacrelstats, lvstate->dead_tuples);
							}

							indstats->stats[i].done = false;
						}
						indstats->nprocessed = 0;
						break;
					}
				case VACSTATE_CLEANUP_INDEX:
					{
						int i;
						LVIndStats *indstats = lvstate->indstats;

						/*
						 * Similar to the VACSTATE_VACUUM_HEAP case, if cleanup a index
						 * by parallel worker failed the leader does here. There is an
						 * another reason to wait for all workers becoming the same state
						 * here: the parallel worker doesn't wait for the leader to attach
						 * to the worker slot at beginning, so this ensures that all workers
						 * doesn't complete lazy vacuum before the leader starts the work.
						 */
						for (i = 0; i < indstats->nindexes; i++)
						{
							if (!indstats->stats[i].done)
							{
								ereport(NOTICE,
										(errmsg("vacuum index \"%s\" again due to failed at the previous time",
												RelationGetRelationName(lvstate->indRels[i])),
										 errdetail("parallel worker failed to vacuum.")));

								/*
								 * Do post-vacuum cleanup. Update statistics for each index if not
								 * in parallel vacuum.
								 */
								lazy_vacuum_index(lvstate->indRels[i], &lvstate->indbulkstats[i],
												  lvstate->vacrelstats, lvstate->dead_tuples);
							}
							indstats->stats[i].done = false;
						}
						break;
					}
				case VACSTATE_INVALID:
				case VACSTATE_SCAN_FINISHED:
				case VACSTATE_VACUUM_INDEX_FINISHED:
				case VACSTATE_VACUUM_HEAP_FINISHED:
				case VACSTATE_BEFORE_CLEANUP_INDEX:
					elog(ERROR, "unexpected vacuum state %d", next_state);
					break;
				default:
					elog(ERROR, "invalid vacuum state %d", next_state);
			}

			/* Advance state to the VACUUM state */
			lazy_set_workers_state(next_state);

			/* Wake up workers */
			ConditionVariableBroadcast(&WorkerState->cv);
			break;
		}

		/* Sleep until next notification */
		ConditionVariableSleep(&WorkerState->cv, WAIT_EVENT_PARALLEL_VACUUM);
	}

	ConditionVariableCancelSleep();
}

/*
 * lv_beginscan() -- begin lazy vacuum heap scan
 *
 * In parallel vacuum, we use parallel heap scan, so initialize parallel
 * heap scan description.
 */
static LVScanDesc
lv_beginscan(Relation onerel, LVShared *lvshared, bool aggressive,
			 bool disable_page_skipping)
{
	LVScanDesc	lvscan = (LVScanDesc) palloc(sizeof(LVScanDescData));

	lvscan->lv_rel = onerel;
	lvscan->lv_nblocks = RelationGetNumberOfBlocks(onerel);

	/* Set scan options */
	lvscan->aggressive = aggressive;
	lvscan->disable_page_skipping = disable_page_skipping;

	if (lvshared)
	{
		/* For parallel lazy vacuum */
		Assert(!IsBootstrapProcessingMode());
		lvscan->lv_heapscan = heap_beginscan_parallel(onerel, &lvshared->heapdesc);
		heap_parallelscan_startblock_init(lvscan->lv_heapscan);
	}
	else
	{
		/* For single lazy vacuum */
		lvscan->lv_heapscan = NULL;
		lvscan->lv_cblock = 0;
		lvscan->lv_next_unskippable_block = 0;
	}

	return lvscan;
}

/*
 * lv_endscan() -- end lazy vacuum heap scan
 */
static void
lv_endscan(LVScanDesc lvscan)
{
	if (lvscan->lv_heapscan != NULL)
		heap_endscan(lvscan->lv_heapscan);
	pfree(lvscan);
}

/*
 * lazy_dead_tuples_is_full - is dead tuple space full?
 *
 * Return true if dead tuple space is full.
 */
static bool
lazy_dead_tuples_is_full(LVTidMap *dead_tuples)
{
	bool isfull;

	if (dead_tuples->shared)
		SpinLockAcquire(&(dead_tuples->mutex));

	isfull = ((dead_tuples->num_items > 0) &&
			  ((dead_tuples->max_items - dead_tuples->num_items) < MaxHeapTuplesPerPage));

	if (isfull)
		ereport(LOG, (errmsg("[%d] dead tuples : num %d max %d",
			 ParallelWorkerNumber,
							 dead_tuples->num_items, dead_tuples->max_items)));

	if (dead_tuples->shared)
		SpinLockRelease(&(dead_tuples->mutex));

	return isfull;
}

/*
 * lazy_get_dead_tuple_count
 *
 * Get the current number of dead tuples we are having.
 */
static int
lazy_get_dead_tuple_count(LVTidMap *dead_tuples)
{
	int num_items;

	if (dead_tuples->shared)
		SpinLockAcquire(&dead_tuples->mutex);

	num_items = dead_tuples->num_items;

	if (dead_tuples->shared)
		SpinLockRelease(&dead_tuples->mutex);

	return num_items;
}

/*
 * lazy_get_next_vacuum_page
 *
 * For vacuum heap pages, return the block number we need to vacuum from the
 * dead tuple space. Also we advance the index of dead tuple up to the next
 * block for the next search.
 *
 * NB: the dead_tuples must be sorted by TID order.
 */
static BlockNumber
lazy_get_next_vacuum_page(LVState *lvstate)
{
	LVTidMap	*dead_tuples = lvstate->dead_tuples;
	BlockNumber tblk;
	BlockNumber	next_tblk;


	if (!dead_tuples->shared)
	{
		if (dead_tuples->item_idx < dead_tuples->max_items)
			return InvalidBlockNumber;

		tblk = ItemPointerGetBlockNumber(&(dead_tuples->itemptrs[dead_tuples->item_idx]));
		dead_tuples->item_idx++;
		return tblk;
	}

	/*
	 * For parallel vacuum, need locks.
	 *
	 * XXX: The number of maximum tuple we need to advance is not a large
	 * number, up to MaxHeapTuplesPerPage. So we use spin lock here.
	 */
	if (dead_tuples->shared)
		SpinLockAcquire(&(dead_tuples->mutex));

	if (dead_tuples->item_idx < dead_tuples->max_items)
	{
		/* Reach to the end of dead tuples array */
		tblk = InvalidBlockNumber;
		goto done;
	}

	/* Advance the index up to the next block for the next search */
	tblk = next_tblk =
		ItemPointerGetBlockNumber(&dead_tuples->itemptrs[dead_tuples->item_idx]);
	while (tblk != InvalidBlockNumber && tblk == next_tblk)
	{
		tblk = next_tblk;
		dead_tuples->item_idx++;
		next_tblk = ItemPointerGetBlockNumber(&dead_tuples->itemptrs[dead_tuples->item_idx]);
	}

done:
	if (dead_tuples->shared)
		SpinLockRelease(&(dead_tuples->mutex));

	return tblk;
}

/*
 * Vacuum all indexes. In parallel vacuum, each workers take indexes
 * one by one. Also after vacuumed index they mark it as done. This marking
 * is necessary to guarantee that all indexes are vacuumed based on
 * the current collected dead tuples. The leader process continues to
 * vacuum even if any indexes is not vacuumed completely due to failure of
 * parallel worker for whatever reason. The mark will be checked before entering
 * the next state.
 */
static void
lazy_vacuum_all_indexes(LVState *lvstate)
{
	int idx;
	int nprocessed = 0;
	LVIndStats *sharedstats = lvstate->indstats;

	/* Take the index number we vacuum */
	if (IsInParallelVacuum())
	{
		Assert(sharedstats != NULL);
		SpinLockAcquire(&(sharedstats->mutex));
		idx = (sharedstats->nprocessed)++;
		SpinLockRelease(&sharedstats->mutex);
	}
	else
		idx = nprocessed++;

	while (idx  < lvstate->nindexes)
	{
		elog(NOTICE, "[%d] %s vacuum index %d",
			 ParallelWorkerNumber,
			 IsInParallelMode() ? "paralell" : "",
			 idx);

		/* Remove index entries */
		lazy_vacuum_index(lvstate->indRels[idx], &lvstate->indbulkstats[idx],
						  lvstate->vacrelstats, lvstate->dead_tuples);

		/* Take the next index number we vacuum */
		if (IsInParallelVacuum())
		{
			SpinLockAcquire(&(sharedstats->mutex));
			sharedstats->stats[idx].done = true;
			idx = (sharedstats->nprocessed)++;
			SpinLockRelease(&sharedstats->mutex);
		}
		else
			idx = nprocessed++;
	}
}

/*
 * Cleanup all indexes.
 * This function is similar to lazy_vacuum_all_indexes.
 */
static void
lazy_cleanup_all_indexes(LVState *lvstate)
{
	int idx;
	int nprocessed = 0;
	LVIndStats *sharedstats = lvstate->indstats;

	if (IsInParallelVacuum())
	{
		Assert(sharedstats != NULL);
		SpinLockAcquire(&(sharedstats->mutex));
		idx = (sharedstats->nprocessed)++;
		SpinLockRelease(&sharedstats->mutex);
	}
	else
		idx = nprocessed++;

	while (idx  < lvstate->nindexes)
	{
		elog(NOTICE, "[%d] cleanup index %d", ParallelWorkerNumber, idx);

		/*
		 * Do post-vacuum cleanup. Update statistics for each index if not
		 * in parallel vacuum.
		 */
		lazy_cleanup_index(lvstate->indRels[idx],
						   lvstate->indbulkstats[idx],
						   lvstate->vacrelstats,
						   (lvstate->indstats) ? &(sharedstats->stats[idx]) : NULL);

		if (IsInParallelVacuum())
		{
			SpinLockAcquire(&(sharedstats->mutex));
			sharedstats->stats[idx].done = true;
			idx = (sharedstats->nprocessed)++;
			SpinLockRelease(&sharedstats->mutex);
		}
		else
			idx = nprocessed++;
	}
}

/*
 * Clean up function for parallel vacuum worker
 */
static void
vacuum_worker_onexit(int code, Datum arg)
{
	if (IsInParallelMode() && MyVacuumWorker)
		vacuum_worker_detach();
}

/*
 * Deatch the worker and cleanup worker information.
 */
static void
vacuum_worker_detach(void)
{
	SpinLockAcquire(&MyVacuumWorker->mutex);
	MyVacuumWorker->state = VACSTATE_INVALID;
	MyVacuumWorker->pid = InvalidPid;
	SpinLockRelease(&MyVacuumWorker->mutex);

	ereport(LOG, (errmsg("[%d] dattached", ParallelWorkerNumber)));

	MyVacuumWorker = NULL;
}

/*
 * Attach to a worker slot. Worker slots are used sequentialy from the
 * beginning of the array.
 */
static void
vacuum_worker_attach(void)
{
	int		i;

	Assert(IsInParallelVacuum());

	LWLockAcquire(&WorkerState->vacuumlock, LW_EXCLUSIVE);

	for (i = 0; i < WorkerState->nparticipantvacuum; i++)
	{
		VacuumWorker *w = &WorkerState->workers[i];

		/* Break if found a unused slot */
		if (w->pid == InvalidPid)
			break;
		else
			ereport(LOG, (errmsg("[%d] SLOT(%d) is ??? by PID %d",
								 ParallelWorkerNumber, i,
								 w->pid)));
	}

	Assert(i < WorkerState->nparticipantvacuum);

	ereport(LOG, (errmsg("[%d] attach to slot %d of %d slots",
						 ParallelWorkerNumber, i, WorkerState->nparticipantvacuum)));

	MyVacuumWorker = &WorkerState->workers[i];
	MyVacuumWorker->pid = MyProcPid;
	MyVacuumWorker->state = VACSTATE_SCAN;	/* first state */

	LWLockRelease(&WorkerState->vacuumlock);

	before_shmem_exit(vacuum_worker_onexit, (Datum) 0);
}

/*
 * lazy_get_my_worker_state - get my current state
 */
static VacWorkerState
lazy_get_my_worker_state(void)
{
	VacWorkerState state;

	Assert(IsInParallelVacuum());

	SpinLockAcquire(&MyVacuumWorker->mutex);
	state = MyVacuumWorker->state;
	SpinLockRelease(&MyVacuumWorker->mutex);

	return state;
}

/*
 * lazy_set_my_worker_state - set new state to my state
 */
static void
lazy_set_my_worker_state(VacWorkerState new_state)
{
	if (!IsInParallelVacuum())
		return;

	ereport(LOG, (errmsg("[%d] set my state from %s to %s",
						 ParallelWorkerNumber, TOSTR(MyVacuumWorker->state), TOSTR(new_state))));

	SpinLockAcquire(&MyVacuumWorker->mutex);
	MyVacuumWorker->state = new_state;
	SpinLockRelease(&MyVacuumWorker->mutex);

	if (IsVacuumWorker())
		ConditionVariableBroadcast(&WorkerState->cv);
}

/*
 * lazy_check_worker_state - all workers are the same status as the leader?
 *
 * Check if the state of the all parallel workers are the same as the leader
 * state. Return true if all status are the same.
 */
static bool
lazy_check_worker_state(void)
{
	VacWorkerState expected_state = MyVacuumWorker->state;
	int i;
	bool ret = true;

	Assert(IsVacuumLeader());

	ereport(LOG, (errmsg("[%d] checking all %d worker status is %s...",
						 ParallelWorkerNumber, WorkerState->nparticipantvacuum_launched,
						 TOSTR(MyVacuumWorker->state))));

	for (i = 0; i < WorkerState->nparticipantvacuum_launched; i++)
	{
		VacuumWorker *w = &WorkerState->workers[i];
		pid_t	pid;
		int		state;

		SpinLockAcquire(&w->mutex);
		pid = w->pid;
		state = w->state;
		SpinLockRelease(&w->mutex);

		/* Skip leader itself */
		if (pid == MyProcPid)
		{
			ereport(LOG, (errmsg("[%d]     slot[%d] is myself skip",
								 ParallelWorkerNumber, i)));
			continue;
		}

		/* Skip unused slot */
		if (pid == InvalidPid)
			continue;

		/* Check used slot state */
		if (state != expected_state)
		{
			ereport(LOG, (errmsg("[%d]     --> failed, slot %d is not ready. status is %s",
								 ParallelWorkerNumber, i, TOSTR(state))));
			ret = false;
			break;
		}

		ereport(LOG, (errmsg("[%d]     slot[%d] state = %s, expected = %s ... OK",
							 ParallelWorkerNumber, i, TOSTR(state), TOSTR(expected_state))));
	}

	if (ret)
		ereport(LOG, (errmsg("[%d]     --> Successed", ParallelWorkerNumber)));

	return ret;
}

/*
 * lazy_set_workers_state - set new state to the all parallel workers
 */
static void
lazy_set_workers_state(VacWorkerState new_state)
{
	int i;

	Assert(!IsParallelWorker());

	ereport(LOG, (errmsg("[%d] setting all worker state to %s",
						 ParallelWorkerNumber, TOSTR(new_state))));

	for (i = 0; i < WorkerState->nparticipantvacuum_launched; i++)
	{
		VacuumWorker *w = &WorkerState->workers[i];

		SpinLockAcquire(&w->mutex);
		if (w->pid != InvalidPid)
			w->state = new_state;
		SpinLockRelease(&w->mutex);
	}
}

/*
 * Wait for a parallel vacuum worker to start up and attach to both the
 * shmem context and a worker slot.
 *
 * This is nedded for checking the state of all launched parallel workers.
 */
static void
lazy_wait_for_vacuum_workers_attach(ParallelContext *pcxt)
{
	int i;

	/* Wait for workers to attach to the shmem context */
	WaitForParallelWorkersToAttach(pcxt);

	/* Wait for workers to attach to the worker slot */
	for (i = 0; i < pcxt->nworkers_launched; i++)
	{
		VacuumWorker	*worker = &WorkerState->workers[i];
		int rc;

		for (;;)
		{
			pid_t pid;

			CHECK_FOR_INTERRUPTS();

			SpinLockAcquire(&worker->mutex);
			pid = worker->pid;
			SpinLockRelease(&worker->mutex);

			/* Check if the slot is attached */
			if (pid != InvalidPid)
				break;

			rc = WaitLatch(MyLatch,
						   WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   10L, WAIT_EVENT_BGWORKER_STARTUP);

			if (rc & WL_POSTMASTER_DEATH)
				return;

			ResetLatch(MyLatch);
		}
	}
}
