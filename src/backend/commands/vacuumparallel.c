/*-------------------------------------------------------------------------
 *
 * vacuumparallel.c
 *	  Support routines for parallel vacuum execution.
 *
 * This file contains routines that are intended to support setting up, using,
 * and tearing down a ParallelVacuumState. ParallelVacuumState contains shared
 * information as well as the memory space for storing dead items allocated in
 * the DSA area. We launch
 *
 * In a parallel vacuum, we perform table scan, index bulk-deletion, index
 * cleanup, or all of them with parallel worker processes depending on the
 * number of parallel workers required for each phase. So different numbers of
 * workers might be required for the table scanning and index processing.
 * We launch parallel worker processes at the start of a phase, and once we
 * complete all work in the phase, parallel workers exit. Each time we process
 * table or indexes in parallel, the parallel context is re-initialized so that
 * the same DSM can be used for multiple passes of each phase.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuumparallel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/amapi.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/instrument.h"
#include "optimizer/paths.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/*
 * DSM keys for parallel vacuum.  Unlike other parallel execution code, since
 * we don't need to worry about DSM keys conflicting with plan_node_id we can
 * use small integers.
 */
#define PARALLEL_VACUUM_KEY_SHARED			1
#define PARALLEL_VACUUM_KEY_QUERY_TEXT		2
#define PARALLEL_VACUUM_KEY_BUFFER_USAGE	3
#define PARALLEL_VACUUM_KEY_WAL_USAGE		4
#define PARALLEL_VACUUM_KEY_INDEX_STATS		5

/* The kind of parallel vacuum phases */
typedef enum
{
	PV_WORK_PHASE_PROCESS_INDEXES,	/* index vacuuming or cleanup */
	PV_WORK_PHASE_COLLECT_DEAD_ITEMS,	/* collect dead tuples */
} PVWorkPhase;

/*
 * Shared information among parallel workers.  So this is allocated in the DSM
 * segment.
 */
typedef struct PVShared
{
	/*
	 * Target table relid, log level (for messages about parallel workers
	 * launched during VACUUM VERBOSE) and query ID.  These fields are not
	 * modified during the parallel vacuum.
	 */
	Oid			relid;
	int			elevel;
	int64		queryid;

	/*
	 * Tell parallel workers what phase to perform: processing indexes or
	 * collecting dead tuples from the table.
	 */
	PVWorkPhase work_phase;

	/*
	 * Fields for both index vacuum and cleanup.
	 *
	 * reltuples is the total number of input heap tuples.  We set either old
	 * live tuples in the index vacuum case or the new live tuples in the
	 * index cleanup case.
	 *
	 * estimated_count is true if reltuples is an estimated value.  (Note that
	 * reltuples could be -1 in this case, indicating we have no idea.)
	 */
	double		reltuples;
	bool		estimated_count;

	/*
	 * In single process vacuum we could consume more memory during index
	 * vacuuming or cleanup apart from the memory for heap scanning.  In
	 * parallel vacuum, since individual vacuum workers can consume memory
	 * equal to maintenance_work_mem, the new maintenance_work_mem for each
	 * worker is set such that the parallel operation doesn't consume more
	 * memory than single process vacuum.
	 */
	int			maintenance_work_mem_worker;

	/*
	 * The number of buffers each worker's Buffer Access Strategy ring should
	 * contain.
	 */
	int			ring_nbuffers;

	/*
	 * Shared vacuum cost balance.  During parallel vacuum,
	 * VacuumSharedCostBalance points to this value and it accumulates the
	 * balance of each parallel vacuum worker.
	 */
	pg_atomic_uint32 cost_balance;

	/*
	 * Number of active parallel workers.  This is used for computing the
	 * minimum threshold of the vacuum cost balance before a worker sleeps for
	 * cost-based delay.
	 */
	pg_atomic_uint32 active_nworkers;

	/* Counter for vacuuming and cleanup */
	pg_atomic_uint32 idx;

	/* DSA handle where the TidStore lives */
	dsa_handle	dead_items_dsa_handle;

	/* DSA pointer to the shared TidStore */
	dsa_pointer dead_items_handle;

	/* Statistics of shared dead items */
	VacDeadItemsInfo dead_items_info;
} PVShared;

/* Status used during parallel index vacuum or cleanup */
typedef enum PVIndVacStatus
{
	PARALLEL_INDVAC_STATUS_INITIAL = 0,
	PARALLEL_INDVAC_STATUS_NEED_BULKDELETE,
	PARALLEL_INDVAC_STATUS_NEED_CLEANUP,
	PARALLEL_INDVAC_STATUS_COMPLETED,
} PVIndVacStatus;

/*
 * Struct for index vacuum statistics of an index that is used for parallel vacuum.
 * This includes the status of parallel index vacuum as well as index statistics.
 */
typedef struct PVIndStats
{
	/*
	 * The following two fields are set by leader process before executing
	 * parallel index vacuum or parallel index cleanup.  These fields are not
	 * fixed for the entire VACUUM operation.  They are only fixed for an
	 * individual parallel index vacuum and cleanup.
	 *
	 * parallel_workers_can_process is true if both leader and worker can
	 * process the index, otherwise only leader can process it.
	 */
	PVIndVacStatus status;
	bool		parallel_workers_can_process;

	/*
	 * Individual worker or leader stores the result of index vacuum or
	 * cleanup.
	 */
	bool		istat_updated;	/* are the stats updated? */
	IndexBulkDeleteResult istat;
} PVIndStats;

/*
 * Struct for maintaining a parallel vacuum state. typedef appears in vacuum.h.
 */
struct ParallelVacuumState
{
	/* NULL for worker processes */
	ParallelContext *pcxt;

	/* Do we need to reinitialize parallel DSM? */
	bool		need_reinitialize_dsm;

	/* Parent Heap Relation */
	Relation	heaprel;

	/* Target indexes */
	Relation   *indrels;
	int			nindexes;

	/* Shared information among parallel vacuum workers */
	PVShared   *shared;

	/*
	 * Shared index statistics among parallel vacuum workers. The array
	 * element is allocated for every index, even those indexes where parallel
	 * index vacuuming is unsafe or not worthwhile (e.g.,
	 * idx_will_parallel_vacuum[] is false).  During parallel vacuum,
	 * IndexBulkDeleteResult of each index is kept in DSM and is copied into
	 * local memory at the end of parallel vacuum.
	 */
	PVIndStats *indstats;

	/* Shared dead items space among parallel vacuum workers */
	TidStore   *dead_items;

	/* Points to buffer usage area in DSM */
	BufferUsage *buffer_usage;

	/* Points to WAL usage area in DSM */
	WalUsage   *wal_usage;

	/*
	 * The number of workers for parallel table vacuuming. If 0, the parallel
	 * table vacuum is disabled.
	 */
	int			nworkers_for_table;

	/*
	 * False if the index is totally unsuitable target for all parallel
	 * processing. For example, the index could be <
	 * min_parallel_index_scan_size cutoff.
	 */
	bool	   *idx_will_parallel_vacuum;

	/*
	 * The number of indexes that support parallel index bulk-deletion and
	 * parallel index cleanup respectively.
	 */
	int			nindexes_parallel_bulkdel;
	int			nindexes_parallel_cleanup;
	int			nindexes_parallel_condcleanup;

	/* Buffer access strategy used by leader process */
	BufferAccessStrategy bstrategy;

	/*
	 * Error reporting state.  The error callback is set only for workers
	 * processes during parallel index vacuum.
	 */
	char	   *relnamespace;
	char	   *relname;
	char	   *indname;
	PVIndVacStatus status;
};

static int	parallel_vacuum_compute_workers(Relation rel, Relation *indrels, int nindexes,
											int nrequested, int *nworkers_for_table,
											bool *idx_will_parallel_vacuum,
											void *state);
static void parallel_vacuum_process_all_indexes(ParallelVacuumState *pvs, int num_index_scans,
												bool vacuum);
static void parallel_vacuum_process_safe_indexes(ParallelVacuumState *pvs);
static void parallel_vacuum_process_unsafe_indexes(ParallelVacuumState *pvs);
static void parallel_vacuum_process_one_index(ParallelVacuumState *pvs, Relation indrel,
											  PVIndStats *indstats);
static bool parallel_vacuum_index_is_parallel_safe(Relation indrel, int num_index_scans,
												   bool vacuum);
static void parallel_vacuum_begin_work_phase(ParallelVacuumState *pvs, int nworkers,
											 PVWorkPhase work_phase);
static void parallel_vacuum_end_worker_phase(ParallelVacuumState *pvs);
static void parallel_vacuum_error_callback(void *arg);

/*
 * Try to enter parallel mode and create a parallel context.  Then initialize
 * shared memory state.
 *
 * nrequested_workers is the requested parallel degree. 0 means that the parallel
 * degrees for table and indexes vacuum are decided differently. See the comments
 * of parallel_vacuum_compute_workers() for details.
 *
 * On success, return parallel vacuum state.  Otherwise return NULL.
 */
ParallelVacuumState *
parallel_vacuum_init(Relation rel, Relation *indrels, int nindexes,
					 int nrequested_workers, int vac_work_mem,
					 int elevel, BufferAccessStrategy bstrategy, void *state)
{
	ParallelVacuumState *pvs;
	ParallelContext *pcxt;
	PVShared   *shared;
	TidStore   *dead_items;
	PVIndStats *indstats;
	BufferUsage *buffer_usage;
	WalUsage   *wal_usage;
	bool	   *idx_will_parallel_vacuum;
	Size		est_indstats_len;
	Size		est_shared_len;
	int			nindexes_mwm = 0;
	int			parallel_workers = 0;
	int			nworkers_for_table;
	int			querylen;

	/* A parallel vacuum must be requested */
	Assert(nrequested_workers >= 0);

	/*
	 * Compute the number of parallel vacuum workers to launch
	 */
	idx_will_parallel_vacuum = (bool *) palloc0(sizeof(bool) * nindexes);
	parallel_workers = parallel_vacuum_compute_workers(rel, indrels, nindexes,
													   nrequested_workers,
													   &nworkers_for_table,
													   idx_will_parallel_vacuum,
													   state);

	if (parallel_workers <= 0)
	{
		/* Can't perform vacuum in parallel -- return NULL */
		pfree(idx_will_parallel_vacuum);
		return NULL;
	}

	pvs = (ParallelVacuumState *) palloc0(sizeof(ParallelVacuumState));
	pvs->indrels = indrels;
	pvs->nindexes = nindexes;
	pvs->idx_will_parallel_vacuum = idx_will_parallel_vacuum;
	pvs->bstrategy = bstrategy;
	pvs->heaprel = rel;

	EnterParallelMode();
	pcxt = CreateParallelContext("postgres", "parallel_vacuum_main",
								 parallel_workers);
	Assert(pcxt->nworkers > 0);
	pvs->pcxt = pcxt;
	pvs->need_reinitialize_dsm = false;
	pvs->nworkers_for_table = nworkers_for_table;

	/* Estimate size for index vacuum stats -- PARALLEL_VACUUM_KEY_INDEX_STATS */
	est_indstats_len = mul_size(sizeof(PVIndStats), nindexes);
	shm_toc_estimate_chunk(&pcxt->estimator, est_indstats_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate size for shared information -- PARALLEL_VACUUM_KEY_SHARED */
	est_shared_len = sizeof(PVShared);
	shm_toc_estimate_chunk(&pcxt->estimator, est_shared_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/*
	 * Estimate space for BufferUsage and WalUsage --
	 * PARALLEL_VACUUM_KEY_BUFFER_USAGE and PARALLEL_VACUUM_KEY_WAL_USAGE.
	 *
	 * If there are no extensions loaded that care, we could skip this.  We
	 * have no way of knowing whether anyone's looking at pgBufferUsage or
	 * pgWalUsage, so do it unconditionally.
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Finally, estimate PARALLEL_VACUUM_KEY_QUERY_TEXT space */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;			/* keep compiler quiet */

	/* Estimate AM-specific space for parallel table vacuum */
	if (pvs->nworkers_for_table > 0)
		table_parallel_vacuum_estimate(rel, pcxt, pvs->nworkers_for_table, state);

	InitializeParallelDSM(pcxt);

	/* Prepare index vacuum stats */
	indstats = (PVIndStats *) shm_toc_allocate(pcxt->toc, est_indstats_len);
	MemSet(indstats, 0, est_indstats_len);
	for (int i = 0; i < nindexes; i++)
	{
		Relation	indrel = indrels[i];
		uint8		vacoptions = indrel->rd_indam->amparallelvacuumoptions;

		/*
		 * Cleanup option should be either disabled, always performing in
		 * parallel or conditionally performing in parallel.
		 */
		Assert(((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) == 0) ||
			   ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0));
		Assert(vacoptions <= VACUUM_OPTION_MAX_VALID_VALUE);

		if (!idx_will_parallel_vacuum[i])
			continue;

		if (indrel->rd_indam->amusemaintenanceworkmem)
			nindexes_mwm++;

		/*
		 * Remember the number of indexes that support parallel operation for
		 * each phase.
		 */
		if ((vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0)
			pvs->nindexes_parallel_bulkdel++;
		if ((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) != 0)
			pvs->nindexes_parallel_cleanup++;
		if ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0)
			pvs->nindexes_parallel_condcleanup++;
	}
	shm_toc_insert(pcxt->toc, PARALLEL_VACUUM_KEY_INDEX_STATS, indstats);
	pvs->indstats = indstats;

	/* Prepare shared information */
	shared = (PVShared *) shm_toc_allocate(pcxt->toc, est_shared_len);
	MemSet(shared, 0, est_shared_len);
	shared->relid = RelationGetRelid(rel);
	shared->elevel = elevel;
	shared->queryid = pgstat_get_my_query_id();
	shared->maintenance_work_mem_worker =
		(nindexes_mwm > 0) ?
		maintenance_work_mem / Min(parallel_workers, nindexes_mwm) :
		maintenance_work_mem;
	shared->dead_items_info.max_bytes = vac_work_mem * (size_t) 1024;

	/* Prepare DSA space for dead items */
	dead_items = TidStoreCreateShared(shared->dead_items_info.max_bytes,
									  LWTRANCHE_PARALLEL_VACUUM_DSA);
	pvs->dead_items = dead_items;
	shared->dead_items_handle = TidStoreGetHandle(dead_items);
	shared->dead_items_dsa_handle = dsa_get_handle(TidStoreGetDSA(dead_items));

	/* Use the same buffer size for all workers */
	shared->ring_nbuffers = GetAccessStrategyBufferCount(bstrategy);

	pg_atomic_init_u32(&(shared->cost_balance), 0);
	pg_atomic_init_u32(&(shared->active_nworkers), 0);
	pg_atomic_init_u32(&(shared->idx), 0);

	shm_toc_insert(pcxt->toc, PARALLEL_VACUUM_KEY_SHARED, shared);
	pvs->shared = shared;

	/*
	 * Allocate space for each worker's BufferUsage and WalUsage; no need to
	 * initialize
	 */
	buffer_usage = shm_toc_allocate(pcxt->toc,
									mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_VACUUM_KEY_BUFFER_USAGE, buffer_usage);
	pvs->buffer_usage = buffer_usage;
	wal_usage = shm_toc_allocate(pcxt->toc,
								 mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_VACUUM_KEY_WAL_USAGE, wal_usage);
	pvs->wal_usage = wal_usage;

	/* Store query string for workers */
	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		sharedquery[querylen] = '\0';
		shm_toc_insert(pcxt->toc,
					   PARALLEL_VACUUM_KEY_QUERY_TEXT, sharedquery);
	}

	/* Initialize AM-specific DSM space for parallel table vacuum */
	if (pvs->nworkers_for_table > 0)
		table_parallel_vacuum_initialize(rel, pcxt, pvs->nworkers_for_table, state);

	/* Success -- return parallel vacuum state */
	return pvs;
}

/*
 * Destroy the parallel context, and end parallel mode.
 *
 * Since writes are not allowed during parallel mode, copy the
 * updated index statistics from DSM into local memory and then later use that
 * to update the index statistics.  One might think that we can exit from
 * parallel mode, update the index statistics and then destroy parallel
 * context, but that won't be safe (see ExitParallelMode).
 */
void
parallel_vacuum_end(ParallelVacuumState *pvs, IndexBulkDeleteResult **istats)
{
	Assert(!IsParallelWorker());

	/* Copy the updated statistics */
	for (int i = 0; i < pvs->nindexes; i++)
	{
		PVIndStats *indstats = &(pvs->indstats[i]);

		if (indstats->istat_updated)
		{
			istats[i] = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
			memcpy(istats[i], &indstats->istat, sizeof(IndexBulkDeleteResult));
		}
		else
			istats[i] = NULL;
	}

	TidStoreDestroy(pvs->dead_items);

	DestroyParallelContext(pvs->pcxt);
	ExitParallelMode();

	pfree(pvs->idx_will_parallel_vacuum);
	pfree(pvs);
}

/*
 * Return the number of parallel workers initialized for parallel table vacuum.
 */
int
parallel_vacuum_get_nworkers_table(ParallelVacuumState *pvs)
{
	return pvs->nworkers_for_table;
}

/*
 * Return the array of indexes associated to the given table to be vacuumed.
 */
Relation *
parallel_vacuum_get_table_indexes(ParallelVacuumState *pvs, int *nindexes)
{
	*nindexes = pvs->nindexes;

	return pvs->indrels;
}

/*
 * Return the buffer strategy for parallel vacuum.
 */
BufferAccessStrategy
parallel_vacuum_get_bstrategy(ParallelVacuumState *pvs)
{
	return pvs->bstrategy;
}

/*
 * Returns the dead items space and dead items information.
 */
TidStore *
parallel_vacuum_get_dead_items(ParallelVacuumState *pvs, VacDeadItemsInfo **dead_items_info_p)
{
	*dead_items_info_p = &(pvs->shared->dead_items_info);
	return pvs->dead_items;
}

/* Forget all items in dead_items */
void
parallel_vacuum_reset_dead_items(ParallelVacuumState *pvs)
{
	VacDeadItemsInfo *dead_items_info = &(pvs->shared->dead_items_info);

	/*
	 * Free the current tidstore and return allocated DSA segments to the
	 * operating system. Then we recreate the tidstore with the same max_bytes
	 * limitation we just used.
	 */
	TidStoreDestroy(pvs->dead_items);
	pvs->dead_items = TidStoreCreateShared(dead_items_info->max_bytes,
										   LWTRANCHE_PARALLEL_VACUUM_DSA);

	/* Update the DSA pointer for dead_items to the new one */
	pvs->shared->dead_items_dsa_handle = dsa_get_handle(TidStoreGetDSA(pvs->dead_items));
	pvs->shared->dead_items_handle = TidStoreGetHandle(pvs->dead_items);

	/* Reset the counter */
	dead_items_info->num_items = 0;
}

/*
 * Do parallel index bulk-deletion with parallel workers.
 */
void
parallel_vacuum_bulkdel_all_indexes(ParallelVacuumState *pvs, long num_table_tuples,
									int num_index_scans)
{
	Assert(!IsParallelWorker());

	/*
	 * We can only provide an approximate value of num_heap_tuples, at least
	 * for now.
	 */
	pvs->shared->reltuples = num_table_tuples;
	pvs->shared->estimated_count = true;

	parallel_vacuum_process_all_indexes(pvs, num_index_scans, true);
}

/*
 * Do parallel index cleanup with parallel workers.
 */
void
parallel_vacuum_cleanup_all_indexes(ParallelVacuumState *pvs, long num_table_tuples,
									int num_index_scans, bool estimated_count)
{
	Assert(!IsParallelWorker());

	/*
	 * We can provide a better estimate of total number of surviving tuples
	 * (we assume indexes are more interested in that than in the number of
	 * nominally live tuples).
	 */
	pvs->shared->reltuples = num_table_tuples;
	pvs->shared->estimated_count = estimated_count;

	parallel_vacuum_process_all_indexes(pvs, num_index_scans, false);
}

/*
 * Compute the number of parallel worker processes to request for table
 * vacuum and index vacuum/cleanup.  Return the maximum number of parallel
 * workers for table vacuuming and index vacuuming.
 *
 * nrequested is the number of parallel workers that user requested, which
 * applies to both the number of workers for table vacuum and index vacuum.
 * If nrequested is 0, we compute the parallel degree for them differently
 * as described below.
 *
 * For parallel table vacuum, we ask AM-specific routine to compute the
 * number of parallel worker processes. The result is set to nworkers_table_p.
 *
 * For parallel index vacuum, the index is eligible for parallel vacuum iff
 * its size is greater than min_parallel_index_scan_size as invoking workers
 * for very small indexes can hurt performance. This function sets
 * idx_will_parallel_vacuum to remember indexes that participate in parallel vacuum.
 */
static int
parallel_vacuum_compute_workers(Relation rel, Relation *indrels, int nindexes,
								int nrequested, int *nworkers_table_p,
								bool *idx_will_parallel_vacuum, void *state)
{
	int			nindexes_parallel = 0;
	int			nindexes_parallel_bulkdel = 0;
	int			nindexes_parallel_cleanup = 0;
	int			nworkers_table = 0;
	int			nworkers_index = 0;

	*nworkers_table_p = 0;

	/*
	 * We don't allow performing parallel operation in standalone backend or
	 * when parallelism is disabled.
	 */
	if (!IsUnderPostmaster || max_parallel_maintenance_workers == 0)
		return 0;

	/* Compute the number of workers for parallel table scan */
	if (rel->rd_tableam->parallel_vacuum_compute_workers != NULL)
		nworkers_table = table_parallel_vacuum_compute_workers(rel, nrequested,
															   state);

	/* Cap by max_parallel_maintenance_workers */
	nworkers_table = Min(nworkers_table, max_parallel_maintenance_workers);

	/*
	 * Compute the number of indexes that can participate in parallel vacuum.
	 */
	for (int i = 0; i < nindexes; i++)
	{
		Relation	indrel = indrels[i];
		uint8		vacoptions = indrel->rd_indam->amparallelvacuumoptions;

		/* Skip index that is not a suitable target for parallel index vacuum */
		if (vacoptions == VACUUM_OPTION_NO_PARALLEL ||
			RelationGetNumberOfBlocks(indrel) < min_parallel_index_scan_size)
			continue;

		idx_will_parallel_vacuum[i] = true;

		if ((vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0)
			nindexes_parallel_bulkdel++;
		if (((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) != 0) ||
			((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0))
			nindexes_parallel_cleanup++;
	}

	nindexes_parallel = Max(nindexes_parallel_bulkdel,
							nindexes_parallel_cleanup);

	/* The leader process takes one index */
	nindexes_parallel--;

	if (nindexes_parallel > 0)
	{
		/* Take into account the requested number of workers */
		nworkers_index = (nrequested > 0) ?
			Min(nrequested, nindexes_parallel) : nindexes_parallel;

		/* Cap by max_parallel_maintenance_workers */
		nworkers_index = Min(nworkers_index, max_parallel_maintenance_workers);
	}

	*nworkers_table_p = nworkers_table;
	return Max(nworkers_table, nworkers_index);
}

/*
 * Perform index vacuum or index cleanup with parallel workers.  This function
 * must be used by the parallel vacuum leader process.
 */
static void
parallel_vacuum_process_all_indexes(ParallelVacuumState *pvs, int num_index_scans,
									bool vacuum)
{
	int			nworkers;
	PVIndVacStatus new_status;

	Assert(!IsParallelWorker());

	if (vacuum)
	{
		new_status = PARALLEL_INDVAC_STATUS_NEED_BULKDELETE;

		/* Determine the number of parallel workers to launch */
		nworkers = pvs->nindexes_parallel_bulkdel;
	}
	else
	{
		new_status = PARALLEL_INDVAC_STATUS_NEED_CLEANUP;

		/* Determine the number of parallel workers to launch */
		nworkers = pvs->nindexes_parallel_cleanup;

		/* Add conditionally parallel-aware indexes if in the first time call */
		if (num_index_scans == 0)
			nworkers += pvs->nindexes_parallel_condcleanup;
	}

	/* The leader process will participate */
	nworkers--;

	/*
	 * It is possible that parallel context is initialized with fewer workers
	 * than the number of indexes that need a separate worker in the current
	 * phase, so we need to consider it.  See
	 * parallel_vacuum_compute_workers().
	 */
	nworkers = Min(nworkers, pvs->pcxt->nworkers);

	/*
	 * Set index vacuum status and mark whether parallel vacuum worker can
	 * process it.
	 */
	for (int i = 0; i < pvs->nindexes; i++)
	{
		PVIndStats *indstats = &(pvs->indstats[i]);

		Assert(indstats->status == PARALLEL_INDVAC_STATUS_INITIAL);
		indstats->status = new_status;
		indstats->parallel_workers_can_process =
			(pvs->idx_will_parallel_vacuum[i] &&
			 parallel_vacuum_index_is_parallel_safe(pvs->indrels[i],
													num_index_scans,
													vacuum));
	}

	/* Reset the parallel index processing and progress counters */
	pg_atomic_write_u32(&(pvs->shared->idx), 0);

	/* Setup the shared cost-based vacuum delay and launch workers */
	if (nworkers > 0)
	{
		/* Start parallel vacuum workers for processing indexes */
		parallel_vacuum_begin_work_phase(pvs, nworkers,
										 PV_WORK_PHASE_PROCESS_INDEXES);

		if (vacuum)
			ereport(pvs->shared->elevel,
					(errmsg(ngettext("launched %d parallel vacuum worker for index vacuuming (planned: %d)",
									 "launched %d parallel vacuum workers for index vacuuming (planned: %d)",
									 pvs->pcxt->nworkers_launched),
							pvs->pcxt->nworkers_launched, nworkers)));
		else
			ereport(pvs->shared->elevel,
					(errmsg(ngettext("launched %d parallel vacuum worker for index cleanup (planned: %d)",
									 "launched %d parallel vacuum workers for index cleanup (planned: %d)",
									 pvs->pcxt->nworkers_launched),
							pvs->pcxt->nworkers_launched, nworkers)));
	}

	/* Vacuum the indexes that can be processed by only leader process */
	parallel_vacuum_process_unsafe_indexes(pvs);

	/*
	 * Join as a parallel worker.  The leader vacuums alone processes all
	 * parallel-safe indexes in the case where no workers are launched.
	 */
	parallel_vacuum_process_safe_indexes(pvs);

	/*
	 * Next, accumulate buffer and WAL usage.  (This must wait for the workers
	 * to finish, or we might get incomplete data.)
	 */
	if (nworkers > 0)
		parallel_vacuum_end_worker_phase(pvs);

	/*
	 * Reset all index status back to initial (while checking that we have
	 * vacuumed all indexes).
	 */
	for (int i = 0; i < pvs->nindexes; i++)
	{
		PVIndStats *indstats = &(pvs->indstats[i]);

		if (indstats->status != PARALLEL_INDVAC_STATUS_COMPLETED)
			elog(ERROR, "parallel index vacuum on index \"%s\" is not completed",
				 RelationGetRelationName(pvs->indrels[i]));

		indstats->status = PARALLEL_INDVAC_STATUS_INITIAL;
	}

	/* Parallel DSM will need to be reinitialized for the next execution */
	pvs->need_reinitialize_dsm = true;
}

/*
 * Index vacuum/cleanup routine used by the leader process and parallel
 * vacuum worker processes to vacuum the indexes in parallel.
 */
static void
parallel_vacuum_process_safe_indexes(ParallelVacuumState *pvs)
{
	/*
	 * Increment the active worker count if we are able to launch any worker.
	 */
	if (VacuumActiveNWorkers)
		pg_atomic_add_fetch_u32(VacuumActiveNWorkers, 1);

	/* Loop until all indexes are vacuumed */
	for (;;)
	{
		int			idx;
		PVIndStats *indstats;

		/* Get an index number to process */
		idx = pg_atomic_fetch_add_u32(&(pvs->shared->idx), 1);

		/* Done for all indexes? */
		if (idx >= pvs->nindexes)
			break;

		indstats = &(pvs->indstats[idx]);

		/*
		 * Skip vacuuming index that is unsafe for workers or has an
		 * unsuitable target for parallel index vacuum (this is vacuumed in
		 * parallel_vacuum_process_unsafe_indexes() by the leader).
		 */
		if (!indstats->parallel_workers_can_process)
			continue;

		/* Do vacuum or cleanup of the index */
		parallel_vacuum_process_one_index(pvs, pvs->indrels[idx], indstats);
	}

	/*
	 * We have completed the index vacuum so decrement the active worker
	 * count.
	 */
	if (VacuumActiveNWorkers)
		pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, 1);
}

/*
 * Perform parallel vacuuming of indexes in leader process.
 *
 * Handles index vacuuming (or index cleanup) for indexes that are not
 * parallel safe.  It's possible that this will vary for a given index, based
 * on details like whether we're performing index cleanup right now.
 *
 * Also performs vacuuming of smaller indexes that fell under the size cutoff
 * enforced by parallel_vacuum_compute_workers().
 */
static void
parallel_vacuum_process_unsafe_indexes(ParallelVacuumState *pvs)
{
	Assert(!IsParallelWorker());

	/*
	 * Increment the active worker count if we are able to launch any worker.
	 */
	if (VacuumActiveNWorkers)
		pg_atomic_add_fetch_u32(VacuumActiveNWorkers, 1);

	for (int i = 0; i < pvs->nindexes; i++)
	{
		PVIndStats *indstats = &(pvs->indstats[i]);

		/* Skip, indexes that are safe for workers */
		if (indstats->parallel_workers_can_process)
			continue;

		/* Do vacuum or cleanup of the index */
		parallel_vacuum_process_one_index(pvs, pvs->indrels[i], indstats);
	}

	/*
	 * We have completed the index vacuum so decrement the active worker
	 * count.
	 */
	if (VacuumActiveNWorkers)
		pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, 1);
}

/*
 * Vacuum or cleanup index either by leader process or by one of the worker
 * process.  After vacuuming the index this function copies the index
 * statistics returned from ambulkdelete and amvacuumcleanup to the DSM
 * segment.
 */
static void
parallel_vacuum_process_one_index(ParallelVacuumState *pvs, Relation indrel,
								  PVIndStats *indstats)
{
	IndexBulkDeleteResult *istat = NULL;
	IndexBulkDeleteResult *istat_res;
	IndexVacuumInfo ivinfo;

	/*
	 * Update the pointer to the corresponding bulk-deletion result if someone
	 * has already updated it
	 */
	if (indstats->istat_updated)
		istat = &(indstats->istat);

	ivinfo.index = indrel;
	ivinfo.heaprel = pvs->heaprel;
	ivinfo.analyze_only = false;
	ivinfo.report_progress = false;
	ivinfo.message_level = DEBUG2;
	ivinfo.estimated_count = pvs->shared->estimated_count;
	ivinfo.num_heap_tuples = pvs->shared->reltuples;
	ivinfo.strategy = pvs->bstrategy;

	/* Update error traceback information */
	pvs->indname = pstrdup(RelationGetRelationName(indrel));
	pvs->status = indstats->status;

	switch (indstats->status)
	{
		case PARALLEL_INDVAC_STATUS_NEED_BULKDELETE:
			istat_res = vac_bulkdel_one_index(&ivinfo, istat, pvs->dead_items,
											  &pvs->shared->dead_items_info);
			break;
		case PARALLEL_INDVAC_STATUS_NEED_CLEANUP:
			istat_res = vac_cleanup_one_index(&ivinfo, istat);
			break;
		default:
			elog(ERROR, "unexpected parallel vacuum index status %d for index \"%s\"",
				 indstats->status,
				 RelationGetRelationName(indrel));
	}

	/*
	 * Copy the index bulk-deletion result returned from ambulkdelete and
	 * amvacuumcleanup to the DSM segment if it's the first cycle because they
	 * allocate locally and it's possible that an index will be vacuumed by a
	 * different vacuum process the next cycle.  Copying the result normally
	 * happens only the first time an index is vacuumed.  For any additional
	 * vacuum pass, we directly point to the result on the DSM segment and
	 * pass it to vacuum index APIs so that workers can update it directly.
	 *
	 * Since all vacuum workers write the bulk-deletion result at different
	 * slots we can write them without locking.
	 */
	if (!indstats->istat_updated && istat_res != NULL)
	{
		memcpy(&(indstats->istat), istat_res, sizeof(IndexBulkDeleteResult));
		indstats->istat_updated = true;

		/* Free the locally-allocated bulk-deletion result */
		pfree(istat_res);
	}

	/*
	 * Update the status to completed. No need to lock here since each worker
	 * touches different indexes.
	 */
	indstats->status = PARALLEL_INDVAC_STATUS_COMPLETED;

	/* Reset error traceback information */
	pvs->status = PARALLEL_INDVAC_STATUS_COMPLETED;
	pfree(pvs->indname);
	pvs->indname = NULL;

	/*
	 * Call the parallel variant of pgstat_progress_incr_param so workers can
	 * report progress of index vacuum to the leader.
	 */
	pgstat_progress_parallel_incr_param(PROGRESS_VACUUM_INDEXES_PROCESSED, 1);
}

/*
 * Returns false, if the given index can't participate in the next execution of
 * parallel index vacuum or parallel index cleanup.
 */
static bool
parallel_vacuum_index_is_parallel_safe(Relation indrel, int num_index_scans,
									   bool vacuum)
{
	uint8		vacoptions;

	vacoptions = indrel->rd_indam->amparallelvacuumoptions;

	/* In parallel vacuum case, check if it supports parallel bulk-deletion */
	if (vacuum)
		return ((vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0);

	/* Not safe, if the index does not support parallel cleanup */
	if (((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) == 0) &&
		((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0))
		return false;

	/*
	 * Not safe, if the index supports parallel cleanup conditionally, but we
	 * have already processed the index (for bulkdelete).  We do this to avoid
	 * the need to invoke workers when parallel index cleanup doesn't need to
	 * scan the index.  See the comments for option
	 * VACUUM_OPTION_PARALLEL_COND_CLEANUP to know when indexes support
	 * parallel cleanup conditionally.
	 */
	if (num_index_scans > 0 &&
		((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0))
		return false;

	return true;
}

/*
 * Begin the parallel scan to collect dead items. Return the number of
 * launched parallel workers.
 *
 * The caller must call parallel_vacuum_collect_dead_items_end() to finish
 * the parallel scan.
 */
int
parallel_vacuum_collect_dead_items_begin(ParallelVacuumState *pvs)
{
	int			nworkers = pvs->nworkers_for_table;
#ifdef USE_INJECTION_POINTS
	static int	ntimes = 0;
#endif

	Assert(!IsParallelWorker());

	if (nworkers == 0)
		return 0;

	/* Start parallel vacuum workers for collecting dead items */
	Assert(nworkers <= pvs->pcxt->nworkers);

#ifdef USE_INJECTION_POINTS
	if (IS_INJECTION_POINT_ATTACHED("parallel-vacuum-ramp-down-workers"))
	{
		nworkers = pvs->nworkers_for_table - Min(ntimes, pvs->nworkers_for_table);
		ntimes++;
	}
#endif

	parallel_vacuum_begin_work_phase(pvs, nworkers,
									 PV_WORK_PHASE_COLLECT_DEAD_ITEMS);

	/* Include the worker count for the leader itself */
	if (pvs->pcxt->nworkers_launched > 0)
		pg_atomic_add_fetch_u32(VacuumActiveNWorkers, 1);

	return pvs->pcxt->nworkers_launched;
}

/*
 * Wait for all workers for parallel vacuum workers launched by
 * parallel_vacuum_collect_dead_items_begin(), and gather workers' statistics.
 */
void
parallel_vacuum_collect_dead_items_end(ParallelVacuumState *pvs)
{
	Assert(!IsParallelWorker());
	Assert(pvs->shared->work_phase == PV_WORK_PHASE_COLLECT_DEAD_ITEMS);

	if (pvs->nworkers_for_table == 0)
		return;

	/* Wait for parallel workers to finish */
	parallel_vacuum_end_worker_phase(pvs);

	/* Decrement the worker count for the leader itself */
	if (VacuumActiveNWorkers)
		pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, 1);
}

/*
 * The function is for parallel workers to execute the parallel scan to
 * collect dead tuples.
 */
static void
parallel_vacuum_process_table(ParallelVacuumState *pvs, void *state)
{
	Assert(VacuumActiveNWorkers);
	Assert(pvs->shared->work_phase == PV_WORK_PHASE_COLLECT_DEAD_ITEMS);

	/* Increment the active worker before starting the table vacuum */
	pg_atomic_add_fetch_u32(VacuumActiveNWorkers, 1);

	/* Do the parallel scan to collect dead tuples */
	table_parallel_vacuum_collect_dead_items(pvs->heaprel, pvs, state);

	/*
	 * We have completed the table vacuum so decrement the active worker
	 * count.
	 */
	pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, 1);
}

/*
 * Perform work within a launched parallel process.
 *
 * Since parallel vacuum workers perform only index vacuum or index cleanup,
 * we don't need to report progress information.
 */
void
parallel_vacuum_main(dsm_segment *seg, shm_toc *toc)
{
	ParallelVacuumState pvs;
	Relation	rel;
	Relation   *indrels;
	PVIndStats *indstats;
	PVShared   *shared;
	TidStore   *dead_items;
	BufferUsage *buffer_usage;
	WalUsage   *wal_usage;
	int			nindexes;
	char	   *sharedquery;
	void	   *state;
	ErrorContextCallback errcallback;

	/*
	 * A parallel vacuum worker must have only PROC_IN_VACUUM flag since we
	 * don't support parallel vacuum for autovacuum as of now.
	 */
	Assert(MyProc->statusFlags == PROC_IN_VACUUM);

	elog(DEBUG1, "starting parallel vacuum worker");

	shared = (PVShared *) shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_SHARED, false);

	/* Set debug_query_string for individual workers */
	sharedquery = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Track query ID */
	pgstat_report_query_id(shared->queryid, false);

	/*
	 * Open table.  The lock mode is the same as the leader process.  It's
	 * okay because the lock mode does not conflict among the parallel
	 * workers.
	 */
	rel = table_open(shared->relid, ShareUpdateExclusiveLock);

	/*
	 * Open all indexes. indrels are sorted in order by OID, which should be
	 * matched to the leader's one.
	 */
	vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indrels);

	/*
	 * Apply the desired value of maintenance_work_mem within this process.
	 * Really we should use SetConfigOption() to change a GUC, but since we're
	 * already in parallel mode guc.c would complain about that.  Fortunately,
	 * by the same token guc.c will not let any user-defined code change it.
	 * So just avert your eyes while we do this:
	 */
	if (shared->maintenance_work_mem_worker > 0)
		maintenance_work_mem = shared->maintenance_work_mem_worker;

	/* Set index statistics */
	indstats = (PVIndStats *) shm_toc_lookup(toc,
											 PARALLEL_VACUUM_KEY_INDEX_STATS,
											 false);

	/* Find dead_items in shared memory */
	dead_items = TidStoreAttach(shared->dead_items_dsa_handle,
								shared->dead_items_handle);

	/* Set cost-based vacuum delay */
	VacuumUpdateCosts();
	VacuumCostBalance = 0;
	VacuumCostBalanceLocal = 0;
	VacuumSharedCostBalance = &(shared->cost_balance);
	VacuumActiveNWorkers = &(shared->active_nworkers);

	/* Set parallel vacuum state */
	pvs.indrels = indrels;
	pvs.nindexes = nindexes;
	pvs.indstats = indstats;
	pvs.shared = shared;
	pvs.dead_items = dead_items;
	pvs.relnamespace = get_namespace_name(RelationGetNamespace(rel));
	pvs.relname = pstrdup(RelationGetRelationName(rel));
	pvs.heaprel = rel;

	/* These fields will be filled during index vacuum or cleanup */
	pvs.indname = NULL;
	pvs.status = PARALLEL_INDVAC_STATUS_INITIAL;

	/* Each parallel VACUUM worker gets its own access strategy. */
	pvs.bstrategy = GetAccessStrategyWithSize(BAS_VACUUM,
											  shared->ring_nbuffers * (BLCKSZ / 1024));

	/* Initialize AM-specific vacuum state for parallel table vacuuming */
	if (shared->work_phase == PV_WORK_PHASE_COLLECT_DEAD_ITEMS)
	{
		ParallelWorkerContext pwcxt;

		pwcxt.toc = toc;
		pwcxt.seg = seg;
		table_parallel_vacuum_initialize_worker(rel, &pvs, &pwcxt,
												&state);
	}

	/* Setup error traceback support for ereport() */
	errcallback.callback = parallel_vacuum_error_callback;
	errcallback.arg = &pvs;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	switch (pvs.shared->work_phase)
	{
		case PV_WORK_PHASE_COLLECT_DEAD_ITEMS:
			/* Scan the table to collect dead items */
			parallel_vacuum_process_table(&pvs, state);
			break;
		case PV_WORK_PHASE_PROCESS_INDEXES:
			/* Process indexes to perform vacuum/cleanup */
			parallel_vacuum_process_safe_indexes(&pvs);
			break;
		default:
			elog(ERROR, "unrecognized parallel vacuum phase %d", pvs.shared->work_phase);
	}

	/* Report buffer/WAL usage during parallel execution */
	buffer_usage = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_BUFFER_USAGE, false);
	wal_usage = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_WAL_USAGE, false);
	InstrEndParallelQuery(&buffer_usage[ParallelWorkerNumber],
						  &wal_usage[ParallelWorkerNumber]);

	/* Report any remaining cost-based vacuum delay time */
	if (track_cost_delay_timing)
		pgstat_progress_parallel_incr_param(PROGRESS_VACUUM_DELAY_TIME,
											parallel_vacuum_worker_delay_ns);

	TidStoreDetach(dead_items);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	vac_close_indexes(nindexes, indrels, RowExclusiveLock);
	table_close(rel, ShareUpdateExclusiveLock);
	FreeAccessStrategy(pvs.bstrategy);
}

/*
 * Launch parallel vacuum workers for the given phase. If at least one
 * worker launched, enable the shared vacuum delay costing.
 */
static void
parallel_vacuum_begin_work_phase(ParallelVacuumState *pvs, int nworkers,
								 PVWorkPhase work_phase)
{
	/* Set the work phase */
	pvs->shared->work_phase = work_phase;

	/* Reinitialize parallel context to relaunch parallel workers */
	if (pvs->need_reinitialize_dsm)
		ReinitializeParallelDSM(pvs->pcxt);

	/*
	 * Set up shared cost balance and the number of active workers for vacuum
	 * delay.  We need to do this before launching workers as otherwise, they
	 * might not see the updated values for these parameters.
	 */
	pg_atomic_write_u32(&(pvs->shared->cost_balance), VacuumCostBalance);
	pg_atomic_write_u32(&(pvs->shared->active_nworkers), 0);

	/*
	 * The number of workers can vary between bulkdelete and cleanup phase.
	 */
	ReinitializeParallelWorkers(pvs->pcxt, nworkers);

	LaunchParallelWorkers(pvs->pcxt);

	/* Enable shared vacuum costing if we are able to launch any worker */
	if (pvs->pcxt->nworkers_launched > 0)
	{
		/*
		 * Reset the local cost values for leader backend as we have already
		 * accumulated the remaining balance of heap.
		 */
		VacuumCostBalance = 0;
		VacuumCostBalanceLocal = 0;

		/* Enable shared cost balance for leader backend */
		VacuumSharedCostBalance = &(pvs->shared->cost_balance);
		VacuumActiveNWorkers = &(pvs->shared->active_nworkers);
	}
}

/*
 * Wait for parallel vacuum workers to finish, accumulate the statistics,
 * and disable shared vacuum delay costing if enabled.
 */
static void
parallel_vacuum_end_worker_phase(ParallelVacuumState *pvs)
{
	/* Wait for all vacuum workers to finish */
	WaitForParallelWorkersToFinish(pvs->pcxt);

	for (int i = 0; i < pvs->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&pvs->buffer_usage[i], &pvs->wal_usage[i]);

	/* Carry the shared balance value and disable shared costing */
	if (VacuumSharedCostBalance)
	{
		VacuumCostBalance = pg_atomic_read_u32(VacuumSharedCostBalance);
		VacuumSharedCostBalance = NULL;
		VacuumActiveNWorkers = NULL;
	}

	/* Parallel DSM will need to be reinitialized for the next execution */
	pvs->need_reinitialize_dsm = true;
}

/*
 * Error context callback for errors occurring during parallel index vacuum.
 * The error context messages should match the messages set in the lazy vacuum
 * error context.  If you change this function, change vacuum_error_callback()
 * as well.
 */
static void
parallel_vacuum_error_callback(void *arg)
{
	ParallelVacuumState *errinfo = arg;

	switch (errinfo->status)
	{
		case PARALLEL_INDVAC_STATUS_NEED_BULKDELETE:
			errcontext("while vacuuming index \"%s\" of relation \"%s.%s\"",
					   errinfo->indname,
					   errinfo->relnamespace,
					   errinfo->relname);
			break;
		case PARALLEL_INDVAC_STATUS_NEED_CLEANUP:
			errcontext("while cleaning up index \"%s\" of relation \"%s.%s\"",
					   errinfo->indname,
					   errinfo->relnamespace,
					   errinfo->relname);
			break;
		case PARALLEL_INDVAC_STATUS_INITIAL:
		case PARALLEL_INDVAC_STATUS_COMPLETED:
		default:
			return;
	}
}
