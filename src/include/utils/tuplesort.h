/*-------------------------------------------------------------------------
 *
 * tuplesort.h
 *	  Generalized tuple sorting routines.
 *
 * This module handles sorting of heap tuples, index tuples, or single
 * Datums (and could easily support other kinds of sortable objects,
 * if necessary).  It works efficiently for both small and large amounts
 * of data.  Small amounts are sorted in-memory using qsort().  Large
 * amounts are sorted using temporary files and a standard external sort
 * algorithm.  Parallel sorts use a variant of this external sort
 * algorithm, and are typically only used for large amounts of data.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tuplesort.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPLESORT_H
#define TUPLESORT_H

#include "access/itup.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "utils/relcache.h"


/*
 * Tuplesortstate and Sharedsort are opaque types whose details are not
 * known outside tuplesort.c.
 */
typedef struct Tuplesortstate Tuplesortstate;
typedef struct Sharedsort		Sharedsort;

/*
 * Tuplesort parallel coordination state.  Caller initializes everything.
 * See usage notes below.
 */
typedef struct SortCoordinateData
{
	/* Worker process?  If not, must be leader. */
	bool				isWorker;

	/*
	 * Leader process passed number of workers known launched (workers set this
	 * to -1).  This typically includes the leader-as-worker process.
	 */
	int					launched;

	/* Private opaque state in shared memory */
	Sharedsort		   *sharedsort;
} SortCoordinateData;

typedef struct SortCoordinateData* SortCoordinate;

/*
 * We provide multiple interfaces to what is essentially the same code,
 * since different callers have different data to be sorted and want to
 * specify the sort key information differently.  There are two APIs for
 * sorting HeapTuples and two more for sorting IndexTuples.  Yet another
 * API supports sorting bare Datums.
 *
 * The "heap" API actually stores/sorts MinimalTuples, which means it doesn't
 * preserve the system columns (tuple identity and transaction visibility
 * info).  The sort keys are specified by column numbers within the tuples
 * and sort operator OIDs.  We save some cycles by passing and returning the
 * tuples in TupleTableSlots, rather than forming actual HeapTuples (which'd
 * have to be converted to MinimalTuples).  This API works well for sorts
 * executed as parts of plan trees.
 *
 * The "cluster" API stores/sorts full HeapTuples including all visibility
 * info. The sort keys are specified by reference to a btree index that is
 * defined on the relation to be sorted.  Note that putheaptuple/getheaptuple
 * go with this API, not the "begin_heap" one!
 *
 * The "index_btree" API stores/sorts IndexTuples (preserving all their
 * header fields).  The sort keys are specified by a btree index definition.
 *
 * The "index_hash" API is similar to index_btree, but the tuples are
 * actually sorted by their hash codes not the raw data.
 *
 * Parallel sort callers are required to coordinate multiple tuplesort
 * states in a leader process, and one or more worker processes.  The
 * leader process must launch workers, and have each perform an
 * independent tuplesort, typically fed by the parallel heap interface.  The
 * leader later produces the final output (internally, it merges runs sorted
 * by workers).
 *
 * Note that callers may use the leader process to sort runs, as if it was
 * an independent worker process (prior to the process performing a leader
 * sort to produce the final sorted output).  Doing so only requires a
 * second state within the leader process, initialized like any worker
 * process.
 *
 * Callers must do the following to perform a sort in parallel using
 * multiple worker processes:
 *
 * 1.  Request tuplesort-private shared memory for n workers.  Use
 *     tuplesort_estimate_shared() to get the required size.
 * 2.  Have leader process initialize allocated shared memory using
 *     tuplesort_initialize_shared().  This assigns a unique identifier for
 *     the sort, among other things.
 * 3.  Initialize a "coordinate" argument (serial case just passes NULL
 *     here), within both the leader process, and for each worker process.
 *     Note that this has a pointer to the shared tuplesort-private
 *     structure.
 * 4.  Begin a tuplesort using some appropriate tuplesort_begin* routine,
 *     passing a "coordinate" argument, within each worker.  The workMem
 *     argument need not be identical.  All other arguments to the
 *     routine should be identical across workers and the leader.
 * 5.  Feed tuples to each worker, and call tuplesort_performsort() for each
 *     when input is exhausted.
 * 6.  Optionally, workers may aggregate information/statistics about the
 *     heap scan someplace; caller must handle all those details.  Then, call
 *     tuplesort_end() in each worker process.  There will be a wait before it
 *     returns.  Note that the leader process should not wait on itself just
 *     because it participates as a worker; call tuplesort_end() for the
 *     leader-as-worker Tuplesortstate last of all.
 * 7.  Begin a tuplesort in the leader using the same tuplesort_begin* routine,
 *     passing a leader-appropriate "coordinate" argument.  The leader must now
 *     wait for workers to finish; have the leader process wait for workers by
 *     calling tuplesort_leader_wait().  Note that the leader requires the
 *     number of workers actually launched now, so this need only happen after
 *     caller has established that number (after step 4).  Note also that the
 *     leader's workMem argument must be >= all worker workMem arguments.
 * 8.  Call tuplesort_performsort() in leader.  When this returns, sorting
 *     proper has completed.  Consume output using the appropriate
 *     tuplesort_get* routine as required.
 * 9.  Leader caller should optionally combine any data that may have been
 *     aggregated by workers in step 6.
 * 10. Call tuplesort_end() in leader.  Leader will release workers from
 *     their wait.
 *
 * This division of labor assumes nothing about how input tuples are produced,
 * but does require that caller combine the state of multiple tuplesorts for
 * any purpose other than producing the final output.  For example, callers
 * must consider that tuplesort_get_stats() reports on only one worker's role
 * in a sort (or the leader's role), and not statistics for the sort as a
 * whole.
 *
 * Note that there is an assumption that temp_tablespaces GUC matches across
 * processes.  Typically, this happens automatically because caller uses
 * parallel infrastructure.  Note also that only a very small amount of
 * memory will be allocated prior to the leader state first consuming input,
 * and that workers will free the vast majority of their memory upon
 * reaching a quiescent state.  Callers can rely on this to arrange for
 * memory to be consumed in a way that respects a workMem-style budget
 * across an entire sort operation, and not just within one backend.
 *
 * Callers are also responsible for parallel safety in general.  However, they
 * can at least rely on there being no parallel safety hazards within
 * tuplesort, because tuplesort conceptualizes the sort as several independent
 * sorts whose results are combined.  Since, in general, the behavior of sort
 * operators is immutable, caller need only worry about the parallel safety of
 * whatever the process is through which input tuples are generated
 * (typically, caller uses a parallel heap scan).
 */

extern Tuplesortstate *tuplesort_begin_heap(TupleDesc tupDesc,
					 int nkeys, AttrNumber *attNums,
					 Oid *sortOperators, Oid *sortCollations,
					 bool *nullsFirstFlags,
					 int workMem, SortCoordinate coordinate,
					 bool randomAccess);
extern Tuplesortstate *tuplesort_begin_cluster(TupleDesc tupDesc,
						Relation indexRel, int workMem,
						SortCoordinate coordinate, bool randomAccess);
extern Tuplesortstate *tuplesort_begin_index_btree(Relation heapRel,
							Relation indexRel,
							bool enforceUnique,
							int workMem, SortCoordinate coordinate,
							bool randomAccess);
extern Tuplesortstate *tuplesort_begin_index_hash(Relation heapRel,
						   Relation indexRel,
						   uint32 hash_mask, int workMem,
						   SortCoordinate coordinate, bool randomAccess);
extern Tuplesortstate *tuplesort_begin_datum(Oid datumType,
					  Oid sortOperator, Oid sortCollation,
					  bool nullsFirstFlag,
					  int workMem, SortCoordinate coordinate,
					  bool randomAccess);

extern void tuplesort_set_bound(Tuplesortstate *state, int64 bound);

extern void tuplesort_puttupleslot(Tuplesortstate *state,
					   TupleTableSlot *slot);
extern void tuplesort_putheaptuple(Tuplesortstate *state, HeapTuple tup);
extern void tuplesort_putindextuplevalues(Tuplesortstate *state,
							  Relation rel, ItemPointer self,
							  Datum *values, bool *isnull);
extern void tuplesort_putdatum(Tuplesortstate *state, Datum val,
				   bool isNull);

extern void tuplesort_performsort(Tuplesortstate *state);

extern bool tuplesort_gettupleslot(Tuplesortstate *state, bool forward,
					   TupleTableSlot *slot, Datum *abbrev);
extern HeapTuple tuplesort_getheaptuple(Tuplesortstate *state, bool forward,
					   bool *should_free);
extern IndexTuple tuplesort_getindextuple(Tuplesortstate *state, bool forward,
						bool *should_free);
extern bool tuplesort_getdatum(Tuplesortstate *state, bool forward,
				   Datum *val, bool *isNull, Datum *abbrev);

extern bool tuplesort_skiptuples(Tuplesortstate *state, int64 ntuples,
					 bool forward);

extern void tuplesort_end(Tuplesortstate *state);

extern void tuplesort_get_stats(Tuplesortstate *state,
					const char **sortMethod,
					const char **spaceType,
					long *spaceUsed);

extern int	tuplesort_merge_order(int64 allowedMem);

extern Size tuplesort_estimate_shared(int nworkers);
extern void tuplesort_initialize_shared(Sharedsort *shared, int nWorkers);
extern void tuplesort_leader_wait(Tuplesortstate *state);

/*
 * These routines may only be called if randomAccess was specified 'true'.
 * Likewise, backwards scan in gettuple/getdatum is only allowed if
 * randomAccess was specified.
 */

extern void tuplesort_rescan(Tuplesortstate *state);
extern void tuplesort_markpos(Tuplesortstate *state);
extern void tuplesort_restorepos(Tuplesortstate *state);

#endif   /* TUPLESORT_H */
