/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *		Tid (ItemPointerData) storage implementation.
 *
 * This module provides a in-memory data structure to store Tids (ItemPointer).
 * Internally, a tid is encoded as a pair of 64-bit key and 64-bit value, and
 * stored in the radix tree.
 *
 * A TidStore can be shared among parallel worker processes by passing DSA area
 * to tidstore_create(). Other backends can attach to the shared TidStore by
 * tidstore_attach().
 *
 * Regarding the concurrency, it basically relies on the concurrency support in
 * the radix tree, but we acquires the lock on a TidStore in some cases, for
 * example, when to reset the store and when to access the number tids in the
 * store (num_tids).
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/tidstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tidstore.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "lib/radixtree.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/*
 * For encoding purposes, tids are represented as a pair of 64-bit key and
 * 64-bit value. First, we construct 64-bit unsigned integer by combining
 * the block number and the offset number. The number of bits used for the
 * offset number is specified by max_offsets in tidstore_create(). We are
 * frugal with the bits, because smaller keys could help keeping the radix
 * tree shallow.
 *
 * For example, a tid of heap with 8kB blocks uses the lowest 9 bits for
 * the offset number and uses the next 32 bits for the block number. That
 * is, only 41 bits are used:
 *
 * uuuuuuuY YYYYYYYY YYYYYYYY YYYYYYYY YYYYYYYX XXXXXXXX
 *
 * X = bits used for offset number
 * Y = bits used for block number
 * u = unused bit
 * (high on the left, low on the right)
 *
 * 9 bits are enough for the offset number, because MaxHeapTuplesPerPage < 2^9
 * on 8kB blocks.
 *
 * The 64-bit value is the bitmap representation of the lowest 6 bits
 * (TIDSTORE_VALUE_NBITS) of the integer, and the rest 35 bits are used
 * as the key:
 *
 * uuuuuuuY YYYYYYYY YYYYYYYY YYYYYYYY YYYYYYYX XXXXXXXX
 *                                                |----| value
 * |---------------------------------------------| key
 *
 * The maximum height of the radix tree is 5 in this case.
 *
 * If the number of bits for offset number fits in a 64-bit value, we don't
 * encode tids but directly use the block number and the offset number as key
 * and value, respectively.
 */
#define TIDSTORE_VALUE_NBITS	6	/* log(64, 2) */

/* A magic value used to identify our TidStores. */
#define TIDSTORE_MAGIC 0x826f6a10

/* The control object for a TidStore */
typedef struct TidStoreControl
{
	/* the number of tids in the store */
	int64	num_tids;

	/* These values are never changed after creation */
	size_t	max_bytes;		/* the maximum bytes a TidStore can use */
	int		max_offset;		/* the maximum offset number */
	int		offset_nbits;	/* the number of bits required for max_offset */
	bool	encode_tids;	/* do we use tid encoding? */
	int		offset_key_nbits;	/* the number of bits of a offset number
								 * used for the key */

	/* The below fields are used only in shared case */

	uint32	magic;
	LWLock	lock;

	/* handles for TidStore and radix tree */
	tidstore_handle		handle;
} TidStoreControl;

/* Per-backend state for a TidStore */
struct TidStore
{
	/*
	 * Control object. This is allocated in DSA area 'area' in the shared
	 * case, otherwise in backend-local memory.
	 */
	TidStoreControl *control;

	/* Storage for Tids. Use either one depending on TidStoreIsShared() */
	union
	{
		radix_tree *local;
	} tree;
};

/* Iterator for TidStore */
typedef struct TidStoreIter
{
	TidStore	*ts;

	/* iterator of radix tree. Use either one depending on TidStoreIsShared() */
	union
	{
		rt_iter	*shared;
		rt_iter	*local;
	} tree_iter;

	/* we returned all tids? */
	bool		finished;

	/* save for the next iteration */
	uint64		next_key;
	uint64		next_val;

	/* output for the caller */
	TidStoreIterResult result;
} TidStoreIter;

static void tidstore_iter_extract_tids(TidStoreIter *iter, uint64 key, uint64 val);
static inline BlockNumber key_get_blkno(TidStore *ts, uint64 key);
static inline uint64 tid_to_key_off(TidStore *ts, ItemPointer tid, uint32 *off);

/*
 * Create a TidStore. The returned object is allocated in backend-local memory.
 * The radix tree for storage is allocated in DSA area is 'area' is non-NULL.
 */
TidStore *
tidstore_create(size_t max_bytes, int max_offset, dsa_area *area)
{
	TidStore	*ts;

	ts = palloc0(sizeof(TidStore));

	/*
	 * Create the radix tree for the main storage.
	 *
	 * Memory consumption depends on the number of stored tids, but also on the
	 * distribution of them, how the radix tree stores, and the memory management
	 * that backed the radix tree. The maximum bytes that a TidStore can
	 * use is specified by the max_bytes in tidstore_create(). We want the total
	 * amount of memory consumption by a TidStore not to exceed the max_bytes.
	 *
	 * In local TidStore cases, the radix tree uses slab allocators for each kind
	 * of node class. The most memory consuming case while adding Tids associated
	 * with one page (i.e. during tidstore_add_tids()) is that we allocate a new
	 * slab block for a new radix tree node, which is approximately 70kB. Therefore,
	 * we deduct 70kB from the max_bytes.
	 *
	 * In shared cases, DSA allocates the memory segments big enough to follow
	 * a geometric series that approximately doubles the total DSA size (see
	 * make_new_segment() in dsa.c). We simulated the how DSA increases segment
	 * size and the simulation revealed, the 75% threshold for the maximum bytes
	 * perfectly works in case where the max_bytes is a power-of-2, and the 60%
	 * threshold works for other cases.
	 */
	ts->tree.local = rt_create(CurrentMemoryContext);

	ts->control = (TidStoreControl *) palloc0(sizeof(TidStoreControl));
	ts->control->max_bytes = max_bytes - (70 * 1024);

	ts->control->max_offset = max_offset;
	ts->control->offset_nbits = pg_ceil_log2_32(max_offset);

	/*
	 * We use tid encoding if the number of bits for the offset number doesn't
	 * fix in a value, uint64.
	 */
	if (ts->control->offset_nbits > TIDSTORE_VALUE_NBITS)
	{
		ts->control->encode_tids = true;
		ts->control->offset_key_nbits =
			ts->control->offset_nbits - TIDSTORE_VALUE_NBITS;
	}
	else
	{
		ts->control->encode_tids = false;
		ts->control->offset_key_nbits = 0;
	}

	return ts;
}

/*
 * Destroy a TidStore, returning all memory.
 *
 * TODO: The caller must be certain that no other backend will attempt to
 * access the TidStore before calling this function. Other backend must
 * explicitly call tidstore_detach to free up backend-local memory associated
 * with the TidStore. The backend that calls tidstore_destroy must not call
 * tidstore_detach.
 */
void
tidstore_destroy(TidStore *ts)
{
	pfree(ts->control);
	rt_free(ts->tree.local);
	pfree(ts);
}

/*
 * Forget all collected Tids. It's similar to tidstore_destroy but we don't free
 * entire TidStore but recreate only the radix tree storage.
 */
void
tidstore_reset(TidStore *ts)
{
	rt_free(ts->tree.local);
	ts->tree.local = rt_create(CurrentMemoryContext);

	/* Reset the statistics */
	ts->control->num_tids = 0;
}

/* Add Tids on a block to TidStore */
void
tidstore_add_tids(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
				  int num_offsets)
{
	ItemPointerData tid;
	uint64	key_base;
	uint64	*values;
	int	nkeys;

	if (ts->control->encode_tids)
	{
		key_base = ((uint64) blkno) << ts->control->offset_key_nbits;
		nkeys = UINT64CONST(1) << ts->control->offset_key_nbits;
	}
	else
	{
		key_base = (uint64) blkno;
		nkeys = 1;
	}
	values = palloc0(sizeof(uint64) * nkeys);

	ItemPointerSetBlockNumber(&tid, blkno);
	for (int i = 0; i < num_offsets; i++)
	{
		uint64	key;
		uint32	off;
		int idx;

		ItemPointerSetOffsetNumber(&tid, offsets[i]);

		/* encode the tid to key and val */
		key = tid_to_key_off(ts, &tid, &off);

		idx = key - key_base;
		Assert(idx >= 0 && idx < nkeys);

		values[idx] |= UINT64CONST(1) << off;
	}

	/* insert the calculated key-values to the tree */
	for (int i = 0; i < nkeys; i++)
	{
		if (values[i])
		{
			uint64 key = key_base + i;

			rt_set(ts->tree.local, key, values[i]);
		}
	}

	/* update statistics */
	ts->control->num_tids += num_offsets;

	pfree(values);
}

/* Return true if the given tid is present in the TidStore */
bool
tidstore_lookup_tid(TidStore *ts, ItemPointer tid)
{
	uint64 key;
	uint64 val = 0;
	uint32 off;
	bool found;

	key = tid_to_key_off(ts, tid, &off);

	found = rt_search(ts->tree.local, key, &val);

	if (!found)
		return false;

	return (val & (UINT64CONST(1) << off)) != 0;
}

/*
 * Prepare to iterate through a TidStore. Since the radix tree is locked during the
 * iteration, so tidstore_end_iterate() needs to called when finished.
 *
 * Concurrent updates during the iteration will be blocked when inserting a
 * key-value to the radix tree.
 */
TidStoreIter *
tidstore_begin_iterate(TidStore *ts)
{
	TidStoreIter *iter;

	iter = palloc0(sizeof(TidStoreIter));
	iter->ts = ts;

	iter->result.blkno = InvalidBlockNumber;
	iter->result.offsets = palloc(sizeof(OffsetNumber) * ts->control->max_offset);

	iter->tree_iter.local = rt_begin_iterate(ts->tree.local);

	/* If the TidStore is empty, there is no business */
	if (tidstore_num_tids(ts) == 0)
		iter->finished = true;

	return iter;
}

static inline bool
tidstore_iter_kv(TidStoreIter *iter, uint64 *key, uint64 *val)
{
	return rt_iterate_next(iter->tree_iter.local, key, val);
}

/*
 * Scan the TidStore and return a pointer to TidStoreIterResult that has tids
 * in one block. We return the block numbers in ascending order and the offset
 * numbers in each result is also sorted in ascending order.
 */
TidStoreIterResult *
tidstore_iterate_next(TidStoreIter *iter)
{
	uint64 key;
	uint64 val;
	TidStoreIterResult *result = &(iter->result);

	if (iter->finished)
		return NULL;

	if (BlockNumberIsValid(result->blkno))
	{
		/* Process the previously collected key-value */
		result->num_offsets = 0;
		tidstore_iter_extract_tids(iter, iter->next_key, iter->next_val);
	}

	while (tidstore_iter_kv(iter, &key, &val))
	{
		BlockNumber blkno;

		blkno = key_get_blkno(iter->ts, key);

		if (BlockNumberIsValid(result->blkno) && result->blkno != blkno)
		{
			/*
			 * We got a key-value pair for a different block. So return the
			 * collected tids, and remember the key-value for the next iteration.
			 */
			iter->next_key = key;
			iter->next_val = val;
			return result;
		}

		/* Collect tids extracted from the key-value pair */
		tidstore_iter_extract_tids(iter, key, val);
	}

	iter->finished = true;
	return result;
}

/*
 * Finish an iteration over TidStore. This needs to be called after finishing
 * or when existing an iteration.
 */
void
tidstore_end_iterate(TidStoreIter *iter)
{
	rt_end_iterate(iter->tree_iter.local);

	pfree(iter->result.offsets);
	pfree(iter);
}

/* Return the number of tids we collected so far */
int64
tidstore_num_tids(TidStore *ts)
{
	return ts->control->num_tids;
}

/* Return true if the current memory usage of TidStore exceeds the limit */
bool
tidstore_is_full(TidStore *ts)
{
	return (tidstore_memory_usage(ts) > ts->control->max_bytes);
}

/* Return the maximum memory TidStore can use */
size_t
tidstore_max_memory(TidStore *ts)
{
	return ts->control->max_bytes;
}

/* Return the memory usage of TidStore */
size_t
tidstore_memory_usage(TidStore *ts)
{
	return sizeof(TidStore) + sizeof(TidStore) + rt_memory_usage(ts->tree.local);
}

/*
void
tidstore_stats(TidStore *ts)
{
#ifdef RT_DEBUG
	rt_stats(ts->tree.local);
#endif
}
*/

/*
 * Get a handle that can be used by other processes to attach to this TidStore
 */
tidstore_handle
tidstore_get_handle(TidStore *ts)
{

	return ts->control->handle;
}

/* Extract tids from the given key-value pair */
static void
tidstore_iter_extract_tids(TidStoreIter *iter, uint64 key, uint64 val)
{
	TidStoreIterResult *result = (&iter->result);

	for (int i = 0; i < sizeof(uint64) * BITS_PER_BYTE; i++)
	{
		uint64	tid_i;
		OffsetNumber	off;

		if (i > iter->ts->control->max_offset)
		{
			Assert(!iter->ts->control->encode_tids);
			break;
		}

		if ((val & (UINT64CONST(1) << i)) == 0)
			continue;

		tid_i = key << TIDSTORE_VALUE_NBITS;
		tid_i |= i;

		off = tid_i & ((UINT64CONST(1) << iter->ts->control->offset_nbits) - 1);

		Assert(result->num_offsets < iter->ts->control->max_offset);
		result->offsets[result->num_offsets++] = off;
	}

	result->blkno = key_get_blkno(iter->ts, key);
}

/* Get block number from the given key */
static inline BlockNumber
key_get_blkno(TidStore *ts, uint64 key)
{
	if (ts->control->encode_tids)
		return (BlockNumber) (key >> ts->control->offset_key_nbits);

	return (BlockNumber) key;
}

/* Encode a tid to key and offset */
static inline uint64
tid_to_key_off(TidStore *ts, ItemPointer tid, uint32 *off)
{
	uint64 key;
	uint64 tid_i;

	if (!ts->control->encode_tids)
	{
		*off = ItemPointerGetOffsetNumber(tid);

		/* Use the block number as the key */
		return (int64) ItemPointerGetBlockNumber(tid);
	}

	tid_i = ItemPointerGetOffsetNumber(tid);
	tid_i |= (uint64) ItemPointerGetBlockNumber(tid) << ts->control->offset_nbits;

	*off = tid_i & ((UINT64CONST(1) << TIDSTORE_VALUE_NBITS) - 1);
	key = tid_i >> TIDSTORE_VALUE_NBITS;
	Assert(*off < (sizeof(uint64) * BITS_PER_BYTE));

	return key;
}