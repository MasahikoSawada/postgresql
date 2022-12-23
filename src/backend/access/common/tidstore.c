/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *		Tid (ItemPointerData) storage implementation.
 *
 * This module provides a in-memory data structure to store Tids (ItemPointer).
 * Internally, Tid are encoded as a pair of 64-bit key and 64-bit value, and
 * stored in the radix tree.
 *
 * A TidStore can be shared among parallel worker processes by passing DSA area
 * to tidstore_create(). Other backends can attach to the shared TidStore by
 * tidstore_attach(). It can support concurrent updates but only one process
 * is allowed to iterate over the TidStore at a time.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/tidstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tidstore.h"
#include "lib/radixtree.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/*
 * For encoding purposes, item pointers are represented as a pair of 64-bit
 * key and 64-bit value. First, we construct 64-bit unsigned integer key that
 * combines the block number and the offset number. The lowest 11 bits represent
 * the offset number, and the next 32 bits are block number. That is, only 43
 * bits are used:
 *
 * XXXXXXXX XXXYYYYY YYYYYYYY YYYYYYYY YYYYYYYY YYYuuuu
 *
 * X = bits used for offset number
 * Y = bits used for block number
 * u = unused bit
 *
 * 11 bits enough for the offset number, because MaxHeapTuplesPerPage < 2^11
 * on all supported block sizes (TidSTORE_OFFSET_NBITS). We are frugal with
 * the bits, because smaller keys could help keeping the radix tree shallow.
 *
 * XXX: If we want to support other table AMs that want to use the full range
 * of possible offset numbers, we'll need to change this.
 *
 * The 64-bit value is the bitmap representation of the lowest 6 bits, and
 * the rest 37 bits are used as the key:
 *
 * value = bitmap representation of XXXXXX
 * key = XXXXXYYY YYYYYYYY YYYYYYYY YYYYYYYY YYYYYuu
 *
 * The maximum height of the radix tree is 5.
 *
 * XXX: if we want to support non-heap table AM, we need to reconsider
 * TIDSTORE_OFFSET_NBITS value.
 */
#define TIDSTORE_OFFSET_NBITS	11
#define TIDSTORE_VALUE_NBITS	6

/*
 * Memory consumption depends on the number of Tids stored, but also on the
 * distribution of them and how the radix tree stores them. The maximum bytes
 * that a TidStore can use is specified by the max_bytes in tidstore_create().
 *
 * In non-shared cases, the radix tree uses a slab allocator for each kind of
 * node class. The most memory consuming case while adding Tids associated
 * with one page (i.e. during tidstore_add_tids()) is that we allocate the
 * largest radix tree node in a new slab block, which is approximately 70kB.
 * Therefore, we deduct 70kB from the maximum bytes.
 *
 * In shared cases, DSA allocates the memory segments to bit enough to follow
 * a geometric series that approximately doubles the total DSA size. So we
 * limit the maximum bytes for a TidStore to 75%. The 75% threshold perfectly
 * works in case where the maximum bytes is power-of-2. In other cases, it's
 * possible that the amount of the total allocated DSA segment exceeds the
 * maximum bytes, but it's not a common case.
 */
#define TIDSTORE_MEMORY_DEDUCT_BYTES (1024L * 70) /* 70kB */

/* Get block number from the key */
#define KEY_GET_BLKNO(key) \
	((BlockNumber) ((key) >> (TIDSTORE_OFFSET_NBITS - TIDSTORE_VALUE_NBITS)))

/* A magic value used to identify our TidStores. */
#define TIDSTORE_MAGIC 0x826f6a10

/* The header object for a TidStore */
typedef struct TidStoreControl
{
	/*
	 * 'num_tids' is the number of Tids stored so far. 'max_byte' is the maximum
	 * bytes a TidStore can use. These two fields are commonly used in both
	 * non-shared case and shared case.
	 */
	uint32	num_tids;
	uint64	max_bytes;

	/* The below fields are used only in shared case */

	uint32	magic;

	/* handles for TidStore and radix tree */
	tidstore_handle handle;
	rt_handle	tree_handle;
} TidStoreControl;

/* Per-backend state for a TidStore */
struct TidStore
{
	/*
	 * Control object. This is allocated in DSA area 'area' in the shared
	 * case, otherwise in backend-local memory.
	 */
	TidStoreControl *control;

	/* Storage for Tids */
	radix_tree	*tree;

	/* DSA area for TidStore if used */
	dsa_area	*area;
};
#define TidStoreIsShared(ts) ((ts)->area != NULL)

/* Iterator for TidStore */
typedef struct TidStoreIter
{
	TidStore	*ts;

	/* iterator of radix tree */
	rt_iter		*tree_iter;

	/* we returned all tids? */
	bool		finished;

	/* save for the next iteration */
	uint64		next_key;
	uint64		next_val;

	/* output for the caller */
	TidStoreIterResult result;
} TidStoreIter;

static void tidstore_iter_extract_tids(TidStoreIter *iter, uint64 key, uint64 val);
static inline uint64 tid_to_key_off(ItemPointer tid, uint32 *off);

/*
 * Create a TidStore. The returned object is allocated in backend-local memory.
 * The radix tree for storage is allocated in DSA area is 'area' is non-NULL.
 */
TidStore *
tidstore_create(uint64 max_bytes, dsa_area *area)
{
	TidStore	*ts;

	ts = palloc0(sizeof(TidStore));

	/* Create the radix tree for the main storage */
	ts->tree = rt_create(CurrentMemoryContext, area);

	/*
	 * We calculate the maximum bytes for the TidStore in different ways
	 * for non-shared case and shared case. Please refer to the comment
	 * TIDSTORE_MEMORY_DEDUCT for details.
	 */
	if (area != NULL)
	{
		dsa_pointer dp;

		dp = dsa_allocate0(area, sizeof(TidStoreControl));
		ts->control = (TidStoreControl *) dsa_get_address(area, dp);
		ts->control->max_bytes = (uint64) (max_bytes * 0.75);
		ts->area = area;

		ts->control->magic = TIDSTORE_MAGIC;
		ts->control->handle = dp;
		ts->control->tree_handle = rt_get_handle(ts->tree);
	}
	else
	{
		ts->control = (TidStoreControl *) palloc0(sizeof(TidStoreControl));
		ts->control->max_bytes = max_bytes - TIDSTORE_MEMORY_DEDUCT_BYTES;
	}

	return ts;
}

/*
 * Attach to the shared TidStore using a handle. The returned object is
 * allocated in backend-local memory using the CurrentMemoryContext.
 */
TidStore *
tidstore_attach(dsa_area *area, tidstore_handle handle)
{
	TidStore *ts;
	dsa_pointer control;

	Assert(area != NULL);
	Assert(DsaPointerIsValid(handle));

	/* create per-backend state */
	ts = palloc0(sizeof(TidStore));

	/* Find the control object in shared memory */
	control = handle;

	/* Set up the TidStore */
	ts->control = (TidStoreControl *) dsa_get_address(area, control);
	Assert(ts->control->magic == TIDSTORE_MAGIC);

	ts->tree = rt_attach(area, ts->control->tree_handle);
	ts->area = area;

	return ts;
}

/*
 * Detach from a TidStore. This detaches from radix tree and frees the
 * backend-local resources. The radix tree will continue to exist until
 * it is either explicitly destroyed, or the area that backs it is returned
 * to the operating system.
 */
void
tidstore_detach(TidStore *ts)
{
	Assert(TidStoreIsShared(ts) && ts->control->magic == TIDSTORE_MAGIC);

	rt_detach(ts->tree);
	pfree(ts);
}

/*
 * Destroy a TidStore, returning all memory. The caller must be certain that
 * no other backend will attempt to access the TidStore before calling this
 * function. Other backend must explicitly call tidstore_detach to free up
 * backend-local memory associated with the TidStore. The backend that calls
 * tidstore_destroy must not call tidstore_detach.
 */
void
tidstore_destroy(TidStore *ts)
{
	if (TidStoreIsShared(ts))
	{
		Assert(ts->control->magic == TIDSTORE_MAGIC);

		/*
		 * Vandalize the control block to help catch programming error where
		 * other backends access the memory formerly occupied by this radix tree.
		 */
		ts->control->magic = 0;
		dsa_free(ts->area, ts->control->handle);
	}
	else
		pfree(ts->control);

	rt_free(ts->tree);
	pfree(ts);
}

/* Forget all collected Tids */
void
tidstore_reset(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	/* Reset the statistics */
	ts->control->num_tids = 0;

	/*
	 * Free the current radix tree, and Return allocated DSM segments
	 * to the operating system, if necessary. */
	rt_free(ts->tree);
	if (TidStoreIsShared(ts))
		dsa_trim(ts->area);

	/* Recreate the radix tree */
	ts->tree = rt_create(CurrentMemoryContext, ts->area);

	if (TidStoreIsShared(ts))
	{
		/* update the radix tree handle as we recreated it */
		ts->control->tree_handle = rt_get_handle(ts->tree);
	}
}

/*
 * Add Tids on a block to TidStore. The caller must ensure the offset numbers
 * in 'offsets' are ordered in ascending order.
 */
void
tidstore_add_tids(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
				  int num_offsets)
{
	uint64 last_key = PG_UINT64_MAX;
	uint64 key;
	uint64 val = 0;
	ItemPointerData tid;

	ItemPointerSetBlockNumber(&tid, blkno);

	for (int i = 0; i < num_offsets; i++)
	{
		uint32	off;

		ItemPointerSetOffsetNumber(&tid, offsets[i]);

		key = tid_to_key_off(&tid, &off);

		if (last_key != PG_UINT64_MAX && last_key != key)
		{
			/* insert the key-value */
			rt_set(ts->tree, last_key, val);
			val = 0;
		}

		last_key = key;
		val |= UINT64CONST(1) << off;
	}

	if (last_key != PG_UINT64_MAX)
	{
		/* insert the key-value */
		rt_set(ts->tree, last_key, val);
	}

	/* update statistics */
	ts->control->num_tids += num_offsets;
}

/* Return true if the given Tid is present in TidStore */
bool
tidstore_lookup_tid(TidStore *ts, ItemPointer tid)
{
	uint64 key;
	uint64 val;
	uint32 off;
	bool found;

	key = tid_to_key_off(tid, &off);

	found = rt_search(ts->tree, key, &val);

	if (!found)
		return false;

	return (val & (UINT64CONST(1) << off)) != 0;
}

/*
 * Prepare to iterate through a TidStore. The caller must be certain that
 * no other backend will attempt to update the TidStore during the iteration.
 */
TidStoreIter *
tidstore_begin_iterate(TidStore *ts)
{
	TidStoreIter *iter;

	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	iter = palloc0(sizeof(TidStoreIter));
	iter->ts = ts;
	iter->tree_iter = rt_begin_iterate(ts->tree);
	iter->result.blkno = InvalidBlockNumber;

	/* If the TidStore is empty, there is no business */
	if (ts->control->num_tids == 0)
		iter->finished = true;

	return iter;
}

/*
 * Scan the TidStore and return a TidStoreIterResult representing Tids
 * in one page. Offset numbers in the result is sorted.
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
		result->num_offsets = 0;
		tidstore_iter_extract_tids(iter, iter->next_key, iter->next_val);
	}

	while (rt_iterate_next(iter->tree_iter, &key, &val))
	{
		BlockNumber blkno;

		blkno = KEY_GET_BLKNO(key);

		if (BlockNumberIsValid(result->blkno) && result->blkno != blkno)
		{
			/*
			 * Remember the key-value pair for the next block for the
			 * next iteration.
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

/* Finish an iteration over TidStore */
void
tidstore_end_iterate(TidStoreIter *iter)
{
	pfree(iter);
}

/* Return the number of Tids we collected so far */
uint64
tidstore_num_tids(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	return ts->control->num_tids;
}

/* Return true if the current memory usage of TidStore exceeds the limit */
bool
tidstore_is_full(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	return (tidstore_memory_usage(ts) > ts->control->max_bytes);
}

/* Return the maximum memory TidStore can use */
uint64
tidstore_max_memory(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	return ts->control->max_bytes;
}

/* Return the memory usage of TidStore */
uint64
tidstore_memory_usage(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	/*
	 * In the shared case, TidStoreControl and radix_tree are backed by the
	 * same DSA area and rt_memory_usage() returns the value including both.
	 * So we don't need to add the size of TidStoreControl separately.
	 */
	if (TidStoreIsShared(ts))
		return (uint64) sizeof(TidStore) + rt_memory_usage(ts->tree);
	else
		return (uint64) sizeof(TidStore) + sizeof(TidStore) +
			rt_memory_usage(ts->tree);
}

/*
 * Get a handle that can be used by other processes to attach to this TidStore
 */
tidstore_handle
tidstore_get_handle(TidStore *ts)
{
	Assert(TidStoreIsShared(ts) && ts->control->magic == TIDSTORE_MAGIC);

	return ts->control->handle;
}

/* Extract Tids from key-value pair */
static void
tidstore_iter_extract_tids(TidStoreIter *iter, uint64 key, uint64 val)
{
	TidStoreIterResult *result = (&iter->result);

	for (int i = 0; i < sizeof(uint64) * BITS_PER_BYTE; i++)
	{
		uint64	tid_i;
		OffsetNumber	off;

		if ((val & (UINT64CONST(1) << i)) == 0)
			continue;

		tid_i = key << TIDSTORE_VALUE_NBITS;
		tid_i |= i;

		off = tid_i & ((UINT64CONST(1) << TIDSTORE_OFFSET_NBITS) - 1);
		result->offsets[result->num_offsets++] = off;
	}

	result->blkno = KEY_GET_BLKNO(key);
}

/*
 * Encode a Tid to key and val.
 */
static inline uint64
tid_to_key_off(ItemPointer tid, uint32 *off)
{
	uint64 upper;
	uint64 tid_i;

	tid_i = ItemPointerGetOffsetNumber(tid);
	tid_i |= (uint64) ItemPointerGetBlockNumber(tid) << TIDSTORE_OFFSET_NBITS;

	*off = tid_i & ((1 << TIDSTORE_VALUE_NBITS) - 1);
	upper = tid_i >> TIDSTORE_VALUE_NBITS;
	Assert(*off < (sizeof(uint64) * BITS_PER_BYTE));

	return upper;
}
