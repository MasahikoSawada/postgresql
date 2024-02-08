/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *		Tid (ItemPointerData) storage implementation.
 *
 * TidStore is a in-memory data structure to store TIDs (ItemPointerData).
 * Internally it uses a radix tree as the storage for TIDs. The key is the
 * BlockNumber and the value is a bitmap of offsets, BlocktableEntry.
 *
 * TidStore can be shared among parallel worker processes by passing DSA area
 * to TidStoreCreate(). Other backends can attach to the shared TidStore by
 * TidStoreAttach().
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
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
#include "nodes/bitmapset.h"
#include "port/pg_bitutils.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"


#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

/* number of active words for a page: */
#define WORDS_PER_PAGE(n) ((n) / BITS_PER_BITMAPWORD + 1)

/*
 * This is named similarly to PagetableEntry in tidbitmap.c
 * because the two have a similar function.
 */
typedef struct BlocktableEntry
{
	uint16		nwords;
	bitmapword	words[FLEXIBLE_ARRAY_MEMBER];
} BlocktableEntry;
#define MaxBlocktableEntrySize \
	offsetof(BlocktableEntry, words) + \
		(sizeof(bitmapword) * WORDS_PER_PAGE(MaxOffsetNumber))

#define RT_PREFIX local_rt
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#define RT_VARLEN_VALUE
#include "lib/radixtree.h"

#define RT_PREFIX shared_rt
#define RT_SHMEM
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#define RT_VARLEN_VALUE
#include "lib/radixtree.h"

/* Per-backend state for a TidStore */
struct TidStore
{
	/* MemoryContext where the TidStore is allocated */
	MemoryContext	context;

	/* Storage for TIDs. Use either one depending on TidStoreIsShared() */
	union
	{
		local_rt_radix_tree *local;
		shared_rt_radix_tree *shared;
	} tree;

	/* DSA area for TidStore if using shared memory */
	dsa_area	*area;
};
#define TidStoreIsShared(ts) ((ts)->area != NULL)

/* Iterator for TidStore */
typedef struct TidStoreIter
{
	TidStore	*ts;

	/* iterator of radix tree. Use either one depending on TidStoreIsShared() */
	union
	{
		shared_rt_iter	*shared;
		local_rt_iter	*local;
	} tree_iter;

	/*
	 * output for the caller. Must be last because variable-size.
	 */
	TidStoreIterResult output;
} TidStoreIter;

static void tidstore_iter_extract_tids(TidStoreIter *iter, BlocktableEntry *page);

/*
 * Create a TidStore. The returned object is allocated in backend-local memory.
 * The radix tree for storage is allocated in DSA area if 'area' is non-NULL.
 */
TidStore *
TidStoreCreate(size_t max_bytes, dsa_area *area)
{
	TidStore	*ts;

	ts = palloc0(sizeof(TidStore));
	ts->context = CurrentMemoryContext;

	if (area != NULL)
	{
		ts->tree.shared = shared_rt_create(ts->context, max_bytes, area,
										   LWTRANCHE_SHARED_TIDSTORE);
		ts->area = area;
	}
	else
		ts->tree.local = local_rt_create(ts->context, max_bytes);

	return ts;
}

/*
 * Attach to the shared TidStore using a handle. The returned object is
 * allocated in backend-local memory using the CurrentMemoryContext.
 */
TidStore *
TidStoreAttach(dsa_area *area, dsa_pointer handle)
{
	TidStore *ts;

	Assert(area != NULL);
	Assert(DsaPointerIsValid(handle));

	/* create per-backend state */
	ts = palloc0(sizeof(TidStore));

	/* Find the shared the shared radix tree */
	ts->tree.shared = shared_rt_attach(area, handle);
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
TidStoreDetach(TidStore *ts)
{
	Assert(TidStoreIsShared(ts));

	shared_rt_detach(ts->tree.shared);
	pfree(ts);
}

/*
 * Lock support functions.
 *
 * We can use the radix tree's lock for shared tidstore as the data we
 * need to protect is only the shared radix tree.
 */
void
TidStoreLockExclusive(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_rt_lock_exclusive(ts->tree.shared);
}

void
TidStoreLockShare(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_rt_lock_share(ts->tree.shared);
}

void
TidStoreUnlock(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_rt_unlock(ts->tree.shared);
}

/*
 * Destroy a TidStore, returning all memory.
 *
 * Note that the caller must be certain that no other backend will attempt to
 * access the TidStore before calling this function. Other backend must
 * explicitly call TidStoreDetach() to free up backend-local memory associated
 * with the TidStore. The backend that calls TidStoreDestroy() must not call
 * TidStoreDetach().
 */
void
TidStoreDestroy(TidStore *ts)
{
	/* Destroy underlying radix tree */
	if (TidStoreIsShared(ts))
		shared_rt_free(ts->tree.shared);
	else
		local_rt_free(ts->tree.local);

	pfree(ts);
}

/*
 * Set the given tids on the blkno to TidStore.
 *
 * NB: the offset numbers in offsets must be sorted in ascending order.
 */
void
TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
						int num_offsets)
{
	char	data[MaxBlocktableEntrySize];
	BlocktableEntry *page = (BlocktableEntry *) data;
	bitmapword word;
	int		wordnum;
	int next_word_threshold;
	int idx = 0;
	size_t page_len;
	bool	found PG_USED_FOR_ASSERTS_ONLY;

	Assert(num_offsets > 0);


	for (wordnum = 0, next_word_threshold = BITS_PER_BITMAPWORD;
		wordnum <= WORDNUM(offsets[num_offsets - 1]);
		wordnum++, next_word_threshold += BITS_PER_BITMAPWORD)
	{
		word = 0;

		while(idx < num_offsets)
		{
			OffsetNumber off = offsets[idx];

			/* safety check to ensure we don't overrun bit array bounds */
			if (!OffsetNumberIsValid(off))
				elog(ERROR, "tuple offset out of range: %u", off);

			if (off >= next_word_threshold)
				break;

			word |= ((bitmapword) 1 << BITNUM(off));
			idx++;
		}

		/* write out offset bitmap for this wordnum */
		page->words[wordnum] = word;
	}

	page->nwords = wordnum + 1;
	Assert(page->nwords = WORDS_PER_PAGE(offsets[num_offsets - 1]));

	page_len = offsetof(BlocktableEntry, words) +
		sizeof(bitmapword) * page->nwords;

	if (TidStoreIsShared(ts))
		found = shared_rt_set(ts->tree.shared, blkno, (BlocktableEntry *) page,
							  page_len);
	else
		found = local_rt_set(ts->tree.local, blkno, (BlocktableEntry *) page,
							 page_len);

	Assert(!found);
}

/* Return true if the given tid is present in the TidStore */
bool
TidStoreIsMember(TidStore *ts, ItemPointer tid)
{
	int wordnum;
	int bitnum;
	BlocktableEntry *page;
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	bool ret;

	if (TidStoreIsShared(ts))
		page = shared_rt_find(ts->tree.shared, blk);
	else
		page = local_rt_find(ts->tree.local, blk);

	/* no entry for the blk */
	if (page == NULL)
		return false;

	wordnum = WORDNUM(off);
	bitnum = BITNUM(off);

	/* no bitmap for the off */
	if (wordnum >= page->nwords)
		return false;

	ret = (page->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0;

	return ret;
}

/*
 * Prepare to iterate through a TidStore. Since the radix tree is locked during
 * the iteration, so TidStoreEndIterate() needs to be called when finished.
 *
 * The TidStoreIter struct is created in the caller's memory context.
 *
 * Concurrent updates during the iteration will be blocked when inserting a
 * key-value to the radix tree.
 */
TidStoreIter *
TidStoreBeginIterate(TidStore *ts)
{
	TidStoreIter *iter;

	iter = palloc0(sizeof(TidStoreIter));
	iter->ts = ts;

	/*
	 * We start with an array large enough to contain at least the offsets
	 * from one completely full bitmap element.
	 */
	iter->output.max_offset = 2 * BITS_PER_BITMAPWORD;
	iter->output.offsets = palloc(sizeof(OffsetNumber) * iter->output.max_offset);

	if (TidStoreIsShared(ts))
		iter->tree_iter.shared = shared_rt_begin_iterate(ts->tree.shared);
	else
		iter->tree_iter.local = local_rt_begin_iterate(ts->tree.local);

	return iter;
}


/*
 * Scan the TidStore and return a pointer to TidStoreIterResult that has tids
 * in one block. We return the block numbers in ascending order and the offset
 * numbers in each result is also sorted in ascending order.
 */
TidStoreIterResult *
TidStoreIterateNext(TidStoreIter *iter)
{
	uint64 key;
	BlocktableEntry *page;
	TidStoreIterResult *result = &(iter->output);

	if (TidStoreIsShared(iter->ts))
		page =  shared_rt_iterate_next(iter->tree_iter.shared, &key);
	else
		page = local_rt_iterate_next(iter->tree_iter.local, &key);

	if (page == NULL)
		return NULL;

	/* Collect tids extracted from the key-value pair */
	result->num_offsets = 0;

	tidstore_iter_extract_tids(iter, page);
	result->blkno = key;

	return result;
}

/*
 * Finish an iteration over TidStore. This needs to be called after finishing
 * or when existing an iteration.
 */
void
TidStoreEndIterate(TidStoreIter *iter)
{
	if (TidStoreIsShared(iter->ts))
		shared_rt_end_iterate(iter->tree_iter.shared);
	else
		local_rt_end_iterate(iter->tree_iter.local);

	pfree(iter);
}

/* Return the memory usage of TidStore */
size_t
TidStoreMemoryUsage(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		return shared_rt_memory_usage(ts->tree.shared);
	else
		return local_rt_memory_usage(ts->tree.local);
}

dsa_pointer
TidStoreGetHandle(TidStore *ts)
{
	Assert(TidStoreIsShared(ts));

	return (dsa_pointer) shared_rt_get_handle(ts->tree.shared);
}

/* Extract tids from the given key-value pair */
static void
tidstore_iter_extract_tids(TidStoreIter *iter, BlocktableEntry *page)
{
	TidStoreIterResult *result = (&iter->output);
	int			wordnum;

	for (wordnum = 0; wordnum < page->nwords; wordnum++)
	{
		bitmapword	w = page->words[wordnum];

		/* Make sure there is enough space to add offsets */
		if ((result->num_offsets + BITS_PER_BITMAPWORD) > result->max_offset)
		{
			result->max_offset *= 2;
			result->offsets = repalloc(result->offsets,
									   sizeof(OffsetNumber) * result->max_offset);
		}

		while (w != 0)
		{
			/* get pos of rightmost bit */
			int bitnum = bmw_rightmost_one_pos(w);
			int			off = wordnum * BITS_PER_BITMAPWORD + bitnum;

			result->offsets[result->num_offsets++] = off;

			/* unset the rightmost bit */
			w &= w - 1;
		}
	}
}
