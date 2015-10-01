/*-------------------------------------------------------------------------
 *
 * amcheck.c
 *		Verifies the integrity of access methods based on invariants.
 *
 * Currently, only checks for the nbtree AM are provided.  Provides
 * SQL-callable functions for verifying that various invariants in the
 * structure of nbtree indexes are respected.  This includes for example the
 * invariant that each page with a high key has data items bound by the high
 * key.  Some functions also check invariant conditions that must hold across
 * multiple pages, such as the requirement each left link within a page (if
 * any) should point to a page that has as its right link a pointer to the
 * page.
 *
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/amcheck.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


PG_MODULE_MAGIC;

#define CHECK_SUPERUSER() { \
		if (!superuser()) \
			ereport(ERROR, \
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), \
					 (errmsg("must be superuser to use verification functions")))); }
#define CHECK_RELATION_BTREE(r) { \
		if ((r)->rd_rel->relkind != RELKIND_INDEX || (r)->rd_rel->relam != BTREE_AM_OID) \
			elog(ERROR, "relation \"%s\" is not a btree index", \
				 RelationGetRelationName(r)); }
/* reject attempts to read non-local temporary relations */
#define CHECK_RELATION_IS_NOT_OTHER_TEMP(rel) { \
		if (RELATION_IS_OTHER_TEMP(rel)) \
			ereport(ERROR, \
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
					 errmsg("cannot access temporary tables of other sessions"))); }
/* note: BlockNumber is unsigned, hence can't be negative */
#define CHECK_RELATION_BLOCK_RANGE(rel, blkno) { \
		if (blkno == 0) \
			elog(ERROR, "block 0 is a meta page"); \
		if (RelationGetNumberOfBlocks(rel) <= (BlockNumber) (blkno) ) \
			 elog(ERROR, "block number out of range"); }

/*
 * Callers to verification functions can reasonably expect to never receive a
 * warning.  Therefore, when using amcheck functions for stress testing it
 * may be useful to temporally change CHCKELEVEL to PANIC, to immediately halt
 * the server in the event of detecting an invariant condition violation.  This
 * may preserve more information about the nature of the underlying problem.
 */
#define CHCKELEVEL		WARNING

PG_FUNCTION_INFO_V1(bt_page_verify);
PG_FUNCTION_INFO_V1(bt_parent_page_verify);
PG_FUNCTION_INFO_V1(bt_index_verify);
PG_FUNCTION_INFO_V1(bt_parent_index_verify);
PG_FUNCTION_INFO_V1(bt_leftright_verify);

static void rangevar_callback_for_childcheck(const RangeVar *relation,
											 Oid relId, Oid oldRelId,
											 void *arg);
static void bt_do_page_verify(Relation rel, BlockNumber blockNum,
							  bool childCheck);
static ScanKey first_item_right_page(Relation rel, BlockNumber rightblockNum,
									 Page *rpage);
static ScanKey verify_btree_items_le(Relation rel, Page childPage,
									 ScanKey downLinkParent, IndexTuple pItup);
static BlockNumber verify_leftright_level(Relation rel, BlockNumber leftMost);
static char *form_btree_data(IndexTuple itup);

/*
 * Verify specified B-Tree block/page.
 *
 * Only acquires AccessShareLock on index relation.
 */
Datum
bt_page_verify(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_P(0);
	uint32		blkno = PG_GETARG_UINT32(1);
	Relation	rel;
	RangeVar   *relrv;

	CHECK_SUPERUSER();

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv_extended(relrv, AccessShareLock, true);

	if (rel == NULL)
		PG_RETURN_VOID();

	CHECK_RELATION_BTREE(rel);

	CHECK_RELATION_IS_NOT_OTHER_TEMP(rel);

	/* Don't proceed until index is at least ready for insertions */
	if (!rel->rd_index->indisready)
		PG_RETURN_VOID();

	CHECK_RELATION_BLOCK_RANGE(rel, blkno);

	bt_do_page_verify(rel, blkno, false);

	relation_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * Verify specified B-Tree parent block/page.  Make sure that each child page
 * has as its right link the page that the parent page has as the next
 * down-link, and other checks between parent and child pages when examining a
 * parent page.
 *
 * Acquires ExclusiveLock on index relation, and ShareLock on the associated
 * heap relation, somewhat like REINDEX.
 */
Datum
bt_parent_page_verify(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_P(0);
	uint32		blkno = PG_GETARG_UINT32(1);
	Relation	iRel, heapRel;
	RangeVar   *relrv;
	Oid			indexId, heapId = InvalidOid;

	CHECK_SUPERUSER();

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));

	/*
	 * Open the target index relation and get an exclusive lock on it, to
	 * ensure that no one else is touching this particular index.
	 */
	indexId = RangeVarGetRelidExtended(relrv, ExclusiveLock, true, false,
									   rangevar_callback_for_childcheck,
									   (void *) &heapId);

	if (indexId == InvalidOid)
		PG_RETURN_VOID();

	/*
	 * Open the target index relations separately (like relation_openrv() but
	 * with heap relation locked first to prevent deadlocking)
	 */
	heapRel = heap_open(heapId, NoLock);
	iRel = index_open(indexId, NoLock);

	/* check for active uses of the index in the current transaction */
	CheckTableNotInUse(iRel, "bt_parent_page_verify");

	CHECK_RELATION_BTREE(iRel);

	CHECK_RELATION_IS_NOT_OTHER_TEMP(iRel);

	/* Don't proceed until index is at least ready for insertions */
	if (!iRel->rd_index->indisready)
		PG_RETURN_VOID();

	CHECK_RELATION_BLOCK_RANGE(iRel, blkno);

	/*
	 * XXX: It might be a good idea to throw an error if this isn't an internal
	 * page for this variant
	 */
	bt_do_page_verify(iRel, blkno, true);

	index_close(iRel, ExclusiveLock);
	heap_close(heapRel, ShareLock);

	PG_RETURN_VOID();
}

/*
 * Verify entire index, without looking at cross-page invariant conditions.
 *
 * Only acquires AccessShareLock on index relation.
 */
Datum
bt_index_verify(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_P(0);
	Relation	rel;
	RangeVar   *relrv;
	BlockNumber i;
	BlockNumber nBlocks;

	CHECK_SUPERUSER();

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv_extended(relrv, AccessShareLock, true);

	if (rel == NULL)
		PG_RETURN_VOID();


	CHECK_RELATION_BTREE(rel);

	CHECK_RELATION_IS_NOT_OTHER_TEMP(rel);

	/* Don't proceed until index is at least ready for insertions */
	if (!rel->rd_index->indisready)
		PG_RETURN_VOID();

	nBlocks = RelationGetNumberOfBlocks(rel);

	/* Verify every page */
	for (i = 1; i < nBlocks; i++)
	{
		bt_do_page_verify(rel, i, false);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * Verify entire index, considering cross-page invariant conditions between
 * parent and child pages.
 *
 * Acquires ExclusiveLock on index relation, and ShareLock on the associated
 * heap relation, somewhat like REINDEX.
 */
Datum
bt_parent_index_verify(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_P(0);
	RangeVar   *relrv;
	BlockNumber i;
	BlockNumber nBlocks;
	Relation	iRel, heapRel;
	Oid			indexId, heapId = InvalidOid;

	CHECK_SUPERUSER();

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));

	/*
	 * Open the target index relation and get an exclusive lock on it, to
	 * ensure that no one else is touching this particular index.
	 */
	indexId = RangeVarGetRelidExtended(relrv, ExclusiveLock, true, false,
									   rangevar_callback_for_childcheck,
									   (void *) &heapId);

	if (indexId == InvalidOid)
		PG_RETURN_VOID();

	/*
	 * Open the target index relations separately (like relation_openrv() but
	 * with heap relation locked first to prevent deadlocking)
	 */
	heapRel = heap_open(heapId, NoLock);
	iRel = index_open(indexId, NoLock);

	/* check for active uses of the index in the current transaction */
	CheckTableNotInUse(iRel, "bt_parent_index_verify");

	CHECK_RELATION_BTREE(iRel);

	CHECK_RELATION_IS_NOT_OTHER_TEMP(iRel);

	/* Don't proceed until index is at least ready for insertions */
	if (!iRel->rd_index->indisready)
		PG_RETURN_VOID();

	nBlocks = RelationGetNumberOfBlocks(iRel);

	/* Verify every page, and parent/child relationships */
	for (i = 1; i < nBlocks; i++)
	{
		bt_do_page_verify(iRel, i, true);
	}

	index_close(iRel, ExclusiveLock);
	heap_close(heapRel, ShareLock);

	PG_RETURN_VOID();
}

/*
 * Verify left-right link consistency at each level, starting from the true
 * root.
 *
 * Only acquires AccessShareLock on index relation.
 */
Datum
bt_leftright_verify(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_P(0);
	Relation	rel;
	RangeVar   *relrv;
	Buffer		buffer;
	Page		page;
	BlockNumber leftMost;
	BTMetaPageData *metad;

	CHECK_SUPERUSER();

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv_extended(relrv, AccessShareLock, true);

	if (rel == NULL)
		PG_RETURN_VOID();

	CHECK_RELATION_BTREE(rel);

	CHECK_RELATION_IS_NOT_OTHER_TEMP(rel);

	/* Don't proceed until index is at least ready for insertions */
	if (!rel->rd_index->indisready)
		PG_RETURN_VOID();

	/* Get true root block from meta-page */
	buffer = ReadBuffer(rel, 0);
	LockBuffer(buffer, BT_READ);

	page = BufferGetPage(buffer);
	metad = BTPageGetMeta(page);

	UnlockReleaseBuffer(buffer);

	/*
	 * Verify every level, starting form true root's level. Move left to right.
	 * Process one level at a time.
	 */
	leftMost = metad->btm_root;

	while (leftMost != P_NONE)
	{
		leftMost = verify_leftright_level(rel, leftMost);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * Worker for bt_page_verify(), bt_index_verify() and "parent" variants of
 * both.
 *
 * It is the caller's responsibility to handle heavyweight locking.  Checking
 * children from down-links has race conditions with only an AccessShareLock,
 * since there is never an attempt to get a consistent view of multiple pages
 * using multiple concurrent buffer locks (in general, we prefer to only lock
 * one buffer at a time, and to only manipulate it in local memory).
 */
static void
bt_do_page_verify(Relation rel, BlockNumber blockNum, bool childCheck)
{
	Buffer			buffer;
	Page			page;
	OffsetNumber	offset, maxoffset;
	BTPageOpaque	opaque;
	int16			relnatts;

	buffer = ReadBuffer(rel, blockNum);
	LockBuffer(buffer, BT_READ);

	/*
	 * We copy the page into local storage to avoid holding pin on the
	 * buffer longer than we must, and possibly failing to release it at
	 * all if the calling query doesn't fetch all rows.
	 */
	page = palloc(BLCKSZ);
	memcpy(page, BufferGetPage(buffer), BLCKSZ);

	UnlockReleaseBuffer(buffer);

	relnatts = rel->rd_rel->relnatts;

	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	maxoffset = PageGetMaxOffsetNumber(page);

	for (offset = P_FIRSTDATAKEY(opaque); offset <= maxoffset; offset++)
	{
		ItemId		itemId;
		IndexTuple	itup;
		ScanKey		skey;
		int32		eqs_hikey, eqs_item;

		/* check for interrupts while we're not holding any buffer lock */
		CHECK_FOR_INTERRUPTS();

		if (P_IGNORE(opaque))
		{
			elog(NOTICE, "page %u of index \"%s\" is ignored", blockNum,
				 RelationGetRelationName(rel));
			break;
		}

		/*
		 * As noted in comments above _bt_compare(), there is special handling
		 * of the first data item on a non-leaf page.  There is clearly no
		 * point in building a ScanKey of whatever data happens to be in this
		 * first data IndexTuple, since its contents are undefined.  (This is
		 * because _bt_compare() is hard-coded to specially treat it as "minus
		 * infinity").
		 */
		if (!P_ISLEAF(opaque) && offset == P_FIRSTDATAKEY(opaque))
			continue;

		itemId = PageGetItemId(page, offset);

		if (!ItemIdIsValid(itemId))
			elog(Max(CHCKELEVEL, ERROR), "invalid itemId");

		itup = (IndexTuple) PageGetItem(page, itemId);

		/*
		 * Build ScanKey that relates to the current item/offset's value
		 */
		skey = _bt_mkscankey(rel, itup);

		eqs_hikey = eqs_item = 0;

		/* If there is a high key, compare this item to it */
		if (!P_RIGHTMOST(opaque))
			eqs_hikey = _bt_compare(rel, relnatts, skey, page, P_HIKEY);

		/*
		 * Check that items are stored on page in logical order.
		 *
		 * In each iteration, check that next item is equal to or greater than
		 * current item, until there is no next item (i.e. don't do this on the
		 * last iteration).
		 */
		if (offset < maxoffset)
		{
			/* Iteration before last case */
			eqs_item = _bt_compare(rel, relnatts, skey, page, offset + 1);
		}
		else if (!P_RIGHTMOST(opaque))
		{
			ScanKey		nLow;
			Page		rpage;

			/* Last iteration (page item) */
			Assert(offset == maxoffset);

			/*
			 * There is no "next" item available to compare this final item to
			 * on this same page (there is only the page highkey).  Look for
			 * one in right-link page, since one should be available.
			 *
			 * Get first item on right page by following right link, and verify
			 * that that item is less than or equal to this last item.  Note
			 * that we always get the first real data item -- not the
			 * right-link's high key, and not the garbage first "real" item
			 * that internal pages have.
			 *
			 * It might be that there has been a page split and the right link
			 * is now not the same as the right link during the time that we
			 * took a local copy of the current page, but (even without
			 * heavyweight locking by caller) this shouldn't matter.  It should
			 * be possible to treat whatever we find to the right (if anything)
			 * as an invariant that must hold.  (Since pages split right, it
			 * might be the same page in a sense, which won't in itself have
			 * changed the first item, or it might be deleted.)
			 *
			 * XXX: In general there is a pretty good chance that this right
			 * page will be visited at some later juncture (if we're in the
			 * process of verifying the entire index, for example).  It seems
			 * wasteful that there will be considerable redundant buffer
			 * locking and page copying.  Revisit this.
			 *
			 * In general, a lot of checking performed by amcheck will probably
			 * turn out to be superfluous when the code is tightened up in the
			 * future.
			 */
			nLow = first_item_right_page(rel, opaque->btpo_next, &rpage);

			if (nLow)
			{
				int32		eqs_nextpage;

				/*
				 * This is a bit different to what earlier iterations do.  They
				 * user their item's scankey to compare to next item (so in
				 * this final iteration, we have just verified that second last
				 * item is less than or equal to this item -- we're now
				 * checking this item against items on next page).
				 *
				 * Since ScanKey comes from next page, and is compared to
				 * current offset, next-page-generated scankey should be
				 * greater than (or equal to) this current offset/item.
				 */
				eqs_nextpage = _bt_compare(rel, relnatts, nLow, page, offset);

				if (eqs_nextpage < 0)
					elog(CHCKELEVEL, "page order invariant violated for index \"%s\" block %u across block and next page",
						 RelationGetRelationName(rel), offset);

				pfree(nLow);
				pfree(rpage);
			}
		}

		/*
		 * It is expected that current iteration's ScanKey is always less than
		 * the high key on this page, and always less than the next
		 * item/offset.
		 */
		if (eqs_hikey > 0 || eqs_item  > 0)
		{
			char	   *hblk, *iblk, *niblk, *dump;

			hblk = psprintf("(%u,%u)",
							BlockIdGetBlockNumber(&(itup->t_tid.ip_blkid)),
							itup->t_tid.ip_posid);

			iblk = psprintf("(%u,%u)",
							blockNum,
							offset);

			niblk = psprintf("(%u,%u)",
							 blockNum,
							 offset + 1);

			dump = form_btree_data(itup);

			if (eqs_hikey > 0)
				elog(CHCKELEVEL, "high key invariant violated for index \"%s\" %s with data %s (points to: %s)",
					 RelationGetRelationName(rel), iblk, dump, hblk);
			if (eqs_item  > 0)
				elog(CHCKELEVEL, "page order invariant violated for index \"%s\" (%s and %s) with data %s (points to: %s)",
					 RelationGetRelationName(rel), iblk, niblk, dump, hblk);
		}

		/*
		 * For internal pages, verify that child pages comport with the
		 * down-link in the parent page.  This is safe against concurrent page
		 * splits because caller must have taken appropriate heavyweight locks
		 * for this case.  If there is an old incomplete page split, detect
		 * that and handle it appropriately.
		 *
		 * This seems unlikely to be too wasteful in any particular scenario
		 * (other than the fact that it necessitates a heavy lmgr lock).  We
		 * should think about doing this in a separate pass with that heavier
		 * lock, or perhaps coming up with a smarter buffer locking protocol
		 * that obviates the need for a hwlock.  For now, it seems far
		 * preferable to not be overly clever with buffer locks.
		 */
		if (childCheck && !P_ISLEAF(opaque))
		{
			Page			childPage;
			BlockNumber		childBlockNum;
			Buffer			childBuffer;
			ScanKey			childHkey;

			childBlockNum = ItemPointerGetBlockNumber(&(itup->t_tid));

			childBuffer = ReadBuffer(rel, childBlockNum);
			LockBuffer(childBuffer, BT_READ);

			/* Copy this page into local storage too */
			childPage = palloc(BLCKSZ);
			memcpy(childPage, BufferGetPage(childBuffer), BLCKSZ);

			UnlockReleaseBuffer(childBuffer);

			/*
			 * Verify child page that this down-link points to has the down-link
			 * key as a lower bound.
			 */
			childHkey = verify_btree_items_le(rel, childPage, skey, itup);

			if (childHkey && offset < maxoffset)
			{
				/* state for child page, and next data item/offset */
				BlockNumber		childRightLinkBlockNum,
								parentNextOffsetBlockNum;
				IndexTuple		nIndTup;
				ItemId			nItemId;
				BTPageOpaque	childOpaque;
				int32			res;

				/*
				 * Since this isn't the last iteration, and there can't have
				 * been an incomplete page split on child page just inspected
				 * (since a child high key was returned), further check that
				 * next item on this page (the parent) is equal to child's high
				 * key.
				 */
				childOpaque = (BTPageOpaque) PageGetSpecialPointer(childPage);
				Assert(!P_RIGHTMOST(childOpaque));
				Assert(!P_INCOMPLETE_SPLIT(childOpaque));

				res = _bt_compare(rel, relnatts, childHkey, page, offset + 1);

				if (res != 0)
				{
					char   *iblk, *piblk;

					/*
					 * Principally blame next item, not this item for problem,
					 * since it was the down-link that completed the split,
					 * which seems most plausible as a proximate cause.
					 */
					iblk = psprintf("(%u,%u)",
									blockNum,
									offset + 1);
					piblk = psprintf("(%u,%u)",
									 blockNum,
									 offset);

					elog(CHCKELEVEL, "down-link in parent page in index \"%s\" %s is %s (not equal to) immediately prior item's %s child's (block %u) high key",
						 RelationGetRelationName(rel), iblk,
						 res  < 0 ? "greater than":"less than",
						 piblk, childBlockNum);
				}

				/*
				 * Match block number of child's right link to next parent
				 * offset item's block number, too.  This is similar to but
				 * slightly distinct from the child high key matching
				 * verification.
				 */
				nItemId = PageGetItemId(page, offset + 1);

				if (!ItemIdIsValid(nItemId))
					elog(Max(CHCKELEVEL, ERROR), "invalid nItemId for next item");

				childRightLinkBlockNum = childOpaque->btpo_next;
				nIndTup = (IndexTuple) PageGetItem(page, nItemId);
				parentNextOffsetBlockNum =
					BlockIdGetBlockNumber(&(nIndTup ->t_tid.ip_blkid));

				if (childRightLinkBlockNum != parentNextOffsetBlockNum)
					elog(CHCKELEVEL, "next item/offset down-link (%u) in parent page does not match child (block %u) right link block number",
						 parentNextOffsetBlockNum, childBlockNum);

				pfree(childHkey);
			}

			pfree(childPage);
		}
		pfree(skey);
	}
	pfree(page);
}

/*
 * Lock the heap before the RangeVarGetRelidExtended takes the index lock, to
 * avoid deadlocks.
 *
 * If there was ever a requirement to check permissions, this would be the
 * place to do it.  Currently, we just insist upon superuser.
 */
static void
rangevar_callback_for_childcheck(const RangeVar *relation,
								 Oid relId, Oid oldRelId, void *arg)
{
	Oid		   *heapOid = (Oid *) arg;

	/*
	 * If we previously locked some other index's heap, and the name we're
	 * looking up no longer refers to that relation, release the now-useless
	 * lock.
	 */
	if (relId != oldRelId && OidIsValid(oldRelId))
	{
		/* lock level here should match reindex_index() heap lock */
		UnlockRelationOid(*heapOid, ShareLock);
		*heapOid = InvalidOid;
	}

	/* If the relation does not exist, there's nothing more to do. */
	if (!OidIsValid(relId))
		return;

	/* Lock heap before index to avoid deadlock. */
	if (relId != oldRelId)
	{
		/*
		 * Lock level here should match elsewhere.  If the OID isn't valid, it
		 * means the index was concurrently dropped, which is not a problem for
		 * us; just return normally.
		 */
		*heapOid = IndexGetRelation(relId, true);
		if (OidIsValid(*heapOid))
			LockRelationOid(*heapOid, ShareLock);
	}
}

/*
 * Worker for bt_do_page_verify()
 *
 * Requires only that AccessShareLock has been acquired by caller on target
 * btree index relation.
 *
 * Given an original non-rightmost page's right link block (that points to a
 * page that may or may not itself be right-most), return a scanKey for the
 * first real data item on the right page sufficient to check ordering
 * invariant on last item in original page.  Also, pass back right-link page,
 * which caller manages memory of (since the ScanKey points into it).
 *
 * Handles concurrent page splits and deletions.
 */
static ScanKey
first_item_right_page(Relation rel, BlockNumber rightblockNum, Page *rpage)
{
	Buffer			buffer;
	BTPageOpaque	opaque;
	ItemId			firstDataId;
	IndexTuple		firstDataTup;

	/* as always, copy the page into local storage */
	*rpage = palloc(BLCKSZ);

	for (;;)
	{
		/* check for interrupts while we're not holding any buffer lock */
		CHECK_FOR_INTERRUPTS();

		buffer = ReadBuffer(rel, rightblockNum);
		LockBuffer(buffer, BT_READ);

		memcpy(*rpage, BufferGetPage(buffer), BLCKSZ);

		UnlockReleaseBuffer(buffer);

		opaque = (BTPageOpaque) PageGetSpecialPointer(*rpage);

		/* Ordinarily, we expect to not ignore the right linked page */
		if (!P_IGNORE(opaque))
			break;

		/*
		 * Handle race -- find first non-ignore page until one is found, or at
		 * rightmost.
		 *
		 * Before freeing memory, check if this is rightmost (which means we're
		 * done, and couldn't generate a useful scankey for caller's target).
		 */
		if (P_RIGHTMOST(opaque))
			goto abort;

		rightblockNum = opaque->btpo_next;
		elog(NOTICE, "left-most page was found deleted or half dead");
	}

	firstDataId = PageGetItemId(*rpage, P_FIRSTDATAKEY(opaque));

	if (!P_ISLEAF(opaque))
	{
		/*
		 * Don't return "minus infinity" garbage item on target's right page --
		 * this isn't a real data key for our purposes.  (This is separately
		 * avoided by our caller, for its target page).
		 */
		if (P_FIRSTDATAKEY(opaque) != PageGetMaxOffsetNumber(*rpage))
			firstDataId = PageGetItemId(*rpage, P_FIRSTDATAKEY(opaque) + 1);
		else
			goto abort;
	}

	if (!ItemIdIsValid(firstDataId))
		elog(Max(CHCKELEVEL, ERROR), "invalid hKeyItemId");

	/*
	 * Return scankey, and leave it to caller to free all memory (Page storage
	 * local memory).
	 */
	firstDataTup = (IndexTuple) PageGetItem(*rpage, firstDataId);

	return _bt_mkscankey(rel, firstDataTup);

abort:
	pfree(*rpage);
	rpage = NULL;
	return NULL;
}

/*
 * Verify that each item on page is greater than or equal to the down-link key
 * in the parent.  Don't assume that the items in the child page are in logical
 * order as generally expected -- exhaustively check each and every data item,
 * and not just the first.
 *
 * XXX: Presently, the checking of every item here is usually somewhat
 * redundant.  Revisit the question of how necessary this is in light of how
 * the code is actually called from a higher level.
 *
 * Returns ScanKey that is initialized with high key value, that can be checked
 * within parent against next offset/down-link.  However, if there was an
 * incomplete page split (and therefore no down-link in parent can reasonably
 * be expected yet) or this is the right-most page (and therefore does not have
 * a high key), just return NULL.
 */
static ScanKey
verify_btree_items_le(Relation rel, Page childPage, ScanKey downLinkParent,
					  IndexTuple pItup)
{
	OffsetNumber	offset, maxoffset;
	BTPageOpaque	opaque;
	int16			relnatts = rel->rd_rel->relnatts;

	opaque = (BTPageOpaque) PageGetSpecialPointer(childPage);
	maxoffset = PageGetMaxOffsetNumber(childPage);

	for (offset = P_FIRSTDATAKEY(opaque); offset <= maxoffset; offset++)
	{
		int32  res;

		/*
		 * No point in checking first data key on non-leaf page, since it's
		 * logically "minus infinity".  All items on child page should be
		 * greater than or equal to supplied down-link ScanKey (so the first
		 * data item on a non-leaf page is only "minus infinity" in a way that
		 * relies on the very invariant now being verified -- it's not "minus
		 * infinity" in any general sense.  It's "minus infinity" with respect
		 * to values less than the first "real" data item, but greater than or
		 * equal to the page's left-link's high key, iff there is a left link).
		 *
		 * Leaving aside discussion of invariant conditions, clearly there is
		 * no point in doing anything with the first data key on a non-leaf
		 * page because its data is undefined, as noted in comments above
		 * _bt_compare().
		 */
		if (!P_ISLEAF(opaque) && offset == P_FIRSTDATAKEY(opaque))
			continue;

		res = _bt_compare(rel, relnatts, downLinkParent, childPage, offset);

		/*
		 * Parent down-link is lower bound.  Report when this invariant
		 * condition has been violated.
		 */
		if (res > 0)
		{
			char	   *dump;

			dump = form_btree_data(pItup);

			elog(CHCKELEVEL, "down-link low key invariant violated for index \"%s\" with data %s (points to: %u)",
				 RelationGetRelationName(rel), dump,
				 BlockIdGetBlockNumber(&(pItup->t_tid.ip_blkid)));
		}
	}

	if (P_INCOMPLETE_SPLIT(opaque))
	{
		elog(NOTICE, "incomplete split in child page (block %u) found when following down-link",
			 BlockIdGetBlockNumber(&(pItup->t_tid.ip_blkid)));
	}
	else if (!P_RIGHTMOST(opaque))
	{
		/*
		 * Useful ScanKey for child page high key exists -- return it to caller
		 * for further checks on parent page
		 */
		ItemId		hKeyItemId;
		IndexTuple	hKeyItup;

		hKeyItemId = PageGetItemId(childPage, P_HIKEY);

		if (!ItemIdIsValid(hKeyItemId))
			elog(Max(CHCKELEVEL, ERROR), "invalid hKeyItemId");

		hKeyItup = (IndexTuple) PageGetItem(childPage, hKeyItemId);

		return _bt_mkscankey(rel, hKeyItup);
	}

	return NULL;
}

/*
 * Given a left-most block at some level, move right, verifying that left/right
 * sibling links match as pages are passed over.  It is the caller's
 * responsibility to acquire appropriate heavyweight locks on the index
 * relation.
 *
 * Moves from left to right, so there is no need to worry about concurrent page
 * splits, as after a page split the original/left page has the same left-link
 * as before.  (And, for what it's worth, the page just under consideration has
 * the same right link as before, so logically nothing is skipped.  Everything
 * in the index moves right, and so is denied the opportunity to be skipped
 * over).  Page deletion receives no special consideration either, because the
 * second stage of page deletion (where a half-dead leaf page is unlinked from
 * its siblings) involves acquiring a lock on the left-sibling, on the deletion
 * target itself, and on the right sibling, all concurrently and in that order.
 * That's the only one of the two stages of deletion where side-links are
 * updated.
 *
 * Returns left-most block number one level lower that should be passed on next
 * level/call, or P_NONE when done.  Caller should call with the true root page
 * initially, and work their way down to level 0 (leaf page level).
 *
 * The return value may become stale (from, say, concurrent page deletion).
 * When passed back here, this is handled in a similar though less invasive
 * manner to _bt_moveright().  It may prove necessary to "move right" one or
 * more times if the left-most page passed here is deleted or half dead, by
 * telling caller about a new left-most page on the same level.
 */
static BlockNumber
verify_leftright_level(Relation rel, BlockNumber leftMost)
{
	Buffer			buffer;
	BTPageOpaque	opaque;
	Page			page;
	BlockNumber		current, currentsLeft, next;

	buffer = ReadBuffer(rel, leftMost);
	LockBuffer(buffer, BT_READ);

	/* as always, copy the page into local storage */
	page = palloc(BLCKSZ);
	memcpy(page, BufferGetPage(buffer), BLCKSZ);

	UnlockReleaseBuffer(buffer);

	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	if (P_IGNORE(opaque))
	{
		/*
		 * Handle race -- tell caller to try again with prior left-most's right
		 * sibling, in a manner not unlike _bt_moveright()
		 */
		if (P_RIGHTMOST(opaque))
			elog(Max(CHCKELEVEL, ERROR), "fell off the end of index \"%s\"",
				 RelationGetRelationName(rel));
		next = opaque->btpo_next;
		pfree(page);
		elog(NOTICE, "left-most page was found deleted or half dead");
		return next;
	}

	if (!P_LEFTMOST(opaque))
		elog(CHCKELEVEL, "page %u of index \"%s\" is not leftmost",
			 leftMost, RelationGetRelationName(rel));

	/* remember down-link block to return */
	if (P_ISLEAF(opaque))
	{
		next = P_NONE;
	}
	else
	{
		IndexTuple		itup;
		ItemId			itemId;

		itemId = PageGetItemId(page, P_FIRSTDATAKEY(opaque));

		if (!ItemIdIsValid(itemId))
			elog(Max(CHCKELEVEL, ERROR), "invalid itemId");

		itup = (IndexTuple) PageGetItem(page, itemId);

		next = ItemPointerGetBlockNumber(&(itup->t_tid));
	}

	currentsLeft = leftMost;
	current = opaque->btpo_next;

	while (!P_RIGHTMOST(opaque))
	{
		/* check for interrupts while we're not holding any buffer lock */
		CHECK_FOR_INTERRUPTS();

		buffer = ReadBuffer(rel, current);
		LockBuffer(buffer, BT_READ);

		memcpy(page, BufferGetPage(buffer), BLCKSZ);

		UnlockReleaseBuffer(buffer);

		opaque = (BTPageOpaque) PageGetSpecialPointer(page);

		if (P_ISDELETED(opaque))
			elog(NOTICE, "page %u of index \"%s\" is deleted",
				 current, RelationGetRelationName(rel));

		if (currentsLeft != opaque->btpo_prev)
		{
			if (!P_ISDELETED(opaque))
				elog(CHCKELEVEL, "left link/right link pair don't comport at level %u, block %u, last: %u, current left: %u",
					 opaque->btpo.level, current, currentsLeft, opaque->btpo_prev);
			else /* level unavailable */
				elog(CHCKELEVEL, "left link/right link pair don't comport, deleted block %u, last: %u, current left: %u",
					 current, currentsLeft, opaque->btpo_prev);
		}

		currentsLeft = current;
		current = opaque->btpo_next;
	}

	pfree(page);

	return next;
}

/*
 * Given an IndexTuple, form hex cstring of data for error reporting purposes
 */
static char *
form_btree_data(IndexTuple itup)
{
	char	   *dump, *odump;
	char	   *ptr;
	int			off;
	int			dlen;

	dlen = IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info);
	ptr = (char *) itup + IndexInfoFindDataOffset(itup->t_info);
	dump = palloc0(dlen * 3 + 1);
	odump = dump;

	for (off = 0; off < dlen; off++)
	{
		if (off > 0)
			*dump++ = ' ';
		sprintf(dump, "%02x", *(ptr + off) & 0xff);
		dump += 2;
	}

	return odump;
}
