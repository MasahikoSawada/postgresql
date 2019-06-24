/*-------------------------------------------------------------------------
 *
 * heap_pageredo.c
 *	  Database page recovery
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_page_recover.h"

#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "common/logging.h"

static void heap_pageredo_insert(char *page, BlockNumber blkno,
								 XLogReaderState *record);
static void heap_pageredo_update(char *page, BlockNumber blkno,
								 XLogReaderState *record);
static void heap_pageredo_delete(char *page, BlockNumber blkno,
								 XLogReaderState *record);

void
heap_pageredo(char *page, BlockNumber blkno, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
			heap_pageredo_insert(page, blkno, record);
			break;
		case XLOG_HEAP_DELETE:
			heap_pageredo_delete(page, blkno, record);
			break;
		case XLOG_HEAP_UPDATE:
			heap_pageredo_update(page, blkno, record);
			break;
		case XLOG_HEAP_TRUNCATE:

			/*
			 * TRUNCATE is a no-op because the actions are already logged as
			 * SMGR WAL records.  TRUNCATE WAL record only exists for logical
			 * decoding.
			 */
			break;
		case XLOG_HEAP_HOT_UPDATE:
			//heap_xlog_update(record, true);
			break;
		case XLOG_HEAP_CONFIRM:
			//heap_xlog_confirm(record);
			break;
		case XLOG_HEAP_LOCK:
			//heap_xlog_lock(record);
			break;
		case XLOG_HEAP_INPLACE:
			//heap_xlog_inplace(record);
			break;
		default:
			pg_log_fatal("unknown op code %u", info);
	}
}

static void
heap_pageredo_insert(char *page, BlockNumber blkno, XLogReaderState *record)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	xl_heap_insert	*xlrec = (xl_heap_insert *) XLogRecGetData(record);
	HeapTupleHeader	htup;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	xl_heap_header	xlhdr;
	uint32			newlen;
	ItemPointerData target_tid;
	BlockNumber		redo_blkno;
	XLogRedoAction	action;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &redo_blkno);

	if (blkno != redo_blkno)
		return;

	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		pageInit(page, BlockSz, 0);
		action = BLK_NEEDS_REDO;
	}
	else
		action = xlogProcessRecord(record, 0, page);

	if (action == BLK_NEEDS_REDO)
	{
		Size	datalen;
		char	*data;

		data = XLogRecGetBlockData(record, 0, &datalen);

		newlen = datalen - SizeOfHeapHeader;
		memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
		data += SizeOfHeapHeader;

		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
		memcpy((char *) htup + SizeofHeapTupleHeader,
			   data,
			   newlen);
		newlen += SizeofHeapTupleHeader;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;

		pageAddItem(page, (Item) htup, newlen, xlrec->offnum,
					PAI_OVERWRITE);

		PageSetLSN(page, lsn);
	}
}

static void
heap_pageredo_update(char *page, BlockNumber blkno, XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
	BlockNumber oldblk;
	BlockNumber newblk;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleData oldtup;
	HeapTupleHeader htup;
	XLogRedoAction oldaction;
	XLogRedoAction newaction;

	/* initialize to keep the compiler quiet */
	oldtup.t_data = NULL;
	oldtup.t_len = 0;

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &newblk);
	if (!XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
		oldblk = newblk;

	ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

	oldaction = xlogProcessRecord(record, (oldblk == newblk) ? 0 : 1,
								  page);

	if (oldaction == BLK_NEEDS_REDO)
	{


	}

}

static void
heap_pageredo_delete(char *page, BlockNumber blkno, XLogReaderState *record)
{

}
