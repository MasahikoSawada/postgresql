/*-------------------------------------------------------------------------
 *
 * xlogutils.c
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pg_page_recover.h"

#include "access/xlog_internal.h"


XLogRedoAction
xlogProcessRecord(XLogReaderState *record, uint8 block_id, char *buffer)
{
	return xlogProcessRecordExtended(record, block_id, buffer);
}

XLogRedoAction
xlogProcessRecordExtended(XLogReaderState *record, uint8 block_id,
						  char *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;
	Page		page = (Page) buffer;

	if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
	{
		/* Caller specified a bogus block_id */
		pg_log_fatal("failed to locate backup block with ID %d", block_id);
	}

	/* If it has a full-page image and it should be restored, do it. */
	if (XLogRecBlockImageApply(record, block_id))
	{
		if (!RestoreBlockImage(record, block_id, page))
			pg_log_error("failed to restore block image");

		/*
		 * The page may be uninitialized. If so, we can't set the LSN because
		 * that would corrupt the page.
		 */
		if (!PageIsNew(page))
		{
			PageSetLSN(page, lsn);
		}

		return BLK_RESTORED;
	}
	else
	{
		if (lsn <= PageGetLSN(page))
			return BLK_DONE;
		else
			return BLK_NEEDS_REDO;
	}
}

