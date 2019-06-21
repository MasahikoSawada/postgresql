/*-------------------------------------------------------------------------
 *
 * pg_page_recover.h
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_PAGE_RECOVER_H
#define PG_PAGE_RECOVER_H

#include "postgres.h"

#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "common/logging.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"

extern int			WalSegSz;
extern int 		BlockSz;


extern void pageInit(Page page, Size pageSize, Size specialSize);
extern OffsetNumber pageAddItem(char *page, char *item, Size size,
								OffsetNumber offnum, int flags);

extern XLogRedoAction xlogProcessRecordExtended(XLogReaderState *record,
												uint8 block_id,	char *buffer);
extern XLogRedoAction xlogProcessRecord(XLogReaderState *record, uint8 block_id,
										char *buffer);


extern void  heap_pageredo(char *page, BlockNumber blkno,
						   XLogReaderState *record);

#endif
