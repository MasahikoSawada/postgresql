/*-------------------------------------------------------------------------
 *
 * pagetransfer.h
 *	  Exports from replication/pagetransfer.c.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/replication/pagetransfer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PAGETRANSDER_H
#define _PAGETRANSDER_H

#include "access/relation.h"
#include "storage/bufmgr.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"

#define PAGE_TXF_NOT_WAITING		0
#define PAGE_TXF_WAITING			1
#define PAGE_TXF_BEING_PROCESSED	2
#define PAGE_TXF_COMPLETE			3
#define PAGE_TXF_FAILED				4

extern void PageTxfReleaseWaiter(RelFileNode rnode, ForkNumber forknum, BlockNumber blknum,
								 Page page);
extern  void PageTxfWaitForRequestedPage(RelFileNode rnode, ForkNumber forknum,
										 BlockNumber blknum);
extern PGPROC *PageTxfGetWaitingRequest(void);

#endif	/* _PAGETRANSFER_H */
