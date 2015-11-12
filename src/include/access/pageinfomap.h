/*-------------------------------------------------------------------------
 *
 * pageinfomap.h
 *		page information map interface
 *
 *
 * Portions Copyright (c) 2007-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/pageinfomap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGEINFOMAP_H
#define PAGEINFOMAP_H

#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/* Flags for bit map */
#define PAGEINFOMAP_ALL_VISIBLE	0x01
#define PAGEINFOMAP_ALL_FROZEN	0x02

#define PAGEINFOMAP_ALL_FLAGS		0x03

/* Macros for pageinfomap test */
#define VM_ALL_VISIBLE(r, b, v) \
	((pageinfomap_get_status((r), (b), (v)) & PAGEINFOMAP_ALL_VISIBLE) != 0)
#define VM_ALL_FROZEN(r, b, v) \
	((pageinfomap_get_status((r), (b), (v)) & PAGEINFOMAP_ALL_FROZEN) != 0)

extern void pageinfomap_clear(Relation rel, BlockNumber heapBlk,
					Buffer vmbuf);
extern void pageinfomap_pin(Relation rel, BlockNumber heapBlk,
				  Buffer *vmbuf);
extern bool pageinfomap_pin_ok(BlockNumber heapBlk, Buffer vmbuf);
extern void pageinfomap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
							  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
							  uint8 flags);
extern uint8 pageinfomap_get_status(Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
extern BlockNumber pageinfomap_count(Relation rel, BlockNumber *all_frozen);
extern void pageinfomap_truncate(Relation rel, BlockNumber nheapblocks);

#endif   /* PAGEINFOMAP_H */
