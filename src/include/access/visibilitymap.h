/*-------------------------------------------------------------------------
 *
 * visibilitymap.h
 *		visibility map interface
 *
 *
 * Portions Copyright (c) 2007-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/visibilitymap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VISIBILITYMAP_H
#define VISIBILITYMAP_H

#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/* Flags for bit map */
#define VISIBILITYMAP_ALL_VISIBLE	0x01
#define VISIBILITYMAP_ALL_FROZEN	0x02

extern void visibilitymap_clear(Relation rel, BlockNumber heapBlk, Buffer vmbuf);
extern void visibilitymap_pin(Relation rel, BlockNumber heapBlk,
				  Buffer *vmbuf);
extern bool visibilitymap_pin_ok(BlockNumber heapBlk, Buffer vmbuf);
extern void visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
							  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
							  uint8 flags);
extern bool visibilitymap_test(Relation rel, BlockNumber heapBlk, Buffer *vmbuf,
							   uint8 flags);
extern void visibilitymap_count(Relation rel, BlockNumber *all_visible,
									   BlockNumber *all_frozen);
extern void visibilitymap_truncate(Relation rel, BlockNumber nheapblocks);

#endif   /* VISIBILITYMAP_H */
