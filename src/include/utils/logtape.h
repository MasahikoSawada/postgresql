/*-------------------------------------------------------------------------
 *
 * logtape.h
 *	  Management of "logical tapes" within temporary files.
 *
 * See logtape.c for explanations.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/logtape.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOGTAPE_H
#define LOGTAPE_H

#include "storage/buffile.h"

/* LogicalTapeSet is an opaque type whose details are not known outside logtape.c. */

typedef struct LogicalTapeSet LogicalTapeSet;

/*
 * TapeShare is used for tape deserialization (unification).  It is minimal
 * metadata about the on-disk structure of tapes.
 */
typedef struct TapeShare
{
	/*
	 * Caller can rely on LogicalTapeFreeze() to determine everything for
	 * worker output tapes (this is done when workers freeze their final
	 * output), but must specifically consider the leader's tape ahead of
	 * unification.
	 *
	 * nlevels is the number of levels of indirection.  For the empty leader
	 * tape, this should be set to 0 ahead of unification.
	 *
	 * buffilesize is the size of a worker's underlying BufFile within the
	 * worker.  For the empty leader tape, this should be set to 0 ahead of
	 * unification.
	 */
	uint32		nlevels;
	off_t		buffilesize;
} TapeShare;

/*
 * prototypes for functions in logtape.c
 */

extern LogicalTapeSet *LogicalTapeSetCreate(int ntapes, BufFileOp ident,
										int workernum);
extern LogicalTapeSet *LogicalTapeSetUnify(int ntapes, BufFileOp ident,
				TapeShare *tapes);
extern void LogicalTapeSetClose(LogicalTapeSet *lts);
extern void LogicalTapeSetForgetFreeSpace(LogicalTapeSet *lts);
extern size_t LogicalTapeRead(LogicalTapeSet *lts, int tapenum,
				void *ptr, size_t size);
extern void LogicalTapeWrite(LogicalTapeSet *lts, int tapenum,
				 void *ptr, size_t size);
extern void LogicalTapeRewind(LogicalTapeSet *lts, int tapenum, bool forWrite);
extern TapeShare LogicalTapeFreeze(LogicalTapeSet *lts, int tapenum,
				 bool serialize);
extern bool LogicalTapeBackspace(LogicalTapeSet *lts, int tapenum,
					 size_t size);
extern bool LogicalTapeSeek(LogicalTapeSet *lts, int tapenum,
				long blocknum, int offset);
extern void LogicalTapeTell(LogicalTapeSet *lts, int tapenum,
				long *blocknum, int *offset);
extern long LogicalTapeSetBlocks(LogicalTapeSet *lts);

#endif   /* LOGTAPE_H */
