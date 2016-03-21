/*-------------------------------------------------------------------------
 *
 * buffile.h
 *	  Management of large buffered files, primarily temporary files.
 *
 * The BufFile routines provide a partial replacement for stdio atop
 * virtual file descriptors managed by fd.c.  Currently they only support
 * buffered access to a virtual file, without any of stdio's formatting
 * features.  That's enough for immediate needs, but the set of facilities
 * could be expanded if necessary.
 *
 * BufFile also supports working with temporary files that exceed the OS
 * file size limit and/or the largest offset representable in an int.
 * It might be better to split that out as a separately accessible module,
 * but currently we have no need for oversize temp files without buffered
 * access.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buffile.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BUFFILE_H
#define BUFFILE_H

/* BufFile is an opaque type whose details are not known outside buffile.c. */

typedef struct BufFile BufFile;

/*
 * BufFileOp is an identifier for a particular parallel operation involving
 * temporary files.  Parallel temp file operations must be discoverable across
 * processes based on these details.
 *
 * These fields should be set by BufFileGetIdent() within leader process.
 * Identifier BufFileOp makes temp files from workers discoverable within
 * leader.
 */
typedef struct BufFileOp
{
	/*
	 * leaderPid is leader process PID.
	 *
	 * tempFileIdent is an identifier for a particular temp file (or parallel
	 * temp file op) for the leader.  Needed to distinguish multiple parallel
	 * temp file operations within a given leader process.
	 */
	int			leaderPid;
	long		tempFileIdent;
} BufFileOp;

/*
 * BufFilePiece describes a BufFile to be unified.  Unified BufFiles should
 * only be accessed through the BufFileSeekBlock() interface.
 */
typedef struct BufFilePiece
{
	/*
	 * bufFileSize is the size of a BufFile, measured in bytes, as reported by
	 * BufFileGetSize() in a worker process.  This must be set by
	 * BufFileUnify() caller for each piece.  This should be zero for the
	 * leader-owned BufFile piece, since that must initially be empty.
	 *
	 * offsetFirst is an offset (in blocks) to the first block of a worker
	 * BufFile within a unified leader BufFile.  BufFileUnify() sets it for
	 * caller (for each piece).  The BufFileUnify() caller (leader) will need
	 * to use this to compensate for the differences between worker BufFile
	 * positioning and unified BufFile positioning while seeking using
	 * BufFileSeekBlock().  Note also that unification creates space at
	 * the end of the unified BufFile space for the leader to write to.
	 */
	off_t		bufFileSize;
	long		offsetFirst;
} BufFilePiece;

/*
 * prototypes for functions in buffile.c
 */

extern BufFile *BufFileCreateTemp(bool interXact);
extern BufFile *BufFileCreateUnifiable(BufFileOp ident, int worker);
extern BufFile *BufFileUnify(BufFileOp ident, int npieces,
							 BufFilePiece *pieces);
extern void BufFileClose(BufFile *file);
extern size_t BufFileRead(BufFile *file, void *ptr, size_t size);
extern size_t BufFileWrite(BufFile *file, void *ptr, size_t size);
extern int	BufFileSeek(BufFile *file, int fileno, off_t offset, int whence);
extern void BufFileTell(BufFile *file, int *fileno, off_t *offset);
extern BufFileOp BufFileGetIdent(void);
extern off_t BufFileGetSize(BufFile *file);
extern int	BufFileSeekBlock(BufFile *file, long blknum);

#endif   /* BUFFILE_H */
