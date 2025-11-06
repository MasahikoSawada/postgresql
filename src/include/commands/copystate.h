/*-------------------------------------------------------------------------
 *
 * copystate.h
 *	  Definitions for state data used by COPY TO and COPY FROM.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copystate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYSTATE_H
#define COPYSTATE_H

#include "postgres.h"
#include "commands/copy.h"
#include "executor/execdesc.h"

/*
 * Represents the different dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_DEST_FILE,				/* to/from file (or a piped program) */
	COPY_DEST_FRONTEND,			/* to/from frontend */
	COPY_DEST_CALLBACK,			/* to/from callback function */
} CopyDest;

/*
 * This struct contains the state variables commonly used throughout a
 * COPY TO operation.
 */
typedef struct CopyToStateData
{
	/* format-specific routines */
	const struct CopyToRoutine *routine;

	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_DEST_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_dest_cb data_dest_cb; /* function for writing data */

	CopyFormatOptions opts;
	Node	   *whereClause;	/* WHERE condition (or NULL) */
	List	   *partitions;		/* OID list of partitions to copy data from */

	/*
	 * Working state
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */
	uint64		bytes_processed;	/* number of bytes processed so far */
} CopyToStateData;

#endif
