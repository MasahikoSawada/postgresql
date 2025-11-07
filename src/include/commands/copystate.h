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
#include "commands/trigger.h"
#include "nodes/miscnodes.h"

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

/*
 * Represents the different source cases we need to worry about at
 * the bottom level
 */
typedef enum CopySource
{
	COPY_FILE,					/* from file (or a piped program) */
	COPY_FRONTEND,				/* from frontend */
	COPY_CALLBACK,				/* from callback function */
} CopySource;

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL,
} EolType;

/*
 * Represents the insert method to be used during COPY FROM.
 */
typedef enum CopyInsertMethod
{
	CIM_SINGLE,					/* use table_tuple_insert or ExecForeignInsert */
	CIM_MULTI,					/* always use table_multi_insert or
								 * ExecForeignBatchInsert */
	CIM_MULTI_CONDITIONAL,		/* use table_multi_insert or
								 * ExecForeignBatchInsert only if valid */
} CopyInsertMethod;

/*
 * This struct contains all the state variables used throughout a COPY FROM
 * operation.
 */
typedef struct CopyFromStateData
{
	/* format routine */
	const struct CopyFromRoutine *routine;

	/* low-level state data */
	CopySource	copy_src;		/* type of copy source */
	FILE	   *copy_file;		/* used if copy_src == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used if copy_src == COPY_FRONTEND */
	bool		reached_eof;	/* true if we reached EOF */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_source_cb data_source_cb; /* function for reading data */

	CopyFormatOptions opts;
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	uint64		cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */
	bool		relname_only;	/* don't output line number, att, etc. */
	StringInfo	line_buf;
	bool		line_buf_valid;

	/*
	 * Working state
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	ErrorSaveContext *escontext;	/* soft error trapped during in_functions
									 * execution */
	uint64		num_errors;		/* total number of rows which contained soft
								 * errors */
	AttrNumber	num_defaults;	/* count of att that are missing and have
								 * default value */
	int		   *defmap;			/* array of default att numbers related to
								 * missing att */
	ExprState **defexprs;		/* array of default att expressions for all
								 * att */
	bool		volatile_defexprs;	/* is any of defexprs volatile? */
	List	   *range_table;	/* single element list of RangeTblEntry */
	List	   *rteperminfos;	/* single element list of RTEPermissionInfo */
	ExprState  *qualexpr;

	TransitionCaptureState *transition_capture;

	uint64		bytes_processed;	/* number of bytes processed so far */
} CopyFromStateData;

#endif
