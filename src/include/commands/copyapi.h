/*-------------------------------------------------------------------------
 *
 * copyapi.h
 *	  API for COPY TO/FROM handlers
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYAPI_H
#define COPYAPI_H

#include "commands/copy.h"
#include "executor/execdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"

/*
 * API structure for a COPY TO format implementation. Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
typedef struct CopyToRoutine
{
	NodeTag		type;

	/*
	 * Set output function information. This callback is called once at the
	 * beginning of COPY TO.
	 *
	 * 'finfo' can be optionally filled to provide the catalog information of
	 * the output function.
	 *
	 * 'atttypid' is the OID of data type used by the relation's attribute.
	 */
	void		(*CopyToOutFunc) (CopyToState cstate, Oid atttypid,
								  FmgrInfo *finfo);

	/*
	 * Start a COPY TO. This callback is called once at the beginning of COPY
	 * FROM.
	 *
	 * 'tupDesc' is the tuple descriptor of the relation from where the data
	 * is read.
	 */
	void		(*CopyToStart) (CopyToState cstate, TupleDesc tupDesc);

	/*
	 * Write one row to the 'slot'.
	 */
	void		(*CopyToOneRow) (CopyToState cstate, TupleTableSlot *slot);

	/* End a COPY TO. This callback is called once at the end of COPY FROM */
	void		(*CopyToEnd) (CopyToState cstate);
} CopyToRoutine;

/*
 * Represents the different dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_DEST_FILE,				/* to file (or a piped program) */
	COPY_DEST_FRONTEND,			/* to frontend */
	COPY_DEST_CALLBACK,			/* to callback function */
} CopyDest;

/*
 * This struct contains all the state variables used throughout a COPY TO
 * operation.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is true
 * when we have to do it the hard way.
 */
typedef struct CopyToStateData
{
	/* format-specific routines */
	const CopyToRoutine *routine;

	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO */

	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_dest_cb data_dest_cb; /* function for writing data */

	CopyFormatOptions opts;
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/*
	 * Working state
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */
	uint64		bytes_processed;	/* number of bytes processed so far */

	/* For custom format implementation */
	void	   *opaque;			/* private space */
} CopyToStateData;

extern void CopyToStateFlush(CopyToState cstate);

/*
 * API structure for a COPY FROM format implementation.	 Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
typedef struct CopyFromRoutine
{
	/*
	 * Set input function information. This callback is called once at the
	 * beginning of COPY FROM.
	 *
	 * 'finfo' can be optionally filled to provide the catalog information of
	 * the input function.
	 *
	 * 'typioparam' can be optionally filled to define the OID of the type to
	 * pass to the input function.'atttypid' is the OID of data type used by
	 * the relation's attribute.
	 */
	void		(*CopyFromInFunc) (CopyFromState cstate, Oid atttypid,
								   FmgrInfo *finfo, Oid *typioparam);

	/*
	 * Start a COPY FROM. This callback is called once at the beginning of
	 * COPY FROM.
	 *
	 * 'tupDesc' is the tuple descriptor of the relation where the data needs
	 * to be copied.  This can be used for any initialization steps required
	 * by a format.
	 */
	void		(*CopyFromStart) (CopyFromState cstate, TupleDesc tupDesc);

	/*
	 * Read one row from the source and fill *values and *nulls.
	 *
	 * 'econtext' is used to evaluate default expression for each column that
	 * is either not read from the file or is using the DEFAULT option of COPY
	 * FROM.  It is NULL if no default values are used.
	 *
	 * Returns false if there are no more tuples to read.
	 */
	bool		(*CopyFromOneRow) (CopyFromState cstate, ExprContext *econtext,
								   Datum *values, bool *nulls);

	/* End a COPY FROM. This callback is called once at the end of COPY FROM */
	void		(*CopyFromEnd) (CopyFromState cstate);
} CopyFromRoutine;

#endif							/* COPYAPI_H */
