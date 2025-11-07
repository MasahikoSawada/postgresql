/*-------------------------------------------------------------------------
 *
 * copyfrom_internal.h
 *	  Internal definitions for COPY FROM command.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyfrom_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYFROM_INTERNAL_H
#define COPYFROM_INTERNAL_H

#include "commands/copy.h"
#include "commands/copystate.h"

/*
 * COPY FROM state data used for builtin formats.
 */
typedef struct CopyFromStateBuiltins
{
	CopyFromStateData base;

	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	Oid			conversion_proc;	/* encoding conversion function */
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */

	bool	   *defaults;		/* if DEFAULT marker was found for
								 * corresponding att */

	/*
	 * These variables are used to reduce overhead in COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 *
	 * In binary COPY FROM, attribute_buf holds the binary data for the
	 * current field, but the usage is otherwise similar.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, and then
	 * extract the individual attribute fields into attribute_buf.  line_buf
	 * is preserved unmodified so that we can display it in error messages if
	 * appropriate.  (In binary mode, line_buf is not used.)
	 */
	StringInfoData line_buf;

	/*
	 * input_buf holds input data, already converted to database encoding.
	 *
	 * In text mode, CopyReadLine parses this data sufficiently to locate line
	 * boundaries, then transfers the data to line_buf. We guarantee that
	 * there is a \0 at input_buf[input_buf_len] at all times.  (In binary
	 * mode, input_buf is not used.)
	 *
	 * If encoding conversion is not required, input_buf is not a separate
	 * buffer but points directly to raw_buf.  In that case, input_buf_len
	 * tracks the number of bytes that have been verified as valid in the
	 * database encoding, and raw_buf_len is the total number of bytes stored
	 * in the buffer.
	 */
#define INPUT_BUF_SIZE 65536	/* we palloc INPUT_BUF_SIZE+1 bytes */
	char	   *input_buf;
	int			input_buf_index;	/* next byte to process */
	int			input_buf_len;	/* total # of bytes stored */
	bool		input_reached_eof;	/* true if we reached EOF */
	bool		input_reached_error;	/* true if a conversion error happened */
	/* Shorthand for number of unconsumed bytes available in input_buf */
#define INPUT_BUF_BYTES(cstate) ((cstate)->input_buf_len - (cstate)->input_buf_index)

	/*
	 * raw_buf holds raw input data read from the data source (file or client
	 * connection), not yet converted to the database encoding.  Like with
	 * 'input_buf', we guarantee that there is a \0 at raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */

	/* Shorthand for number of unconsumed bytes available in raw_buf */
#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)
}			CopyFromStateBuiltins;

extern void ReceiveCopyBegin(CopyFromState cstate);
extern void ReceiveCopyBinaryHeader(CopyFromState cstate);

/* One-row callbacks for built-in formats defined in copyfromparse.c */
extern bool CopyFromTextOneRow(CopyFromState cstate, ExprContext *econtext,
							   Datum *values, bool *nulls, CopyFromRowInfo * rowinfo);
extern bool CopyFromCSVOneRow(CopyFromState cstate, ExprContext *econtext,
							  Datum *values, bool *nulls, CopyFromRowInfo * rowinfo);
extern bool CopyFromBinaryOneRow(CopyFromState cstate, ExprContext *econtext,
								 Datum *values, bool *nulls, CopyFromRowInfo * rowinfo);

extern Size CopyFromBuiltinsEstimateSpace(void);

#endif							/* COPYFROM_INTERNAL_H */
