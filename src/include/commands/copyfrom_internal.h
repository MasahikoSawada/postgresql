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

#include "commands/copyapi.h"
#include "nodes/miscnodes.h"

extern void ReceiveCopyBegin(CopyFromState cstate);
extern void ReceiveCopyBinaryHeader(CopyFromState cstate);

/* One-row callbacks for built-in formats defined in copyfromparse.c */
extern bool CopyFromTextOneRow(CopyFromState cstate, ExprContext *econtext,
							   Datum *values, bool *nulls);
extern bool CopyFromCSVOneRow(CopyFromState cstate, ExprContext *econtext,
							  Datum *values, bool *nulls);
extern bool CopyFromBinaryOneRow(CopyFromState cstate, ExprContext *econtext,
								 Datum *values, bool *nulls);

#endif							/* COPYFROM_INTERNAL_H */
