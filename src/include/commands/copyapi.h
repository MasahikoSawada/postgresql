/*-------------------------------------------------------------------------
 *
 * copyapi.h
 *	  API for COPY TO/FROM
 *
 * Copyright (c) 2015-2023, PostgreSQL Global Development Group
 *
 * src/include/command/copyapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYAPI_H
#define COPYAPI_H

#include "executor/tuptable.h"

typedef struct CopyFromStateData *CopyFromState;
typedef struct CopyToStateData *CopyToState;

typedef void (*CopyToStart_function) (CopyToState cstate, TupleDesc tupDesc);
typedef void (*CopyToOneRow_function) (CopyToState cstate, TupleTableSlot *slot);
typedef void (*CopyToEnd_function) (CopyToState cstate);

/* XXX: just copied from COPY TO routines */
typedef void (*CopyFromStart_function) (CopyFromState cstate, TupleDesc tupDesc);
typedef void (*CopyFromOneRow_function) (CopyFromState cstate, TupleTableSlot *slot);
typedef void (*CopyFromEnd_function) (CopyFromState cstate);

typedef struct CopyFormatRoutine
{
	NodeTag		type;

	bool		is_from;
	Node	   *routine;
}			CopyFormatRoutine;

typedef struct CopyToFormatRoutine
{
	NodeTag		type;

	CopyToStart_function start_fn;
	CopyToOneRow_function onerow_fn;
	CopyToEnd_function end_fn;
}			CopyToFormatRoutine;

/* XXX: just copied from COPY TO routines */
typedef struct CopyFromFormatRoutine
{
	NodeTag		type;

	CopyFromStart_function start_fn;
	CopyFromOneRow_function onerow_fn;
	CopyFromEnd_function end_fn;
}			CopyFromFormatRoutine;

extern CopyToFormatRoutine * GetCopyToFormatRoutine(char *format_name);
extern CopyFromFormatRoutine * GetCopyFromFormatRoutine(char *format_name);

#endif							/* COPYAPI_H */
