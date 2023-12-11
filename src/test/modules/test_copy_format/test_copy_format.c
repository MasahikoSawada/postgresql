/*--------------------------------------------------------------------------
 *
 * test_copy_format.c
 *		Code for custom COPY format.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/test_copy_format/test_copy_format.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "commands/copyapi.h"
#include "fmgr.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

static void
testfmt_copyto_start(CopyToState cstate, TupleDesc tupDesc)
{
	ereport(NOTICE,
			(errmsg("testfmt_copyto_start called")));
}

static void
testfmt_copyto_onerow(CopyToState cstate, TupleTableSlot *slot)
{
	ereport(NOTICE,
			(errmsg("testfmt_copyto_onerow called")));
}

static void
testfmt_copyto_end(CopyToState cstate)
{
	ereport(NOTICE,
			(errmsg("testfmt_copyto_end called")));
}

PG_FUNCTION_INFO_V1(copy_testfmt_handler);
Datum
copy_testfmt_handler(PG_FUNCTION_ARGS)
{
	bool		is_from = PG_GETARG_BOOL(0);
	CopyFormatRoutine *cp = makeNode(CopyFormatRoutine);;

	ereport(NOTICE,
			(errmsg("testfmt_handler called with is_from %d", is_from)));

	cp->is_from = is_from;
	if (!is_from)
	{
		CopyToFormatRoutine *cpt = makeNode(CopyToFormatRoutine);

		cpt->start_fn = testfmt_copyto_start;
		cpt->onerow_fn = testfmt_copyto_onerow;
		cpt->end_fn = testfmt_copyto_end;

		cp->routine = (Node *) cpt;
	}
	else
		elog(ERROR, "custom COPY format \"testfmt\" does not support COPY FROM");

	PG_RETURN_POINTER(cp);
}
