/*--------------------------------------------------------------------------
 *
 * test_copy_custom_format.c
 *		Code for testing custom COPY format.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_copy_custom_format/test_copy_custom_format.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/copystate.h"
#include "commands/copyapi.h"
#include "commands/defrem.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

typedef struct TestCopyCommonOption
{
	int			common_int;
	bool		common_bool;
}			TestCopyCommonOption;

typedef struct TestCopyFromState
{
	CopyFromStateData base;

	TestCopyCommonOption common_opts;
	char	   *from_str_option;
}			TestCopyFromState;

typedef struct TestCopyToState
{
	CopyToStateData base;

	TestCopyCommonOption common_opts;
	char	   *to_str_option;
}			TestCopyToState;

static Size
TestCopyToEsimateSpace(void)
{
	return sizeof(TestCopyToState);
}

static Size
TestCopyFromEsimateSpace(void)
{
	return sizeof(TestCopyFromState);
}

static bool
TestCopyProcessCommonOption(TestCopyCommonOption * opt, DefElem *option)
{
	if (strcmp(option->defname, "common_int") == 0)
	{
		int			val = defGetInt32(option);

		opt->common_int = val;

		return true;
	}
	else if (strcmp(option->defname, "common_bool") == 0)
	{
		bool		val = defGetBoolean(option);

		opt->common_bool = val;

		return true;
	}

	return false;
}

static bool
TestCopyFromProcessOneOption(CopyFromState ccstate, DefElem *option)
{
	TestCopyFromState *cstate = (TestCopyFromState *) ccstate;

	if (TestCopyProcessCommonOption(&cstate->common_opts, option))
	{
		return true;
	}
	else if (strcmp(option->defname, "from_str") == 0)
	{
		char	   *val = defGetString(option);

		cstate->from_str_option = val;
		return true;
	}

	return false;
}

static bool
TestCopyToProcessOneOption(CopyToState ccstate, DefElem *option)
{
	TestCopyToState *cstate = (TestCopyToState *) ccstate;

	if (TestCopyProcessCommonOption(&cstate->common_opts, option))
	{
		return true;
	}
	else if (strcmp(option->defname, "to_str") == 0)
	{
		char	   *val = defGetString(option);

		cstate->to_str_option = val;
		return true;
	}

	return false;
}

static void
TestCopyFromInFunc(CopyFromState cstate, Oid atttypid,
				   FmgrInfo *finfo, Oid *typioparam)
{
	ereport(NOTICE,
			errmsg("CopyFromInFunc: attribute: %s", format_type_be(atttypid)));
}

static void
TestCopyFromStart(CopyFromState ccstate, TupleDesc tupDesc)
{
	TestCopyFromState *cstate = (TestCopyFromState *) ccstate;

	ereport(NOTICE,
			errmsg("CopyFromStart: the number of attributes: %d", tupDesc->natts));
	ereport(NOTICE,
			errmsg("common_int %d common_bool %d from_str \"%s\"",
				   cstate->common_opts.common_int,
				   cstate->common_opts.common_bool,
				   cstate->from_str_option));
}

static bool
TestCopyFromOneRow(CopyFromState cstate, ExprContext *econtext, Datum *values, bool *nulls,
				   CopyFromRowInfo * rowinfo)
{
	ereport(NOTICE,
			errmsg("CopyFromOneRow"));

	return false;
}

static void
TestCopyFromEnd(CopyFromState cstate)
{
	ereport(NOTICE,
			errmsg("CopyFromEnd"));
}

static void
TestCopyToOutFunc(CopyToState cstate, Oid atttypid, FmgrInfo *finfo)
{
	ereport(NOTICE,
			errmsg("CopyToOutFunc: attribute: %s", format_type_be(atttypid)));
}

static void
TestCopyToStart(CopyToState ccstate, TupleDesc tupDesc)
{
	TestCopyToState *cstate = (TestCopyToState *) ccstate;

	ereport(NOTICE,
			errmsg("CopyToStart: the number of attributes: %d", tupDesc->natts));
	ereport(NOTICE,
			errmsg("common_int %d common_bool %d to_str \"%s\"",
				   cstate->common_opts.common_int,
				   cstate->common_opts.common_bool,
				   cstate->to_str_option));
}

static void
TestCopyToOneRow(CopyToState cstate, TupleTableSlot *slot)
{
	ereport(NOTICE, (errmsg("CopyToOneRow: the number of valid values: %u", slot->tts_nvalid)));
}

static void
TestCopyToEnd(CopyToState cstate)
{
	ereport(NOTICE, (errmsg("CopyToEnd")));
}

static const CopyToRoutine CopyToRoutineTestCopyFormat = {
	.CopyToEstimateStateSpace = TestCopyToEsimateSpace,
	.CopyToProcessOneOption = TestCopyToProcessOneOption,
	.CopyToOutFunc = TestCopyToOutFunc,
	.CopyToStart = TestCopyToStart,
	.CopyToOneRow = TestCopyToOneRow,
	.CopyToEnd = TestCopyToEnd,
};


static const CopyFromRoutine CopyFromRoutineTestCopyFormat = {
	.CopyFromEstimateStateSpace = TestCopyFromEsimateSpace,
	.CopyFromProcessOneOption = TestCopyFromProcessOneOption,
	.CopyFromInFunc = TestCopyFromInFunc,
	.CopyFromStart = TestCopyFromStart,
	.CopyFromOneRow = TestCopyFromOneRow,
	.CopyFromEnd = TestCopyFromEnd,
};

void
_PG_init(void)
{
	RegisterCopyCustomFormat("test_format",
							 &CopyFromRoutineTestCopyFormat,
							 &CopyToRoutineTestCopyFormat);
}
