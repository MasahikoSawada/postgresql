/*-------------------------------------------------------------------------
 *
 * copy_custom_format.c
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copy_custom_format.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/copy.h"
#include "commands/copyapi.h"
#include "utils/memutils.h"


typedef struct CopyCustomFormat
{
	const char *fmt_name;
	const CopyFromRoutine *from_routine;
	const CopyToRoutine *to_routine;
}			CopyCustomFormat;

static CopyCustomFormat * CopyCustomFormatArray = NULL;
static int	CopyCustomFormatAssigned = 0;
static int	CopyCustomFormatAllocated = 0;

static char *BuiltinFormatNames[] = {
	"text", "csv", "binary"
};

void
RegisterCopyCustomFormat(const char *fmt_name, const CopyFromRoutine *from_routine,
						 const CopyToRoutine *to_routine)
{
	CopyCustomFormat *entry;

	/* Check the duplication with built-in formats */
	for (int i = 0; i < lengthof(BuiltinFormatNames); i++)
	{
		if (strcmp(BuiltinFormatNames[i], fmt_name) == 0)
			ereport(ERROR,
					errmsg("failed to register custom COPY format \"%s\"", fmt_name),
					errdetail("COPY format \"%s\" is a builtin format",
							  BuiltinFormatNames[i]));

	}

	/* Check the duplication with the registered custom formats */
	if (FindCustomCopyFormat(fmt_name))
		ereport(ERROR,
				errmsg("failed to register custom COPY format \"%s\"", fmt_name),
				errdetail("Custom COPY format \"%s\" already registered",
						  fmt_name));

	/* If there is no array yet, create one */
	if (CopyCustomFormatArray == NULL)
	{
		CopyCustomFormatAllocated = 16;
		CopyCustomFormatArray = (CopyCustomFormat *)
			MemoryContextAlloc(TopMemoryContext,
							   CopyCustomFormatAllocated * sizeof(CopyCustomFormat));
	}

	/* If there's an array but it's current full, expand it */
	if (CopyCustomFormatAssigned >= CopyCustomFormatAllocated)
	{
		int			i = pg_nextpower2_32(CopyCustomFormatAllocated + 1);

		CopyCustomFormatArray = (CopyCustomFormat *)
			repalloc(CopyCustomFormatArray, i * sizeof(CopyCustomFormat));
		CopyCustomFormatAllocated = i;
	}

	Assert(to_routine != NULL || from_routine != NULL);
	Assert((to_routine == NULL) ||
		   (to_routine->CopyToEstimateStateSpace != NULL &&
			to_routine->CopyToOutFunc != NULL &&
			to_routine->CopyToStart != NULL &&
			to_routine->CopyToOneRow != NULL &&
			to_routine->CopyToEnd != NULL));
	Assert((from_routine == NULL) ||
		   (from_routine->CopyFromEstimateStateSpace != NULL &&
			from_routine->CopyFromInFunc != NULL &&
			from_routine->CopyFromStart != NULL &&
			from_routine->CopyFromOneRow != NULL &&
			from_routine->CopyFromEnd != NULL));

	entry = &CopyCustomFormatArray[CopyCustomFormatAssigned++];
	entry->fmt_name = fmt_name;
	entry->from_routine = from_routine;
	entry->to_routine = to_routine;
}

/*
 * Returns true if the given custom format name is registered.
 */
bool
FindCustomCopyFormat(const char *fmt_name)
{
	for (int i = 0; i < CopyCustomFormatAssigned; i++)
	{
		if (strcmp(CopyCustomFormatArray[i].fmt_name, fmt_name) == 0)
			return true;
	}

	return false;
}

bool
GetCustomCopyToRoutine(const char *fmt_name, const CopyToRoutine **to_routine)
{
	for (int i = 0; i < CopyCustomFormatAssigned; i++)
	{
		if (strcmp(CopyCustomFormatArray[i].fmt_name, fmt_name) == 0)
		{
			*to_routine = CopyCustomFormatArray[i].to_routine;
			return true;
		}
	}

	return false;
}

bool
GetCustomCopyFromRoutine(const char *fmt_name, const CopyFromRoutine **from_routine)
{
	for (int i = 0; i < CopyCustomFormatAssigned; i++)
	{
		if (strcmp(CopyCustomFormatArray[i].fmt_name, fmt_name) == 0)
		{
			*from_routine = CopyCustomFormatArray[i].from_routine;
			return true;
		}
	}

	return false;
}
