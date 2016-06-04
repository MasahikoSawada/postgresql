/*-------------------------------------------------------------------------
 *
 * pgoutput_param.c
 *		  Logical Replication output plugin parameter parsing
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pgoutput_param.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/catversion.h"

#include "mb/pg_wchar.h"

#include "nodes/makefuncs.h"

#include "replication/pgoutput.h"

#include "utils/builtins.h"
#include "utils/int8.h"


typedef enum OutputParamType
{
	OUTPUT_PARAM_TYPE_UNDEFINED,
	OUTPUT_PARAM_TYPE_UINT32,
	OUTPUT_PARAM_TYPE_STRING
} OutputParamType;

/* param parsing */
static int get_param_key(const char * const param_name);
static Datum get_param_value(DefElem *elem, OutputParamType type,
							 bool null_ok);
static uint32 parse_param_uint32(DefElem *elem);

enum {
	PARAM_UNRECOGNISED,
	PARAM_PROTOCOL_VERSION,
	PARAM_ENCODING,
	PARAM_PG_VERSION,
	PARAM_PUBLICATION_NAMES,
} OutputPluginParamKey;

typedef struct {
	const char * const paramname;
	int	paramkey;
} OutputPluginParam;

/* Oh, if only C had switch on strings */
static OutputPluginParam param_lookup[] = {
	{"proto_version", PARAM_PROTOCOL_VERSION},
	{"encoding", PARAM_ENCODING},
	{"pg_version", PARAM_PG_VERSION},
	{"publication_names", PARAM_PUBLICATION_NAMES},
	{NULL, PARAM_UNRECOGNISED}
};


/*
 * Read parameters sent by client at startup and store recognised
 * ones in the parameters PGOutputData.
 *
 * The data must have all client-supplied parameter fields zeroed,
 * such as by memset or palloc0, since values not supplied
 * by the client are not set.
 */
void
pgoutput_process_parameters(List *options, PGOutputData *data)
{
	ListCell	*lc;

	/* Examine all the other params in the message. */
	foreach(lc, options)
	{
		DefElem    *elem = lfirst(lc);
		Datum		val;

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		/* Check each param, whether or not we recognise it */
		switch(get_param_key(elem->defname))
		{
			case PARAM_PROTOCOL_VERSION:
				val = get_param_value(elem, OUTPUT_PARAM_TYPE_UINT32, false);
				data->protocol_version = DatumGetUInt32(val);
				break;

			case PARAM_ENCODING:
				val = get_param_value(elem, OUTPUT_PARAM_TYPE_STRING, false);
				data->client_encoding = DatumGetCString(val);
				break;

			case PARAM_PG_VERSION:
				val = get_param_value(elem, OUTPUT_PARAM_TYPE_UINT32, false);
				data->client_pg_version = DatumGetUInt32(val);
				break;

			case PARAM_PUBLICATION_NAMES:
				val = get_param_value(elem, OUTPUT_PARAM_TYPE_STRING, false);
				if (!SplitIdentifierString(DatumGetCString(val), ',',
										   &data->publication_names))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_NAME),
							 errmsg("invalid publication name syntax")));

				break;

			default:
				ereport(ERROR,
						(errmsg("Unrecognised pgoutput parameter %s",
								elem->defname)));
				break;
		}
	}
}

/*
 * Look up a param name to find the enum value for the
 * param, or PARAM_UNRECOGNISED if not found.
 */
static int
get_param_key(const char * const param_name)
{
	OutputPluginParam *param = &param_lookup[0];

	do {
		if (strcmp(param->paramname, param_name) == 0)
			return param->paramkey;
		param++;
	} while (param->paramname != NULL);

	return PARAM_UNRECOGNISED;
}

/*
 * Parse parameter as given type and return the value as Datum.
 */
static Datum
get_param_value(DefElem *elem, OutputParamType type, bool null_ok)
{
	/* Check for NULL value */
	if (elem->arg == NULL || strVal(elem->arg) == NULL)
	{
		if (null_ok)
			return (Datum) 0;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("parameter \"%s\" cannot be NULL", elem->defname)));
	}

	switch (type)
	{
		case OUTPUT_PARAM_TYPE_UINT32:
			return UInt32GetDatum(parse_param_uint32(elem));
		case OUTPUT_PARAM_TYPE_STRING:
			return CStringGetDatum(pstrdup(strVal(elem->arg)));
		default:
			elog(ERROR, "unknown parameter type %d", type);
	}
}

/*
 * Parse string DefElem as uint32.
 */
static uint32
parse_param_uint32(DefElem *elem)
{
	int64		res;

	if (!scanint8(strVal(elem->arg), true, &res))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse integer value \"%s\" for parameter \"%s\"",
						strVal(elem->arg), elem->defname)));

	if (res > PG_UINT32_MAX || res < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("value \"%s\" out of range for parameter \"%s\"",
						strVal(elem->arg), elem->defname)));

	return (uint32) res;
}
