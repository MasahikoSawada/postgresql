/*-------------------------------------------------------------------------
 *
 * subscriptioncmds.c
 *		subscription catalog manipulation functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		subscriptioncmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"

#include "commands/defrem.h"

#include "executor/spi.h"

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"
#include "commands/replicationcmds.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


static void
check_subscription_permissions(void)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to manipulate subscriptions"))));
}

static void
parse_subscription_options(List *options,
						   bool *enabled_given, bool *enabled,
						   char **conninfo, List **publications)
{
	ListCell   *lc;

	*enabled_given = false;
	*enabled = true;
	*conninfo = NULL;
	*publications = NIL;

	/* Parse options */
	foreach (lc, options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		if (strcmp(defel->defname, "enabled") == 0)
		{
			if (*enabled_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			*enabled_given = true;
			*enabled = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "conninfo") == 0)
		{
			if (*conninfo)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			*conninfo = defGetString(defel);
		}
		else if (strcmp(defel->defname, "publication") == 0)
		{
			if (*publications)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			if (defel->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a parameter", defel->defname)));

			*publications = (List *) defel->arg;
		}
		else
			elog(ERROR, "unrecognized option: %s", defel->defname);
	}
}

/*
 * Auxiliary function to return a TEXT array out of a list of String nodes.
 */
static Datum
publicationListToArray(List *publist)
{
	ArrayType  *arr;
	Datum	   *datums;
	int			j = 0;
	ListCell   *cell;
	MemoryContext memcxt;
	MemoryContext oldcxt;

	/* Create memory context for temporary allocations. */
	memcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "publicationListToArray to array",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(memcxt);

	datums = palloc(sizeof(text *) * list_length(publist));
	foreach(cell, publist)
	{
		char	   *name = strVal(lfirst(cell));
		ListCell   *pcell;

		/* Check for duplicates. */
		foreach(pcell, publist)
		{
			char	   *pname = strVal(lfirst(cell));

			if (name == pname)
				break;

			if (strcmp(name, pname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("publication name \"%s\" used more than once",
								pname)));
		}

		datums[j++] = CStringGetTextDatum(name);
	}

	MemoryContextSwitchTo(oldcxt);

	arr = construct_array(datums, list_length(publist),
						  TEXTOID, -1, false, 'i');
	MemoryContextDelete(memcxt);

	return PointerGetDatum(arr);
}

/*
 * Create new subscription.
 */
ObjectAddress
CreateSubscription(CreateSubscriptionStmt *stmt)
{
	Relation	rel;
	ObjectAddress myself;
	Oid			subid;
	bool		nulls[Natts_pg_subscription];
	Datum		values[Natts_pg_subscription];
	HeapTuple	tup;
	bool		enabled_given;
	bool		enabled;
	char	   *conninfo;
	List	   *publications;

	check_subscription_permissions();

	rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

	/* Check if name is used */
	subid = GetSysCacheOid2(SUBSCRIPTIONNAME, MyDatabaseId,
							CStringGetDatum(stmt->subname));
	if (OidIsValid(subid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("subscription \"%s\" already exists",
						stmt->subname)));
	}

	/* Parse and check options. */
	parse_subscription_options(stmt->options, &enabled_given, &enabled,
							   &conninfo, &publications);

	/* TODO: improve error messages here. */
	if (conninfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("connection not specified")));

	if (list_length(publications) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("publication not specified")));

	/* Everything ok, form a new tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_subscription_subdbid - 1] = ObjectIdGetDatum(MyDatabaseId);
	values[Anum_pg_subscription_subname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->subname));
	values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(enabled);
	values[Anum_pg_subscription_subconninfo - 1] =
		CStringGetTextDatum(conninfo);
	values[Anum_pg_subscription_subslotname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->subname));
	values[Anum_pg_subscription_subpublications - 1] =
		 publicationListToArray(publications);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* Insert tuple into catalog. */
	subid = simple_heap_insert(rel, tup);
	CatalogUpdateIndexes(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, SubscriptionRelationId, subid);

	heap_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Alter the existing subscription.
 */
ObjectAddress
AlterSubscription(AlterSubscriptionStmt *stmt)
{
	Relation	rel;
	ObjectAddress myself;
	bool		nulls[Natts_pg_subscription];
	bool		replaces[Natts_pg_subscription];
	Datum		values[Natts_pg_subscription];
	HeapTuple	tup;
	Oid			subid;
	bool		enabled_given;
	bool		enabled;
	char	   *conninfo;
	List	   *publications;

	check_subscription_permissions();

	rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

	/* Fetch the existing tuple. */
	tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, MyDatabaseId,
							  CStringGetDatum(stmt->subname));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("subscription \"%s\" does not exist",
						stmt->subname)));

	subid = HeapTupleGetOid(tup);

	/* Parse options. */
	parse_subscription_options(stmt->options, &enabled_given, &enabled,
							   &conninfo, &publications);

	/* Form a new tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	if (enabled_given)
	{
		values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(enabled);
		replaces[Anum_pg_subscription_subenabled - 1] = true;
	}
	if (conninfo)
	{
		values[Anum_pg_subscription_subconninfo - 1] =
			CStringGetTextDatum(conninfo);
		replaces[Anum_pg_subscription_subconninfo - 1] = true;
	}
	if (publications != NIL)
	{
		values[Anum_pg_subscription_subpublications - 1] =
			 publicationListToArray(publications);
		replaces[Anum_pg_subscription_subpublications - 1] = true;
	}

	tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
							replaces);

	/* Update the catalog. */
	simple_heap_update(rel, &tup->t_self, tup);
	CatalogUpdateIndexes(rel, tup);

	ObjectAddressSet(myself, SubscriptionRelationId, subid);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Drop subscription by OID
 */
void
DropSubscriptionById(Oid subid)
{
	Relation	rel;
	HeapTuple	tup;

	check_subscription_permissions();

	rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

	tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for subscription %u", subid);

	simple_heap_delete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(rel, RowExclusiveLock);
}
