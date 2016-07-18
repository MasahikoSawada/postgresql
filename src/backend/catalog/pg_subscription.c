/*-------------------------------------------------------------------------
 *
 * pg_subscription.c
 *		replication subscriptions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_subscription.c
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

#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"

#include "commands/replicationcmds.h"

#include "executor/spi.h"

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"


static List *textarray_to_stringlist(ArrayType *textarray);

/*
 * Fetch the subscription from the syscache.
 */
Subscription *
GetSubscription(Oid subid, bool missing_ok)
{
	HeapTuple		tup;
	Subscription   *sub;
	Form_pg_subscription	subform;
	Datum			datum;
	bool			isnull;

	tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
	{
		if (missing_ok)
			return NULL;

		elog(ERROR, "cache lookup failed for subscription %u", subid);
	}

	subform = (Form_pg_subscription) GETSTRUCT(tup);

	sub = (Subscription *) palloc(sizeof(Subscription));
	sub->oid = subid;
	sub->dbid = subform->subdbid;
	sub->name = pstrdup(NameStr(subform->subname));
	sub->enabled = subform->subenabled;

	/* Get conninfo */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID,
							tup,
							Anum_pg_subscription_subconninfo,
							&isnull);
	Assert(!isnull);
	sub->conninfo = pstrdup(TextDatumGetCString(datum));

	/* Get slotname */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID,
							tup,
							Anum_pg_subscription_subslotname,
							&isnull);
	Assert(!isnull);
	sub->slotname = pstrdup(NameStr(*DatumGetName(datum)));

	/* Get publications */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID,
							tup,
							Anum_pg_subscription_subpublications,
							&isnull);
	Assert(!isnull);
	sub->publications = textarray_to_stringlist(DatumGetArrayTypeP(datum));

	ReleaseSysCache(tup);

	return sub;
}

/*
 * Free memory allocated by subscription struct. */
void
FreeSubscription(Subscription *sub)
{
	pfree(sub->name);
	pfree(sub->conninfo);
	pfree(sub->slotname);
	list_free_deep(sub->publications);
	pfree(sub);
}

/*
 * get_subscription_oid - given a subscription name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid
get_subscription_oid(const char *subname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid2(SUBSCRIPTIONNAME, MyDatabaseId,
						  CStringGetDatum(subname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("subscription \"%s\" does not exist", subname)));
	return oid;
}

/*
 * Convert text array to list of strings.
 *
 * Note: the resulting list of strings is pallocated here.
 */
static List *
textarray_to_stringlist(ArrayType *textarray)
{
	Datum		   *elems;
	int				nelems, i;
	List		   *res = NIL;

	deconstruct_array(textarray,
					  TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	if (nelems == 0)
		return NIL;

	for (i = 0; i < nelems; i++)
		res = lappend(res, makeString(pstrdup(TextDatumGetCString(elems[i]))));

	return res;
}

/*
 * Set the state of a subscription table.
 */
Oid
SetSubscriptionRelState(Oid subid, Oid relid, char state,
						   XLogRecPtr sublsn)
{
	Relation	rel;
	HeapTuple	tup;
	Oid			subrelid;
	bool		nulls[Natts_pg_subscription_rel];
	Datum		values[Natts_pg_subscription_rel];

	rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

	/* Try finding existing mapping. */
	tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP,
							  ObjectIdGetDatum(relid),
							  ObjectIdGetDatum(subid));

	memset(values, 0, sizeof(values));

	/*
	 * If the record for given table does not exist yet create new
	 * record, otherwise update the existing one.
	 */
	if (!HeapTupleIsValid(tup))
	{
		/* Form the tuple. */
		memset(nulls, false, sizeof(nulls));
		values[Anum_pg_subscription_rel_subid - 1] = ObjectIdGetDatum(subid);
		values[Anum_pg_subscription_rel_subrelid - 1] = ObjectIdGetDatum(relid);
		values[Anum_pg_subscription_rel_substate - 1] = CharGetDatum(state);
		if (sublsn != InvalidXLogRecPtr)
			values[Anum_pg_subscription_rel_sublsn - 1] = LSNGetDatum(sublsn);
		else
			nulls[Anum_pg_subscription_rel_sublsn - 1] = true;

		tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

		/* Insert tuple into catalog. */
		subrelid = simple_heap_insert(rel, tup);
		CatalogUpdateIndexes(rel, tup);

		heap_freetuple(tup);
	}
	else
	{
		bool		replaces[Natts_pg_subscription_rel];

		/* Update the tuple. */
		memset(nulls, true, sizeof(nulls));
		memset(replaces, false, sizeof(replaces));

		replaces[Anum_pg_subscription_rel_substate - 1] = true;
		nulls[Anum_pg_subscription_rel_substate - 1] = false;
		values[Anum_pg_subscription_rel_substate - 1] = CharGetDatum(state);

		replaces[Anum_pg_subscription_rel_sublsn - 1] = true;
		if (sublsn != InvalidXLogRecPtr)
		{
			nulls[Anum_pg_subscription_rel_sublsn - 1] = false;
			values[Anum_pg_subscription_rel_sublsn - 1] = LSNGetDatum(sublsn);
		}

		tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
								replaces);

		/* Update the catalog. */
		simple_heap_update(rel, &tup->t_self, tup);
		CatalogUpdateIndexes(rel, tup);

		subrelid = HeapTupleGetOid(tup);
	}

	/* Cleanup. */
	heap_close(rel, NoLock);

	/* Make the changes visible. */
	CommandCounterIncrement();

	return subrelid;
}

/*
 * Get state of subscription table.
 *
 * Returns SUBREL_STATE_UNKNOWN when not found and missing_ok is true.
 */
char
GetSubscriptionRelState(Oid subid, Oid relid, XLogRecPtr *sublsn,
						bool missing_ok)
{
	Relation	rel;
	HeapTuple	tup;
	char		substate;
	bool		isnull;
	Datum		d;

	rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

	/* Try finding the mapping. */
	tup = SearchSysCache2(SUBSCRIPTIONRELMAP,
						  ObjectIdGetDatum(relid),
						  ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
	{
		if (missing_ok)
		{
			heap_close(rel, RowExclusiveLock);
			*sublsn = InvalidXLogRecPtr;
			return '\0';
		}

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("subscription table %u in subscription %d does not exist",
						relid, subid)));
	}

	/* Get the state. */
	d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
						Anum_pg_subscription_rel_substate, &isnull);
	Assert(!isnull);
	substate = DatumGetChar(d);
	d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
						Anum_pg_subscription_rel_sublsn, &isnull);
	if (isnull)
		*sublsn = InvalidXLogRecPtr;
	else
		*sublsn = DatumGetLSN(d);

	/* Cleanup */
	ReleaseSysCache(tup);
	heap_close(rel, RowExclusiveLock);

	return substate;
}

/*
 * Drop subscription relation mapping. These can be for a particular
 * subscription, or for a particular relation (or both but that option
 * is not make sense for current uses).
 */
void
DropSubscriptionRel(Oid subid, Oid relid)
{
	Relation	rel;
	HeapScanDesc scan;
	ScanKeyData keys[2];
	HeapTuple	tup;
	int			numkeys = 0;

	rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

	if (OidIsValid(subid))
	{
		ScanKeyInit(&keys[numkeys],
					Anum_pg_subscription_rel_subid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(subid));
		numkeys++;
	}
	if (OidIsValid(relid))
	{
		ScanKeyInit(&keys[numkeys],
					Anum_pg_subscription_rel_subrelid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(relid));
		numkeys++;
	}

	scan = heap_beginscan_catalog(rel, numkeys, keys);
	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		simple_heap_delete(rel, &tup->t_self);
	}
	heap_endscan(scan);

	heap_close(rel, RowExclusiveLock);
}
