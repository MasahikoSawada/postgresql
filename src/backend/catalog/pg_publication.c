/*-------------------------------------------------------------------------
 *
 * publication.c
 *		publication C api manipulation
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		publication.c
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

#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"

#include "executor/spi.h"

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * Check if relation can be in given publication and throws appropriate
 * error if not.
 */
static void
check_publication_add_relation(Publication *pub, Relation targetrel)
{
	/* Must be table */
	if (RelationGetForm(targetrel)->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only tables can be added to publication"),
				 errdetail("%s is not a table",
						   RelationGetRelationName(targetrel))));

	/* Can't be system table */
	if (IsSystemRelation(targetrel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only user tables can be added to publication"),
				 errdetail("%s is a system table",
						   RelationGetRelationName(targetrel))));

	/* UNLOGGED and TEMP relations cannot be part of publication. */
	if (!RelationNeedsWAL(targetrel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("UNLOGGED and TEMP relations cannot be replicated")));

	/*
	 * Must have replica identity index if UPDATEs or DELETEs are
	 * replicated.
	 */
	if (!OidIsValid(RelationGetReplicaIndex(targetrel)) &&
		(pub->replicate_update || pub->replicate_delete))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("table %s cannot be added to publication %s",
						RelationGetRelationName(targetrel), pub->name),
				 errdetail("table does not have REPLICA IDENTITY index "
						   "and given publication is configured to "
						   "replicate UPDATEs and/or DELETEs"),
				 errhint("A PRIMARY KEY is used as REPLICA IDENTITY by default")));
}

/*
 * Insert new publication / relation mapping.
 */
Oid
publication_add_relation(Oid pubid, Relation targetrel,
						 bool if_not_exists)
{
	Relation	rel;
	HeapTuple	tup;
	Datum		values[Natts_pg_publication_rel];
	bool		nulls[Natts_pg_publication_rel];
	Oid			relid = RelationGetRelid(targetrel);
	Oid			prid;
	Publication *pub = GetPublication(pubid);
	ObjectAddress	myself,
					referenced;

	rel = heap_open(PublicationRelRelationId, RowExclusiveLock);

	/* Check for duplicates */
	if (SearchSysCacheExists2(PUBLICATIONRELMAP, relid, pubid))
	{
		heap_close(rel, RowExclusiveLock);

		if (if_not_exists)
			return InvalidOid;

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("relation %s is already member of publication %s",
						RelationGetRelationName(targetrel), pub->name)));
	}

	check_publication_add_relation(pub, targetrel);

	/* Form a tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_publication_rel_pubid - 1] =
		ObjectIdGetDatum(pubid);
	values[Anum_pg_publication_rel_relid - 1] =
		ObjectIdGetDatum(RelationGetRelid(targetrel));

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* Insert tuple into catalog. */
	prid = simple_heap_insert(rel, tup);
	CatalogUpdateIndexes(rel, tup);
	heap_freetuple(tup);

	/* Add dependency on the publication */
	ObjectAddressSet(myself, PublicationRelRelationId, prid);
	ObjectAddressSet(referenced, PublicationRelationId, pubid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	/* Add dependency on the relation */
	ObjectAddressSet(referenced, RelationRelationId, relid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	/*
	 * In case the publication replicates updates and deletes we also
	 * need to record dependency on the replica index.
	 *
	 * XXX: this has unpleasant sideeffect that replacing replica index
	 * (PKey in most cases) means removal of table from publication. We
	 * could habdle is better if we checked specifically for this during
	 * execution of the related commands.
	 */
	if (pub->replicate_update || pub->replicate_delete)
	{
		ObjectAddressSet(referenced, RelationRelationId,
						 RelationGetReplicaIndex(targetrel));
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* Close the table. */
	heap_close(rel, RowExclusiveLock);

	return prid;
}


/*
 * Gets list of publication oids for a relation oid.
 */
List *
GetRelationPublications(Oid relid)
{
	List		   *result = NIL;
	Relation		pubrelsrel;
	ScanKeyData		scankey;
	SysScanDesc		scan;
	HeapTuple		tup;

	/* Find all publications associated with the relation. */
	pubrelsrel = heap_open(PublicationRelRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_publication_rel_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pubrelsrel, PublicationRelMapIndexId, true,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_publication_rel		pubrel;

		pubrel = (Form_pg_publication_rel) GETSTRUCT(tup);

		result = lappend_oid(result, pubrel->pubid);
	}

	systable_endscan(scan);
	heap_close(pubrelsrel, NoLock);

	return result;
}

/*
 * Gets list of publication oids for publications marked as alltables.
 */
List *
GetAllTablesPublications(void)
{
	List		   *result;
	Relation		rel;
	ScanKeyData		scankey;
	SysScanDesc		scan;
	HeapTuple		tup;

	/* Find all publications that are marked as for all tables. */
	rel = heap_open(PublicationRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_publication_puballtables,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 1, &scankey);

	result = NIL;
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
		result = lappend_oid(result, HeapTupleGetOid(tup));

	systable_endscan(scan);
	heap_close(rel, NoLock);

	return result;
}

/*
 * Gets list of relation oids for a publication.
 */
List *
GetPublicationRelations(Oid pubid)
{
	List		   *result;
	Relation		pubrelsrel;
	ScanKeyData		scankey;
	SysScanDesc		scan;
	HeapTuple		tup;

	/* Find all publications associated with the relation. */
	pubrelsrel = heap_open(PublicationRelRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_publication_rel_pubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(pubid));

	scan = systable_beginscan(pubrelsrel, PublicationRelMapIndexId, true,
							  NULL, 1, &scankey);

	result = NIL;
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_publication_rel		pubrel;

		pubrel = (Form_pg_publication_rel) GETSTRUCT(tup);

		result = lappend_oid(result, pubrel->relid);
	}

	systable_endscan(scan);
	heap_close(pubrelsrel, NoLock);

	return result;
}

Publication *
GetPublication(Oid pubid)
{
	HeapTuple		tup;
	Publication	   *pub;
	Form_pg_publication	pubform;

	tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for publication %u", pubid);

	pubform = (Form_pg_publication) GETSTRUCT(tup);

	pub = (Publication *) palloc(sizeof(Publication));
	pub->oid = pubid;
	pub->name = pstrdup(NameStr(pubform->pubname));
	pub->alltables = pubform->puballtables;
	pub->replicate_insert = pubform->pubreplins;
	pub->replicate_update = pubform->pubreplupd;
	pub->replicate_delete = pubform->pubrepldel;

	ReleaseSysCache(tup);

	return pub;
}


/*
 * Get Publication using name.
 */
Publication *
GetPublicationByName(const char *pubname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid1(PUBLICATIONNAME, CStringGetDatum(pubname));
	if (!OidIsValid(oid))
	{
		if (missing_ok)
			return NULL;

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("publication \"%s\" does not exist", pubname)));
	}

	return GetPublication(oid);
}

/*
 * get_publication_oid - given a publication name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid
get_publication_oid(const char *pubname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid1(PUBLICATIONNAME, CStringGetDatum(pubname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("publication \"%s\" does not exist", pubname)));
	return oid;
}

/*
 * get_publication_name - given a publication Oid, look up the name
 */
char *
get_publication_name(Oid pubid)
{
	HeapTuple		tup;
	char		   *pubname;
	Form_pg_publication	pubform;

	tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for publication %u", pubid);

	pubform = (Form_pg_publication) GETSTRUCT(tup);
	pubname = pstrdup(NameStr(pubform->pubname));

	ReleaseSysCache(tup);

	return pubname;
}
