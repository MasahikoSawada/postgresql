/*-------------------------------------------------------------------------
 *
 * publicationcmds.c
 *		publication manipulation
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		publicationcmds.c
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
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"

#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/replicationcmds.h"

#include "executor/spi.h"

#include "nodes/makefuncs.h"

#include "parser/parse_clause.h"

#include "replication/reorderbuffer.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static List *GatherTables(char *schema);
static List *GatherTableList(List *tables);
static void PublicationAddTables(Oid pubid, List *rels, bool if_not_exists,
					 AlterPublicationStmt *stmt);
static void PublicationDropTables(Oid pubid, List *rels, bool missing_ok);
static void CloseTables(List *rels);

static void
check_replication_permissions(void)
{
	if (!superuser() && !has_rolreplication(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser or replication role to manipulate publications"))));
}

static void
parse_publication_options(List *options,
						  bool *replicate_insert_given,
						  bool *replicate_insert,
						  bool *replicate_update_given,
						  bool *replicate_update,
						  bool *replicate_delete_given,
						  bool *replicate_delete)
{
	ListCell   *lc;

	*replicate_insert_given = false;
	*replicate_update_given = false;
	*replicate_delete_given = false;

	/* Defaults are true */
	*replicate_insert = true;
	*replicate_update = true;
	*replicate_delete = true;

	/* Parse options */
	foreach (lc, options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		if (strcmp(defel->defname, "replicate_insert") == 0)
		{
			if (*replicate_insert_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			*replicate_insert_given = true;
			*replicate_insert = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "replicate_update") == 0)
		{
			if (*replicate_update_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			*replicate_update_given = true;
			*replicate_update = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "replicate_delete") == 0)
		{
			if (*replicate_delete_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			*replicate_delete_given = true;
			*replicate_delete = defGetBoolean(defel);
		}
		else
			elog(ERROR, "unrecognized option: %s", defel->defname);
	}
}

/*
 * Create new publication.
 * TODO ACL check
 */
ObjectAddress
CreatePublication(CreatePublicationStmt *stmt)
{
	Relation	rel;
	ObjectAddress myself;
	Oid			puboid;
	bool		nulls[Natts_pg_publication];
	Datum		values[Natts_pg_publication];
	HeapTuple	tup;
	bool		replicate_insert_given;
	bool		replicate_update_given;
	bool		replicate_delete_given;
	bool		replicate_insert;
	bool		replicate_update;
	bool		replicate_delete;

	check_replication_permissions();

	rel = heap_open(PublicationRelationId, RowExclusiveLock);

	/* Check if name is used */
	puboid = GetSysCacheOid1(PUBLICATIONNAME, CStringGetDatum(stmt->pubname));
	if (OidIsValid(puboid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("publication \"%s\" already exists",
						stmt->pubname)));
	}

	/* Form a tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_publication_pubname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->pubname));

	parse_publication_options(stmt->options,
							  &replicate_insert_given, &replicate_insert,
							  &replicate_update_given, &replicate_update,
							  &replicate_delete_given, &replicate_delete);

	values[Anum_pg_publication_puballtables - 1] =
		BoolGetDatum(stmt->for_all_tables);
	values[Anum_pg_publication_pubreplins - 1] =
		BoolGetDatum(replicate_insert);
	values[Anum_pg_publication_pubreplupd - 1] =
		BoolGetDatum(replicate_update);
	values[Anum_pg_publication_pubrepldel - 1] =
		BoolGetDatum(replicate_delete);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* Insert tuple into catalog. */
	puboid = simple_heap_insert(rel, tup);
	CatalogUpdateIndexes(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, PublicationRelationId, puboid);

	/* Make the changes visible. */
	CommandCounterIncrement();

	if (stmt->tables)
	{
		List	   *rels;

		Assert(list_length(stmt->tables) > 0);

		rels = GatherTableList(stmt->tables);
		PublicationAddTables(puboid, rels, true, NULL);
		CloseTables(rels);
	}
	else if (stmt->for_all_tables || stmt->schema)
	{
		List	   *rels;

		rels = GatherTables(stmt->schema);
		PublicationAddTables(puboid, rels, true, NULL);
		CloseTables(rels);
	}

	heap_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	return myself;
}

/*
 * Change options of a publication.
 */
static void
AlterPublicationOptions(AlterPublicationStmt *stmt, Relation rel,
					   HeapTuple tup)
{
	bool		nulls[Natts_pg_publication];
	bool		replaces[Natts_pg_publication];
	Datum		values[Natts_pg_publication];
	bool		replicate_insert_given;
	bool		replicate_update_given;
	bool		replicate_delete_given;
	bool		replicate_insert;
	bool		replicate_update;
	bool		replicate_delete;
	ObjectAddress		obj;
	Form_pg_publication pubform = (Form_pg_publication) GETSTRUCT(tup);

	parse_publication_options(stmt->options,
							  &replicate_insert_given, &replicate_insert,
							  &replicate_update_given, &replicate_update,
							  &replicate_delete_given, &replicate_delete);

	/*
	 * If the publication of UPDATEs or DELETEs changed we need to process
	 * existing tables in publication to check if they are still valid and
	 * also record or remove dependencies on the replica identity index.
	 */
	if ((replicate_update_given || replicate_delete_given) &&
		(pubform->pubreplupd || pubform->pubrepldel) !=
		(replicate_update || replicate_delete))
	{
		Relation		pubrelsrel;
		ScanKeyData		scankey;
		SysScanDesc		scan;
		HeapTuple		reltup;
		bool			needrepindex;

		needrepindex = (replicate_update_given && replicate_update) ||
			(replicate_delete_given && replicate_delete);

		pubrelsrel = heap_open(PublicationRelRelationId, AccessShareLock);

		/* Loop over all relations in the publication. */
		ScanKeyInit(&scankey,
					Anum_pg_publication_rel_pubid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(HeapTupleGetOid(tup)));

		scan = systable_beginscan(pubrelsrel, 0, true, NULL, 1, &scankey);

		/* Process every individual table in the publication. */
		while (HeapTupleIsValid(reltup = systable_getnext(scan)))
		{
			Form_pg_publication_rel	t;
			Relation				pubrel;
			ObjectAddress			myself,
									referenced;

			t = (Form_pg_publication_rel) GETSTRUCT(reltup);

			pubrel = heap_open(t->relid, AccessShareLock);

			/* Check if relation has replication index. */
			Assert(RelationGetForm(pubrel)->relkind == RELKIND_RELATION);

			if (needrepindex && !OidIsValid(RelationGetReplicaIndex(pubrel)))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("publication %s cannot be altered to "
								"replicate UPDATEs or DELETEs because it "
								"contains tables without REPLICA "
								"IDENTITY index",
								NameStr(pubform->pubname))));

			/* Update dependency. */
			ObjectAddressSet(myself, PublicationRelRelationId,
							 HeapTupleGetOid(tup));
			ObjectAddressSet(referenced, RelationRelationId,
							 RelationGetReplicaIndex(pubrel));
			if (needrepindex)
				recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
			else
				deleteDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

			heap_close(pubrel, NoLock);
		}

		systable_endscan(scan);
		heap_close(pubrelsrel, NoLock);
	}

	/* Everything ok, form a new tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	if (replicate_insert_given)
	{
		values[Anum_pg_publication_pubreplins - 1] =
			BoolGetDatum(replicate_insert);
		replaces[Anum_pg_publication_pubreplins - 1] = true;
	}
	if (replicate_update_given)
	{
		values[Anum_pg_publication_pubreplupd - 1] =
			BoolGetDatum(replicate_update);
		replaces[Anum_pg_publication_pubreplupd - 1] = true;
	}
	if (replicate_delete_given)
	{
		values[Anum_pg_publication_pubrepldel - 1] =
			BoolGetDatum(replicate_delete);
		replaces[Anum_pg_publication_pubrepldel - 1] = true;
	}

	tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
							replaces);

	/* Update the catalog. */
	simple_heap_update(rel, &tup->t_self, tup);
	CatalogUpdateIndexes(rel, tup);

	CommandCounterIncrement();

	ObjectAddressSet(obj, PublicationRelationId, HeapTupleGetOid(tup));
	EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress,
									 (Node *) stmt);
}

/*
 * Add or remove table to/from publication.
 */
static void
AlterPublicationTables(AlterPublicationStmt *stmt, Relation rel,
					   HeapTuple tup)
{
	Oid			pubid = HeapTupleGetOid(tup);
	bool		missing_ok = false;
	bool		if_not_exists = false;
	List	   *rels = NIL;
	Form_pg_publication pubform = (Form_pg_publication) GETSTRUCT(tup);

	if (stmt->tables)
	{
		Assert(list_length(stmt->tables) > 0);

		rels = GatherTableList(stmt->tables);
	}
	else
	{
		Assert(stmt->for_all_tables || stmt->schema);

		rels = GatherTables(stmt->schema);

		/* Don't error on missing tables on DROP */
		missing_ok = true;

		/* Don't error on already existing tables on ADD. */
		if_not_exists = true;
	}

	if (stmt->tableAction == DEFELEM_ADD)
		PublicationAddTables(pubid, rels, if_not_exists, stmt);
	else if (stmt->tableAction == DEFELEM_DROP)
	{
		if (pubform->puballtables)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("tables cannot be dropped from FOR ALL TABLES publications"),
					errdetail("publication %s is defined as FOR ALL TABLES",
							  NameStr(pubform->pubname))));
		PublicationDropTables(pubid, rels, missing_ok);
	}
	else /* DEFELEM_SET */
	{
		List	   *oldrelids = GetPublicationRelations(pubid);
		List	   *delrels = NIL;
		ListCell   *oldlc;

		/*
		 * If FOR ALL TABLES was changed, update the publication record
		 * first.
		 */
		if (stmt->for_all_tables != pubform->puballtables)
		{
			bool		nulls[Natts_pg_publication];
			bool		replaces[Natts_pg_publication];
			Datum		values[Natts_pg_publication];
			ObjectAddress	obj;

			/* Form a new tuple. */
			memset(values, 0, sizeof(values));
			memset(nulls, false, sizeof(nulls));
			memset(replaces, false, sizeof(replaces));

			values[Anum_pg_publication_puballtables - 1] =
				BoolGetDatum(stmt->for_all_tables);
			replaces[Anum_pg_publication_puballtables - 1] = true;

			tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
									nulls, replaces);

			/* Update the catalog. */
			simple_heap_update(rel, &tup->t_self, tup);
			CatalogUpdateIndexes(rel, tup);

			CommandCounterIncrement();

			ObjectAddressSet(obj, PublicationRelationId,
							 HeapTupleGetOid(tup));
			EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress,
											 (Node *) stmt);
		}

		/* Calculate which relations to drop. */
		foreach(oldlc, oldrelids)
		{
			Oid			oldrelid = lfirst_oid(oldlc);
			ListCell   *newlc;
			bool		found = false;

			foreach(newlc, rels)
			{
				Relation	newrel = (Relation) lfirst(newlc);

				if (RelationGetRelid(newrel) == oldrelid)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				Relation	oldrel = heap_open(oldrelid, AccessShareLock);
				delrels = lappend(delrels, oldrel);
			}
		}

		/* And drop them. */
		PublicationDropTables(pubid, delrels, true);

		/*
		 * Don't bother calculating the difference for adding, we'll catch
		 * and skip existing ones when doing catalog update.
		 */
		PublicationAddTables(pubid, rels, true, stmt);

		CloseTables(delrels);
	}

	CloseTables(rels);
}

/*
 * Alter the existing publication.
 *
 * This is dispatcher function for AlterPublicationOptions and
 * AlterPublicationTables.
 */
void
AlterPublication(AlterPublicationStmt *stmt)
{
	Relation		rel;
	HeapTuple		tup;

	check_replication_permissions();

	rel = heap_open(PublicationRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(PUBLICATIONNAME,
							  CStringGetDatum(stmt->pubname));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("publication \"%s\" does not exist",
						stmt->pubname)));

	if (stmt->options)
		AlterPublicationOptions(stmt, rel, tup);
	else
		AlterPublicationTables(stmt, rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, RowExclusiveLock);
}

/*
 * Drop publication by OID
 */
void
DropPublicationById(Oid pubid)
{
	Relation	rel;
	HeapTuple	tup;

	check_replication_permissions();

	rel = heap_open(PublicationRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for publication %u", pubid);

	simple_heap_delete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * Remove relation from publication by mapping OID.
 */
void
RemovePublicationRelById(Oid prid)
{
	Relation        rel;
	HeapTuple       tup;

	rel = heap_open(PublicationRelRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PUBLICATIONREL, ObjectIdGetDatum(prid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for publication table %u",
			 prid);

	simple_heap_delete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * Gather all tables optinally filtered by schema name.
 * The gathered tables are locked in access share lock mode.
 */
static List *
GatherTables(char *nspname)
{
	Oid			nspid = InvalidOid;
	List	   *rels = NIL;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tup;

	/* Resolve and validate the schema if specified */
	if (nspname)
	{
		nspid = LookupExplicitNamespace(nspname, false);
		if (IsSystemNamespace(nspid) || IsToastNamespace(nspid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only tables in user schemas can be added to publication"),
					 errdetail("%s is a system schema", strVal(nspname))));
	}

	/* Filter out anything that's not a table already in scan. */
	ScanKeyInit(&key[0],
				Anum_pg_class_relkind,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(RELKIND_RELATION));

	rel = heap_open(RelationRelationId, AccessShareLock);
	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 1, key);

	while ((tup = systable_getnext(scan)) != NULL)
	{
		Form_pg_class	relform = (Form_pg_class) GETSTRUCT(tup);
		Oid			schemarelid = HeapTupleGetOid(tup);
		Relation	schemarel;

		/*
		 * Filter out system tables (this is mostly for the case when schema
		 * is not specified).
		 */
		if (IsSystemClass(schemarelid, relform))
			continue;

		/* Filter out unlogged/temp tables. */
		if (relform->relpersistence != RELPERSISTENCE_PERMANENT)
			continue;

		/* Filter out tables in different schemas. */
		if (OidIsValid(nspid) && nspid != relform->relnamespace)
			continue;

		schemarel = heap_open(schemarelid, AccessShareLock);
		rels = lappend(rels, schemarel);
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return rels;
}

/*
 * Gather Relations based o provided by RangeVar list.
 * The gathered tables are locked in access share lock mode.
 */
static List *
GatherTableList(List *tables)
{
	List	   *relids = NIL;
	List	   *rels = NIL;
	ListCell   *lc;

	/*
	 * Open, share-lock, and check all the explicitly-specified relations
	 */
	foreach(lc, tables)
	{
		RangeVar   *rv = lfirst(lc);
		Relation	rel;
		bool		recurse = interpretInhOption(rv->inhOpt);
		Oid			myrelid;

		rel = heap_openrv(rv, AccessShareLock);
		myrelid = RelationGetRelid(rel);
		/* don't throw error for "foo, foo" */
		if (list_member_oid(relids, myrelid))
		{
			heap_close(rel, AccessShareLock);
			continue;
		}
		rels = lappend(rels, rel);
		relids = lappend_oid(relids, myrelid);

		if (recurse)
		{
			ListCell   *child;
			List	   *children;

			children = find_all_inheritors(myrelid, AccessShareLock,
										   NULL);

			foreach(child, children)
			{
				Oid			childrelid = lfirst_oid(child);

				if (list_member_oid(relids, childrelid))
					continue;

				/* find_all_inheritors already got lock */
				rel = heap_open(childrelid, NoLock);
				rels = lappend(rels, rel);
				relids = lappend_oid(relids, childrelid);
			}
		}
	}

	list_free(relids);

	return rels;
}

/*
 * Close all relations in the list.
 */
static void
CloseTables(List *rels)
{
	ListCell   *lc;

	foreach(lc, rels)
	{
		Relation	rel = (Relation) lfirst(lc);

		heap_close(rel, NoLock);
	}
}

/*
 * Add listed tables to the publication.
 */
static void
PublicationAddTables(Oid pubid, List *rels, bool if_not_exists,
					 AlterPublicationStmt *stmt)
{
	ListCell	   *lc;
	Oid				prid;

	foreach(lc, rels)
	{
		Relation	rel = (Relation) lfirst(lc);

		prid = publication_add_relation(pubid, rel, if_not_exists);
		if (stmt && !stmt->for_all_tables)
		{
			ObjectAddress	obj;
			ObjectAddressSet(obj, PublicationRelRelationId, prid);
			EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress,
											 (Node *) stmt);
		}
	}
}

/*
 * Remove listed tables to the publication.
 */
static void
PublicationDropTables(Oid pubid, List *rels, bool missing_ok)
{
	ObjectAddress	obj;
	ListCell	   *lc;
	Oid				prid;

	foreach(lc, rels)
	{
		Relation	rel = (Relation) lfirst(lc);
		Oid			relid = RelationGetRelid(rel);

		prid = GetSysCacheOid2(PUBLICATIONRELMAP, relid, pubid);
		if (!OidIsValid(prid))
		{
			if (missing_ok)
				continue;

			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("relation \"%s\" is not part of the publication",
							RelationGetRelationName(rel))));
		}

		ObjectAddressSet(obj, PublicationRelRelationId, prid);
		performDeletion(&obj, DROP_CASCADE, 0);
	}
}
