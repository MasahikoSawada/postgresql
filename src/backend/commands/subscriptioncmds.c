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

#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"

#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/replicationcmds.h"

#include "executor/spi.h"

#include "nodes/makefuncs.h"

#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/logicalworker.h"
#include "replication/origin.h"
#include "replication/reorderbuffer.h"
#include "replication/walreceiver.h"

#include "storage/ipc.h"
#include "storage/proc.h"

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
						   bool *create_slot, bool *copy_data,
						   char **conninfo, List **publications)
{
	ListCell   *lc;
	bool		create_slot_given = false;
	bool		copy_data_given = false;

	*enabled_given = false;
	*enabled = true;
	*create_slot = true;
	*copy_data = true;
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
		else if (strcmp(defel->defname, "create_slot") == 0)
		{
			if (create_slot_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			create_slot_given = true;
			*create_slot = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "copy_data") == 0)
		{
			if (copy_data_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			copy_data_given = true;
			*copy_data = defGetBoolean(defel);
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
	bool		create_slot;
	bool		copy_data;
	char	   *conninfo;
	char	   *slotname;
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
							   &create_slot, &copy_data,
							   &conninfo, &publications);
	slotname = stmt->subname;

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
		DirectFunctionCall1(namein, CStringGetDatum(slotname));
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

	if (create_slot || copy_data)
	{
		TimeLineID	startpointTLI;
		char	   *publisher_sysid;
		char	   *publisher_dbname;
		char		my_sysid[32];
		WalReceiverConnHandle  *wrchandle;
		WalReceiverConnAPI	   *wrcapi;
		walrcvconn_init_fn		walrcvconn_init;

		/*
		 * Now that the catalog update is done, try to reserve slot at the
		 * provider node using replication connection.
		 */
		wrcapi = palloc0(sizeof(WalReceiverConnAPI));

		walrcvconn_init = (walrcvconn_init_fn)
			load_external_function("libpqwalreceiver",
								   "_PG_walreceirver_conn_init", false, NULL);

		if (walrcvconn_init == NULL)
			elog(ERROR, "libpqwalreceiver does not declare _PG_walreceirver_conn_init symbol");

		wrchandle = walrcvconn_init(wrcapi);
		if (wrcapi->connect == NULL ||
			wrcapi->create_slot == NULL)
			elog(ERROR, "libpqwalreceiver didn't initialize correctly");

		/* Try to connect to the publisher. */
		wrcapi->connect(wrchandle, conninfo, true, stmt->subname);

		/* Identify the instance and check if it's not the same database. */
		publisher_sysid = wrcapi->identify_system(wrchandle, &startpointTLI,
												  &publisher_dbname);
		snprintf(my_sysid, sizeof(my_sysid), UINT64_FORMAT,
				 GetSystemIdentifier());
		if (strcmp(publisher_sysid, my_sysid) == 0 &&
			startpointTLI == ThisTimeLineID &&
			strcmp(publisher_dbname, get_database_name(MyDatabaseId)) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 (errmsg("the connection info provided points to the current database"))));

		/*
		 * If requested, create the replication slot on remote side for our
		 * newly created subscription.
		 *
		 * Note, we can't cleanup slot in case of failure as reason for
		 * failure might be already existing slot of the same name and we
		 * don't want to drop somebody else's slot by mistake.
		 */
		if (create_slot)
		{
			XLogRecPtr	lsn;

			wrcapi->create_slot(wrchandle, slotname, true, &lsn);
			ereport(NOTICE,
					(errmsg("created replication slot \"%s\" on provider",
							slotname)));
			/*
			 * Setup replication origin tracking.
			 */
			replorigin_create(slotname);
		}

		/*
		 * Also if requested set the copy status of all the existing tables
		 * so that the sync apply workers will be started for them.
		 */
		if (copy_data)
		{
			char	   *options;
			List	   *tables;
			ListCell   *lc;

			/* Build option string for the plugin. */
			options = logicalrep_build_options(publications);

			/*
			 * Get the table list from provider and build local table status
			 * info.
			 */
			tables = wrcapi->list_tables(wrchandle, slotname, options);
			foreach (lc, tables)
			{
				LogicalRepTableListEntry *entry = lfirst(lc);
				Oid		nspid = LookupExplicitNamespace(entry->nspname,
														false);
				Oid		relid = get_relname_relid(entry->relname, nspid);

				SetSubscriptionRelState(subid, relid, SUBREL_STATE_INIT,
										InvalidXLogRecPtr);
			}

			ereport(NOTICE,
					(errmsg("synchronized table states")));
		}

		/* And we are done with the remote side. */
		wrcapi->disconnect(wrchandle);
	}

	heap_close(rel, RowExclusiveLock);

	/* Make the changes visible. */
	CommandCounterIncrement();

	ApplyLauncherWakeupOnCommit();

	ObjectAddressSet(myself, SubscriptionRelationId, subid);

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
	bool		create_slot;
	bool		copy_data;
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
							   &create_slot  /* not used */,
							   &copy_data  /* not used */,
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

	/* Make the changes visible. */
	CommandCounterIncrement();

	ApplyLauncherWakeupOnCommit();

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
	Datum		datum;
	bool		isnull;
	char	   *subname;
	char	   *conninfo;
	char	   *slotname;
	RepOriginId	originid;
	MemoryContext			tmpctx,
							oldctx;
	WalReceiverConnHandle  *wrchandle = NULL;
	WalReceiverConnAPI	   *wrcapi = NULL;
	walrcvconn_init_fn		walrcvconn_init;
	LogicalRepWorker	   *worker;

	check_subscription_permissions();

	rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

	tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for subscription %u", subid);

	/*
	 * Create temporary memory context to keep copy of subscription
	 * info needed later in the execution.
	 */
	tmpctx = AllocSetContextCreate(TopMemoryContext,
										  "DropSubscription Ctx",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);
	oldctx = MemoryContextSwitchTo(tmpctx);

	/* Get subname */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup,
							Anum_pg_subscription_subname, &isnull);
	Assert(!isnull);
	subname = pstrdup(NameStr(*DatumGetName(datum)));

	/* Get conninfo */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup,
							Anum_pg_subscription_subconninfo, &isnull);
	Assert(!isnull);
	conninfo = pstrdup(TextDatumGetCString(datum));

	/* Get slotname */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup,
							Anum_pg_subscription_subslotname, &isnull);
	Assert(!isnull);
	slotname = pstrdup(NameStr(*DatumGetName(datum)));

	MemoryContextSwitchTo(oldctx);

	/* Remove the tuple from catalog. */
	simple_heap_delete(rel, &tup->t_self);

	/* Remove any associated relation synchronization states. */
	DropSubscriptionRel(subid, InvalidOid);

	/* Protect against launcher restarting the worker. */
	LWLockAcquire(LogicalRepLauncherLock, LW_EXCLUSIVE);

	/* Kill the apply worker so that the slot becomes accessible. */
	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
	worker = logicalrep_worker_find(subid, InvalidOid);
	if (worker)
		logicalrep_worker_stop(worker);
	LWLockRelease(LogicalRepWorkerLock);

	/* Wait for apply process to die. */
	for (;;)
	{
		int	rc;

		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
		if (logicalrep_worker_count(subid) < 1)
		{
			LWLockRelease(LogicalRepWorkerLock);
			break;
		}
		LWLockRelease(LogicalRepWorkerLock);

		/* Wait for more work. */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ResetLatch(&MyProc->procLatch);
	}

	/* Remove the origin trakicking. */
	originid = replorigin_by_name(slotname, true);
	if (originid != InvalidRepOriginId)
		replorigin_drop(originid);

	/*
	 * Now that the catalog update is done, try to reserve slot at the
	 * provider node using replication connection.
	 */
	wrcapi = palloc0(sizeof(WalReceiverConnAPI));

	walrcvconn_init = (walrcvconn_init_fn)
		load_external_function("libpqwalreceiver",
							   "_PG_walreceirver_conn_init", false, NULL);

	if (walrcvconn_init == NULL)
		elog(ERROR, "libpqwalreceiver does not declare _PG_walreceirver_conn_init symbol");

	wrchandle = walrcvconn_init(wrcapi);
	if (wrcapi->connect == NULL ||
		wrcapi->drop_slot == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	/*
	 * We must ignore error as that would make it impossible to drop
	 * subscription when provider is down.
	 */
	oldctx = CurrentMemoryContext;
	PG_TRY();
	{
		wrcapi->connect(wrchandle, conninfo, true, subname);
		wrcapi->drop_slot(wrchandle, slotname);
		ereport(NOTICE,
				(errmsg("dropped replication slot \"%s\" on provider",
						slotname)));
		wrcapi->disconnect(wrchandle);
	}
	PG_CATCH();
	{
		MemoryContext	ectx;
		ErrorData	   *edata;

		ectx = MemoryContextSwitchTo(oldctx);
		/* Save error info */
		edata = CopyErrorData();
		MemoryContextSwitchTo(ectx);
		FlushErrorState();

		ereport(WARNING,
				(errmsg("there was problem dropping the replication slot "
						"\"%s\" on provider", slotname),
				 errdetail("The error was: %s", edata->message),
				 errhint("You may have to drop it manually")));
		FreeErrorData(edata);
	}
	PG_END_TRY();

	/* Cleanup. */
	ReleaseSysCache(tup);
	heap_close(rel, NoLock);
}
