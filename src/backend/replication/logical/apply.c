/*-------------------------------------------------------------------------
 * apply.c
 *	   PostgreSQL logical replication
 *
 * Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/apply.c
 *
 * NOTES
 *	  This file contains the worker which applies logical changes as they come
 *	  from remote logical replication stream.
 *
 *	  The main worker (apply) is started by logical replication worker
 *	  launcher for every enabled subscription in a database. It uses
 *	  walsender protocol to communicate with publisher.
 *
 *	  The apply worker may spawn additional workers (sync) for initial data
 *	  synchronization of tables.
 *
 *	  This module includes server facing code and shares libpqwalreceiver
 *	  module with walreceiver for providing the libpq specific functionality.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/xact.h"
#include "access/xlog_internal.h"

#include "catalog/namespace.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"

#include "commands/trigger.h"

#include "executor/executor.h"
#include "executor/nodeModifyTable.h"

#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"

#include "mb/pg_wchar.h"

#include "optimizer/planner.h"

#include "parser/parse_relation.h"

#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/logicalworker.h"
#include "replication/reorderbuffer.h"
#include "replication/origin.h"
#include "replication/snapbuild.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"

#include "rewrite/rewriteHandler.h"

#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timeout.h"
#include "utils/tqual.h"
#include "utils/syscache.h"

typedef struct FlushPosition
{
	dlist_node	node;
	XLogRecPtr	local_end;
	XLogRecPtr	remote_end;
} FlushPosition;

static dlist_head lsn_mapping = DLIST_STATIC_INIT(lsn_mapping);


typedef struct TableState
{
	dlist_node	node;
	Oid			relid;
	XLogRecPtr	lsn;
	char		state;
} TableState;

static dlist_head table_states = DLIST_STATIC_INIT(table_states);
static XLogRecPtr		last_commit_lsn;

static MemoryContext	ApplyContext;

typedef struct LogicalRepRelMapEntry {
	LogicalRepRelation	remoterel;		/* key is remoterel.remoteid */

	/* Mapping to local relation, filled as needed. */
	Oid					reloid;			/* local relation id */
	Relation			rel;			/* relcache entry */
	int                *attmap;			/* map of remote attributes to
										 * local ones */
	AttInMetadata	   *attin;			/* cached info used in type
										 * conversion */
	char				state;
	XLogRecPtr			statelsn;
} LogicalRepRelMapEntry;

static HTAB *LogicalRepRelMap = NULL;

WalReceiverConnAPI	   *wrcapi = NULL;
WalReceiverConnHandle  *wrchandle = NULL;

LogicalRepWorker   *MyLogicalRepWorker = NULL;
Subscription	   *MySubscription = NULL;
bool				MySubscriptionValid = false;

static char	   *myslotname = NULL;
bool			in_remote_transaction = false;

static void send_feedback(XLogRecPtr recvpos, int64 now, bool force);
void pglogical_apply_main(Datum main_arg);

static bool tuple_find_by_replidx(Relation rel, LockTupleMode lockmode,
					  TupleTableSlot *searchslot, TupleTableSlot *slot);

static void reread_subscription(void);

/*
 * Should this worker apply changes for given relation.
 *
 * This is mainly needed for initial relation data sync as that runs in
 * parallel worker and we need some way to skip changes coming to the main
 * apply worker during the sync of a table.
 */
static bool
interesting_relation(LogicalRepRelMapEntry *rel)
{
	return rel->state == SUBREL_STATE_READY ||
		(rel->state == SUBREL_STATE_SYNCDONE &&
		 rel->statelsn < replorigin_session_origin_lsn) ||
		rel->reloid == MyLogicalRepWorker->relid;
}


/*
 * Relcache invalidation callback for our relation map cache.
 */
static void
logicalreprelmap_invalidate_cb(Datum arg, Oid reloid)
{
	LogicalRepRelMapEntry  *entry;

	/* Just to be sure. */
	if (LogicalRepRelMap == NULL)
		return;

	if (reloid != InvalidOid)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		/* TODO, use inverse lookup hastable? */
		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->reloid == reloid)
				entry->reloid = InvalidOid;
		}
	}
	else
	{
		/* invalidate all cache entries */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
			entry->reloid = InvalidOid;
	}
}

/*
 * Initialize the relation map cache.
 */
static void
remoterelmap_init(void)
{
	HASHCTL		ctl;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(LogicalRepRelMapEntry);
	ctl.hcxt = CacheMemoryContext;

	LogicalRepRelMap = hash_create("logicalrep relation map cache", 128, &ctl,
								   HASH_ELEM | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(logicalreprelmap_invalidate_cb,
								  (Datum) 0);
}

/*
 * Free the entry of a relation map cache.
 */
static void
remoterelmap_free_entry(LogicalRepRelMapEntry *entry)
{
	LogicalRepRelation *remoterel;

	remoterel = &entry->remoterel;

	pfree(remoterel->nspname);
	pfree(remoterel->relname);

	if (remoterel->natts > 0)
	{
		int	i;

		for (i = 0; i < remoterel->natts; i++)
			pfree(remoterel->attnames[i]);

		pfree(remoterel->attnames);
	}

	if (entry->attmap)
		pfree(entry->attmap);

	remoterel->natts = 0;
	entry->reloid = InvalidOid;
	entry->rel = NULL;
}

/*
 * Add new entry or update existing entry in the relation map cache.
 *
 * Called when new relation mapping is sent by the provider to update
 * our expected view of incoming data from said provider.
 */
static void
remoterelmap_update(LogicalRepRelation *remoterel)
{
	MemoryContext			oldctx;
	LogicalRepRelMapEntry  *entry;
	bool					found;
	int						i;

	if (LogicalRepRelMap == NULL)
		remoterelmap_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(LogicalRepRelMap, (void *) &remoterel->remoteid,
						HASH_ENTER, &found);

	if (found)
		remoterelmap_free_entry(entry);

	/* Make cached copy of the data */
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);
	entry->remoterel.remoteid = remoterel->remoteid;
	entry->remoterel.nspname = pstrdup(remoterel->nspname);
	entry->remoterel.relname = pstrdup(remoterel->relname);
	entry->remoterel.natts = remoterel->natts;
	entry->remoterel.attnames = palloc(remoterel->natts * sizeof(char*));
	for (i = 0; i < remoterel->natts; i++)
		entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
	entry->attmap = palloc(remoterel->natts * sizeof(int));
	entry->reloid = InvalidOid;
	MemoryContextSwitchTo(oldctx);
}

/*
 * Find attribute index in TupleDesc struct by attribute name.
 */
static int
tupdesc_get_att_by_name(LogicalRepRelation *remoterel, TupleDesc desc,
						const char *attname)
{
	int		i;

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = desc->attrs[i];

		if (strcmp(NameStr(att->attname), attname) == 0)
			return i;
	}

	elog(ERROR, "unknown column name %s for logical replication table %s",
		 attname, quote_qualified_identifier(remoterel->nspname,
											 remoterel->relname));
}

/*
 * Open the local relation associated with the remote one.
 *
 * Optionally rebuilds the Relcache mapping if it was invalidated
 * by local DDL.
 */
static LogicalRepRelMapEntry *
logicalreprel_open(uint32 remoteid, LOCKMODE lockmode)
{
	LogicalRepRelMapEntry  *entry;
	bool		found;

	if (LogicalRepRelMap == NULL)
		remoterelmap_init();

	/* Search for existing entry. */
	entry = hash_search(LogicalRepRelMap, (void *) &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(FATAL, "cache lookup failed for remote relation %u",
			 remoteid);

	/* Need to update the local cache? */
	if (!OidIsValid(entry->reloid))
	{
		Oid			nspid;
		Oid			relid;
		int			i;
		TupleDesc	desc;
		LogicalRepRelation *remoterel;

		remoterel = &entry->remoterel;

		nspid = LookupExplicitNamespace(remoterel->nspname, false);
		if (!OidIsValid(nspid))
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the logical replication target %s not found",
							quote_qualified_identifier(remoterel->nspname,
													   remoterel->relname))));
		relid = get_relname_relid(remoterel->relname, nspid);
		if (!OidIsValid(relid))
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the logical replication target %s not found",
							quote_qualified_identifier(remoterel->nspname,
													   remoterel->relname))));

		entry->rel = heap_open(relid, lockmode);

		/* We currently only support writing to regular tables. */
		if (entry->rel->rd_rel->relkind != RELKIND_RELATION)
			ereport(FATAL,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("the logical replication target %s is not a table",
							quote_qualified_identifier(remoterel->nspname,
													   remoterel->relname))));

		desc = RelationGetDescr(entry->rel);
		for (i = 0; i < remoterel->natts; i++)
			entry->attmap[i] = tupdesc_get_att_by_name(remoterel, desc,
													   remoterel->attnames[i]);

		entry->reloid = relid;
	}
	else
		entry->rel = heap_open(entry->reloid, lockmode);

	if (entry->state != SUBREL_STATE_READY)
		entry->state = GetSubscriptionRelState(MySubscription->oid,
											   entry->reloid,
											   &entry->statelsn,
											   true);

	return entry;
}

/*
 * Close the previously opened logical relation.
 */
static void
logicalreprel_close(LogicalRepRelMapEntry *rel, LOCKMODE lockmode)
{
	heap_close(rel->rel, lockmode);
	rel->rel = NULL;
}

/*
 * Make sure that we started local transaction.
 *
 * Also switches to ApplyContext as necessary.
 */
static bool
ensure_transaction(void)
{
	if (IsTransactionState())
	{
		if (CurrentMemoryContext != ApplyContext)
			MemoryContextSwitchTo(ApplyContext);
		return false;
	}

	StartTransactionCommand();

	if (!MySubscriptionValid)
		reread_subscription();

	MemoryContextSwitchTo(ApplyContext);
	return true;
}


/*
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
static EState *
create_estate_for_relation(LogicalRepRelMapEntry *rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	RangeTblEntry *rte;

	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel->rel);
	rte->relkind = rel->rel->rd_rel->relkind;
	estate->es_range_table = list_make1(rte);

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel->rel, 1, 0);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	/* Triggers might need a slot */
	if (resultRelInfo->ri_TrigDesc)
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);

	return estate;
}

/*
 * Check if the local attribute is present in relation definition used
 * by upstream and hence updated by the replication.
 */
static bool
physatt_in_attmap(LogicalRepRelMapEntry *rel, int attid)
{
	AttrNumber	i;

	/* Fast path for tables that are same on upstream and downstream. */
	if (attid < rel->remoterel.natts && rel->attmap[attid] == attid)
		return true;

	/* Try to find the attribute in the map. */
	for (i = 0; i < rel->remoterel.natts; i++)
		if (rel->attmap[i] == attid)
			return true;

	return false;
}

/*
 * Executes default values for columns for which we can't map to remote
 * relation columns.
 *
 * This allows us to support tables which have more columns on the downstream
 * than on the upsttream.
 */
static void
FillSlotDefaults(LogicalRepRelMapEntry *rel, EState *estate,
				 TupleTableSlot *slot)
{
	TupleDesc	desc = RelationGetDescr(rel->rel);
	AttrNumber	num_phys_attrs = desc->natts;
	int			i;
	AttrNumber	attnum,
				num_defaults = 0;
	int		   *defmap;
	ExprState **defexprs;
	ExprContext *econtext;

	econtext = GetPerTupleExprContext(estate);

	/* We got all the data via replication, no need to evaluate anything. */
	if (num_phys_attrs == rel->remoterel.natts)
		return;

	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 0; attnum < num_phys_attrs; attnum++)
	{
		Expr	   *defexpr;

		if (desc->attrs[attnum]->attisdropped)
			continue;

		if (physatt_in_attmap(rel, attnum))
			continue;

		defexpr = (Expr *) build_column_default(rel->rel, attnum + 1);

		if (defexpr != NULL)
		{
			/* Run the expression through planner */
			defexpr = expression_planner(defexpr);

			/* Initialize executable expression in copycontext */
			defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
			defmap[num_defaults] = attnum;
			num_defaults++;
		}

	}

	for (i = 0; i < num_defaults; i++)
		slot->tts_values[defmap[i]] =
			ExecEvalExpr(defexprs[i], econtext, &slot->tts_isnull[defmap[i]],
						 NULL);
}

/*
 * Store data in C string form into slot.
 * This is similar to BuildTupleFromCStrings but TupleTableSlot fits our
 * use better.
 */
static void
SlotStoreCStrings(TupleTableSlot *slot, char **values)
{
	int		natts = slot->tts_tupleDescriptor->natts;
	int		i;

	ExecClearTuple(slot);

	/* Call the "in" function for each non-dropped attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = slot->tts_tupleDescriptor->attrs[i];

		if (!att->attisdropped && values[i] != NULL)
		{
			Oid typinput;
			Oid typioparam;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput, values[i],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			/* We assign NULL for both NULL values and dropped attributes. */
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	ExecStoreVirtualTuple(slot);
}

/*
 * Modify slot with user data provided as C strigs.
 * This is somewhat similar to heap_modify_tuple but also calls the type
 * input fuction on the user data as the input is the rext representation
 * of the types.
 */
static void
SlotModifyCStrings(TupleTableSlot *slot, char **values, bool *replaces)
{
	int		natts = slot->tts_tupleDescriptor->natts;
	int		i;

	slot_getallattrs(slot);
	ExecClearTuple(slot);

	/* Call the "in" function for each replaced attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = slot->tts_tupleDescriptor->attrs[i];

		if (!replaces[i])
			continue;

		if (values[i] != NULL)
		{
			Oid typinput;
			Oid typioparam;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput, values[i],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	ExecStoreVirtualTuple(slot);
}

/*
 * Handle BEGIN message.
 */
static void
handle_begin(StringInfo s)
{
	XLogRecPtr		commit_lsn;
	TimestampTz		commit_time;
	TransactionId	remote_xid;

	logicalrep_read_begin(s, &commit_lsn, &commit_time, &remote_xid);

	replorigin_session_origin_timestamp = commit_time;
	replorigin_session_origin_lsn = commit_lsn;

	in_remote_transaction = true;

	pgstat_report_activity(STATE_RUNNING, NULL);
}

/*
 * Handle COMMIT message.
 *
 * TODO, support tracking of multiple origins
 */
static void
handle_commit(StringInfo s)
{
	XLogRecPtr		commit_lsn;
	XLogRecPtr		end_lsn;
	TimestampTz		commit_time;

	logicalrep_read_commit(s, &commit_lsn, &end_lsn, &commit_time);

	Assert(commit_lsn == replorigin_session_origin_lsn);
	Assert(commit_time == replorigin_session_origin_timestamp);

	if (IsTransactionState())
	{
		FlushPosition *flushpos;

		CommitTransactionCommand();
		MemoryContextSwitchTo(CacheMemoryContext);

		/* Track commit lsn  */
		flushpos = (FlushPosition *) palloc(sizeof(FlushPosition));
		flushpos->local_end = XactLastCommitEnd;
		flushpos->remote_end = end_lsn;

		dlist_push_tail(&lsn_mapping, &flushpos->node);
		MemoryContextSwitchTo(ApplyContext);
	}

	in_remote_transaction = false;

	last_commit_lsn = end_lsn;
	process_syncing_tables(myslotname, end_lsn);

	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Handle ORIGIN message.
 *
 * TODO, support tracking of multiple origins
 */
static void
handle_origin(StringInfo s)
{
	/*
	 * ORIGIN message can only come inside remote transaction and before
	 * any actual writes.
	 */
	if (!in_remote_transaction || IsTransactionState())
		elog(ERROR, "ORIGIN message sent out of order");
}

/*
 * Handle RELATION message.
 *
 * Note we don't do validation against local schema here. The validation is
 * posponed until first change for given relation comes.
 */
static void
handle_relation(StringInfo s)
{
	LogicalRepRelation  *rel;

	rel = logicalrep_read_rel(s);
	remoterelmap_update(rel);
}


/*
 * Handle INSERT message.
 */
static void
handle_insert(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepTupleData	newtup;
	LogicalRepRelId		relid;
	EState			   *estate;
	TupleTableSlot	   *remoteslot;
	MemoryContext		oldctx;

	ensure_transaction();

	relid = logicalrep_read_insert(s, &newtup);
	rel = logicalreprel_open(relid, RowExclusiveLock);
	if (!interesting_relation(rel))
	{
		/*
		 * The relation can't become interestin in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalreprel_close(rel, RowExclusiveLock);
		return;
	}

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(remoteslot, RelationGetDescr(rel->rel));

	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	SlotStoreCStrings(remoteslot, newtup.values);
	FillSlotDefaults(rel, estate, remoteslot);
	MemoryContextSwitchTo(oldctx);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	ExecInsert(NULL, /* mtstate is only used for onconflict handling which we don't support atm */
			   remoteslot,
			   remoteslot,
			   NIL,
			   ONCONFLICT_NONE,
			   estate,
			   false);

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	logicalreprel_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Handle UPDATE message.
 *
 * TODO: FDW support
 */
static void
handle_update(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepRelId		relid;
	EState			   *estate;
	EPQState			epqstate;
	LogicalRepTupleData	oldtup;
	LogicalRepTupleData	newtup;
	bool				hasoldtup;
	TupleTableSlot	   *localslot;
	TupleTableSlot	   *remoteslot;
	bool				found;
	MemoryContext		oldctx;

	ensure_transaction();

	relid = logicalrep_read_update(s, &hasoldtup, &oldtup,
								   &newtup);
	rel = logicalreprel_open(relid, RowExclusiveLock);
	if (!interesting_relation(rel))
	{
		/*
		 * The relation can't become interestin in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalreprel_close(rel, RowExclusiveLock);
		return;
	}

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(remoteslot, RelationGetDescr(rel->rel));
	localslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Find the tuple using the replica identity index. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	SlotStoreCStrings(remoteslot, hasoldtup ? oldtup.values : newtup.values);
	MemoryContextSwitchTo(oldctx);
	found = tuple_find_by_replidx(rel->rel, LockTupleExclusive,
								  remoteslot, localslot);
	ExecClearTuple(remoteslot);

	/*
	 * Tuple found.
	 *
	 * Note this will fail if there are other conflicting unique indexes.
	 */
	if (found)
	{
		/* Process and store remote tuple in the slot */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		ExecStoreTuple(localslot->tts_tuple, remoteslot, InvalidBuffer, false);
		SlotModifyCStrings(remoteslot, newtup.values, newtup.changed);
		MemoryContextSwitchTo(oldctx);

		EvalPlanQualSetSlot(&epqstate, remoteslot);

		ExecUpdate(&localslot->tts_tuple->t_self,
				   localslot->tts_tuple,
				   remoteslot,
				   localslot,
				   &epqstate,
				   estate,
				   false);
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 * TODO what to do here?
		 */
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	logicalreprel_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Handle DELETE message.
 *
 * TODO: FDW support
 */
static void
handle_delete(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepTupleData	oldtup;
	LogicalRepRelId		relid;
	EState			   *estate;
	EPQState			epqstate;
	TupleTableSlot	   *remoteslot;
	TupleTableSlot	   *localslot;
	bool				found;
	MemoryContext		oldctx;

	ensure_transaction();

	relid = logicalrep_read_delete(s, &oldtup);
	rel = logicalreprel_open(relid, RowExclusiveLock);
	if (!interesting_relation(rel))
	{
		/*
		 * The relation can't become interestin in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalreprel_close(rel, RowExclusiveLock);
		return;
	}

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(remoteslot, RelationGetDescr(rel->rel));
	localslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Find the tuple using the replica identity index. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	SlotStoreCStrings(remoteslot, oldtup.values);
	MemoryContextSwitchTo(oldctx);
	found = tuple_find_by_replidx(rel->rel, LockTupleExclusive,
								  remoteslot, localslot);
	/* If found delete it. */
	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);
		ExecDelete(&localslot->tts_tuple->t_self,
				   localslot->tts_tuple,
				   localslot,
				   &epqstate,
				   estate,
				   false);
	}
	else
	{
		/* The tuple to be deleted could not be found.*/
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	logicalreprel_close(rel, NoLock);

	CommandCounterIncrement();
}


/*
 * Logical replication protocol message dispatcher.
 */
static void
handle_message(StringInfo s)
{
	char action = pq_getmsgbyte(s);

	switch (action)
	{
		/* BEGIN */
		case 'B':
			handle_begin(s);
			break;
		/* COMMIT */
		case 'C':
			handle_commit(s);
			break;
		/* INSERT */
		case 'I':
			handle_insert(s);
			break;
		/* UPDATE */
		case 'U':
			handle_update(s);
			break;
		/* DELETE */
		case 'D':
			handle_delete(s);
			break;
		/* RELATION */
		case 'R':
			handle_relation(s);
			break;
		/* ORIGIN */
		case 'O':
			handle_origin(s);
			break;
		default:
			elog(ERROR, "unknown action of type %c", action);
	}
}

/*
 * Figure out which write/flush positions to report to the walsender process.
 *
 * We can't simply report back the last LSN the walsender sent us because the
 * local transaction might not yet be flushed to disk locally. Instead we
 * build a list that associates local with remote LSNs for every commit. When
 * reporting back the flush position to the sender we iterate that list and
 * check which entries on it are already locally flushed. Those we can report
 * as having been flushed.
 *
 * Returns true if there's no outstanding transactions that need to be
 * flushed.
 */
static bool
get_flush_position(XLogRecPtr *write, XLogRecPtr *flush)
{
	dlist_mutable_iter iter;
	XLogRecPtr	local_flush = GetFlushRecPtr();

	*write = InvalidXLogRecPtr;
	*flush = InvalidXLogRecPtr;

	dlist_foreach_modify(iter, &lsn_mapping)
	{
		FlushPosition *pos =
			dlist_container(FlushPosition, node, iter.cur);

		*write = pos->remote_end;

		if (pos->local_end <= local_flush)
		{
			*flush = pos->remote_end;
			dlist_delete(iter.cur);
			pfree(pos);
		}
		else
		{
			/*
			 * Don't want to uselessly iterate over the rest of the list which
			 * could potentially be long. Instead get the last element and
			 * grab the write position from there.
			 */
			pos = dlist_tail_element(FlushPosition, node,
									 &lsn_mapping);
			*write = pos->remote_end;
			return false;
		}
	}

	return dlist_is_empty(&lsn_mapping);
}

/* Update statistics of the worker. */
static void
UpdateWorkerStats(XLogRecPtr last_lsn, TimestampTz send_time, bool reply)
{
	MyLogicalRepWorker->last_lsn = last_lsn;
	MyLogicalRepWorker->last_send_time = send_time;
	MyLogicalRepWorker->last_recv_time = GetCurrentTimestamp();
	if (reply)
	{
		MyLogicalRepWorker->reply_lsn = last_lsn;
		MyLogicalRepWorker->reply_time = send_time;
	}
}

/*
 * Apply main loop.
 */
void
LogicalRepApplyLoop(XLogRecPtr last_received)
{
	/* Init the ApplyContext which we use for easier cleanup. */
	ApplyContext = AllocSetContextCreate(TopMemoryContext,
										 "ApplyContext",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	/* mark as idle, before starting to loop */
	pgstat_report_activity(STATE_IDLE, NULL);

	while (!got_SIGTERM)
	{
		pgsocket	fd = PGINVALID_SOCKET;
		int			rc;
		int			len;
		char	   *buf = NULL;
		bool		endofstream = false;

		MemoryContextSwitchTo(ApplyContext);

		len = wrcapi->receive(wrchandle, &buf, &fd);

		if (len != 0)
		{
			/* Process the data */
			for (;;)
			{
				CHECK_FOR_INTERRUPTS();

				if (len == 0)
				{
					break;
				}
				else if (len < 0)
				{
					elog(NOTICE, "data stream from provider has ended");
					endofstream = true;
					break;
				}
				else
				{
					int c;
					StringInfoData s;

					/* Ensure we are reading the data into our memory context. */
					MemoryContextSwitchTo(ApplyContext);

					initStringInfo(&s);
					s.data = buf;
					s.len = len;
					s.maxlen = -1;

					c = pq_getmsgbyte(&s);

					if (c == 'w')
					{
						XLogRecPtr	start_lsn;
						XLogRecPtr	end_lsn;
						TimestampTz	send_time;

						start_lsn = pq_getmsgint64(&s);
						end_lsn = pq_getmsgint64(&s);
						send_time =
							IntegerTimestampToTimestampTz(pq_getmsgint64(&s));

						if (last_received < start_lsn)
							last_received = start_lsn;

						if (last_received < end_lsn)
							last_received = end_lsn;

						UpdateWorkerStats(last_received, send_time, false);

						handle_message(&s);
					}
					else if (c == 'k')
					{
						XLogRecPtr endpos;
						TimestampTz	timestamp;
						bool reply_requested;

						endpos = pq_getmsgint64(&s);
						timestamp =
							IntegerTimestampToTimestampTz(pq_getmsgint64(&s));
						reply_requested = pq_getmsgbyte(&s);

						send_feedback(endpos,
									  GetCurrentTimestamp(),
									  reply_requested);
						UpdateWorkerStats(last_received, timestamp, true);
					}
					/* other message types are purposefully ignored */
				}

				len = wrcapi->receive(wrchandle, &buf, &fd);
			}
		}

		if (!in_remote_transaction)
		{
			/*
			 * If we didn't get any transactions for a while there might be
			 * unconsumed invalidation messages in the queue, consume them now.
			 */
			StartTransactionCommand();
			/* Check for subscription change */
			if (!MySubscriptionValid)
				reread_subscription();
			CommitTransactionCommand();

			/* Process any table synchronization changes. */
			process_syncing_tables(myslotname, last_received);
		}

		/* confirm all writes at once */
		send_feedback(last_received, GetCurrentTimestamp(), false);

		/* Cleanup the memory. */
		MemoryContextResetAndDeleteChildren(ApplyContext);
		MemoryContextSwitchTo(TopMemoryContext);

		/* Check if we need to exit the streaming loop. */
		if (endofstream)
		{
			TimeLineID	tli;
			wrcapi->endstreaming(wrchandle, &tli);
			break;
		}

		/*
		 * Wait for more data or latch.
		 */
		rc = WaitLatchOrSocket(&MyProc->procLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET |
							   WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   fd, 1000L);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ResetLatch(&MyProc->procLatch);
	}
}

/*
 * Send a Standby Status Update message to server.
 *
 * 'recvpos' is the latest LSN we've received data to, force is set if we need
 * to send a response to avoid timeouts.
 */
static void
send_feedback(XLogRecPtr recvpos, int64 now, bool force)
{
	static StringInfo	reply_message = NULL;

	static XLogRecPtr last_recvpos = InvalidXLogRecPtr;
	static XLogRecPtr last_writepos = InvalidXLogRecPtr;
	static XLogRecPtr last_flushpos = InvalidXLogRecPtr;

	XLogRecPtr writepos;
	XLogRecPtr flushpos;

	/* It's legal to not pass a recvpos */
	if (recvpos < last_recvpos)
		recvpos = last_recvpos;

	if (get_flush_position(&writepos, &flushpos))
	{
		/*
		 * No outstanding transactions to flush, we can report the latest
		 * received position. This is important for synchronous replication.
		 */
		flushpos = writepos = recvpos;
	}

	if (writepos < last_writepos)
		writepos = last_writepos;

	if (flushpos < last_flushpos)
		flushpos = last_flushpos;

	/* if we've already reported everything we're good */
	if (!force &&
		writepos == last_writepos &&
		flushpos == last_flushpos)
		return;

	if (!reply_message)
	{
		MemoryContext	oldctx = MemoryContextSwitchTo(CacheMemoryContext);
		reply_message = makeStringInfo();
		MemoryContextSwitchTo(oldctx);
	}
	else
		resetStringInfo(reply_message);

	pq_sendbyte(reply_message, 'r');
	pq_sendint64(reply_message, recvpos);		/* write */
	pq_sendint64(reply_message, flushpos);		/* flush */
	pq_sendint64(reply_message, writepos);		/* apply */
	pq_sendint64(reply_message, now);			/* sendTime */
	pq_sendbyte(reply_message, false);			/* replyRequested */

	elog(DEBUG2, "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X",
		 force,
		 (uint32) (recvpos >> 32), (uint32) recvpos,
		 (uint32) (writepos >> 32), (uint32) writepos,
		 (uint32) (flushpos >> 32), (uint32) flushpos
		);

	wrcapi->send(wrchandle, reply_message->data, reply_message->len);

	if (recvpos > last_recvpos)
		last_recvpos = recvpos;
	if (writepos > last_writepos)
		last_writepos = writepos;
	if (flushpos > last_flushpos)
		last_flushpos = flushpos;
}


/*
 * Reread subscription info and exit on change.
 */
static void
reread_subscription(void)
{
	MemoryContext	oldctx;
	Subscription   *newsub;

	/* Ensure allocations in permanent context. */
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);

	newsub = GetSubscription(MyLogicalRepWorker->subid, true);

	/*
	 * Exit if connection string was changed. The launcher will start
	 * new worker.
	 */
	if (strcmp(newsub->conninfo, MySubscription->conninfo) != 0)
	{
		elog(LOG, "logical replication worker for subscription %s will "
			 "restart because the connection info was changed",
			 MySubscription->name);

		wrcapi->disconnect(wrchandle);
		proc_exit(0);
	}

	/*
	 * Exit if publication list was changed. The launcher will start
	 * new worker.
	 */
	if (!equal(newsub->publications, MySubscription->publications))
	{
		elog(LOG, "logical replication worker for subscription %s will "
			 "restart because ssubscription's publications were changed",
			 MySubscription->name);

		wrcapi->disconnect(wrchandle);
		proc_exit(0);
	}

	/*
	 * Exit if the subscription was removed.
	 * This normally should not happen as the worker gets killed
	 * during DROP SUBSCRIPTION.
	 */
	if (!newsub)
	{
		elog(LOG, "logical replication worker for subscription %s has "
			 "stopped because the subscription was removed",
			 MySubscription->name);

		wrcapi->disconnect(wrchandle);
		proc_exit(0);
	}

	/*
	 * Exit if the subscription was disabled.
	 * This normally should not happen as the worker gets killed
	 * during ALTER SUBSCRIPTION ... DISABLE.
	 */
	if (!newsub->enabled)
	{
		elog(LOG, "logical replication worker for subscription %s has "
			 "stopped because the subscription was disabled",
			 MySubscription->name);

		wrcapi->disconnect(wrchandle);
		proc_exit(0);
	}

	/* Check for other changes that should never happen too. */
	if (newsub->dbid != MySubscription->dbid ||
		strcmp(newsub->name, MySubscription->name) != 0 ||
		strcmp(newsub->slotname, MySubscription->slotname) != 0)
	{
		elog(ERROR, "subscription %u changed unexpectedly",
			 MyLogicalRepWorker->subid);
	}

	/* Clean old subscription info and switch to new one. */
	FreeSubscription(MySubscription);
	MySubscription = newsub;

	MemoryContextSwitchTo(oldctx);

	MySubscriptionValid = true;
}

/*
 * Callback from subscription syscache invalidation.
 */
static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	MySubscriptionValid = false;
}


/* Logical Replication Apply worker entry point */
void
ApplyWorkerMain(Datum main_arg)
{
	int				worker_slot = DatumGetObjectId(main_arg);
	MemoryContext	oldctx;
	XLogRecPtr		origin_startpos;
	char		   *options;
	walrcvconn_init_fn walrcvconn_init;

	/* Attach to slot */
	logicalrep_worker_attach(worker_slot);

	/* Setup signal handling */
	pqsignal(SIGTERM, logicalrep_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Initialise stats to a sanish value */
	MyLogicalRepWorker->last_send_time = MyLogicalRepWorker->last_recv_time =
		MyLogicalRepWorker->reply_time = GetCurrentTimestamp();

	/* Make it easy to identify our processes. */
	SetConfigOption("application_name", MyBgworkerEntry->bgw_name,
					PGC_USERSET, PGC_S_SESSION);

	/* Load the libpq-specific functions */
	wrcapi = palloc0(sizeof(WalReceiverConnAPI));

	walrcvconn_init = (walrcvconn_init_fn)
		load_external_function("libpqwalreceiver",
							   "_PG_walreceirver_conn_init", false, NULL);

	if (walrcvconn_init == NULL)
		elog(ERROR, "libpqwalreceiver does not declare _PG_walreceirver_conn_init symbol");

	wrchandle = walrcvconn_init(wrcapi);
	if (wrcapi->connect == NULL ||
		wrcapi->startstreaming_logical == NULL ||
		wrcapi->identify_system == NULL ||
		wrcapi->receive == NULL || wrcapi->send == NULL ||
		wrcapi->disconnect == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL,
											   "logical replication apply");

	/* Setup synchronous commit according to the user's wishes */
/*	SetConfigOption("synchronous_commit",
					logical_apply_synchronous_commit,
					PGC_BACKEND, PGC_S_OVERRIDE);
*/
	/* Run as replica session replication role. */
	SetConfigOption("session_replication_role", "replica",
					PGC_SUSET, PGC_S_OVERRIDE);

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyLogicalRepWorker->dbid,
											  InvalidOid);

	/* Load the subscription into persistent memory context. */
	StartTransactionCommand();
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);
	MySubscription = GetSubscription(MyLogicalRepWorker->subid, false);
	MySubscriptionValid = true;
	MemoryContextSwitchTo(oldctx);

	if (!MySubscription->enabled)
	{
		elog(LOG, "logical replication worker for subscription %s will not "
			 "start because the subscription was disabled during startup",
			 MySubscription->name);

		wrcapi->disconnect(wrchandle);
		proc_exit(0);
	}

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

	if (OidIsValid(MyLogicalRepWorker->relid))
		elog(LOG, "logical replication sync for subscription %s, table %s started",
			 MySubscription->name, get_rel_name(MyLogicalRepWorker->relid));
	else
		elog(LOG, "logical replication apply for subscription %s started",
			 MySubscription->name);
	CommitTransactionCommand();

	/* Connect to the origin and start the replication. */
	elog(DEBUG1, "connecting to provider using connection string %s",
		 MySubscription->conninfo);

	/* Build option string for the plugin. */
	options = logicalrep_build_options(MySubscription->publications);

	if (OidIsValid(MyLogicalRepWorker->relid))
	{
		/* This is table synchroniation worker, call initial sync. */
		myslotname = LogicalRepSyncTableStart(&origin_startpos);
	}
	else
	{
		/* This is main apply worker */
		RepOriginId		originid;

		myslotname = MySubscription->slotname;

		StartTransactionCommand();
		originid = replorigin_by_name(myslotname, false);
		replorigin_session_setup(originid);
		replorigin_session_origin = originid;
		origin_startpos = replorigin_session_get_progress(false);
		CommitTransactionCommand();

		wrcapi->connect(wrchandle, MySubscription->conninfo, true,
						myslotname);
	}

	/*
	 * Setup callback for syscache so that we know when something
	 * changes in the subscription relation state.
	 */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONRELOID,
								  invalidate_syncing_table_states,
								  (Datum) 0);

	/* Start normal logical streaming replication. */
	wrcapi->startstreaming_logical(wrchandle, origin_startpos,
								   myslotname, options);

	pfree(options);

	/* Run the main loop. */
	LogicalRepApplyLoop(origin_startpos);

	wrcapi->disconnect(wrchandle);

	/* We should only get here if we received sigTERM */
	proc_exit(0);
}

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 *
 * Returns whether any column contains NULLs.
 *
 * This is not generic routine, it expects the idxrel to be replication
 * identity of a rel and meet all limitations associated with that.
 */
static bool
build_replindex_scan_key(ScanKey skey, Relation rel, Relation idxrel,
						 TupleTableSlot *searchslot)
{
	int			attoff;
	bool		isnull;
	Datum		indclassDatum;
	oidvector  *opclass;
	int2vector *indkey = &idxrel->rd_index->indkey;
	bool		hasnulls = false;

	Assert(RelationGetReplicaIndex(rel) == RelationGetRelid(idxrel));

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	/* Build scankey for every attribute in the index. */
	for (attoff = 0; attoff < RelationGetNumberOfAttributes(idxrel); attoff++)
	{
		Oid			operator;
		Oid			opfamily;
		RegProcedure regop;
		int			pkattno = attoff + 1;
		int			mainattno = indkey->values[attoff];
		Oid			atttype = attnumTypeId(rel, mainattno);
		Oid			optype = get_opclass_input_type(opclass->values[attoff]);

		/*
		 * Load the operator info, we need this to get the equality operator
		 * function for the scankey.
		 */
		opfamily = get_opclass_family(opclass->values[attoff]);

		operator = get_opfamily_member(opfamily, optype,
									   optype,
									   BTEqualStrategyNumber);

		if (!OidIsValid(operator))
			elog(ERROR,
				 "could not lookup equality operator for type %u, optype %u in opfamily %u",
				 atttype, optype, opfamily);

		regop = get_opcode(operator);

		/* Initialize the scankey. */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					searchslot->tts_values[mainattno - 1]);

		/* Check for null value. */
		if (searchslot->tts_isnull[mainattno - 1])
		{
			hasnulls = true;
			skey[attoff].sk_flags |= SK_ISNULL;
		}
	}

	return hasnulls;
}

/*
 * Search the relation 'rel' for tuple using the replication index.
 *
 * If a matching tuple is found lock it with lockmode, fill the slot with its
 * contents and return true, return false is returned otherwise.
 */
static bool
tuple_find_by_replidx(Relation rel, LockTupleMode lockmode,
					  TupleTableSlot *searchslot, TupleTableSlot *slot)
{
	HeapTuple		scantuple;
	ScanKeyData		skey[INDEX_MAX_KEYS];
	IndexScanDesc	scan;
	SnapshotData	snap;
	TransactionId	xwait;
	Oid				idxoid;
	Relation		idxrel;
	bool			found;

	/* Open REPLICA IDENTITY index.*/
	idxoid = RelationGetReplicaIndex(rel);
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find configured replica identity for table \"%s\"",
			 RelationGetRelationName(rel));
		return false;
	}
	idxrel = index_open(idxoid, RowExclusiveLock);

	/* Start an index scan. */
	InitDirtySnapshot(snap);
	scan = index_beginscan(rel, idxrel, &snap,
						   RelationGetNumberOfAttributes(idxrel),
						   0);

	/* Build scan key. */
	build_replindex_scan_key(skey, rel, idxrel, searchslot);

retry:
	found = false;

	index_rescan(scan, skey, RelationGetNumberOfAttributes(idxrel), NULL, 0);

	/* Try to find the tuple */
	if ((scantuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		found = true;
		ExecStoreTuple(scantuple, slot, InvalidBuffer, false);
		ExecMaterializeSlot(slot);

		xwait = TransactionIdIsValid(snap.xmin) ?
			snap.xmin : snap.xmax;

		/*
		 * If the tuple is locked, wait for locking transaction to finish
		 * and retry.
		 */
		if (TransactionIdIsValid(xwait))
		{
			XactLockTableWait(xwait, NULL, NULL, XLTW_None);
			goto retry;
		}
	}

	/* Found tuple, try to lock it in the lockmode. */
	if (found)
	{
		Buffer buf;
		HeapUpdateFailureData hufd;
		HTSU_Result res;
		HeapTupleData locktup;

		ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

		PushActiveSnapshot(GetLatestSnapshot());

		res = heap_lock_tuple(rel, &locktup, GetCurrentCommandId(false),
							  lockmode,
							  false /* wait */,
							  false /* don't follow updates */,
							  &buf, &hufd);
		/* the tuple slot already has the buffer pinned */
		ReleaseBuffer(buf);

		PopActiveSnapshot();

		switch (res)
		{
			case HeapTupleMayBeUpdated:
				break;
			case HeapTupleUpdated:
				/* XXX: Improve handling here */
				ereport(LOG,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("concurrent update, retrying")));
				goto retry;
			case HeapTupleInvisible:
				elog(ERROR, "attempted to lock invisible tuple");
			default:
				elog(ERROR, "unexpected heap_lock_tuple status: %u", res);
				break;
		}
	}

	index_endscan(scan);

	/* Don't release lock until commit. */
	index_close(idxrel, NoLock);

	return found;
}
