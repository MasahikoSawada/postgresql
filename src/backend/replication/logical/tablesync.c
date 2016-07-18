/*-------------------------------------------------------------------------
 * tablesync.c
 *	   PostgreSQL logical replication
 *
 * Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/tablesync.c
 *
 * NOTES
 *	  This file contains code for initial table data synchronization for
 *	  logical replication.
 *
 *    The initial data synchronization is done separately for each table,
 *    in separate apply worker that only fetches the initial snapshot data
 *    from the provider and then synchronizes the position in stream with
 *    the main apply worker.
 *
 *    The stream position synchronization works in multiple steps.
 *     - sync finishes copy and sets table state as SYNCWAIT and waits
 *       for state to change in a loop
 *     - apply periodically checks unsynced tables for SYNCWAIT, when it
 *       appears it will compare its position in the stream with the
 *       SYNCWAIT position and decides to either set it to CATCHUP when
 *       the apply was infront (and wait for the sync to do the catchup),
 *       or set the state to SYNCDONE if the sync was infront or in case
 *       both sync and apply are at the same position it will set it to
 *       READY and stops tracking it
 *     - if the state was set to CATCHUP sync will read the stream and
 *       apply changes until it catches up to the specified stream
 *       position and then sets state to READY and signals apply that it
 *       can stop waiting and exits, if the state was set to something
 *       else than CATCHUP the sync process will simply end
 *     - if the state was set to SYNCDONE by apply, the apply will
 *       continue tracking the table until it reaches the SYNCDONE stream
 *       position at which point it sets state to READY and stops tracking
 *
 *    Example flows look like this:
 *     - Apply is infront:
 *	      sync:8   -> set SYNCWAIT
 *        apply:10 -> set CATCHUP
 *        sync:10  -> set ready
 *          exit
 *        apply:10
 *          stop tracking
 *          continue rep
 *    - Sync infront:
 *        sync:10
 *          set SYNCWAIT
 *        apply:8
 *          set SYNCDONE
 *        sync:10
 *          exit
 *        apply:10
 *          set READY
 *          stop tracking
 *          continue rep
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

typedef struct TableState
{
	dlist_node	node;
	Oid			relid;
	XLogRecPtr	lsn;
	char		state;
} TableState;

static dlist_head table_states = DLIST_STATIC_INIT(table_states);
static bool table_states_valid = false;


/*
 * Exit routine for synchronization worker.
 */
static void
finish_sync_worker(char *slotname)
{
	LogicalRepWorker   *worker;
	RepOriginId			originid;
	MemoryContext		oldctx = CurrentMemoryContext;

	/*
	 * Drop the replication slot on remote server.
	 * We want to continue even in the case that the slot on remote side
	 * is already gone. This means that we can leave slot on the remote
	 * side but that can happen for other reasons as well so we can't
	 * really protect against that.
	 */
	PG_TRY();
	{
		wrcapi->drop_slot(wrchandle, slotname);
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

	/* Also remove the origin tracking for the slot if it exists. */
	StartTransactionCommand();
	originid = replorigin_by_name(slotname, true);
	if (originid != InvalidRepOriginId)
	{
		if (originid == replorigin_session_origin)
		{
			replorigin_session_reset();
			replorigin_session_origin = InvalidRepOriginId;
		}
		replorigin_drop(originid);
	}
	CommitTransactionCommand();

	/* Flush all writes. */
	XLogFlush(GetXLogWriteRecPtr());

	/* Find the main apply worker and signal it. */
	LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);
	worker = logicalrep_worker_find(MyLogicalRepWorker->subid, InvalidOid);
	if (worker && worker->proc)
		SetLatch(&worker->proc->procLatch);
	LWLockRelease(LogicalRepWorkerLock);

	ereport(LOG,
			(errmsg("logical replication synchronization worker finished processing")));

	/* Stop gracefully */
	wrcapi->disconnect(wrchandle);
	proc_exit(0);
}

/*
 * Wait until the table synchronization change matches the desired state.
 */
static bool
wait_for_sync_status_change(TableState *tstate)
{
	int		rc;
	char	state = tstate->state;

	while (!got_SIGTERM)
	{
		StartTransactionCommand();
		tstate->state = GetSubscriptionRelState(MyLogicalRepWorker->subid,
												tstate->relid,
												&tstate->lsn,
												true);
		CommitTransactionCommand();

		/* Status record was removed. */
		if (tstate->state == SUBREL_STATE_UNKNOWN)
			return false;

		if (tstate->state != state)
			return true;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10000L);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

        ResetLatch(&MyProc->procLatch);
	}

	return false;
}

/*
 * Read the state of the tables in the subscription and update our table
 * state list.
 */
static void
reread_sync_state(Oid relid)
{
	dlist_mutable_iter	iter;
	Relation	rel;
	HeapTuple	tup;
	ScanKeyData	skey[2];
	HeapScanDesc	scan;

	/* Clean the old list. */
	dlist_foreach_modify(iter, &table_states)
	{
		TableState *tstate = dlist_container(TableState, node, iter.cur);

		dlist_delete(iter.cur);
		pfree(tstate);
	}

	/*
	 * Fetch all the subscription relation states that are not marked as
	 * ready and push them into our table state tracking list.
	 */
	rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_subscription_rel_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(MyLogicalRepWorker->subid));

	if (OidIsValid(relid))
	{
		ScanKeyInit(&skey[1],
					Anum_pg_subscription_rel_subrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relid));
	}
	else
	{
		ScanKeyInit(&skey[1],
					Anum_pg_subscription_rel_substate,
					BTEqualStrategyNumber, F_CHARNE,
					CharGetDatum(SUBREL_STATE_READY));
	}

	scan = heap_beginscan_catalog(rel, 2, skey);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_subscription_rel	subrel;
		TableState	   *tstate;
		MemoryContext	oldctx;

		subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

		/* Allocate the tracking info in a permament memory context. */
		oldctx = MemoryContextSwitchTo(CacheMemoryContext);

		tstate = (TableState *) palloc(sizeof(TableState));
		tstate->relid = subrel->relid;
		tstate->state = subrel->substate;
		tstate->lsn = subrel->sublsn;

		dlist_push_tail(&table_states, &tstate->node);
		MemoryContextSwitchTo(oldctx);
	}

	/* Cleanup */
	heap_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	table_states_valid = true;
}

/*
 * Callback from syscache invalidation.
 */
void
invalidate_syncing_table_states(Datum arg, int cacheid, uint32 hashvalue)
{
	table_states_valid = false;
}

/*
 * Handle table synchronization cooperation from the synchroniation
 * worker.
 */
static void
process_syncing_tables_sync(char *slotname, XLogRecPtr end_lsn)
{
	TableState *tstate;
	TimeLineID	tli;

	Assert(!IsTransactionState());

	/*
	 * Synchronization workers don't keep track of all synchronization
	 * tables, they only care about their table.
	 */
	if (!table_states_valid)
	{
		StartTransactionCommand();
		reread_sync_state(MyLogicalRepWorker->relid);
		CommitTransactionCommand();
	}

	/* Somebody removed table underneath this worker, nothing more to do. */
	if (dlist_is_empty(&table_states))
	{
		wrcapi->endstreaming(wrchandle, &tli);
		finish_sync_worker(slotname);
	}

	/* Check if we are done with catchup now. */
	tstate = dlist_container(TableState, node, dlist_head_node(&table_states));
	if (tstate->state == SUBREL_STATE_CATCHUP)
	{
		Assert(tstate->lsn != InvalidXLogRecPtr);

		if (tstate->lsn == end_lsn)
		{
			tstate->state = SUBREL_STATE_READY;
			tstate->lsn = InvalidXLogRecPtr;
			/* Update state of the synchronization. */
			StartTransactionCommand();
			SetSubscriptionRelState(MyLogicalRepWorker->subid,
									tstate->relid, tstate->state,
									tstate->lsn);
			CommitTransactionCommand();

			wrcapi->endstreaming(wrchandle, &tli);
			finish_sync_worker(slotname);
		}
		return;
	}
}

/*
 * Handle table synchronization cooperation from the apply worker.
 */
static void
process_syncing_tables_apply(char *slotname, XLogRecPtr end_lsn)
{
	dlist_mutable_iter	iter;

	Assert(!IsTransactionState());

	if (!table_states_valid)
	{
		StartTransactionCommand();
		reread_sync_state(InvalidOid);
		CommitTransactionCommand();
	}

	dlist_foreach_modify(iter, &table_states)
	{
		TableState *tstate = dlist_container(TableState, node, iter.cur);
		bool		start_worker;
		LogicalRepWorker   *worker;

		/*
		 * When the synchronization process is at the cachup phase we need
		 * to ensure that we are not behind it (it's going to wait at this
		 * point for the change of state). Once we are infront or at the same
		 * position as the synchronization proccess we can signal it to
		 * finish the catchup.
		 */
		if (tstate->state == SUBREL_STATE_SYNCWAIT)
		{
			if (end_lsn > tstate->lsn)
			{
				/*
				 * Apply is infront, tell sync to catchup. and wait until
				 * it does.
				 */
				tstate->state = SUBREL_STATE_CATCHUP;
				tstate->lsn = end_lsn;
				StartTransactionCommand();
				SetSubscriptionRelState(MyLogicalRepWorker->subid,
										tstate->relid, tstate->state,
										tstate->lsn);
				CommitTransactionCommand();

				/* Signal the worker as it may be waiting for us. */
				LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
				worker = logicalrep_worker_find(MyLogicalRepWorker->subid,
												tstate->relid);
				if (worker && worker->proc)
					SetLatch(&worker->proc->procLatch);
				LWLockRelease(LogicalRepWorkerLock);

				if (wait_for_sync_status_change(tstate))
					Assert(tstate->state == SUBREL_STATE_READY);
			}
			else
			{
				/*
				 * Apply is either behind in which case sync worker is done
				 * but apply needs to keep tracking the table until it
				 * catches up to where sync finished.
				 * Or apply and sync are at the same position in which case
				 * table can be switched to standard replication mode
				 * immediately.
				 */
				if (end_lsn < tstate->lsn)
					tstate->state = SUBREL_STATE_SYNCDONE;
				else
					tstate->state = SUBREL_STATE_READY;

				StartTransactionCommand();
				SetSubscriptionRelState(MyLogicalRepWorker->subid,
										tstate->relid, tstate->state,
										tstate->lsn);
				CommitTransactionCommand();

				/* Signal the worker as it may be waiting for us. */
				LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
				worker = logicalrep_worker_find(MyLogicalRepWorker->subid,
												tstate->relid);
				if (worker && worker->proc)
					SetLatch(&worker->proc->procLatch);
				LWLockRelease(LogicalRepWorkerLock);
			}
		}
		else if (tstate->state == SUBREL_STATE_SYNCDONE &&
				 end_lsn >= tstate->lsn)
		{
			/*
			 * Apply catched up to the position where table sync finished,
			 * mark the table as ready for normal replication.
			 */
			tstate->state = SUBREL_STATE_READY;
			tstate->lsn = InvalidXLogRecPtr;
			StartTransactionCommand();
			SetSubscriptionRelState(MyLogicalRepWorker->subid,
									tstate->relid, tstate->state,
									tstate->lsn);
			CommitTransactionCommand();
		}

		/*
		 * In case table is supposed to be synchronizing but the
		 * synchronization worker is not running, start it.
		 * Limit the number of launched workers here to one (for now).
		 */
		if (tstate->state != SUBREL_STATE_READY &&
			tstate->state != SUBREL_STATE_SYNCDONE)
		{
			LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
			worker = logicalrep_worker_find(MyLogicalRepWorker->subid,
											tstate->relid);
			start_worker = !worker &&
				logicalrep_worker_count(MyLogicalRepWorker->subid) < 2;
			LWLockRelease(LogicalRepWorkerLock);
			if (start_worker)
				logicalrep_worker_launch(MyLogicalRepWorker->dbid,
										 MyLogicalRepWorker->subid,
										 tstate->relid);

		}
	}
}

/*
 * Proccess state possible change(s) of tables that are being synchronized
 * in parallel.
 */
void
process_syncing_tables(char *slotname, XLogRecPtr end_lsn)
{
	if (OidIsValid(MyLogicalRepWorker->relid))
		process_syncing_tables_sync(slotname, end_lsn);
	else
		process_syncing_tables_apply(slotname, end_lsn);
}

/*
 * Setup replication origin tracking.
 */
static XLogRecPtr
setup_origin_tracking(char *origin_name)
{
	RepOriginId		originid;

	StartTransactionCommand();
	originid = replorigin_by_name(origin_name, true);
	if (!OidIsValid(originid))
		originid = replorigin_create(origin_name);
	replorigin_session_setup(originid);
	replorigin_session_origin = originid;
	CommitTransactionCommand();
	return replorigin_session_get_progress(false);
}


/*
 * Start syncing the table in the sync worker.
 */
char *
LogicalRepSyncTableStart(XLogRecPtr *origin_startpos)
{
	StringInfoData	s;
	TableState		tstate;
	MemoryContext	oldctx;
	char		   *slotname;

	/* Check the state of the table synchronization. */
	StartTransactionCommand();
	tstate.relid = MyLogicalRepWorker->relid;
	tstate.state = GetSubscriptionRelState(MySubscription->oid, tstate.relid,
										   &tstate.lsn, false);

	/*
	 * Build unique slot name.
	 * TODO: protect against too long slot name.
	 */
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);
	initStringInfo(&s);
	appendStringInfo(&s, "%s_sync_%s", MySubscription->slotname,
					 get_rel_name(tstate.relid));
	slotname = s.data;
	MemoryContextSwitchTo(oldctx);

	CommitTransactionCommand();

	wrcapi->connect(wrchandle, MySubscription->conninfo, true, slotname);

	switch (tstate.state)
	{
		case SUBREL_STATE_INIT:
		case SUBREL_STATE_DATA:
			{
				Relation	rel;
				XLogRecPtr	lsn;
				char	   *options;

				/* Update the state and make it visible to others. */
				StartTransactionCommand();
				SetSubscriptionRelState(MySubscription->oid, tstate.relid,
										SUBREL_STATE_DATA,
										InvalidXLogRecPtr);
				CommitTransactionCommand();

				*origin_startpos = setup_origin_tracking(slotname);

				/*
				 * We want to do the table data sync in single
				 * transaction so do not close the transaction opened
				 * above.
				 * There will be no BEGIN or COMMIT messages coming via
				 * logical replication while the copy table command is
				 * running so start the transaction here.
				 * Note the memory context for data handling will still
				 * be done using ensure_transaction called by the insert
				 * handler.
				 */
				StartTransactionCommand();

				/*
				 * Don't allow parallel access other than SELECT while
				 * the initial contents are being copied.
				 */
				rel = heap_open(tstate.relid, ExclusiveLock);

				/* Create temporary slot for the sync proccess. */
				wrcapi->create_slot(wrchandle, slotname, true,  &lsn);

				/* Build option string for the plugin. */
				options = logicalrep_build_options(MySubscription->publications);

				wrcapi->copy_table(wrchandle, slotname,
								   get_namespace_name(RelationGetNamespace(rel)),
								   RelationGetRelationName(rel),
								   options);

				/*
				 * Run the standard apply loop for the initial data
				 * stream.
				 */
				in_remote_transaction = true;
				LogicalRepApplyLoop(*origin_startpos);

				/*
				 * We are done with the initial data synchronization,
				 * update the state.
				 */
				SetSubscriptionRelState(MySubscription->oid, tstate.relid,
										SUBREL_STATE_SYNCWAIT, lsn);
				heap_close(rel, NoLock);

				/* End the transaction. */
				CommitTransactionCommand();
				in_remote_transaction = false;

				/*
				 * Wait for main apply worker to either tell us to
				 * catchup or that we are done.
				 */
				wait_for_sync_status_change(&tstate);
				if (tstate.state != SUBREL_STATE_CATCHUP)
					finish_sync_worker(slotname);
				break;
			}

		case SUBREL_STATE_SYNCWAIT:
			*origin_startpos = setup_origin_tracking(slotname);
			/*
			 * Wait for main apply worker to either tell us to
			 * catchup or that we are done.
			 */
			wait_for_sync_status_change(&tstate);
			if (tstate.state != SUBREL_STATE_CATCHUP)
				finish_sync_worker(slotname);
			break;
		case SUBREL_STATE_CATCHUP:
			/* Catchup is handled by streaming loop. */
			*origin_startpos = setup_origin_tracking(slotname);
			break;
		case SUBREL_STATE_SYNCDONE:
		case SUBREL_STATE_READY:
			/* Nothing to do here but finish. */
			finish_sync_worker(slotname);
		default:
			elog(ERROR, "unknown relation state \"%c\"", tstate.state);
	}

	return slotname;
}
