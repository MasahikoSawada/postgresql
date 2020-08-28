/*-------------------------------------------------------------------------
 *
 * fdwxact.c
 *		PostgreSQL global transaction manager for foreign servers.
 *
 * This module contains the code for managing transactions started on foreign
 * servers.
 *
 * FDW who implements both commit and rollback APIs can request to register the
 * foreign transaction by FdwXactRegisterXact() to participate it to a
 * group of distributed tranasction.  The registered foreign transactions are
 * identified by OIDs of server and user.  On commit and rollback, the global
 * transaction manager calls corresponding FDW API to end the tranasctions.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/fdwxact/fdwxact.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/fdwxact.h"
#include "access/xlog.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/memutils.h"

/* Check the FdwXactParticipant is capable of two-phase commit  */
#define ServerSupportTransactionCallback(fdw_part) \
	(((FdwXactParticipant *)(fdw_part))->commit_foreign_xact_fn != NULL)

/*
 * Structure to bundle the foreign transaction participant.	 This struct
 * needs to live until the end of transaction where we cannot look at
 * syscaches. Therefore, this is allocated in the TopTransactionContext.
 */
typedef struct FdwXactParticipant
{
	/* Foreign server and user mapping info, passed to callback routines */
	ForeignServer *server;
	UserMapping *usermapping;

	/* Callbacks for foreign transaction */
	CommitForeignTransaction_function commit_foreign_xact_fn;
	RollbackForeignTransaction_function rollback_foreign_xact_fn;
} FdwXactParticipant;

/*
 * List of foreign transactions involved in the transaction.  A member of
 * participants must support both commit and rollback APIs.
 */
static List *FdwXactParticipants = NIL;

static void ForgetAllFdwXactParticipants(void);
static void FdwXactParticipantEndTransaction(FdwXactParticipant *fdw_part,
											 bool commit);
static FdwXactParticipant *create_fdwxact_participant(Oid serverid, Oid userid,
													  FdwRoutine *routine);

/*
 * Register the given foreign transaction identified by the given arguments
 * as a participant of the transaction.
 */
void
FdwXactRegisterXact(Oid serverid, Oid userid)
{
	FdwXactParticipant *fdw_part;
	MemoryContext old_ctx;
	FdwRoutine *routine;
	ListCell   *lc;

	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (fdw_part->server->serverid == serverid &&
			fdw_part->usermapping->userid == userid)
		{
			/* Already registered */
			return;
		}
	}

	routine = GetFdwRoutineByServerId(serverid);

	/*
	 * Foreign server managed by the transaction manager must implement
	 * transaction callbacks.
	 */
	if (!routine->CommitForeignTransaction)
		ereport(ERROR,
				(errmsg("cannot register foreign server not supporting transaction callback")));

	/*
	 * Participant's information is also used at the end of a transaction,
	 * where system cache are not available. Save it in TopTransactionContext
	 * so that these can live until the end of transaction.
	 */
	old_ctx = MemoryContextSwitchTo(TopTransactionContext);

	fdw_part = create_fdwxact_participant(serverid, userid, routine);

	/* Add to the participants list */
	FdwXactParticipants = lappend(FdwXactParticipants, fdw_part);

	/* Revert back the context */
	MemoryContextSwitchTo(old_ctx);
}

/* Remove the given foreign server from FdwXactParticipants */
void
FdwXactUnregisterXact(Oid serverid, Oid userid)
{
	ListCell   *lc;

	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (fdw_part->server->serverid == serverid &&
			fdw_part->usermapping->userid == userid)
		{
			/* Remove the entry */
			FdwXactParticipants =
				foreach_delete_current(FdwXactParticipants, lc);
			break;
		}
	}
}

/* Return palloc'd FdwXactParticipant variable */
static FdwXactParticipant *
create_fdwxact_participant(Oid serverid, Oid userid, FdwRoutine *routine)
{
	FdwXactParticipant *fdw_part;
	ForeignServer *foreign_server;
	UserMapping *user_mapping;

	foreign_server = GetForeignServer(serverid);
	user_mapping = GetUserMapping(userid, serverid);

	fdw_part = (FdwXactParticipant *) palloc(sizeof(FdwXactParticipant));

	fdw_part->server = foreign_server;
	fdw_part->usermapping = user_mapping;
	fdw_part->commit_foreign_xact_fn = routine->CommitForeignTransaction;
	fdw_part->rollback_foreign_xact_fn = routine->RollbackForeignTransaction;

	return fdw_part;
}

/*
 * The routine for committing or rolling back the given transaction participant.
 */
static void
FdwXactParticipantEndTransaction(FdwXactParticipant *fdw_part, bool commit)
{
	FdwXactRslvState state;

	Assert(ServerSupportTransactionCallback(fdw_part));

	state.server = fdw_part->server;
	state.usermapping = fdw_part->usermapping;
	state.flags = FDWXACT_FLAG_ONEPHASE;

	if (commit)
	{
		fdw_part->commit_foreign_xact_fn(&state);
		elog(DEBUG1, "successfully committed the foreign transaction for server %u user %u",
			 fdw_part->usermapping->serverid,
			 fdw_part->usermapping->userid);
	}
	else
	{
		fdw_part->rollback_foreign_xact_fn(&state);
		elog(DEBUG1, "successfully rolled back the foreign transaction for server %u user %u",
			 fdw_part->usermapping->serverid,
			 fdw_part->usermapping->userid);
	}
}

/*
 * Clear the FdwXactParticipants list.
 */
static void
ForgetAllFdwXactParticipants(void)
{
	if (FdwXactParticipants == NIL)
		return;

	list_free_deep(FdwXactParticipants);
	FdwXactParticipants = NIL;
}

/*
 * Commit or rollback all foreign transactions.
 */
void
AtEOXact_FdwXact(bool is_commit)
{
	ListCell   *lc;

	/* If there are no foreign servers involved, we have no business here */
	if (FdwXactParticipants == NIL)
		return;

	Assert(!RecoveryInProgress());

	/* Commit or rollback foreign transactions in the participant list */
	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		Assert(ServerSupportTransactionCallback(fdw_part));
		FdwXactParticipantEndTransaction(fdw_part, is_commit);
	}

	ForgetAllFdwXactParticipants();
}

/*
 * Check if the local transaction has any foreign transaction.
 */
void
PrePrepare_FdwXact(void)
{
	/* We don't support to prepare foreign transactions */
	if (FdwXactParticipants != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has operated on foreign tables")));
}
