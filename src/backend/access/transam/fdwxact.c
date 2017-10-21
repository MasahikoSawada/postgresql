/*-------------------------------------------------------------------------
 *
 * fdwxact.c
 *		PostgreSQL distributed transaction manager for foreign servers.
 *
 * This module manages the transactions involving foreign servers.
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/fdwxact.c
 *
 * This comment summarises how the transaction manager handles transactions
 * involving one or more foreign servers.
 *
 * When an foreign data wrapper starts transaction on a foreign server. it is
 * required to register the foreign server and user who initiated the
 * transaction using function RegisterXactForeignServer(). A foreign server
 * connection is identified by oid fo foreign server and user.
 *
 * The commit is executed in two phases. In the first phase executed during
 * pre-commit phase, transactions are prepared on all the foreign servers,
 * which can participate in two-phase commit protocol. Transaction on other
 * foreign servers are committed in the same phase. In the second phase, if
 * first phase doesn not succeed for whatever reason, the foreign servers
 * are asked to rollback respective prepared transactions or abort the
 * transactions if they are not prepared. This process is executed by backend
 * process that executed the first phase. If the first phase succeeds, the
 * backend process registers ourselves to the queue in the shared memory and then
 * ask the foreign transaction resolver process to resolve foreign transactions
 * that are associated with the its transaction. After resolved all foreign
 * transactions by foreign transaction resolve process the backend wakes up
 * and resume to process.
 *
 * Any network failure, server crash after preparing foreign transaction leaves
 * that prepared transaction unresolved (aka dangling transaction). During the
 * first phase, before actually preparing the transactions, enough information
 * is persisted to the disk and logs in order to resolve such transactions.
 *
 * During replay WAL and replication FdwXactCtl also holds information about
 * active prepared foreign transaction that haven't been moved to disk yet.
 *
 * Replay of fdwxact records happens by the following rules:
 *
 * 	* On PREPARE redo we add the foreign transaction to FdwXactCtl->fdw_xacts.
 *	  We set fdw_xact->inredo to true for such entries.
 *	* On Checkpoint redo we iterate through FdwXactCtl->fdw_xacts entries that
 *	  that have fdw_xact->inredo set and are behind the redo_horizon.
 *	  We save them to disk and alos set fdw_xact->ondisk to true.
 *	* On COMMIT and ABORT we delete the entry from FdwXactCtl->fdw_xacts.
 *	  If fdw_xact->ondisk is true, we delete the corresponding entry from
 *	  the disk as well.
 *	* RecoverPreparedTrasactions() and StandbyRecoverPreparedTransactions()
 *	  have been modified to go through fdw_xact->inredo entries that have
 *	  not made to disk yet.
 *-------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "miscadmin.h"
#include "funcapi.h"

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/htup_details.h"
#include "access/twophase.h"
#include "access/resolver_private.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "libpq/pqsignal.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"

/* Structure to bundle the foreign connection participating in transaction */
typedef struct
{
	Oid			serverid;
	Oid			userid;
	Oid			umid;
	char	   *servername;
	FdwXact		fdw_xact;		/* foreign prepared transaction entry in case
								 * prepared */
	bool		two_phase_commit;		/* Should use two phase commit
										 * protocol while committing
										 * transaction on this server,
										 * whenever necessary. */
	bool		modified;		/* modified on foreign server in the transaction */
	GetPrepareId_function get_prepare_id;
	EndForeignTransaction_function end_foreign_xact;
	PrepareForeignTransaction_function prepare_foreign_xact;
	ResolvePreparedForeignTransaction_function resolve_prepared_foreign_xact;
}	FdwConnection;

/* List of foreign connections participating in the transaction */
List	   *MyFdwConnections = NIL;

/* Shmem hash entry */
typedef struct
{
	/* tag */
	TransactionId	xid;

	/* data */
	FdwXact	first_entry;
} FdwXactHashEntry;

static HTAB	*FdwXactHash;

/*
 * By default we assume that all the foreign connections participating in this
 * transaction can use two phase commit protocol.
 */
bool		TwoPhaseReady = true;

/* Directory where the foreign prepared transaction files will reside */
#define FDW_XACTS_DIR "pg_fdw_xact"

/*
 * Name of foreign prepared transaction file is 8 bytes xid, 8 bytes foreign
 * server oid and 8 bytes user oid separated by '_'.
 */
#define FDW_XACT_FILE_NAME_LEN (8 + 1 + 8 + 1 + 8)
#define FdwXactFilePath(path, xid, serverid, userid)	\
	snprintf(path, MAXPGPATH, FDW_XACTS_DIR "/%08X_%08X_%08X", xid, \
			 serverid, userid)

static FdwXact FdwXactRegisterFdwXact(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
				  Oid umid, char *fdw_xact_info);
static void FdwXactPrepareForeignTransactions(void);
static void AtProcExit_FdwXact(int code, Datum arg);
static bool FdwXactResolveForeignTransaction(FdwXact fdw_xact,
											 ResolvePreparedForeignTransaction_function prepared_foreign_xact_resolver);
static void UnlockMyFdwXacts(void);
static void remove_fdw_xact(FdwXact fdw_xact);
static FdwXact insert_fdw_xact(Oid dboid, TransactionId xid, Oid serverid, Oid userid,
							   Oid umid, char *fdw_xact_id);
static int	GetFdwXactList(FdwXact * fdw_xacts);
static ResolvePreparedForeignTransaction_function get_prepared_foreign_xact_resolver(FdwXact fdw_xact);
static FdwXactOnDiskData *ReadFdwXactFile(TransactionId xid, Oid serverid,
				Oid userid);
static void RemoveFdwXactFile(TransactionId xid, Oid serverid, Oid userid,
				  bool giveWarning);
static void RecreateFdwXactFile(TransactionId xid, Oid serverid, Oid userid,
					void *content, int len);
static void XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len);
static FdwXact get_fdw_xact(TransactionId xid, Oid serverid, Oid userid);
static bool search_fdw_xact(TransactionId xid, Oid dbid, Oid serverid, Oid userid,
							List **qualifying_xacts);

static void FdwXactQueueInsert(void);
static void FdwXactCancelWait(void);

/* Guc parameters */
int			max_prepared_foreign_xacts = 0;
int			max_foreign_xact_resolvers = 0;

/* Keep track of registering process exit call back. */
static bool fdwXactExitRegistered = false;

/* Foreign transaction entries locked by this backend */
List	   *MyLockedFdwXacts = NIL;
FdwXactResolver *MyFdwXactResolver = NULL;

/* Record the server, userid participating in the transaction. */
void
FdwXactRegisterForeignServer(Oid serverid, Oid userid, bool two_phase_commit,
							 bool modify)
{
	FdwConnection *fdw_conn;
	ListCell   *lcell;
	ForeignServer *foreign_server;
	ForeignDataWrapper *fdw;
	UserMapping *user_mapping;
	FdwRoutine *fdw_routine;
	MemoryContext old_context;

	TwoPhaseReady = TwoPhaseReady && two_phase_commit;

	/* Quick return if the entry already exists */
	foreach(lcell, MyFdwConnections)
	{
		fdw_conn = lfirst(lcell);

		/* Quick return if there is already registered connection */
		if (fdw_conn->serverid == serverid && fdw_conn->userid == userid)
		{
			fdw_conn->modified |= modify;
			return;
		}
	}

	/*
	 * This list and its contents needs to be saved in the transaction context
	 * memory
	 */
	old_context = MemoryContextSwitchTo(TopTransactionContext);
	/* Add this foreign connection to the list for transaction management */
	fdw_conn = (FdwConnection *) palloc(sizeof(FdwConnection));

	/* Make sure that the FDW has at least a transaction handler */
	foreign_server = GetForeignServer(serverid);
	fdw = GetForeignDataWrapper(foreign_server->fdwid);
	fdw_routine = GetFdwRoutine(fdw->fdwhandler);
	user_mapping = GetUserMapping(userid, serverid);

	if (!fdw_routine->EndForeignTransaction)
		ereport(ERROR,
				(errmsg("no function to end a foreign transaction provided for FDW %s",
						fdw->fdwname)));

	if (two_phase_commit)
	{
		if (max_prepared_foreign_xacts == 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("prepread foreign transactions are disabled"),
					 errhint("Set max_prepared_foreign_transactions to a nonzero value.")));

		if (max_foreign_xact_resolvers == 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("prepread foreign transactions are disabled"),
					 errhint("Set max_foreign_xact_resolvers to a nonzero value.")));

		if (!fdw_routine->PrepareForeignTransaction)
			ereport(ERROR,
					(errmsg("no function provided for preparing foreign transaction for FDW %s",
							fdw->fdwname)));

		if (!fdw_routine->ResolvePreparedForeignTransaction)
			ereport(ERROR,
					(errmsg("no function provided for resolving prepared foreign transaction for FDW %s",
							fdw->fdwname)));
	}

	fdw_conn->serverid = serverid;
	fdw_conn->userid = userid;
	fdw_conn->umid = user_mapping->umid;

	/*
	 * We may need following information at the end of a transaction, when the
	 * system caches are not available. So save it before hand.
	 */
	fdw_conn->servername = foreign_server->servername;
	fdw_conn->get_prepare_id = fdw_routine->GetPrepareId;
	fdw_conn->prepare_foreign_xact = fdw_routine->PrepareForeignTransaction;
	fdw_conn->resolve_prepared_foreign_xact = fdw_routine->ResolvePreparedForeignTransaction;
	fdw_conn->end_foreign_xact = fdw_routine->EndForeignTransaction;
	fdw_conn->fdw_xact = NULL;
	fdw_conn->modified = modify;
	fdw_conn->two_phase_commit = two_phase_commit;
	MyFdwConnections = lappend(MyFdwConnections, fdw_conn);
	/* Revert back the context */
	MemoryContextSwitchTo(old_context);

	return;
}

/*
 * FdwXactShmemSize
 * Calculates the size of shared memory allocated for maintaining foreign
 * prepared transaction entries.
 */
Size
FdwXactShmemSize(void)
{
	Size		size;

	/* Need the fixed struct, foreign transaction information array */
	size = offsetof(FdwXactCtlData, fdw_xacts);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXact)));
	size = MAXALIGN(size);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXactData)));

	size = MAXALIGN(size);
	size = add_size(size, hash_estimate_size(max_prepared_foreign_xacts,
											 sizeof(FdwXactHashEntry)));

	return size;
}

/*
 * FdwXactShmemInit
 * Initialization of shared memory for maintaining foreign prepared transaction
 * entries. The shared memory layout is defined in definition of
 * FdwXactCtlData structure.
 */
void
FdwXactShmemInit(void)
{
	bool		found;

	FdwXactCtl = ShmemInitStruct("Foreign transactions table",
								 FdwXactShmemSize(),
								 &found);
	if (!IsUnderPostmaster)
	{
		FdwXact		fdw_xacts;
		HASHCTL		info;
		long		init_hash_size;
		long		max_hash_size;
		int			cnt;

		Assert(!found);
		FdwXactCtl->freeFdwXacts = NULL;
		FdwXactCtl->numFdwXacts = 0;

		/* Initialise the linked list of free FDW transactions */
		fdw_xacts = (FdwXact)
			((char *) FdwXactCtl +
			 MAXALIGN(offsetof(FdwXactCtlData, fdw_xacts) +
					  sizeof(FdwXact) * max_prepared_foreign_xacts));
		for (cnt = 0; cnt < max_prepared_foreign_xacts; cnt++)
		{
			fdw_xacts[cnt].fx_free_next = FdwXactCtl->freeFdwXacts;
			FdwXactCtl->freeFdwXacts = &fdw_xacts[cnt];
		}

		MemSet(&info, 0, sizeof(info));
		info.keysize = sizeof(TransactionId);
		info.entrysize = sizeof(FdwXactHashEntry);

		max_hash_size = max_prepared_foreign_xacts;
		init_hash_size = max_hash_size / 2;

		FdwXactHash = ShmemInitHash("FdwXact hash",
									init_hash_size,
									max_hash_size,
									&info,
									HASH_ELEM | HASH_BLOBS);
	}
	else
	{
		Assert(FdwXactCtl);
		Assert(found);
	}
}


/*
 * PreCommit_FdwXacts
 *
 * The function is responsible for pre-commit processing on foreign connections.
 * Basically the foreign transactions are prepared on the foreign servers which
 * can execute two-phase-commit protocol. But in case of where only one server
 * that can execute two-phase-commit protocol is involved with transaction and
 * no changes is made on local data then we don't need to two-phase-commit protocol,
 * so try to commit transaction on the server. Those will be aborted or committed
 * after the current transaction has been aborted or committed resp. We try to
 * commit transactions on rest of the foreign servers now. For these foreign
 * servers it is possible that some transactions commit even if the local
 * transaction aborts.
 */
void
PreCommit_FdwXacts(void)
{
	ListCell   *cur;
	ListCell   *prev;
	ListCell   *next;

	/* If there are no foreign servers involved, we have no business here */
	if (list_length(MyFdwConnections) < 1)
		return;

	/*
	 * Try committing transactions on the foreign servers, which can not
	 * execute two-phase-commit protocol.
	 */
	for (cur = list_head(MyFdwConnections), prev = NULL; cur; cur = next)
	{
		FdwConnection *fdw_conn = lfirst(cur);

		next = lnext(cur);

		/*
		 * We commit the foreign transactions on servers either that cannot
		 * execute two-phase-commit protocol or that we didn't modified on
		 * in pre-commit phase.
		 */
		if (!fdw_conn->two_phase_commit || !fdw_conn->modified)
		{
			/*
			 * The FDW has to make sure that the connection opened to the
			 * foreign server is out of transaction. Even if the handler
			 * function returns failure statue, there's hardly anything to do.
			 */
			if (!fdw_conn->end_foreign_xact(fdw_conn->serverid, fdw_conn->userid,
											fdw_conn->umid, true))
				elog(WARNING, "could not commit transaction on server %s",
					 fdw_conn->servername);

			/* The connection is no more part of this transaction, forget it */
			MyFdwConnections = list_delete_cell(MyFdwConnections, cur, prev);
		}
		else
			prev = cur;
	}

	/*
	 * Here, we have committed foreign transactions on foreign servers that can
	 * not execute two-phase-commit protocol and MyFdwConnections has only foreign
	 * servers that can execute two-phase-commit protocol. We prepare foreign
	 * transactions if two-phase-commit protocol is required.
	 */
	if (TwoPhaseCommitRequired())
	{
		/*
		 * Prepare the transactions on the all foreign servers, which can
		 * execute two-phase-commit protocol.
		 */
		FdwXactPrepareForeignTransactions();
	}
	else
	{
		FdwConnection *fdw_conn;

		/* Return if we've committed all foreign servers */
		if (list_length(MyFdwConnections) == 0)
			return;

		Assert(list_length(MyFdwConnections) == 1);

		fdw_conn = lfirst(list_head(MyFdwConnections));

		/*
		 * We don't need to use two-phase commit protocol only one server
		 * remaining even if this server can execute two-phase-commit
		 * protocol.
		 */
		if (!fdw_conn->end_foreign_xact(fdw_conn->serverid, fdw_conn->userid,
										fdw_conn->umid, true))
			elog(WARNING, "could not commit transaction on server %s",
				 fdw_conn->servername);

		/* MyFdwConnections should be cleared here */
		MyFdwConnections = list_delete_cell(MyFdwConnections, cur, prev);
	}
}

/*
 * prepare_foreign_transactions
 *
 * Prepare transactions on the foreign servers which can execute two phase
 * commit protocol. Rest of the foreign servers are ignored.
 */
static void
FdwXactPrepareForeignTransactions(void)
{
	ListCell   *lcell;
	FdwXact		prev_fdwxact = NULL;

	/*
	 * Loop over the foreign connections
	 */
	foreach(lcell, MyFdwConnections)
	{
		FdwConnection *fdw_conn = (FdwConnection *) lfirst(lcell);
		char	    *fdw_xact_id;
		int			fdw_xact_id_len;
		FdwXact		fdw_xact;

		if (!fdw_conn->two_phase_commit || !fdw_conn->modified)
			continue;


		/* Generate prepare transaction id for foreign server */
		Assert(fdw_conn->get_prepare_id);
		fdw_xact_id = fdw_conn->get_prepare_id(fdw_conn->serverid,
											   fdw_conn->userid,
											   &fdw_xact_id_len);

		/*
		 * Register the foreign transaction with the identifier used to
		 * prepare it on the foreign server. Registration persists this
		 * information to the disk and logs (that way relaying it on standby).
		 * Thus in case we loose connectivity to the foreign server or crash
		 * ourselves, we will remember that we have prepared transaction on
		 * the foreign server and try to resolve it when connectivity is
		 * restored or after crash recovery.
		 *
		 * If we crash after persisting the information but before preparing
		 * the transaction on the foreign server, we will try to resolve a
		 * never-prepared transaction, and get an error. This is fine as long
		 * as the FDW provides us unique prepared transaction identifiers.
		 *
		 * If we prepare the transaction on the foreign server before
		 * persisting the information to the disk and crash in-between these
		 * two steps, we will forget that we prepared the transaction on the
		 * foreign server and will not be able to resolve it after the crash.
		 * Hence persist first then prepare.
		 */
		fdw_xact = FdwXactRegisterFdwXact(MyDatabaseId, GetTopTransactionId(),
									 fdw_conn->serverid, fdw_conn->userid,
									 fdw_conn->umid, fdw_xact_id);

		/*
		 * Between FdwXactRegisterFdwXact call above till this backend hears back
		 * from foreign server, the backend may abort the local transaction
		 * (say, because of a signal). During abort processing, it will send
		 * an ABORT message to the foreign server. If the foreign server has
		 * not prepared the transaction, the message will succeed. If the
		 * foreign server has prepared transaction, it will throw an error,
		 * which we will ignore and the prepared foreign transaction will be
		 * resolved by the foreign transaction resolver.
		 */
		if (!fdw_conn->prepare_foreign_xact(fdw_conn->serverid, fdw_conn->userid,
											fdw_conn->umid, fdw_xact_id))
		{
			StringInfo servername;
			/*
			 * An error occurred, and we didn't prepare the transaction.
			 * Delete the entry from foreign transaction table. Raise an
			 * error, so that the local server knows that one of the foreign
			 * server has failed to prepare the transaction.
			 *
			 * XXX : FDW is expected to print the error as a warning and then
			 * we raise actual error here. But instead, we should pull the
			 * error text from FDW and add it here in the message or as a
			 * context or a hint.
			 */
			remove_fdw_xact(fdw_xact);

			/*
			 * Delete the connection, since it doesn't require any further
			 * processing. This deletion will invalidate current cell pointer,
			 * but that is fine since we will not use that pointer because the
			 * subsequent ereport will get us out of this loop.
			 */
			servername = makeStringInfo();
			appendStringInfoString(servername, fdw_conn->servername);
			MyFdwConnections = list_delete_ptr(MyFdwConnections, fdw_conn);
			ereport(ERROR,
					(errmsg("can not prepare transaction on foreign server %s",
							servername->data)));
		}

		/* Prepare succeeded, remember it in the connection */
		fdw_conn->fdw_xact = fdw_xact;

		/*
		 * If this is the first fdwxact entry we keep it in the hash table for
		 * the later use.
		 */
		if (!prev_fdwxact)
		{
			FdwXactHashEntry	*fdwxact_entry;
			bool				found;
			TransactionId		key;

			key = fdw_xact->local_xid;

			LWLockAcquire(FdwXactLock,LW_EXCLUSIVE);
			fdwxact_entry = (FdwXactHashEntry *) hash_search(FdwXactHash,
															 &key,
															 HASH_ENTER, &found);
			LWLockRelease(FdwXactLock);

			Assert(!found);
			fdwxact_entry->first_entry = fdw_xact;
		}
		else
		{
			/*
			 * Make a list of fdwxacts that are associated with the
			 * same local transaction.
			 */
			Assert(fdw_xact->fx_next == NULL);
			prev_fdwxact->fx_next = fdw_xact;
		}

		prev_fdwxact = fdw_xact;
	}

	return;
}

/*
 * FdwXactRegisterFdwXact
 *
 * This function is used to create new foreign transaction entry before an FDW
 * executes the first phase of two-phase commit. The function adds the entry to
 * WAL and will be persisted to the disk under pg_fdw_xact directory when checkpoint.
 */
static FdwXact
FdwXactRegisterFdwXact(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
					   Oid umid, char *fdw_xact_id)
{
	FdwXact		fdw_xact;
	FdwXactOnDiskData *fdw_xact_file_data;
	MemoryContext	old_context;
	int			data_len;

	/* Enter the foreign transaction in the shared memory structure */
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	fdw_xact = insert_fdw_xact(dbid, xid, serverid, userid, umid, fdw_xact_id);
	fdw_xact->status = FDW_XACT_PREPARING;
	fdw_xact->locking_backend = MyBackendId;

	LWLockRelease(FdwXactLock);

	/* Remember that we have locked this entry. */
	old_context = MemoryContextSwitchTo(TopTransactionContext);
	MyLockedFdwXacts = lappend(MyLockedFdwXacts, fdw_xact);
	MemoryContextSwitchTo(old_context);

	/*
	 * Prepare to write the entry to a file. Also add xlog entry. The contents
	 * of the xlog record are same as what is written to the file.
	 */
	data_len = offsetof(FdwXactOnDiskData, fdw_xact_id);
	data_len = data_len + FDW_XACT_ID_LEN;
	data_len = MAXALIGN(data_len);
	fdw_xact_file_data = (FdwXactOnDiskData *) palloc0(data_len);
	fdw_xact_file_data->dboid = fdw_xact->dboid;
	fdw_xact_file_data->local_xid = fdw_xact->local_xid;
	fdw_xact_file_data->serverid = fdw_xact->serverid;
	fdw_xact_file_data->userid = fdw_xact->userid;
	fdw_xact_file_data->umid = fdw_xact->umid;
	memcpy(fdw_xact_file_data->fdw_xact_id, fdw_xact->fdw_xact_id,
		   FDW_XACT_ID_LEN);

	START_CRIT_SECTION();

	/* Add the entry in the xlog and save LSN for checkpointer */
	XLogBeginInsert();
	XLogRegisterData((char *) fdw_xact_file_data, data_len);
	fdw_xact->fdw_xact_end_lsn = XLogInsert(RM_FDW_XACT_ID, XLOG_FDW_XACT_INSERT);
	XLogFlush(fdw_xact->fdw_xact_end_lsn);

	/* Store record's start location to read that later on CheckPoint */
	fdw_xact->fdw_xact_start_lsn = ProcLastRecPtr;

	/* File is written completely, checkpoint can proceed with syncing */
	fdw_xact->valid = true;

	END_CRIT_SECTION();

	pfree(fdw_xact_file_data);
	return fdw_xact;
}

/*
 * insert_fdw_xact
 *
 * Insert a new entry for a given foreign transaction identified by transaction
 * id, foreign server and user mapping, in the shared memory. Caller must hold
 * FdwXactLock in exclusive mode.
 *
 * If the entry already exists, the function raises an error.
 */
static FdwXact
insert_fdw_xact(Oid dboid, TransactionId xid, Oid serverid, Oid userid, Oid umid,
				char *fdw_xact_id)
{
	int i;
	FdwXact fdw_xact;

	if (!fdwXactExitRegistered)
	{
		before_shmem_exit(AtProcExit_FdwXact, 0);
		fdwXactExitRegistered = true;
	}

	/* Check for duplicating foreign transaction entry */
	for (i = 0; i < FdwXactCtl->numFdwXacts; i++)
	{
		fdw_xact = FdwXactCtl->fdw_xacts[i];
		if (fdw_xact->local_xid == xid &&
			fdw_xact->serverid == serverid &&
			fdw_xact->userid == userid)
			elog(ERROR, "duplicate entry for foreign transaction with transaction id %u, serverid %u, userid %u found",
				 xid, serverid, userid);
	}

	/*
	 * Get a next free foreign transaction entry. Raise error if there are
	 * none left.
	 */
	if (!FdwXactCtl->freeFdwXacts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("maximum number of foreign transactions reached"),
				 errhint("Increase max_prepared_foreign_transactions : \"%d\".",
						 max_prepared_foreign_xacts)));
	}

	fdw_xact = FdwXactCtl->freeFdwXacts;
	FdwXactCtl->freeFdwXacts = fdw_xact->fx_free_next;

	/* Insert the entry to active array */
	Assert(FdwXactCtl->numFdwXacts < max_prepared_foreign_xacts);
	FdwXactCtl->fdw_xacts[FdwXactCtl->numFdwXacts++] = fdw_xact;

	/* Stamp the entry with backend id before releasing the LWLock */
	fdw_xact->locking_backend = InvalidBackendId;
	fdw_xact->dboid = dboid;
	fdw_xact->local_xid = xid;
	fdw_xact->serverid = serverid;
	fdw_xact->userid = userid;
	fdw_xact->umid = umid;
	fdw_xact->fdw_xact_start_lsn = InvalidXLogRecPtr;
	fdw_xact->fdw_xact_end_lsn = InvalidXLogRecPtr;
	fdw_xact->valid = false;
	fdw_xact->ondisk = false;
	fdw_xact->inredo = false;
	memcpy(fdw_xact->fdw_xact_id, fdw_xact_id, FDW_XACT_ID_LEN);

	return fdw_xact;
}

/*
 * remove_fdw_xact
 *
 * Removes the foreign prepared transaction entry from shared memory, disk and
 * logs about the removal in WAL.
 */
static void
remove_fdw_xact(FdwXact fdw_xact)
{
	int			cnt;

	Assert(fdw_xact != NULL);

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	/* Search the slot where this entry resided */
	for (cnt = 0; cnt < FdwXactCtl->numFdwXacts; cnt++)
	{
		if (FdwXactCtl->fdw_xacts[cnt] == fdw_xact)
		{
			/* Remove the entry from active array */
			FdwXactCtl->numFdwXacts--;
			FdwXactCtl->fdw_xacts[cnt] = FdwXactCtl->fdw_xacts[FdwXactCtl->numFdwXacts];

			/* Put it back into free list */
			fdw_xact->fx_free_next = FdwXactCtl->freeFdwXacts;
			FdwXactCtl->freeFdwXacts = fdw_xact;

			/* Unlock the entry */
			fdw_xact->locking_backend = InvalidBackendId;
			fdw_xact->fx_next = NULL;
			MyLockedFdwXacts = list_delete_ptr(MyLockedFdwXacts, fdw_xact);

			LWLockRelease(FdwXactLock);

			if (!RecoveryInProgress())
			{
				FdwRemoveXlogRec fdw_remove_xlog;
				XLogRecPtr	recptr;

				/* Fill up the log record before releasing the entry */
				fdw_remove_xlog.serverid = fdw_xact->serverid;
				fdw_remove_xlog.dbid = fdw_xact->dboid;
				fdw_remove_xlog.xid = fdw_xact->local_xid;
				fdw_remove_xlog.userid = fdw_xact->userid;

				START_CRIT_SECTION();

				/*
				 * Log that we are removing the foreign transaction entry and
				 * remove the file from the disk as well.
				 */
				XLogBeginInsert();
				XLogRegisterData((char *) &fdw_remove_xlog, sizeof(fdw_remove_xlog));
				recptr = XLogInsert(RM_FDW_XACT_ID, XLOG_FDW_XACT_REMOVE);
				XLogFlush(recptr);

				END_CRIT_SECTION();
			}

			/* Remove the file from the disk if exists. */
			if (fdw_xact->ondisk)
				RemoveFdwXactFile(fdw_xact->local_xid, fdw_xact->serverid,
								  fdw_xact->userid, true);
			return;
		}
	}

	LWLockRelease(FdwXactLock);

	/* We did not find the given entry in global array */
	elog(ERROR, "failed to find foreign transaction entry for xid %u, foreign server %u, and user %u",
		 fdw_xact->local_xid, fdw_xact->serverid, fdw_xact->userid);
}

/*
 * We need to use two-phase commit protocol in two cases: we modified data on more
 * than one foreign servers in a transaction, or we modified data on both one foreign
 * server and local server in a transaction.
 */
bool
TwoPhaseCommitRequired(void)
{
	TransactionId xid = GetTopTransactionIdIfAny();
	bool	markXidCommitted = TransactionIdIsValid(xid);
	bool	modifiedOnLocal;

	modifiedOnLocal = (XactLastRecEnd != 0) && markXidCommitted;

	if ((list_length(MyFdwConnections) > 1) ||
		(list_length(MyFdwConnections) == 1 && modifiedOnLocal))
		return true;

	return false;
}

/*
 * UnlockMyFdwXacts
 *
 * Unlock the foreign transaction entries locked by this backend and removing
 * it from the backend's list of foreign transactions.
 */
static void
UnlockMyFdwXacts(void)
{
	ListCell *cell;
	ListCell *next;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	for (cell = list_head(MyLockedFdwXacts); cell != NULL; cell = next)
	{
		FdwXact	fdwxact = (FdwXact) lfirst(cell);

		next = lnext(cell);

		/*
		 * Because the resolver process can removed a fdwxact entry
		 * after resolved and it will be free, it can happen that
		 * a fdwxact entry in MyLockedFdwXacts is locked by an another
		 * backend if it locked the entry before we unlock. So we need to
		 * check if entries are begin locked by MyBackendId.
		 */
		if (fdwxact->locking_backend == MyBackendId)
		{
			/*
			 * We set the locking backend as invalid, and then remove it from
			 * the list of locked foreign transactions, under the LWLock. If we reverse
			 * the order and process exits in-between those two, we will left an
			 * entry locked by this backend, which gets unlocked only at the server
			 * restart.
			 */

			fdwxact->locking_backend = InvalidBackendId;
			MyLockedFdwXacts = list_delete_ptr(MyLockedFdwXacts, fdwxact);
		}
	}

	LWLockRelease(FdwXactLock);
}

/*
 * AtProcExit_FdwXact
 *
 * When the process exits, unlock the entries it held.
 */
static void
AtProcExit_FdwXact(int code, Datum arg)
{
	UnlockMyFdwXacts();
}

/*
 * Wait for foreign transaction resolution, if requested by user.
 *
 * Initially backends start in state FDW_XACT_NOT_WAITING and then
 * change that state to FDW_XACT_WAITING before adding ourselves
 * to the wait queue. During FdwXactResolveForeignTransactions a fdwxact
 * resolver changes the state to FDW_XACT_WAIT_COMPLETE once foreign
 * transactions are resolved. This backend then resets its state
 * to FDW_XACT_NOT_WAITING. If fdwxact_list is NULL, it means that
 * we use the list of FdwXact just used, so set it to MyLockedFdwXacts.
 *
 * This function is inspired by SyncRepWaitForLSN.
 */
void
FdwXactWaitForResolution(TransactionId wait_xid, bool is_commit)
{
	char		*new_status = NULL;
	const char	*old_status;
	ListCell	*cell;
	List		*entries_to_resolve;

	/*
	 * Quick exit if user has not requested foreign transaction resolution
	 * or there are no foreign servers that are modified in the current
	 * transaction.
	 */
	if (!FdwXactEnabled())
		return;

	Assert(FdwXactCtl != NULL);
	Assert(TransactionIdIsValid(wait_xid));
	Assert(SHMQueueIsDetached(&(MyProc->fdwXactLinks)));
	Assert(MyProc->fdwXactState == FDW_XACT_NOT_WAITING);

	/*
	 * Get the list of foreign transactions that are involved with the
	 * given wait_xid.
	 */
	search_fdw_xact(wait_xid, MyDatabaseId, InvalidOid, InvalidOid,
					&entries_to_resolve);

	/* Quick exit if we found no foreign transaction that we need to resolve */
	if (list_length(entries_to_resolve) <= 0)
		return;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	/* Change status of fdw_xact entries according to is_commit */
	foreach (cell, entries_to_resolve)
	{
		FdwXact fdw_xact = (FdwXact) lfirst(cell);

		/* Don't overwrite status if fate is determined */
		if (fdw_xact->status == FDW_XACT_PREPARING)
			fdw_xact->status = (is_commit ?
								FDW_XACT_COMMITTING_PREPARED :
								FDW_XACT_ABORTING_PREPARED);
	}

	/* Set backend status and enqueue itself */
	MyProc->fdwXactState = FDW_XACT_WAITING;
	MyProc->waitXid = wait_xid;
	FdwXactQueueInsert();
	LWLockRelease(FdwXactLock);

	/* Launch a resolver process if not yet and then wake up it */
	fdwxact_maybe_launch_resolver();

	/*
	 * Alter ps display to show waiting for foreign transaction
	 * resolution.
	 */
	if (update_process_title)
	{
		int len;

		old_status = get_ps_display(&len);
		new_status = (char *) palloc(len + 31 + 1);
		memcpy(new_status, old_status, len);
		sprintf(new_status + len, " waiting for resolve %d", wait_xid);
		set_ps_display(new_status, false);
		new_status[len] = '\0';	/* truncate off "waiting ..." */
	}

	/* Wait for all foreign transactions to be resolved */
	for (;;)
	{
		/* Must reset the latch before testing state */
		ResetLatch(MyLatch);

		/*
		 * Acquiring the lock is not needed, the latch ensures proper
		 * barriers. If it looks like we're done, we must really be done,
		 * because once walsender changes the state to FDW_XACT_WAIT_COMPLETE,
		 * it will never update it again, so we can't be seeing a stale value
		 * in that case.
		 */
		if (MyProc->fdwXactState == FDW_XACT_WAIT_COMPLETE)
			break;

		/*
		 * If a wait for foreign transaction resolution is pending, we can
		 * neither acknowledge the commit nor raise ERROR or FATAL.  The latter
		 * would lead the client to believe that the distributed transaction
		 * aborted, which is not true: it's already committed locally. The
		 * former is no good either: the client has requested committing a
		 * distributed transaction, and is entitled to assume that a acknowledged
		 * commit is also commit on all foreign servers, which might not be
		 * true. So in this case we issue a WARNING (which some clients may
		 * be able to interpret) and shut off further output. We do NOT reset
		 * PorcDiePending, so that the process will die after the commit is
		 * cleaned up.
		 */
		if (ProcDiePending)
		{
			ereport(WARNING,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("canceling the wait for resolving foreign transaction and terminating connection due to administrator command"),
					 errdetail("The transaction has already committed locally, but might not have been committed on the foreign server.")));
			whereToSendOutput = DestNone;
			FdwXactCancelWait();
			break;
		}

		/*
		 * If a query cancel interrupt arrives we just terminate the wait with
		 * a suitable warning. The foreign transactions can be orphaned but
		 * the foreign xact resolver can pick up them and tries to resolve them
		 * later.
		 */
		if (QueryCancelPending)
		{
			QueryCancelPending = false;
			ereport(WARNING,
					(errmsg("canceling wait for resolving foreign transaction due to user request"),
					 errdetail("The transaction has already committed locally, but might not have been committed on the foreign server.")));
			FdwXactCancelWait();
			break;
		}

		/*
		 * If the postmaster dies, we'll probably never get an
		 * acknowledgement, because all the wal sender processes will exit. So
		 * just bail out.
		 */
		if (!PostmasterIsAlive())
		{
			ProcDiePending = true;
			whereToSendOutput = DestNone;
			FdwXactCancelWait();
			break;
		}

		/*
		 * Wait on latch.  Any condition that should wake us up will set the
		 * latch, so no need for timeout.
		 */
		WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
				  WAIT_EVENT_FDW_XACT_RESOLUTION);
	}

	pg_read_barrier();

	Assert(SHMQueueIsDetached(&(MyProc->fdwXactLinks)));
	MyProc->fdwXactState = FDW_XACT_NOT_WAITING;

	/*
	 * Unlock the list of locked entries, also means that the entries
	 * that could not resolved are remained as dangling transactions.
	 */
	UnlockMyFdwXacts();
	MyLockedFdwXacts = NIL;

	if (new_status)
	{
		set_ps_display(new_status, false);
		pfree(new_status);
	}
}

static void
FdwXactCancelWait(void)
{
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	if (!SHMQueueIsDetached(&(MyProc->fdwXactLinks)))
		SHMQueueDelete(&(MyProc->fdwXactLinks));
	MyProc->fdwXactState = FDW_XACT_NOT_WAITING;
	LWLockRelease(FdwXactLock);
}

/*
 * Insert MyProc into the FdwXactQueue.
 */
static void
FdwXactQueueInsert(void)
{
	SHMQueueInsertBefore(&(FdwXactRslvCtl->FdwXactQueue),
						 &(MyProc->fdwXactLinks));
}

/*
 * Resolve all foreign transactions associated with the same local
 * transaction in the given database. rAfter resolved all foreign
 * transactions we release the waiter. Return the number of foreign
 * transaction we resolved.
 */
int
FdwXactResolveForeignTransactions(Oid dbid)
{
	TransactionId		key;
	volatile FdwXact	fdwxact = NULL;
	volatile FdwXact	fx_next;
	FdwXactHashEntry	*fdwxact_entry;
	int					n_resolved = 0;
	bool				found;
	PGPROC				*proc;

	Assert(OidIsValid(dbid));

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	/* Fetch an proc from beginning of the queue */
	for (;;)
	{
		proc = (PGPROC *) SHMQueueNext(&(FdwXactRslvCtl->FdwXactQueue),
									   &(FdwXactRslvCtl->FdwXactQueue),
									   offsetof(PGPROC, fdwXactLinks));

		/* Return if there is not any entry in the queue */
		if (!proc)
		{
			LWLockRelease(FdwXactLock);
			return false;
		}

		/* Found a target proc we need to resolve */
		if (proc->databaseId == dbid)
			break;
	}

	Assert(TransactionIdIsValid(proc->waitXid));

	/* Search fdwxact entry from the shmem hash by local transaction id */
	key = proc->waitXid;
	fdwxact_entry = (FdwXactHashEntry *) hash_search(FdwXactHash,
													 (void *) &key,
													 HASH_ENTER, &found);

	/*
	 * The fdwxact entry can not be found in the hash table after recovery.
	 * In this case, we initialize it and construct a list of fdw xact entries
	 * in FdwXactHashEntry with foreign transaction associated with the same
	 * transaction.
	 */
	if (!found)
	{
		int i;
		FdwXact prev_fx = NULL;

		/* Initialize entry */
		fdwxact_entry->xid = proc->waitXid;
		fdwxact_entry->first_entry = NULL;

		for (i = 0; i < FdwXactCtl->numFdwXacts; i++)
		{
			FdwXact fx = FdwXactCtl->fdw_xacts[i];

			if (fx->dboid == dbid && fx->local_xid == proc->waitXid)
			{
				/* Save first entry of the list */
				if (fdwxact_entry->first_entry == NULL)
					fdwxact_entry->first_entry = fx;

				/* Link from previous entry to this entry */
				if (prev_fx)
					prev_fx->fx_next = fx;

				prev_fx = fx;
			}
		}
	}

	LWLockRelease(FdwXactLock);
	fdwxact = fdwxact_entry->first_entry;

	/* Resolve all foreign transactions associated with pgxact->xid */
	while (fdwxact)
	{
		/*
		 * Remember the next entry to resolve since current entry will be
		 * removed after resolved.
		 */
		fx_next = fdwxact->fx_next;

		/*
		 * Resolve foreign transaction. We keep trying to resolve foreign
		 * transaction until got successful.
		 *
		 * XXX : We might want to try to resolve other foreign transactions
		 * as mush as possible.
		 */
		while (!FdwXactResolveForeignTransaction(fdwxact,
												 get_prepared_foreign_xact_resolver(fdwxact)))
		{
			CHECK_FOR_INTERRUPTS();

			/*
			 * If failed to resolve, we try to resolve the foreign transaction
			 * after for a while.
			 */
			pg_usleep(1 * 1000L * 1000L);	/* 1 sec */
		}

		n_resolved++;
		fdwxact = fx_next;
	}

	/* We remove proc from shmem hash table as well */
	hash_search(FdwXactHash, (void *) &key, HASH_REMOVE, NULL);

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	/*
	 * Remove proc from queue, if not detached yet. If the waiter has
	 * canceled to wait before resolution, this proc is already detached.
	 */
	if (!SHMQueueIsDetached(&(proc->fdwXactLinks)))
	{
		SHMQueueDelete(&(proc->fdwXactLinks));

		pg_write_barrier();

		/* Set state to complete */
		proc->fdwXactState = FDW_XACT_WAIT_COMPLETE;

		/* Wake up the waiter only when we have set state and removed from queue */
		SetLatch(&(proc->procLatch));
	}

	LWLockRelease(FdwXactLock);

	return n_resolved;
}

/*
 * Resolve all dangling foreign transactions as much as possible in the given
 * database. Return the number of foreign transaction we resolved.
 */
int
FdwXactResolveDanglingTransactions(Oid dbid)
{
	List		*fxact_list = NIL;
	ListCell	*cell;
	bool		n_resolved = 0;
	int			i;

	Assert(OidIsValid(dbid));

	LWLockAcquire(FdwXactLock, LW_SHARED);

	/*
	 * Make a list of dangling transactions of which corresponding local
	 * transaction is on the same database.
	 */
	for (i = 0; i < FdwXactCtl->numFdwXacts; i++)
	{
		FdwXact fxact = FdwXactCtl->fdw_xacts[i];

		/*
		 * Append it to the list if the fdwxact entry that is on the same
		 * database if it's not locked by anyone and its local transaction
		 * is not prepared.
		 */
		if (fxact->dboid == dbid &&
			fxact->locking_backend == InvalidBackendId &&
			!TwoPhaseExists(fxact->local_xid))
			fxact_list = lappend(fxact_list, fxact);
	}

	LWLockRelease(FdwXactLock);

	/* There is no foreign transaction we need to resolve */
	if (list_length(fxact_list) == 0)
		return 0;

	foreach(cell, fxact_list)
	{
		FdwXact fdwxact = (FdwXact) lfirst(cell);

		if (!FdwXactResolveForeignTransaction(fdwxact,
											  get_prepared_foreign_xact_resolver(fdwxact)))
		{
			/*
			 * If failed to resolve this foreign transaction we skip it in
			 * this resolution cycle. Try to resolve again in next cycle.
			 */
			ereport(WARNING, (errmsg("could not resolve dangling foreign transaction for xid %u, foreign server %u and user %d",
									 fdwxact->local_xid, fdwxact->serverid, fdwxact->userid)));
			continue;
		}

		n_resolved++;
	}

	list_free(fxact_list);

	return n_resolved;
}

/*
 * AtEOXact_FdwXacts
 *
 */
extern void
AtEOXact_FdwXacts(bool is_commit)
{
	ListCell   *lcell;

	/*
	 * In commit case, we already committed the foreign transactions on the
	 * servers that cannot execute two-phase commit protocol, and prepared
	 * transaction on the server that can use two-phase commit protocol
	 * in-precommit phase. And the prepared transactions should be resolved
	 * by the resolver process. On the other hand in abort case, since we
	 * might either prepare or be preparing some transactions on foreign
	 * servers we need to abort prepared transactions while just abort the
	 * foreign transaction that are not prepared yet.
	 */
	if (!is_commit)
	{
		foreach (lcell, MyFdwConnections)
		{
			FdwConnection	*fdw_conn = lfirst(lcell);

			/*
			 * Since the prepared foreign transaction should have been
			 * resolved we abort the remaining not-prepared foreign
			 * transactions.
			 */
			if (!fdw_conn->fdw_xact)
			{
				bool ret;

				ret = fdw_conn->end_foreign_xact(fdw_conn->serverid, fdw_conn->userid,
												 fdw_conn->umid, is_commit);
				if (!ret)
					ereport(WARNING, (errmsg("could not abort transaction on server \"%s\"",
											 fdw_conn->servername)));
			}
		}
	}

	/*
	 * Unlock any locked foreign transactions. Other backend might lock the
	 * entry we used to lock, but there is no reason for a foreign transaction
	 * entry to be locked after the transaction which locked it has ended.
	 */
	UnlockMyFdwXacts();
	MyLockedFdwXacts = NIL;

	/*
	 * Reset the list of registered connections. Since the memory for the list
	 * and its nodes comes from transaction memory context, it will be freed
	 * after this call.
	 */
	MyFdwConnections = NIL;

	/* Set TwoPhaseReady to its default value */
	TwoPhaseReady = true;
}

/*
 * AtPrepare_FdwXacts
 *
 * The function is called while preparing a transaction. If there are foreign
 * servers involved in the transaction, this function prepares transactions
 * on those servers.
 *
 * Note that it can happen that the transaction abort after we prepared foreign
 * transactions. So we cannot unlock both MyLockedFdwXacts and MyFdwConnections
 * here. These are unlocked after rollbacked by resolver process during
 * aborting, or at EOXact_FdwXacts().
 */
void
AtPrepare_FdwXacts(void)
{
	/* If there are no foreign servers involved, we have no business here */
	if (list_length(MyFdwConnections) < 1)
		return;

	/*
	 * All foreign servers participating in a transaction to be prepared
	 * should be two phase compliant.
	 */
	if (!TwoPhaseReady)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("can not prepare the transaction because some foreign servers involved in transaction can not prepare the transaction")));

	/* Prepare transactions on participating foreign servers. */
	FdwXactPrepareForeignTransactions();
}

/*
 * get_prepared_foreign_xact_resolver
 */
static ResolvePreparedForeignTransaction_function
get_prepared_foreign_xact_resolver(FdwXact fdw_xact)
{
	ForeignServer *foreign_server;
	ForeignDataWrapper *fdw;
	FdwRoutine *fdw_routine;

	foreign_server = GetForeignServer(fdw_xact->serverid);
	fdw = GetForeignDataWrapper(foreign_server->fdwid);
	fdw_routine = GetFdwRoutine(fdw->fdwhandler);
	if (!fdw_routine->ResolvePreparedForeignTransaction)
		elog(ERROR, "no foreign transaction resolver routine provided for FDW %s",
			 fdw->fdwname);

	return fdw_routine->ResolvePreparedForeignTransaction;
}

/*
 * FdwXactResolveForeignTransaction
 *
 * Resolve the foreign transaction using the foreign data wrapper's transaction
 * handler routine. The foreign transaction can be a dangling transaction
 * that is not decided to commit or abort.
 * If the resolution is successful, remove the foreign transaction entry from
 * the shared memory and also remove the corresponding on-disk file.
 */
static bool
FdwXactResolveForeignTransaction(FdwXact fdw_xact,
			   ResolvePreparedForeignTransaction_function fdw_xact_handler)
{
	bool		resolved;
	bool		is_commit;

	if(!(fdw_xact->status == FDW_XACT_COMMITTING_PREPARED ||
		 fdw_xact->status == FDW_XACT_ABORTING_PREPARED))
		elog(DEBUG1, "fdwxact status : %d", fdw_xact->status);

	/*
	 * Determine whether we commit or abort this foreign transaction.
	 */
	if (fdw_xact->status == FDW_XACT_COMMITTING_PREPARED)
		is_commit = true;

	else if (fdw_xact->status == FDW_XACT_ABORTING_PREPARED)
		is_commit = false;

	/*
	 * If the local transaction is already committed, commit prepared
	 * foreign transactions as well.
	 */
	else if (TransactionIdDidCommit(fdw_xact->local_xid))
	{
		fdw_xact->status = FDW_XACT_COMMITTING_PREPARED;
		is_commit = true;
	}

	/*
	 * If the local transaction is already aborted, abort prepared
	 * foreign transactions as well.
	 */
	else if (TransactionIdDidAbort(fdw_xact->local_xid))
	{
		fdw_xact->status = FDW_XACT_ABORTING_PREPARED;
		is_commit = false;
	}

	/*
	 * The local transaction is not in progress but the foreign
	 * transaction is not prepared on the foreign server. This
	 * can happen when we crashed after registered this entry but
	 * before actual preparing on the foreign server. So we assume
	 * it to be aborted.
	 */
	else if (!TransactionIdIsInProgress(fdw_xact->local_xid))
		is_commit = false;

	/*
	 * The Local transaction is in progress and foreign transaction
	 * state is neither committing or aborting. This should not
	 * happen because we cannot determine to do commit or abort for
	 * foreign transaction associated with the in-progress local transaction.
	 */
	else
		ereport(ERROR,
				(errmsg("cannot resolve foreign transaction associated with in-progress transaction %u on server %u",
						fdw_xact->local_xid, fdw_xact->serverid)));

	resolved = fdw_xact_handler(fdw_xact->serverid, fdw_xact->userid,
								fdw_xact->umid, is_commit,
								fdw_xact->fdw_xact_id);

	/* If we succeeded in resolving the transaction, remove the entry */
	if (resolved)
		remove_fdw_xact(fdw_xact);

	return resolved;
}

/*
 * Get foreign transaction entry from FdwXactCtl->fdw_xacts. Return NULL
 * if foreign transaction does not exist.
 */
static FdwXact
get_fdw_xact(TransactionId xid, Oid serverid, Oid userid)
{
	int i;
	FdwXact fdw_xact;

	LWLockAcquire(FdwXactLock, LW_SHARED);

	for (i = 0; i < FdwXactCtl->numFdwXacts; i++)
	{
		fdw_xact = FdwXactCtl->fdw_xacts[i];

		if (fdw_xact->local_xid == xid &&
			fdw_xact->serverid == serverid &&
			fdw_xact->userid == userid)
		{
			LWLockRelease(FdwXactLock);
			return fdw_xact;
		}
	}

	LWLockRelease(FdwXactLock);
	return NULL;
}

/*
 * fdw_xact_exists
 * Returns true if there exists at least one prepared foreign transaction which
 * matches criteria. This function is wrapper around search_fdw_xact. Check that
 * function's prologue for details.
 */
bool
fdw_xact_exists(TransactionId xid, Oid dbid, Oid serverid, Oid userid)
{
	return search_fdw_xact(xid, dbid, serverid, userid, NULL);
}

/*
 * search_fdw_xact
 * Return true if there exists at least one prepared foreign transaction
 * entry with given criteria. The criteria is defined by arguments with
 * valid values for respective datatypes.
 *
 * The table below explains the same
 * xid	   | dbid	 | serverid | userid  | search for entry with
 * invalid | invalid | invalid	| invalid | nothing
 * invalid | invalid | invalid	| valid   | given userid
 * invalid | invalid | valid	| invalid | given serverid
 * invalid | invalid | valid	| valid   | given serverid and userid
 * invalid | valid	 | invalid	| invalid | given dbid
 * invalid | valid	 | invalid	| valid   | given dbid and userid
 * invalid | valid	 | valid	| invalid | given dbid and serverid
 * invalid | valid	 | valid	| valid   | given dbid, serveroid and userid
 * valid   | invalid | invalid	| invalid | given xid
 * valid   | invalid | invalid	| valid   | given xid and userid
 * valid   | invalid | valid	| invalid | given xid, serverid
 * valid   | invalid | valid	| valid   | given xid, serverid, userid
 * valid   | valid	 | invalid	| invalid | given xid and dbid
 * valid   | valid	 | invalid	| valid   | given xid, dbid and userid
 * valid   | valid	 | valid	| invalid | given xid, dbid, serverid
 * valid   | valid	 | valid	| valid   | given xid, dbid, serverid, userid
 *
 * When the criteria is void (all arguments invalid) the
 * function returns true, since any entry would match the criteria.
 *
 * If qualifying_fdw_xacts is not NULL, the qualifying entries are locked and
 * returned in a linked list. Any entry which is already locked is ignored. If
 * all the qualifying entries are locked, nothing will be returned in the list
 * but returned value will be true.
 */
static bool
search_fdw_xact(TransactionId xid, Oid dbid, Oid serverid, Oid userid,
				List **qualifying_xacts)
{
	int			cnt;
	LWLockMode	lock_mode;

	/* Return value if a qualifying entry exists */
	bool		entry_exists = false;

	if (qualifying_xacts)
	{
		*qualifying_xacts = NIL;
		/* The caller expects us to lock entries */
		lock_mode = LW_EXCLUSIVE;
	}
	else
		lock_mode = LW_SHARED;

	LWLockAcquire(FdwXactLock, lock_mode);
	for (cnt = 0; cnt < FdwXactCtl->numFdwXacts; cnt++)
	{
		FdwXact		fdw_xact = FdwXactCtl->fdw_xacts[cnt];
		bool		entry_matches = true;

		/* xid */
		if (xid != InvalidTransactionId && xid != fdw_xact->local_xid)
			entry_matches = false;

		/* dbid */
		if (OidIsValid(dbid) && fdw_xact->dboid != dbid)
			entry_matches = false;

		/* serverid */
		if (OidIsValid(serverid) && serverid != fdw_xact->serverid)
			entry_matches = false;

		/* userid */
		if (OidIsValid(userid) && fdw_xact->userid != userid)
			entry_matches = false;

		if (entry_matches)
		{
			entry_exists = true;
			if (qualifying_xacts)
			{
				/*
				 * User has requested list of qualifying entries. If the
				 * matching entry is not locked, lock it and add to the list.
				 * If the entry is locked by some other backend, ignore it.
				 */
				if (fdw_xact->locking_backend == InvalidBackendId)
				{
					MemoryContext oldcontext;

					fdw_xact->locking_backend = MyBackendId;

					/*
					 * The list and its members may be required at the end of
					 * the transaction
					 */
					oldcontext = MemoryContextSwitchTo(TopTransactionContext);
					MyLockedFdwXacts = lappend(MyLockedFdwXacts, fdw_xact);
					MemoryContextSwitchTo(oldcontext);
				}
				else if (fdw_xact->locking_backend != MyBackendId)
					continue;

				*qualifying_xacts = lappend(*qualifying_xacts, fdw_xact);
			}
			else
			{
				/*
				 * User wants to check the existence, and we have found one
				 * matching entry. No need to check other entries.
				 */
				break;
			}
		}
	}

	LWLockRelease(FdwXactLock);

	return entry_exists;
}

/*
 * fdw_xact_redo
 * Apply the redo log for a foreign transaction.
 */
void
fdw_xact_redo(XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_FDW_XACT_INSERT)
		FdwXactRedoAdd(record);
	else if (info == XLOG_FDW_XACT_REMOVE)
	{
		FdwRemoveXlogRec *fdw_remove_xlog = (FdwRemoveXlogRec *) rec;

		/* Delete FdwXact entry and file if exists */
		FdwXactRedoRemove(fdw_remove_xlog->xid, fdw_remove_xlog->serverid,
						  fdw_remove_xlog->userid);
	}
	else
		elog(ERROR, "invalid log type %d in foreign transction log record", info);

	return;
}

/*
 * CheckPointFdwXact
 *
 * Function syncs the foreign transaction files created between the two
 * checkpoints. The foreign transaction entries and hence the corresponding
 * files are expected to be very short-lived. By executing this function at the
 * end, we might have lesser files to fsync, thus reducing some I/O. This is
 * similar to CheckPointTwoPhase().
 *
 * In order to avoid disk I/O while holding a light weight lock, the function
 * first collects the files which need to be synced under FdwXactLock and then
 * syncs them after releasing the lock. This approach creates a race condition:
 * after releasing the lock, and before syncing a file, the corresponding
 * foreign transaction entry and hence the file might get removed. The function
 * checks whether that's true and ignores the error if so.
 */
void
CheckPointFdwXacts(XLogRecPtr redo_horizon)
{
	int			cnt;
	int			serialized_fdw_xacts = 0;

	/* Quick get-away, before taking lock */
	if (max_prepared_foreign_xacts <= 0)
		return;

	TRACE_POSTGRESQL_FDWXACT_CHECKPOINT_START();

	LWLockAcquire(FdwXactLock, LW_SHARED);

	/* Another quick, before we allocate memory */
	if (FdwXactCtl->numFdwXacts <= 0)
	{
		LWLockRelease(FdwXactLock);
		return;
	}

	/*
	 * We are expecting there to be zero FdwXact that need to be copied to
	 * disk, so we perform all I/O while holding FdwXactLock for simplicity.
	 * This presents any new foreign xacts from preparing while this occurs,
	 * which shouldn't be a problem since the presence fo long-lived prepared
	 * foreign xacts indicated the transaction manager isn't active.
	 *
	 * it's also possible to move I/O out of the lock, but on every error we
	 * should check whether somebody committed our transaction in different
	 * backend. Let's leave this optimisation for future, if somebody will
	 * spot that this place cause bottleneck.
	 *
	 * Note that it isn't possible for there to be a FdwXact with a
	 * fdw_xact_end_lsn set prior to the last checkpoint yet is marked
	 * invalid, because of the efforts with delayChkpt.
	 */
	for (cnt = 0; cnt < FdwXactCtl->numFdwXacts; cnt++)
	{
		FdwXact		fdw_xact = FdwXactCtl->fdw_xacts[cnt];

		if ((fdw_xact->valid || fdw_xact->inredo) &&
			!fdw_xact->ondisk &&
			fdw_xact->fdw_xact_end_lsn <= redo_horizon)
		{
			char	   *buf;
			int			len;

			XlogReadFdwXactData(fdw_xact->fdw_xact_start_lsn, &buf, &len);
			RecreateFdwXactFile(fdw_xact->local_xid, fdw_xact->serverid,
								fdw_xact->userid, buf, len);
			fdw_xact->ondisk = true;
			serialized_fdw_xacts++;
			pfree(buf);
		}
	}

	LWLockRelease(FdwXactLock);

	TRACE_POSTGRESQL_FDWXACT_CHECKPOINT_DONE();

	if (log_checkpoints && serialized_fdw_xacts > 0)
		ereport(LOG,
			  (errmsg_plural("%u foreign transaction state file was written "
							 "for long-running prepared transactions",
							 "%u foreign transaction state files were written "
							 "for long-running prepared transactions",
							 serialized_fdw_xacts,
							 serialized_fdw_xacts)));
}

/*
 * Reads foreign trasasction data from xlog. During checkpoint this data will
 * be moved to fdwxact files and ReadFdwXactFile should be used instead.
 *
 * Note clearly that this function accesses WAL during normal operation, similarly
 * to the way WALSender or Logical Decoding would do. It does not run during
 * crash recovery or standby processing.
 */
static void
XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;

	xlogreader = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
		   errdetail("Failed while allocating an XLog reading processor.")));

	record = XLogReadRecord(xlogreader, lsn, &errormsg);

	if (record == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
		errmsg("could not read foreign transaction state from xlog at %X/%X",
			   (uint32) (lsn >> 32),
			   (uint32) lsn)));

	if (XLogRecGetRmid(xlogreader) != RM_FDW_XACT_ID ||
		(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_FDW_XACT_INSERT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("expected foreign transaction state data is not present in xlog at %X/%X",
						(uint32) (lsn >> 32),
						(uint32) lsn)));

	if (len != NULL)
		*len = XLogRecGetDataLen(xlogreader);

	*buf = palloc(sizeof(char) * XLogRecGetDataLen(xlogreader));
	memcpy(*buf, XLogRecGetData(xlogreader), sizeof(char) * XLogRecGetDataLen(xlogreader));

	XLogReaderFree(xlogreader);
}

/*
 * Recreates a foreign transaction state file. This is used in WAL replay and
 * during checkpoint creation.
 *
 * Note: content and len don't include CRC.
 */
void
RecreateFdwXactFile(TransactionId xid, Oid serverid, Oid userid,
					void *content, int len)
{
	char		path[MAXPGPATH];
	pg_crc32c	fdw_xact_crc;
	pg_crc32c	bogus_crc;
	int			fd;

	/* Recompute CRC */
	INIT_CRC32C(fdw_xact_crc);
	COMP_CRC32C(fdw_xact_crc, content, len);

	FdwXactFilePath(path, xid, serverid, userid);

	fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
		errmsg("could not recreate foreign transaction state file \"%s\": %m",
			   path)));

	if (write(fd, content, len) != len)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
			  errmsg("could not write foreign transcation state file: %m")));
	}
	FIN_CRC32C(fdw_xact_crc);

	/*
	 * Write a deliberately bogus CRC to the state file; this is just paranoia
	 * to catch the case where four more bytes will run us out of disk space.
	 */
	bogus_crc = ~fdw_xact_crc;
	if ((write(fd, &bogus_crc, sizeof(pg_crc32c))) != sizeof(pg_crc32c))
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
			  errmsg("could not write foreing transaction state file: %m")));
	}
	/* Back up to prepare for rewriting the CRC */
	if (lseek(fd, -((off_t) sizeof(pg_crc32c)), SEEK_CUR) < 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
			errmsg("could not seek in foreign transaction state file: %m")));
	}

	/* write correct CRC and close file */
	if ((write(fd, &fdw_xact_crc, sizeof(pg_crc32c))) != sizeof(pg_crc32c))
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
			  errmsg("could not write foreign transaction state file: %m")));
	}

	/*
	 * We must fsync the file because the end-of-replay checkpoint will not do
	 * so, there being no GXACT in shared memory yet to tell it to.
	 */
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
			  errmsg("could not fsync foreign transaction state file: %m")));
	}

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close foreign transaction file: %m")));
}

/* Built in functions */
/*
 * Structure to hold and iterate over the foreign transactions to be displayed
 * by the built-in functions.
 */
typedef struct
{
	FdwXact		fdw_xacts;
	int			num_xacts;
	int			cur_xact;
}	WorkingStatus;

Datum
pg_prepared_fdw_xacts(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	WorkingStatus *status;
	char	   *xact_status;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		/* this had better match pg_fdw_xacts view in system_views.sql */
		tupdesc = CreateTemplateTupleDesc(6, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "dbid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "transaction",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "serverid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "userid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "identifier",
						   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/*
		 * Collect status information that we will format and send out as a
		 * result set.
		 */
		status = (WorkingStatus *) palloc(sizeof(WorkingStatus));
		funcctx->user_fctx = (void *) status;

		status->num_xacts = GetFdwXactList(&status->fdw_xacts);
		status->cur_xact = 0;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	status = funcctx->user_fctx;

	while (status->cur_xact < status->num_xacts)
	{
		FdwXact		fdw_xact = &status->fdw_xacts[status->cur_xact++];
		Datum		values[6];
		bool		nulls[6];
		HeapTuple	tuple;
		Datum		result;

		if (!fdw_xact->valid)
			continue;

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(fdw_xact->dboid);
		values[1] = TransactionIdGetDatum(fdw_xact->local_xid);
		values[2] = ObjectIdGetDatum(fdw_xact->serverid);
		values[3] = ObjectIdGetDatum(fdw_xact->userid);
		switch (fdw_xact->status)
		{
			case FDW_XACT_PREPARING:
				xact_status = "prepared";
				break;
			case FDW_XACT_COMMITTING_PREPARED:
				xact_status = "committing";
				break;
			case FDW_XACT_ABORTING_PREPARED:
				xact_status = "aborting";
				break;
			default:
				xact_status = "unknown";
				break;
		}
		values[4] = CStringGetTextDatum(xact_status);
		/* should this be really interpreted by FDW */
		values[5] = PointerGetDatum(cstring_to_text_with_len(fdw_xact->fdw_xact_id,
												 FDW_XACT_ID_LEN));

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Returns an array of all foreign prepared transactions for the user-level
 * function pg_fdw_xact.
 *
 * The returned array and all its elements are copies of internal data
 * structures, to minimize the time we need to hold the FdwXactLock.
 *
 * WARNING -- we return even those transactions whose information is not
 * completely filled yet. The caller should filter them out if he doesn't want them.
 *
 * The returned array is palloc'd.
 */
static int
GetFdwXactList(FdwXact * fdw_xacts)
{
	int			num_xacts;
	int			cnt_xacts;

	LWLockAcquire(FdwXactLock, LW_SHARED);

	if (FdwXactCtl->numFdwXacts == 0)
	{
		LWLockRelease(FdwXactLock);
		*fdw_xacts = NULL;
		return 0;
	}

	num_xacts = FdwXactCtl->numFdwXacts;
	*fdw_xacts = (FdwXact) palloc(sizeof(FdwXactData) * num_xacts);
	for (cnt_xacts = 0; cnt_xacts < num_xacts; cnt_xacts++)
		memcpy((*fdw_xacts) + cnt_xacts, FdwXactCtl->fdw_xacts[cnt_xacts],
			   sizeof(FdwXactData));

	LWLockRelease(FdwXactLock);

	return num_xacts;
}

/*
 * Built-in function to remove prepared foreign transaction entry/s without
 * resolving. The function gives a way to forget about such prepared
 * transaction in case
 * 1. The foreign server where it is prepared is no longer available
 * 2. The user which prepared this transaction needs to be dropped
 * 3. PITR is recovering before a transaction id, which created the prepared
 *	  foreign transaction
 * 4. The database containing the entries needs to be dropped
 *
 * Or any such conditions in which resolution is no longer possible.
 *
 * The function accepts 4 arguments transaction id, dbid, serverid and userid,
 * which define the criteria in the same way as search_fdw_xact(). The entries
 * matching the criteria are removed. The function does not remove an entry
 * which is locked by some other backend.
 */
Datum
pg_remove_fdw_xacts(PG_FUNCTION_ARGS)
{
/* Some #defines only for this function to deal with the arguments */
#define XID_ARGNUM	0
#define DBID_ARGNUM 1
#define SRVID_ARGNUM 2
#define USRID_ARGNUM 3

	TransactionId xid;
	Oid			dbid;
	Oid			serverid;
	Oid			userid;
	List	   *entries_to_remove;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to remove foreign transactions"))));

	xid = PG_ARGISNULL(XID_ARGNUM) ? InvalidTransactionId :
		DatumGetTransactionId(PG_GETARG_DATUM(XID_ARGNUM));
	dbid = PG_ARGISNULL(DBID_ARGNUM) ? InvalidOid :
		PG_GETARG_OID(DBID_ARGNUM);
	serverid = PG_ARGISNULL(SRVID_ARGNUM) ? InvalidOid :
		PG_GETARG_OID(SRVID_ARGNUM);
	userid = PG_ARGISNULL(USRID_ARGNUM) ? InvalidOid :
		PG_GETARG_OID(USRID_ARGNUM);

	search_fdw_xact(xid, dbid, serverid, userid, &entries_to_remove);

	while (entries_to_remove)
	{
		FdwXact		fdw_xact = linitial(entries_to_remove);

		entries_to_remove = list_delete_first(entries_to_remove);

		remove_fdw_xact(fdw_xact);
	}

	PG_RETURN_VOID();
}

/*
 * Resolve foreign transactions on the connecting database manually. This
 * function returns true if we resolve any foreign transaction, otherwise
 * return false.
 */
Datum
pg_resolve_fdw_xacts(PG_FUNCTION_ARGS)
{
	bool    ret;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to resolve foreign transactions"))));

	ret = FdwXactResolveForeignTransactions(MyDatabaseId);
	ret |= FdwXactResolveDanglingTransactions(MyDatabaseId);

	PG_RETURN_BOOL(ret);
}

/*
 * Code dealing with the on disk files used to store foreign transaction
 * information.
 */

/*
 * ReadFdwXactFile
 * Read the foreign transction state file and return the contents in a
 * structure allocated in-memory. The structure can be later freed by the
 * caller.
 */
static FdwXactOnDiskData *
ReadFdwXactFile(TransactionId xid, Oid serverid, Oid userid)
{
	char		path[MAXPGPATH];
	int			fd;
	FdwXactOnDiskData *fdw_xact_file_data;
	struct stat stat;
	uint32		crc_offset;
	pg_crc32c	calc_crc;
	pg_crc32c	file_crc;
	char	   *buf;

	FdwXactFilePath(path, xid, serverid, userid);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
			   errmsg("could not open FDW transaction state file \"%s\": %m",
					  path)));

	/*
	 * Check file length.  We can determine a lower bound pretty easily. We
	 * set an upper bound to avoid palloc() failure on a corrupt file, though
	 * we can't guarantee that we won't get an out of memory error anyway,
	 * even on a valid file.
	 */
	if (fstat(fd, &stat))
	{
		CloseTransientFile(fd);

		ereport(WARNING,
				(errcode_for_file_access(),
			   errmsg("could not stat FDW transaction state file \"%s\": %m",
					  path)));
		return NULL;
	}

	if (stat.st_size < offsetof(FdwXactOnDiskData, fdw_xact_id) ||
		stat.st_size > MaxAllocSize)
	{
		CloseTransientFile(fd);
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("Too large FDW transaction state file \"%s\": %m",
						path)));
		return NULL;
	}

	buf = (char *) palloc(stat.st_size);
	fdw_xact_file_data = (FdwXactOnDiskData *) buf;
	crc_offset = stat.st_size - sizeof(pg_crc32c);
	/* Slurp the file */
	if (read(fd, fdw_xact_file_data, stat.st_size) != stat.st_size)
	{
		CloseTransientFile(fd);
		ereport(WARNING,
				(errcode_for_file_access(),
			   errmsg("could not read FDW transaction state file \"%s\": %m",
					  path)));
		pfree(fdw_xact_file_data);
		return NULL;
	}

	CloseTransientFile(fd);

	/*
	 * Check the CRC.
	 */
	INIT_CRC32C(calc_crc);
	COMP_CRC32C(calc_crc, buf, crc_offset);
	FIN_CRC32C(calc_crc);

	file_crc = *((pg_crc32c *) (buf + crc_offset));

	if (!EQ_CRC32C(calc_crc, file_crc))
	{
		pfree(buf);
		return NULL;
	}

	if (fdw_xact_file_data->serverid != serverid ||
		fdw_xact_file_data->userid != userid ||
		fdw_xact_file_data->local_xid != xid)
	{
		ereport(WARNING,
			(errmsg("removing corrupt foreign transaction state file \"%s\"",
					path)));
		CloseTransientFile(fd);
		pfree(buf);
		return NULL;
	}

	return fdw_xact_file_data;
}

/*
 * PrescanFdwXacts
 *
 * Read the foreign prepared transactions directory for oldest active
 * transaction. The transactions corresponding to the xids in this directory
 * are not necessarily active per say locally. But we still need those XIDs to
 * be alive so that
 * 1. we can determine whether they are committed or aborted
 * 2. the file name contains xid which shouldn't get used again to avoid
 *	  conflicting file names.
 *
 * The function accepts the oldest active xid determined by other functions
 * (e.g. PrescanPreparedTransactions()). It then compares every xid it comes
 * across while scanning foreign prepared transactions directory with the oldest
 * active xid. It returns the oldest of those xids or oldest active xid
 * whichever is older.
 *
 * If any foreign prepared transaction is part of a future transaction (PITR),
 * the function removes the corresponding file as
 * 1. We can not know the status of the local transaction which prepared this
 * foreign transaction
 * 2. The foreign server or the user may not be available as per new timeline
 *
 * Anyway, the local transaction which prepared the foreign prepared transaction
 * does not exist as per the new timeline, so it's better to forget the foreign
 * prepared transaction as well.
 */
TransactionId
PrescanFdwXacts(TransactionId oldestActiveXid)
{
	TransactionId nextXid = ShmemVariableCache->nextXid;
	DIR		   *cldir;
	struct dirent *clde;

	cldir = AllocateDir(FDW_XACTS_DIR);
	while ((clde = ReadDir(cldir, FDW_XACTS_DIR)) != NULL)
	{
		if (strlen(clde->d_name) == FDW_XACT_FILE_NAME_LEN &&
		 strspn(clde->d_name, "0123456789ABCDEF_") == FDW_XACT_FILE_NAME_LEN)
		{
			Oid			serverid;
			Oid			userid;
			TransactionId local_xid;

			sscanf(clde->d_name, "%08x_%08x_%08x", &local_xid, &serverid,
				   &userid);

			/*
			 * Remove a foreign prepared transaction file corresponding to an
			 * XID, which is too new.
			 */
			if (TransactionIdFollowsOrEquals(local_xid, nextXid))
			{
				ereport(WARNING,
						(errmsg("removing future foreign prepared transaction file \"%s\"",
								clde->d_name)));
				RemoveFdwXactFile(local_xid, serverid, userid, true);
				continue;
			}

			if (TransactionIdPrecedesOrEquals(local_xid, oldestActiveXid))
				oldestActiveXid = local_xid;
		}
	}

	FreeDir(cldir);
	return oldestActiveXid;
}

/*
 * RecoverFdwXacts
 * Read the foreign prepared transaction information and set it up for further
 * usage.
 */
void
RecoverFdwXacts(void)
{
	DIR		   *cldir;
	struct dirent *clde;

	cldir = AllocateDir(FDW_XACTS_DIR);
	while ((clde = ReadDir(cldir, FDW_XACTS_DIR)) != NULL)
	{
		if (strlen(clde->d_name) == FDW_XACT_FILE_NAME_LEN &&
		 strspn(clde->d_name, "0123456789ABCDEF_") == FDW_XACT_FILE_NAME_LEN)
		{
			Oid			serverid;
			Oid			userid;
			TransactionId local_xid;
			FdwXactOnDiskData *fdw_xact_file_data;
			FdwXact		fdw_xact;

			sscanf(clde->d_name, "%08x_%08x_%08x", &local_xid, &serverid,
				   &userid);

			fdw_xact_file_data = ReadFdwXactFile(local_xid, serverid, userid);

			if (!fdw_xact_file_data)
			{
				ereport(WARNING,
				  (errmsg("Removing corrupt foreign transaction file \"%s\"",
						  clde->d_name)));
				RemoveFdwXactFile(local_xid, serverid, userid, false);
				continue;
			}

			ereport(LOG,
					(errmsg("recovering foreign transaction entry for xid %u, foreign server %u and user %u",
							local_xid, serverid, userid)));

			fdw_xact = get_fdw_xact(local_xid, serverid, userid);

			LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
			if (!fdw_xact)
			{
				/*
				 * Add this entry into the table of foreign transactions. The
				 * status of the transaction is set as preparing, since we do not
				 * know the exact status right now. Resolver will set it later
				 * based on the status of local transaction which prepared this
				 * foreign transaction.
				 */
				fdw_xact = insert_fdw_xact(fdw_xact_file_data->dboid, local_xid,
										   serverid, userid,
										   fdw_xact_file_data->umid,
										   fdw_xact_file_data->fdw_xact_id);
				fdw_xact->locking_backend = MyBackendId;
				fdw_xact->status = FDW_XACT_PREPARING;
			}
			else
			{
				Assert(fdw_xact->inredo);
				fdw_xact->inredo = false;
			}

			/* Mark the entry as ready */
			fdw_xact->valid = true;
			/* Already synced to disk */
			fdw_xact->ondisk = true;
			pfree(fdw_xact_file_data);
			LWLockRelease(FdwXactLock);
		}
	}

	FreeDir(cldir);
}

/*
 * Remove the foreign transaction file for given entry.
 *
 * If giveWarning is false, do not complain about file-not-present;
 * this is an expected case during WAL replay.
 */
static void
RemoveFdwXactFile(TransactionId xid, Oid serverid, Oid userid, bool giveWarning)
{
	char		path[MAXPGPATH];

	FdwXactFilePath(path, xid, serverid, userid);
	if (unlink(path))
		if (errno != ENOENT || giveWarning)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove foreign transaction state file \"%s\": %m",
							path)));
}

/*
 * FdwXactRedoAdd
 *
 * Store pointer to the start/end of the WAL record along with the xid in
 * a fdw_xact entry in shared memory FdwXactData structure.
 */
void
FdwXactRedoAdd(XLogReaderState *record)
{
	FdwXactOnDiskData *fdw_xact_data = (FdwXactOnDiskData *) XLogRecGetData(record);
	FdwXact fdw_xact;

	Assert(RecoveryInProgress());

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	fdw_xact = insert_fdw_xact(fdw_xact_data->dboid, fdw_xact_data->local_xid,
							   fdw_xact_data->serverid, fdw_xact_data->userid,
							   fdw_xact_data->umid, fdw_xact_data->fdw_xact_id);
	fdw_xact->status = FDW_XACT_PREPARING;
	fdw_xact->fdw_xact_start_lsn = record->ReadRecPtr;
	fdw_xact->fdw_xact_end_lsn = record->EndRecPtr;
	fdw_xact->inredo = true;
	fdw_xact->valid = true;
	LWLockRelease(FdwXactLock);
}
/*
 * FdwXactRedoRemove
 *
 * Remove the corresponding fdw_xact entry from FdwXactCtl.
 * Also remove fdw_xact file if a foreign transaction was saved
 * via an earlier checkpoint.
 */
void
FdwXactRedoRemove(TransactionId xid, Oid serverid, Oid userid)
{
	FdwXact	fdw_xact;

	Assert(RecoveryInProgress());

	fdw_xact = get_fdw_xact(xid, serverid, userid);

	if (fdw_xact)
	{
		/* Now we can clean up any files we already left */
		Assert(fdw_xact->inredo);
		remove_fdw_xact(fdw_xact);
	}
	else
	{
		/*
		 * Entry could be on disk. Call with giveWarning = false
		 * since it can be expected during replay.
		 */
		RemoveFdwXactFile(xid, serverid, userid, false);
	}
}
