/*-------------------------------------------------------------------------
 *
 * fdw_xact.c
 *		PostgreSQL distributed transaction manager for foreign server.
 *
 * This module manages the transactions involving foreign servers.
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/fdw_xact.c
 *
 *-------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "miscadmin.h"
#include "funcapi.h"

#include "access/fdw_xact.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "libpq/pqsignal.h"
#include "pg_trace.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

/*
 * This comment summarises how the transaction manager handles transactions
 * involving one or more foreign server/s.
 *
 * When an foreign data wrapper starts transaction on a foreign server, it is
 * required to register the foreign server and user who initiated the
 * transaction using function RegisterXactForeignServer(). A foreign server
 * connection is identified by oid of foreign server and user.
 *
 * The commit is executed in two phases:
 * First phase (executed during pre-commit processing)
 * -----------
 * Transactions are prepared on all the foreign servers, which can participate
 * in two-phase commit protocol. Transaction on other foreign servers are
 * committed in the same phase.
 *
 * Second phase (executed during post-commit/abort processing)
 * ------------
 * If first phase succeeds, foreign servers are requested to commit respective
 * prepared transactions. If the first phase  does not succeed because of any
 * failure, the foreign servers are asked to rollback respective prepared
 * transactions or abort the transactions if they are not prepared.
 *
 * Any network failure, server crash after preparing foreign transaction leaves
 * that prepared transaction unresolved. During the first phase, before actually
 * preparing the transactions, enough information is persisted to the disk and
 * logs in order to resolve such transactions.
 *
 * During replay and replication FDWXactGlobal also holds information about
 * active prepared foreign transaction that haven't been moved to disk yet.
 *
 * Replay of fdw_xact records happens by the following rules:
 *
 *		* On PREPARE redo we add the foreign transaction to
 *		  FDWXactGlobal->fdw_xacts. We set fdw_xact->inredo to true for
 *		  such entries.
 *
 *		* On Checkpoint redo we iterate through FDWXactGlobal->fdw_xacts.
 *		  entries that have fdw_xact->inredo set and are behind the redo_horizon.
 *		  We save them to disk and also set fdw_xact->ondisk to true.
 *
 *		* On COMMIT/ABORT we delete the entry from FDWXactGlobal->fdw_xacts.
 *		  If fdw_xact->ondisk is true, we delete the corresponding entry from
 *		  the disk as well.
 *
 *		* RecoverPreparedTransactions(), StnadbyReoverPreparedTransactions() and
 *		  PrescanPreparedTransactions() have been modified to go through
 *		  fdw_xact->inredo entries that have not made to disk yet.
 */

/* Shared memory entry for a prepared or being prepared foreign transaction */
typedef struct FDWXactData *FDWXact;

/* Structure to bundle the foreign connection participating in transaction */
typedef struct
{
	Oid			serverid;
	Oid			userid;
	Oid			umid;
	char	   *servername;
	FDWXact		fdw_xact;		/* foreign prepared transaction entry in case
								 * prepared */
	bool		two_phase_commit;		/* Should use two phase commit
										 * protocol while committing
										 * transaction on this server,
										 * whenever necessary. */
	EndForeignTransaction_function end_foreign_xact;
	PrepareForeignTransaction_function prepare_foreign_xact;
	ResolvePreparedForeignTransaction_function resolve_prepared_foreign_xact;
}	FDWConnection;

/* List of foreign connections participating in the transaction */
List	   *MyFDWConnections = NIL;

/*
 * By default we assume that all the foreign connections participating in this
 * transaction can use two phase commit protocol.
 */
bool		TwoPhaseReady = true;

/* Record the server, userid participating in the transaction. */
void
RegisterXactForeignServer(Oid serverid, Oid userid, bool two_phase_commit)
{
	FDWConnection *fdw_conn;
	ListCell   *lcell;
	ForeignServer *foreign_server;
	ForeignDataWrapper *fdw;
	UserMapping *user_mapping;
	FdwRoutine *fdw_routine;
	MemoryContext old_context;

	TwoPhaseReady = TwoPhaseReady && two_phase_commit;

	/* Check if the entry already exists, if so, raise an error */
	foreach(lcell, MyFDWConnections)
	{
		fdw_conn = lfirst(lcell);

		if (fdw_conn->serverid == serverid &&
			fdw_conn->userid == userid)
			ereport(ERROR,
			(errmsg("attempt to start transction again on server %u user %u",
					serverid, userid)));
	}

	/*
	 * This list and its contents needs to be saved in the transaction context
	 * memory
	 */
	old_context = MemoryContextSwitchTo(TopTransactionContext);
	/* Add this foreign connection to the list for transaction management */
	fdw_conn = (FDWConnection *) palloc(sizeof(FDWConnection));

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
	fdw_conn->prepare_foreign_xact = fdw_routine->PrepareForeignTransaction;
	fdw_conn->resolve_prepared_foreign_xact = fdw_routine->ResolvePreparedForeignTransaction;
	fdw_conn->end_foreign_xact = fdw_routine->EndForeignTransaction;
	fdw_conn->fdw_xact = NULL;
	fdw_conn->two_phase_commit = two_phase_commit;
	MyFDWConnections = lappend(MyFDWConnections, fdw_conn);
	/* Revert back the context */
	MemoryContextSwitchTo(old_context);

	return;
}

/* Enum to track the status of prepared foreign transaction */
typedef enum
{
	FDW_XACT_PREPARING,			/* foreign transaction is (being) prepared */
	FDW_XACT_COMMITTING_PREPARED,		/* foreign prepared transaction is to
										 * be committed */
	FDW_XACT_ABORTING_PREPARED, /* foreign prepared transaction is to be
								 * aborted */
	FDW_XACT_RESOLVED			/* Status used only by pg_fdw_xact_resolve().
								 * It doesn't appear in the in-memory entry. */
}	FDWXactStatus;

typedef struct FDWXactData
{
	FDWXact		fx_next;		/* Next free FDWXact entry */
	Oid			dboid;			/* database oid where to find foreign server
								 * and user mapping */
	TransactionId local_xid;	/* XID of local transaction */
	Oid			serverid;		/* foreign server where transaction takes
								 * place */
	Oid			userid;			/* user who initiated the foreign transaction */
	Oid			umid;			/* user mapping id for connection key */
	FDWXactStatus status;		/* The state of the foreign
								 * transaction. This doubles as the
								 * action to be taken on this entry. */

	/*
	 * Note that we need to keep track of two LSNs for each FDWXact. We keep
	 * track of the start LSN because this is the address we must use to read
	 * state data back from WAL when committing a FDWXact. We keep track of
	 * the end LSN because that is the LSN we need to wait for prior to
	 * commit.
	 */
	XLogRecPtr	fdw_xact_start_lsn;		/* XLOG offset of inserting this entry
										 * start */
	XLogRecPtr	fdw_xact_end_lsn;		/* XLOG offset of inserting this entry
										 * end */

	bool		valid; /* Has the entry been complete and written to file? */
	BackendId	locking_backend;	/* Backend working on this entry */
	bool		ondisk;			/* TRUE if prepare state file is on disk */
	bool		inredo;			/* TRUE if entry was added via xlog_redo */
	char		fdw_xact_id[FDW_XACT_ID_LEN];		/* prepared transaction
														 * identifier */
}	FDWXactData;

/* Directory where the foreign prepared transaction files will reside */
#define FDW_XACTS_DIR "pg_fdw_xact"

/*
 * Name of foreign prepared transaction file is 8 bytes xid, 8 bytes foreign
 * server oid and 8 bytes user oid separated by '_'.
 */
#define FDW_XACT_FILE_NAME_LEN (8 + 1 + 8 + 1 + 8)
#define FDWXactFilePath(path, xid, serverid, userid)	\
	snprintf(path, MAXPGPATH, FDW_XACTS_DIR "/%08X_%08X_%08X", xid, \
			 serverid, userid)

/* Shared memory layout for maintaining foreign prepared transaction entries. */
typedef struct
{
	/* Head of linked list of free FDWXactData structs */
	FDWXact		freeFDWXacts;

	/* Number of valid FDW transaction entries */
	int			numFDWXacts;

	/* Upto max_prepared_foreign_xacts entries in the array */
	FDWXact		fdw_xacts[FLEXIBLE_ARRAY_MEMBER];		/* Variable length array */
}	FDWXactGlobalData;

static void AtProcExit_FDWXact(int code, Datum arg);
static bool resolve_fdw_xact(FDWXact fdw_xact,
  ResolvePreparedForeignTransaction_function prepared_foreign_xact_resolver);
static FDWXact insert_fdw_xact(Oid dboid, TransactionId xid, Oid serverid, Oid userid,
							   Oid umid, char *fdw_xact_id);
static void unlock_fdw_xact(FDWXact fdw_xact);
static void unlock_fdw_xact_entries();
static void remove_fdw_xact(FDWXact fdw_xact);
static FDWXact register_fdw_xact(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
				  Oid umid, char *fdw_xact_info);
static int	GetFDWXactList(FDWXact * fdw_xacts);
static ResolvePreparedForeignTransaction_function get_prepared_foreign_xact_resolver(FDWXact fdw_xact);
static FDWXactOnDiskData *ReadFDWXactFile(TransactionId xid, Oid serverid,
				Oid userid);
static void RemoveFDWXactFile(TransactionId xid, Oid serverid, Oid userid,
				  bool giveWarning);
static void RecreateFDWXactFile(TransactionId xid, Oid serverid, Oid userid,
					void *content, int len);
static void XlogReadFDWXactData(XLogRecPtr lsn, char **buf, int *len);
static void prepare_foreign_transactions(void);
bool search_fdw_xact(TransactionId xid, Oid dbid, Oid serverid, Oid userid,
				List **qualifying_xacts);

/*
 * Maximum number of foreign prepared transaction entries at any given time
 * GUC variable, change requires restart.
 */
int			max_prepared_foreign_xacts = 0;

/* Keep track of registering process exit call back. */
static bool fdwXactExitRegistered = false;

/* Pointer to the shared memory holding the foreign transactions data */
static FDWXactGlobalData *FDWXactGlobal;

/* foreign transaction entries locked by this backend */
List	   *MyLockedFDWXacts = NIL;

/*
 * FDWXactShmemSize
 * Calculates the size of shared memory allocated for maintaining foreign
 * prepared transaction entries.
 */
extern Size
FDWXactShmemSize(void)
{
	Size		size;

	/* Need the fixed struct, foreign transaction information array */
	size = offsetof(FDWXactGlobalData, fdw_xacts);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FDWXact)));
	size = MAXALIGN(size);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FDWXactData)));

	return size;
}

/*
 * FDWXactShmemInit
 * Initialization of shared memory for maintaining foreign prepared transaction
 * entries. The shared memory layout is defined in definition of
 * FDWXactGlobalData structure.
 */
extern void
FDWXactShmemInit(void)
{
	bool		found;

	FDWXactGlobal = ShmemInitStruct("Foreign transactions table",
									FDWXactShmemSize(),
									&found);
	if (!IsUnderPostmaster)
	{
		FDWXact		fdw_xacts;
		int			cnt;

		Assert(!found);
		FDWXactGlobal->freeFDWXacts = NULL;
		FDWXactGlobal->numFDWXacts = 0;

		/* Initialise the linked list of free FDW transactions */
		fdw_xacts = (FDWXact)
			((char *) FDWXactGlobal +
			 MAXALIGN(offsetof(FDWXactGlobalData, fdw_xacts) +
					  sizeof(FDWXact) * max_prepared_foreign_xacts));
		for (cnt = 0; cnt < max_prepared_foreign_xacts; cnt++)
		{
			fdw_xacts[cnt].fx_next = FDWXactGlobal->freeFDWXacts;
			FDWXactGlobal->freeFDWXacts = &fdw_xacts[cnt];
		}
	}
	else
	{
		Assert(FDWXactGlobal);
		Assert(found);
	}
}

/*
 * PreCommit_FDWXacts
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
PreCommit_FDWXacts(void)
{
	ListCell   *cur;
	ListCell   *prev;
	ListCell   *next;

	/* If there are no foreign servers involved, we have no business here */
	if (list_length(MyFDWConnections) < 1)
		return;

	/*
	 * Try committing transactions on the foreign servers, which can not
	 * execute two-phase-commit protocol.
	 */
	for (cur = list_head(MyFDWConnections), prev = NULL; cur; cur = next)
	{
		FDWConnection *fdw_conn = lfirst(cur);

		next = lnext(cur);

		if (!fdw_conn->two_phase_commit)
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
			MyFDWConnections = list_delete_cell(MyFDWConnections, cur, prev);
		}
		else
			prev = cur;
	}

	/*
	 * Here foreign servers that can not execute two-phase-commit protocol
	 * already commit the transaction and MyFDWConnections has only foreign
	 * servers that can execute two-phase-commit protocol. We don't need to
	 * use two-phase-commit protocol if there is only one foreign server that
	 * that can execute two-phase-commit and didn't write no local node.
	 */
	if ((list_length(MyFDWConnections) > 1) ||
		(list_length(MyFDWConnections) == 1 && XactWriteLocalNode))
	{
		/*
		 * Prepare the transactions on the all foreign servers, which can
		 * execute two-phase-commit protocol.
		 */
		prepare_foreign_transactions();
	}
	else if (list_length(MyFDWConnections) == 1)
	{
		FDWConnection *fdw_conn = lfirst(list_head(MyFDWConnections));

		/*
		 * We don't need to use two-phase commit protocol only one server
		 * remaining even if this server can execute two-phase-commit
		 * protocol.
		 */
		if (!fdw_conn->end_foreign_xact(fdw_conn->serverid, fdw_conn->userid,
										fdw_conn->umid, true))
			elog(WARNING, "could not commit transaction on server %s",
				 fdw_conn->servername);

		/* MyFDWConnections should be cleared here */
		MyFDWConnections = list_delete_cell(MyFDWConnections, cur, prev);
	}
}

/*
 * prepare_foreign_transactions
 *
 * Prepare transactions on the foreign servers which can execute two phase
 * commit protocol. Rest of the foreign servers are ignored.
 */
static void
prepare_foreign_transactions(void)
{
	ListCell   *lcell;

	/*
	 * Loop over the foreign connections
	 */
	foreach(lcell, MyFDWConnections)
	{
		FDWConnection *fdw_conn = (FDWConnection *) lfirst(lcell);
		char	    fdw_xact_id[FDW_XACT_ID_LEN];
		FDWXact		fdw_xact;

		if (!fdw_conn->two_phase_commit)
			continue;

		/* Generate prepare transaction id for foreign server */
		FDWXactId(fdw_xact_id, "px", GetTopTransactionId(),
				  fdw_conn->serverid, fdw_conn->userid);

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
		fdw_xact = register_fdw_xact(MyDatabaseId, GetTopTransactionId(),
									 fdw_conn->serverid, fdw_conn->userid,
									 fdw_conn->umid, fdw_xact_id);

		/*
		 * Between register_fdw_xact call above till this backend hears back
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
			MyFDWConnections = list_delete_ptr(MyFDWConnections, fdw_conn);
			ereport(ERROR,
				  (errmsg("can not prepare transaction on foreign server %s",
						  fdw_conn->servername)));
		}

		/* Prepare succeeded, remember it in the connection */
		fdw_conn->fdw_xact = fdw_xact;
	}
	return;
}

/*
 * register_fdw_xact
 *
 * This function is used to create new foreign transaction entry before an FDW
 * executes the first phase of two-phase commit. The function adds the entry to
 * WAL and will be persisted to the disk under pg_fdw_xact directory when checkpoint.
 */
static FDWXact
register_fdw_xact(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
				  Oid umid, char *fdw_xact_id)
{
	FDWXact		fdw_xact;
	FDWXactOnDiskData *fdw_xact_file_data;
	int			data_len;

	/* Enter the foreign transaction in the shared memory structure */
	fdw_xact = insert_fdw_xact(dbid, xid, serverid, userid, umid,
							   fdw_xact_id);
	fdw_xact->status = FDW_XACT_PREPARING;
	fdw_xact->locking_backend = MyBackendId;

	/* Remember that we have locked this entry. */
	MyLockedFDWXacts = lappend(MyLockedFDWXacts, fdw_xact);

	/*
	 * Prepare to write the entry to a file. Also add xlog entry. The contents
	 * of the xlog record are same as what is written to the file.
	 */
	data_len = offsetof(FDWXactOnDiskData, fdw_xact_id);
	data_len = data_len + FDW_XACT_ID_LEN;
	data_len = MAXALIGN(data_len);
	fdw_xact_file_data = (FDWXactOnDiskData *) palloc0(data_len);
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
 * id, foreign server and user mapping, in the shared memory.
 *
 * If the entry already exists, the function raises an error.
 */
static FDWXact
insert_fdw_xact(Oid dboid, TransactionId xid, Oid serverid, Oid userid, Oid umid,
				char *fdw_xact_id)
{
	FDWXact		fdw_xact;
	int			cnt;

	if (!fdwXactExitRegistered)
	{
		before_shmem_exit(AtProcExit_FDWXact, 0);
		fdwXactExitRegistered = true;
	}

	LWLockAcquire(FDWXactLock, LW_EXCLUSIVE);
	fdw_xact = NULL;
	for (cnt = 0; cnt < FDWXactGlobal->numFDWXacts; cnt++)
	{
		fdw_xact = FDWXactGlobal->fdw_xacts[cnt];

		if (fdw_xact->local_xid == xid &&
			fdw_xact->serverid == serverid &&
			fdw_xact->userid == userid)
			elog(ERROR, "duplicate entry for foreign transaction with transaction id %u, serverid %u, userid %u found",
				 xid, serverid, userid);
	}

	/*
	 * Get the next free foreign transaction entry. Raise error if there are
	 * none left.
	 */
	if (!FDWXactGlobal->freeFDWXacts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("maximum number of foreign transactions reached"),
		errhint("Increase max_prepared_foreign_transactions (currently %d).",
				max_prepared_foreign_xacts)));
	}

	fdw_xact = FDWXactGlobal->freeFDWXacts;
	FDWXactGlobal->freeFDWXacts = fdw_xact->fx_next;

	/* Insert the entry to active array */
	Assert(FDWXactGlobal->numFDWXacts < max_prepared_foreign_xacts);
	FDWXactGlobal->fdw_xacts[FDWXactGlobal->numFDWXacts++] = fdw_xact;

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

	LWLockRelease(FDWXactLock);

	return fdw_xact;
}

/*
 * remove_fdw_xact
 *
 * Removes the foreign prepared transaction entry from shared memory, disk and
 * logs about the removal in WAL.
 */
static void
remove_fdw_xact(FDWXact fdw_xact)
{
	int			cnt;

	LWLockAcquire(FDWXactLock, LW_EXCLUSIVE);
	/* Search the slot where this entry resided */
	for (cnt = 0; cnt < FDWXactGlobal->numFDWXacts; cnt++)
	{
		if (FDWXactGlobal->fdw_xacts[cnt] == fdw_xact)
		{
			/* Remove the entry from active array */
			FDWXactGlobal->numFDWXacts--;
			FDWXactGlobal->fdw_xacts[cnt] = FDWXactGlobal->fdw_xacts[FDWXactGlobal->numFDWXacts];

			/* Put it back into free list */
			fdw_xact->fx_next = FDWXactGlobal->freeFDWXacts;
			FDWXactGlobal->freeFDWXacts = fdw_xact;

			/* Unlock the entry */
			fdw_xact->locking_backend = InvalidBackendId;
			MyLockedFDWXacts = list_delete_ptr(MyLockedFDWXacts, fdw_xact);

			LWLockRelease(FDWXactLock);

			if (RecoveryInProgress())
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
				RemoveFDWXactFile(fdw_xact->local_xid, fdw_xact->serverid,
								  fdw_xact->userid, true);
			return;
		}
	}
	LWLockRelease(FDWXactLock);

	/* We did not find the given entry in global array */
	elog(ERROR, "failed to find %p in FDWXactGlobal array", fdw_xact);
}

/*
 * unlock_fdw_xact
 *
 * Unlock the foreign transaction entry by wiping out the locking_backend and
 * removing it from the backend's list of foreign transaction.
 */
static void
unlock_fdw_xact(FDWXact fdw_xact)
{
	/* Only the backend holding the lock is allowed to unlock */
	Assert(fdw_xact->locking_backend == MyBackendId);

	/*
	 * First set the locking backend as invalid, and then remove it from the
	 * list of locked foreign transactions, under the LW lock. If we reverse
	 * the order and process exits in-between those two, we will be left an
	 * entry locked by this backend, which gets unlocked only at the server
	 * restart.
	 */

	LWLockAcquire(FDWXactLock, LW_EXCLUSIVE);
	fdw_xact->locking_backend = InvalidBackendId;
	MyLockedFDWXacts = list_delete_ptr(MyLockedFDWXacts, fdw_xact);
	LWLockRelease(FDWXactLock);
}

/*
 * unlock_fdw_xact_entries
 *
 * Unlock the foreign transaction entries locked by this backend.
 */
static void
unlock_fdw_xact_entries()
{
	while (MyLockedFDWXacts)
	{
		FDWXact		fdw_xact = (FDWXact) linitial(MyLockedFDWXacts);

		unlock_fdw_xact(fdw_xact);
	}
}

/*
 * AtProcExit_FDWXact
 *
 * When the process exits, unlock the entries it held.
 */
static void
AtProcExit_FDWXact(int code, Datum arg)
{
	unlock_fdw_xact_entries();
}

/*
 * AtEOXact_FDWXacts
 *
 * The function executes phase 2 of two-phase commit protocol.
 * At the end of transaction perform following actions
 * 1. Mark the entries locked by this backend as ABORTING or COMMITTING
 *	  according the result of transaction.
 * 2. Try to commit or abort the transactions on foreign servers. If that
 *	  succeeds, remove them from foreign transaction entries, otherwise unlock
 *	  them.
 */
extern void
AtEOXact_FDWXacts(bool is_commit)
{
	ListCell   *lcell;

	foreach(lcell, MyFDWConnections)
	{
		FDWConnection *fdw_conn = lfirst(lcell);

		/* Commit/abort prepared foreign transactions */
		if (fdw_conn->fdw_xact)
		{
			FDWXact		fdw_xact = fdw_conn->fdw_xact;

			fdw_xact->status = (is_commit ?
										 FDW_XACT_COMMITTING_PREPARED :
										 FDW_XACT_ABORTING_PREPARED);

			/*
			 * Try aborting or committing the transaction on the foreign
			 * server
			 */
			if (!resolve_fdw_xact(fdw_xact, fdw_conn->resolve_prepared_foreign_xact))
			{
				/*
				 * The transaction was not resolved on the foreign server,
				 * unlock it, so that someone else can take care of it.
				 */
				unlock_fdw_xact(fdw_xact);
			}
		}
		else
		{
			/*
			 * On servers where two phase commit protocol could not be
			 * executed we have tried to commit the transactions during
			 * pre-commit phase. Any remaining transactions need to be
			 * aborted.
			 */
			Assert(!is_commit);

			/*
			 * The FDW has to make sure that the connection opened to the
			 * foreign server is out of transaction. Even if the handler
			 * function returns failure statue, there's hardly anything to do.
			 */
			if (!fdw_conn->end_foreign_xact(fdw_conn->serverid, fdw_conn->userid,
											fdw_conn->umid, is_commit))
				elog(WARNING, "could not %s transaction on server %s",
					 is_commit ? "commit" : "abort",
					 fdw_conn->servername);

		}
	}

	/*
	 * Unlock any locked foreign transactions. Resolver might lock the
	 * entries, and may not be able to unlock them if aborted in-between. In
	 * any case, there is no reason for a foreign transaction entry to be
	 * locked after the transaction which locked it has ended.
	 */
	unlock_fdw_xact_entries();

	/*
	 * Reset the list of registered connections. Since the memory for the list
	 * and its nodes comes from transaction memory context, it will be freed
	 * after this call.
	 */
	MyFDWConnections = NIL;
	/* Set TwoPhaseReady to its default value */
	TwoPhaseReady = true;
}

/*
 * AtPrepare_FDWXacts
 *
 * The function is called while preparing a transaction. If there are foreign
 * servers involved in the transaction, this function prepares transactions
 * on those servers.
 */
extern void
AtPrepare_FDWXacts(void)
{
	/* If there are no foreign servers involved, we have no business here */
	if (list_length(MyFDWConnections) < 1)
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
	prepare_foreign_transactions();

	/*
	 * Unlock the foreign transaction entries so COMMIT/ROLLBACK PREPARED from
	 * some other backend will be able to lock those if required.
	 */
	unlock_fdw_xact_entries();

	/*
	 * Reset the list of registered connections. Since the memory for the list
	 * and its nodes comes from transaction memory context, it will be freed
	 * after this call.
	 */
	MyFDWConnections = NIL;

	/* Set TwoPhaseReady to its default value */
	TwoPhaseReady = true;
}

/*
 * FDWXactTwoPhaseFinish
 *
 * This function is called as part of the COMMIT/ROLLBACK PREPARED command to
 * commit/rollback the foreign transactions prepared as part of the local
 * prepared transaction. The function looks for the foreign transaction entries
 * with local_xid equal to xid of the prepared transaction and tries to resolve them.
 */
extern void
FDWXactTwoPhaseFinish(bool isCommit, TransactionId xid)
{
	List	   *entries_to_resolve;

	FDWXactStatus status = isCommit ? FDW_XACT_COMMITTING_PREPARED :
	FDW_XACT_ABORTING_PREPARED;

	/*
	 * Get all the entries belonging to the given transaction id locked. If
	 * foreign transaction resolver is running, it might lock entries to check
	 * whether they can be resolved. The search function will skip such
	 * entries. The resolver will resolve them at a later point of time.
	 */
	search_fdw_xact(xid, InvalidOid, InvalidOid, InvalidOid, &entries_to_resolve);

	/* Try resolving the foreign transactions */
	while (entries_to_resolve)
	{
		FDWXact		fdw_xact = linitial(entries_to_resolve);

		entries_to_resolve = list_delete_first(entries_to_resolve);
		fdw_xact->status = status;

		/*
		 * Resolve the foreign transaction. If resolution is not successful,
		 * unlock the entry so that someone else can pick it up.
		 */
		if (!resolve_fdw_xact(fdw_xact,
							  get_prepared_foreign_xact_resolver(fdw_xact)))
			unlock_fdw_xact(fdw_xact);
	}
}

/*
 * get_prepared_foreign_xact_resolver
 */
static ResolvePreparedForeignTransaction_function
get_prepared_foreign_xact_resolver(FDWXact fdw_xact)
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
 * resolve_fdw_xact
 *
 * Resolve the foreign transaction using the foreign data wrapper's transaction
 * handler routine.
 * If the resolution is successful, remove the foreign transaction entry from
 * the shared memory and also remove the corresponding on-disk file.
 */
static bool
resolve_fdw_xact(FDWXact fdw_xact,
				 ResolvePreparedForeignTransaction_function fdw_xact_handler)
{
	bool		resolved;
	bool		is_commit;

	Assert(fdw_xact->status == FDW_XACT_COMMITTING_PREPARED ||
		   fdw_xact->status == FDW_XACT_ABORTING_PREPARED);

	is_commit = (fdw_xact->status == FDW_XACT_COMMITTING_PREPARED) ?
		true : false;

	resolved = fdw_xact_handler(fdw_xact->serverid, fdw_xact->userid,
								fdw_xact->umid, is_commit,
								fdw_xact->fdw_xact_id);

	/* If we succeeded in resolving the transaction, remove the entry */
	if (resolved)
		remove_fdw_xact(fdw_xact);

	return resolved;
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
bool
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

	LWLockAcquire(FDWXactLock, lock_mode);
	for (cnt = 0; cnt < FDWXactGlobal->numFDWXacts; cnt++)
	{
		FDWXact		fdw_xact = FDWXactGlobal->fdw_xacts[cnt];
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
					MyLockedFDWXacts = lappend(MyLockedFDWXacts, fdw_xact);
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

	LWLockRelease(FDWXactLock);

	return entry_exists;
}

/*
 * fdw_xact_redo
 * Apply the redo log for a foreign transaction.
 */
extern void
fdw_xact_redo(XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_FDW_XACT_INSERT)
		FDWXactRedoAdd(record);
	else if (info == XLOG_FDW_XACT_REMOVE)
	{
		FdwRemoveXlogRec *fdw_remove_xlog = (FdwRemoveXlogRec *) rec;

		/* Delete FDWXact entry and file if exists */
		FDWXactRedoRemove(fdw_remove_xlog->xid, fdw_remove_xlog->serverid,
						  fdw_remove_xlog->userid);
	}
	else
		elog(ERROR, "invalid log type %d in foreign transction log record", info);

	return;
}

/*
 * CheckPointFDWXact
 *
 * Function syncs the foreign transaction files created between the two
 * checkpoints. The foreign transaction entries and hence the corresponding
 * files are expected to be very short-lived. By executing this function at the
 * end, we might have lesser files to fsync, thus reducing some I/O. This is
 * similar to CheckPointTwoPhase().
 *
 * In order to avoid disk I/O while holding a light weight lock, the function
 * first collects the files which need to be synced under FDWXactLock and then
 * syncs them after releasing the lock. This approach creates a race condition:
 * after releasing the lock, and before syncing a file, the corresponding
 * foreign transaction entry and hence the file might get removed. The function
 * checks whether that's true and ignores the error if so.
 */
void
CheckPointFDWXact(XLogRecPtr redo_horizon)
{
	int			cnt;
	int			serialized_fdw_xacts = 0;

	/* Quick get-away, before taking lock */
	if (max_prepared_foreign_xacts <= 0)
		return;

	TRACE_POSTGRESQL_FDWXACT_CHECKPOINT_START();

	LWLockAcquire(FDWXactLock, LW_SHARED);

	/* Another quick, before we allocate memory */
	if (FDWXactGlobal->numFDWXacts <= 0)
	{
		LWLockRelease(FDWXactLock);
		return;
	}

	/*
	 * We are expecting there to be zero FDWXact that need to be copied to
	 * disk, so we perform all I/O while holding FDWXactLock for simplicity.
	 * This presents any new foreign xacts from preparing while this occurs,
	 * which shouldn't be a problem since the presence fo long-lived prepared
	 * foreign xacts indicated the transaction manager isn't active.
	 *
	 * it's also possible to move I/O out of the lock, but on every error we
	 * should check whether somebody committed our transaction in different
	 * backend. Let's leave this optimisation for future, if somebody will
	 * spot that this place cause bottleneck.
	 *
	 * Note that it isn't possible for there to be a FDWXact with a
	 * fdw_xact_end_lsn set prior to the last checkpoint yet is marked
	 * invalid, because of the efforts with delayChkpt.
	 */
	for (cnt = 0; cnt < FDWXactGlobal->numFDWXacts; cnt++)
	{
		FDWXact		fdw_xact = FDWXactGlobal->fdw_xacts[cnt];

		if ((fdw_xact->valid || fdw_xact->inredo) &&
			!fdw_xact->ondisk &&
			fdw_xact->fdw_xact_end_lsn <= redo_horizon)
		{
			char	   *buf;
			int			len;

			XlogReadFDWXactData(fdw_xact->fdw_xact_start_lsn, &buf, &len);
			RecreateFDWXactFile(fdw_xact->local_xid, fdw_xact->serverid,
								fdw_xact->userid, buf, len);
			fdw_xact->ondisk = true;
			serialized_fdw_xacts++;
			pfree(buf);
		}
	}

	LWLockRelease(FDWXactLock);

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
 * be moved to fdwxact files and ReadFDWXactFile should be used instead.
 *
 * Note clearly that this function accesses WAL during normal operation, similarly
 * to the way WALSender or Logical Decoding would do. It does not run during
 * crash recovery or standby processing.
 */
static void
XlogReadFDWXactData(XLogRecPtr lsn, char **buf, int *len)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;

	xlogreader = XLogReaderAllocate(&read_local_xlog_page, NULL);
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
RecreateFDWXactFile(TransactionId xid, Oid serverid, Oid userid,
					void *content, int len)
{
	char		path[MAXPGPATH];
	pg_crc32c	fdw_xact_crc;
	pg_crc32c	bogus_crc;
	int			fd;

	/* Recompute CRC */
	INIT_CRC32C(fdw_xact_crc);
	COMP_CRC32C(fdw_xact_crc, content, len);

	FDWXactFilePath(path, xid, serverid, userid);

	fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY,
						   S_IRUSR | S_IWUSR);

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
	FDWXact		fdw_xacts;
	int			num_xacts;
	int			cur_xact;
}	WorkingStatus;

/*
 * pg_fdw_xact
 *		Produce a view with one row per prepared transaction on foreign server.
 *
 * This function is here so we don't have to export the
 * FDWXactGlobalData struct definition.
 *
 */
Datum
pg_fdw_xacts(PG_FUNCTION_ARGS)
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

		status->num_xacts = GetFDWXactList(&status->fdw_xacts);
		status->cur_xact = 0;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	status = funcctx->user_fctx;

	while (status->cur_xact < status->num_xacts)
	{
		FDWXact		fdw_xact = &status->fdw_xacts[status->cur_xact++];
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
 * structures, to minimize the time we need to hold the FDWXactLock.
 *
 * WARNING -- we return even those transactions whose information is not
 * completely filled yet. The caller should filter them out if he doesn't want them.
 *
 * The returned array is palloc'd.
 */
static int
GetFDWXactList(FDWXact * fdw_xacts)
{
	int			num_xacts;
	int			cnt_xacts;

	LWLockAcquire(FDWXactLock, LW_SHARED);

	if (FDWXactGlobal->numFDWXacts == 0)
	{
		LWLockRelease(FDWXactLock);
		*fdw_xacts = NULL;
		return 0;
	}

	num_xacts = FDWXactGlobal->numFDWXacts;
	*fdw_xacts = (FDWXact) palloc(sizeof(FDWXactData) * num_xacts);
	for (cnt_xacts = 0; cnt_xacts < num_xacts; cnt_xacts++)
		memcpy((*fdw_xacts) + cnt_xacts, FDWXactGlobal->fdw_xacts[cnt_xacts],
			   sizeof(FDWXactData));

	LWLockRelease(FDWXactLock);

	return num_xacts;
}

/*
 * pg_fdw_xact_resolve
 * a user interface to initiate foreign transaction resolution. The function
 * tries to resolve the prepared transactions on foreign servers in the database
 * from where it is run.
 * The function prints the status of all the foreign transactions it
 * encountered, whether resolved or not.
 */
Datum
pg_fdw_xact_resolve(PG_FUNCTION_ARGS)
{
	MemoryContext oldcontext;
	FuncCallContext *funcctx;
	WorkingStatus *status;
	char	   *xact_status;
	List	   *entries_to_resolve;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;

		/* We will be modifying the shared memory. Prepare to clean up on exit */
		if (!fdwXactExitRegistered)
		{
			before_shmem_exit(AtProcExit_FDWXact, 0);
			fdwXactExitRegistered = true;
		}

		/* Allocate space for and prepare the returning set */
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		/* Switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
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
		status->fdw_xacts = (FDWXact) palloc(sizeof(FDWXactData) * FDWXactGlobal->numFDWXacts);
		status->num_xacts = 0;
		status->cur_xact = 0;

		/* Done preparation for the result. */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Get entries whose foreign servers are part of the database where
		 * this function was called. We can get information about only such
		 * foreign servers. The function will lock the entries. The entries
		 * which are locked by other backends and whose foreign servers belong
		 * to this database are left out, since we can not work on those.
		 */
		search_fdw_xact(InvalidTransactionId, MyDatabaseId, InvalidOid, InvalidOid,
						&entries_to_resolve);

		/* Work to resolve the resolvable entries */
		while (entries_to_resolve)
		{
			FDWXact		fdw_xact = linitial(entries_to_resolve);

			/* Remove the entry as we will not use it again */
			entries_to_resolve = list_delete_first(entries_to_resolve);

			/* Copy the data for the sake of result. */
			memcpy(status->fdw_xacts + status->num_xacts++,
				   fdw_xact, sizeof(FDWXactData));

			if (fdw_xact->status == FDW_XACT_COMMITTING_PREPARED ||
				fdw_xact->status == FDW_XACT_ABORTING_PREPARED)
			{
				/*
				 * We have already decided what to do with the foreign
				 * transaction nothing to be done.
				 */
			}
			else if (TransactionIdDidCommit(fdw_xact->local_xid))
				fdw_xact->status = FDW_XACT_COMMITTING_PREPARED;
			else if (TransactionIdDidAbort(fdw_xact->local_xid))
				fdw_xact->status = FDW_XACT_ABORTING_PREPARED;
			else if (!TransactionIdIsInProgress(fdw_xact->local_xid))
			{
				/*
				 * The transaction is in progress but not on any of the
				 * backends. So probably, it crashed before actual abort or
				 * commit. So assume it to be aborted.
				 */
				fdw_xact->status = FDW_XACT_ABORTING_PREPARED;
			}
			else
			{
				/*
				 * Local transaction is in progress, should not resolve the
				 * foreign transaction. This can happen when the foreign
				 * transaction is prepared as part of a local prepared
				 * transaction. Just continue with the next one.
				 */
				unlock_fdw_xact(fdw_xact);
				continue;
			}

			/*
			 * Resolve the foreign transaction. If resolution was not
			 * successful, unlock the entry so that someone else can pick it
			 * up
			 */
			if (!resolve_fdw_xact(fdw_xact, get_prepared_foreign_xact_resolver(fdw_xact)))
				unlock_fdw_xact(fdw_xact);
			else
				/* Update the status in the result set */
				status->fdw_xacts[status->num_xacts - 1].status = FDW_XACT_RESOLVED;
		}
	}

	/* Print the result set */
	funcctx = SRF_PERCALL_SETUP();
	status = funcctx->user_fctx;

	while (status->cur_xact < status->num_xacts)
	{
		FDWXact		fdw_xact = &status->fdw_xacts[status->cur_xact++];
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
				xact_status = "preparing";
				break;
			case FDW_XACT_COMMITTING_PREPARED:
				xact_status = "committing";
				break;
			case FDW_XACT_ABORTING_PREPARED:
				xact_status = "aborting";
				break;
			case FDW_XACT_RESOLVED:
				xact_status = "resolved";
				break;
			default:
				xact_status = "unknown";
				break;
		}
		values[4] = CStringGetTextDatum(xact_status);
		/* should this be really interpreted by FDW? */
		values[5] = PointerGetDatum(cstring_to_text_with_len(fdw_xact->fdw_xact_id,
															 FDW_XACT_ID_LEN));

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
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
pg_fdw_xact_remove(PG_FUNCTION_ARGS)
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
		FDWXact		fdw_xact = linitial(entries_to_remove);

		entries_to_remove = list_delete_first(entries_to_remove);

		remove_fdw_xact(fdw_xact);
	}

	PG_RETURN_VOID();
}

/*
 * Code dealing with the on disk files used to store foreign transaction
 * information.
 */

/*
 * ReadFDWXactFile
 * Read the foreign transction state file and return the contents in a
 * structure allocated in-memory. The structure can be later freed by the
 * caller.
 */
static FDWXactOnDiskData *
ReadFDWXactFile(TransactionId xid, Oid serverid, Oid userid)
{
	char		path[MAXPGPATH];
	int			fd;
	FDWXactOnDiskData *fdw_xact_file_data;
	struct stat stat;
	uint32		crc_offset;
	pg_crc32c	calc_crc;
	pg_crc32c	file_crc;
	char	   *buf;

	FDWXactFilePath(path, xid, serverid, userid);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
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

	if (stat.st_size < offsetof(FDWXactOnDiskData, fdw_xact_id) ||
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
	fdw_xact_file_data = (FDWXactOnDiskData *) buf;
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
 * PrescanFDWXacts
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
PrescanFDWXacts(TransactionId oldestActiveXid)
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
				RemoveFDWXactFile(local_xid, serverid, userid, true);
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
 * RecoverFDWXactFromFiles
 * Read the foreign prepared transaction information and set it up for further
 * usage.
 */
void
RecoverFDWXactFromFiles(void)
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
			FDWXactOnDiskData *fdw_xact_file_data;
			FDWXact		fdw_xact;

			sscanf(clde->d_name, "%08x_%08x_%08x", &local_xid, &serverid,
				   &userid);

			fdw_xact_file_data = ReadFDWXactFile(local_xid, serverid, userid);

			if (!fdw_xact_file_data)
			{
				ereport(WARNING,
				  (errmsg("Removing corrupt foreign transaction file \"%s\"",
						  clde->d_name)));
				RemoveFDWXactFile(local_xid, serverid, userid, false);
				continue;
			}

			ereport(LOG,
					(errmsg("recovering foreign transaction entry for xid %u, foreign server %u and user %u",
							local_xid, serverid, userid)));

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
			fdw_xact->valid = false;
			fdw_xact->ondisk = false;
			fdw_xact->inredo = false;
			fdw_xact->status = FDW_XACT_PREPARING;

			/* Remember that we have locked this entry. */
			MyLockedFDWXacts = lappend(MyLockedFDWXacts, fdw_xact);

			/* Add some valid LSNs */
			fdw_xact->fdw_xact_start_lsn = 0;
			fdw_xact->fdw_xact_end_lsn = 0;
			/* Mark the entry as ready */
			fdw_xact->valid = true;
			/* Already synced to disk */
			fdw_xact->ondisk = true;
			/* Unlock the entry as we don't need it any further */
			unlock_fdw_xact(fdw_xact);
			pfree(fdw_xact_file_data);
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
RemoveFDWXactFile(TransactionId xid, Oid serverid, Oid userid, bool giveWarning)
{
	char		path[MAXPGPATH];

	FDWXactFilePath(path, xid, serverid, userid);
	if (unlink(path))
		if (errno != ENOENT || giveWarning)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove foreign transaction state file \"%s\": %m",
							path)));
}

/*
 * FDWXactRedoAdd
 *
 * Store pointer to the start/end of the WAL record along with the xid in
 * a fdw_xact entry in shared memory FDWXactData structure.
 */
void
FDWXactRedoAdd(XLogReaderState *record)
{
	FDWXactOnDiskData *fdw_xact_data = (FDWXactOnDiskData *) XLogRecGetData(record);
	FDWXact fdw_xact;

	Assert(RecoveryInProgress());

	fdw_xact = insert_fdw_xact(fdw_xact_data->dboid, fdw_xact_data->local_xid,
							   fdw_xact_data->serverid, fdw_xact_data->userid,
							   fdw_xact_data->umid, fdw_xact_data->fdw_xact_id);
	fdw_xact->status = FDW_XACT_PREPARING;
	fdw_xact->fdw_xact_start_lsn = record->ReadRecPtr;
	fdw_xact->fdw_xact_end_lsn = record->EndRecPtr;
	fdw_xact->inredo = true;
}
/*
 * FDWXactRedoRemove
 *
 * Remove the corresponding fdw_xact entry from FDWXactGlobal.
 * Also remove fdw_xact file if a foreign transaction was saved
 * via an earlier chechpoint.
 */
void
FDWXactRedoRemove(TransactionId xid, Oid serverid, Oid userid)
{
	bool	found = false;
	int		i;
	FDWXact	fdw_xact;

	Assert(RecoveryInProgress());

	LWLockAcquire(FDWXactLock, LW_SHARED);
	for (i = 0; i < FDWXactGlobal->numFDWXacts; i++)
	{
		fdw_xact = FDWXactGlobal->fdw_xacts[i];

		if (fdw_xact->local_xid == xid &&
			fdw_xact->serverid == serverid &&
			fdw_xact->userid == userid)
		{
			Assert(fdw_xact->inredo);
			found = true;
			break;
		}
	}

	LWLockRelease(FDWXactLock);

	if (found)
	{
		/* Now we can clean up any files we already left */
		remove_fdw_xact(fdw_xact);
	}
	else
	{
		/*
		 * Entry could be on disk. Call with giveWarning = false
		 * since it can be expected during replay.
		 */
		RemoveFDWXactFile(fdw_xact->local_xid, fdw_xact->serverid,
						  fdw_xact->userid, false);
	}
}
