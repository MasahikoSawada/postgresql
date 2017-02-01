/* -------------------------------------------------------------------------
 *
 * pg_fdw_xact_resolver.c
 *
 * Contrib module to launch foreign transaction resolver to resolve unresolved
 * transactions prepared on foreign servers.
 *
 * The extension launches foreign transaction resolver launcher process as a
 * background worker. The launcher then launches separate background worker
 * process to resolve the foreign transaction in each database. The worker
 * process simply connects to the database specified and calls pg_fdw_xact_resolve()
 * function, which tries to resolve the transactions. The launcher process
 * launches at most one worker at a time.
 *
 * Copyright (C) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pg_fdw_xact_resolver/pg_fdw_xact_resolver.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/fdw_xact.h"
#include "catalog/pg_database.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/*
 * Flags set by interrupt handlers of foreign transaction resolver for later
 * service in the main loop.
 */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigquit = false;
static volatile sig_atomic_t got_sigusr1 = false;

static void FDWXactResolver_worker_main(Datum dbid_datum);
static void FDWXactResolverMain(Datum main_arg);
static List *get_database_list(void);

/* GUC variable */
static int fx_resolver_naptime;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
FDWXactResolver_SIGTERM(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGQUIT
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
FDWXactResolver_SIGQUIT(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigquit = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
FDWXactResolver_SIGHUP(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
FDWXactResolver_SIGUSR1(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigusr1 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Entrypoint of this module.
 *
 * Launches the foreign transaction resolver demon.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_fdw_xact_resolver.naptime",
							"Time to sleep between pg_fdw_xact_resolver runs.",
							NULL,
							&fx_resolver_naptime,
							60,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	/* set up common data for all our workers */
	/*
	 * For some reason unless background worker set
	 * BGWORKER_BACKEND_DATABASE_CONNECTION, it's not added to BackendList and
	 * hence notification to this backend is not enabled. So set that flag even
	 * if the backend itself doesn't need database connection.
	 */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_name, BGW_MAXLEN, "foreign transaction resolver launcher");
	worker.bgw_main = FDWXactResolverMain;
	worker.bgw_main_arg = (Datum) 0;/* Craft some dummy arg. */
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

void
FDWXactResolverMain(Datum main_arg)
{
	/* For launching background worker */
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;
	pid_t		pid;
	List	*dbid_list = NIL;
	TimestampTz launched_time = GetCurrentTimestamp();
	TimestampTz next_launch_time = launched_time + (fx_resolver_naptime * 1000L);

	ereport(LOG,
			(errmsg("pg_fdw_xact_resolver launcher started")));

	/* Properly accept or ignore signals the postmaster might send us */
	pqsignal(SIGHUP, FDWXactResolver_SIGHUP);		/* set flag to read config
												 * file */
	pqsignal(SIGTERM, FDWXactResolver_SIGTERM);	/* request shutdown */
	pqsignal(SIGQUIT, FDWXactResolver_SIGQUIT);	/* hard crash time */
	pqsignal(SIGUSR1, FDWXactResolver_SIGUSR1);

	/* Unblock signals */
	BackgroundWorkerUnblockSignals();

	/* Initialize connection */
	BackgroundWorkerInitializeConnection(NULL, NULL);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int		rc;
		int naptime_msec;
		TimestampTz current_time = GetCurrentTimestamp();

		/* Determine sleep time */
		naptime_msec = (fx_resolver_naptime * 1000L) - (current_time - launched_time);

		if (naptime_msec < 0)
			naptime_msec = 0;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   naptime_msec,
					   WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * Postmaster wants to stop this process. Exit with non-zero code, so
		 * that the postmaster starts this process again. The worker processes
		 * will receive the signal and end themselves. This process will restart
		 * them if necessary.
		 */
		if (got_sigquit)
			proc_exit(2);

		/* In case of a SIGHUP, just reload the configuration */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (got_sigusr1)
		{
			got_sigusr1 = false;

			/* If we had started a worker check whether it completed */
			if (handle)
			{
				BgwHandleStatus status;

				status = GetBackgroundWorkerPid(handle, &pid);
				if (status == BGWH_STOPPED)
					handle = NULL;
			}
		}

		current_time = GetCurrentTimestamp();

		/*
		 * If no background worker is running, we can start one if there are
		 * unresolved foreign transactions.
		 */
		if (!handle &&
			TimestampDifferenceExceeds(next_launch_time, current_time, naptime_msec))
		{
			Oid dbid;

			/* Get the database list if empty*/
			if (!dbid_list)
				dbid_list = get_database_list();

			/* Launch a worker if dbid_list has database */
			if (dbid_list)
			{
				/* Work on the first dbid, and remove it from the list */
				dbid = linitial_oid(dbid_list);
				dbid_list = list_delete_oid(dbid_list, dbid);

				Assert(OidIsValid(dbid));

				/* Start the foreign transaction resolver */
				worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					BGWORKER_BACKEND_DATABASE_CONNECTION;
				worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

				/* We will start another worker if needed */
				worker.bgw_restart_time = BGW_NEVER_RESTART;
				worker.bgw_main = FDWXactResolver_worker_main;
				snprintf(worker.bgw_name, BGW_MAXLEN, "foreign transaction resolver (dbid %u)", dbid);
				worker.bgw_main_arg = ObjectIdGetDatum(dbid);

				/* set bgw_notify_pid so that we can wait for it to finish */
				worker.bgw_notify_pid = MyProcPid;

				RegisterDynamicBackgroundWorker(&worker, &handle);
			}

			/* Set next launch time */
			launched_time = current_time;
			next_launch_time = TimestampTzPlusMilliseconds(launched_time,
														   fx_resolver_naptime * 1000L);
		}
	}

	/* Time to exit */
	ereport(LOG,
			(errmsg("foreign transaction resolver shutting down")));

	proc_exit(0);				/* done */
}

/* FDWXactWorker_SIGTERM
 * Terminates the foreign transaction resolver worker process */
static void
FDWXactWorker_SIGTERM(SIGNAL_ARGS)
{
	/* Just terminate the current process */
	proc_exit(1);
}

/* Per database foreign transaction resolver */
static void
FDWXactResolver_worker_main(Datum dbid_datum)
{
	char	*command = "SELECT * FROM pg_fdw_xact_resolve() WHERE status = 'resolved'";
	Oid		dbid = DatumGetObjectId(dbid_datum);
	int		ret;

	/*
	 * This background worker does not loop infinitely, so we need handler only
	 * for SIGTERM, in which case the process should just exit quickly.
	 */
	pqsignal(SIGTERM, FDWXactWorker_SIGTERM);
	pqsignal(SIGQUIT, FDWXactWorker_SIGTERM);

	/* Unblock signals */
	BackgroundWorkerUnblockSignals();

	/*
	 * Run this background worker in superuser mode, so that all the foreign
	 * server and user information isaccessible.
	 */
	BackgroundWorkerInitializeConnectionByOid(dbid, InvalidOid);

	/*
	 * Start a transaction on which we can call resolver function.
	 * Note that each StartTransactionCommand() call should be preceded by a
	 * SetCurrentStatementStartTimestamp() call, which sets both the time
	 * for the statement we're about the run, and also the transaction
	 * start time.  Also, each other query sent to SPI should probably be
	 * preceded by SetCurrentStatementStartTimestamp(), so that statement
	 * start time is always up to date.
	 *
	 * The SPI_connect() call lets us run queries through the SPI manager,
	 * and the PushActiveSnapshot() call creates an "active" snapshot
	 * which is necessary for queries to have MVCC data to work on.
	 *
	 * The pgstat_report_activity() call makes our activity visible
	 * through the pgstat views.
	 */
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, command);

	/* Run the resolver function */
	ret = SPI_execute(command, false, 0);

	if (ret < 0)
		elog(LOG, "error running pg_fdw_xact_resolve() within database %d",
			 dbid);

	if (SPI_processed > 0)
		ereport(LOG,
				(errmsg("resolved %lu foreign transactions", SPI_processed)));

	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	/* Done exit now */
	proc_exit(0);
}

/* Get database list */
static List *
get_database_list(void)
{
	List *dblist = NIL;
	ListCell *cell;
	ListCell *next;
	ListCell *prev = NULL;
	HeapScanDesc scan;
	HeapTuple tup;
	Relation rel;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		MemoryContext oldcxt;

		/*
		 * Allocate our results in the caller's context, not the
		 * transaction's. We do this inside the loop, and restore the original
		 * context at the end, so that leaky things like heap_getnext() are
		 * not called in a potentially long-lived context.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);
		dblist = lappend_oid(dblist, HeapTupleGetOid(tup));
		MemoryContextSwitchTo(oldcxt);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	/*
	 * Check if database has foreign transaction entry. Delete entry
	 * from the list if the database has.
	 */
	for (cell = list_head(dblist); cell != NULL; cell = next)
	{
		Oid dbid = lfirst_oid(cell);
		bool exists;

		next = lnext(cell);

		exists = fdw_xact_exists(InvalidTransactionId, dbid, InvalidOid, InvalidOid);

		if (!exists)
			dblist = list_delete_cell(dblist, cell, prev);
		else
			prev = cell;
	}

	return dblist;
}
