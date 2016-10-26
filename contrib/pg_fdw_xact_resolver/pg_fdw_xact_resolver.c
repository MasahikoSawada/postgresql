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
 * process simply connects to the database specified and calls pg_fdw_resolve()
 * function, which tries to resolve the transactions.
 *
 * Copyright (C) 2016, PostgreSQL Global Development Group
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
#include "access/xact.h"
#include "access/fdw_xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
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

/* How frequently the resolver demon checks for unresolved transactions? */
#define FDW_XACT_RESOLVE_NAP_TIME (10 * 1000L)

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

	/* set up common data for all our workers */
	/*
	 * For some reason unless background worker set
	 * BGWORKER_BACKEND_DATABASE_CONNECTION, it's not added to BackendList and
	 * hence notification to this backend is not enabled. So set that flag even
	 * if the backend itself doesn't need database connection.
	 */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 0;	/* restart immediately */
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

	/* Properly accept or ignore signals the postmaster might send us */
	pqsignal(SIGHUP, FDWXactResolver_SIGHUP);		/* set flag to read config
												 * file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, FDWXactResolver_SIGTERM);	/* request shutdown */
	pqsignal(SIGQUIT, FDWXactResolver_SIGQUIT);	/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, FDWXactResolver_SIGUSR1);
	pqsignal(SIGUSR2, SIG_IGN);

	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* Unblock signals */
	BackgroundWorkerUnblockSignals();

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int		rc;
		List	*dbid_list = NIL;
		/*
		 * If no background worker is running, we can start one if there are
		 * unresolved foreign transactions.
		 */
		if (!handle)
		{
			/*
			 * If we do not know which databases have foreign servers with
			 * unresolved foreign transactions, get the list.
			 */
			if (!dbid_list)
				dbid_list = get_dbids_with_unresolved_xact();

			if (dbid_list)
			{
				/* Work on the first dbid, and remove it from the list */
				Oid dbid = linitial_oid(dbid_list);
				dbid_list = list_delete_first(dbid_list);

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
		}

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   FDW_XACT_RESOLVE_NAP_TIME,
					   WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
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

		/*
		 * Postmaster wants to stop this process. Exit with non-zero code, so
		 * that the postmaster starts this process again. The worker processes
		 * will receive the signal and end themselves. This process will restart
		 * them if necessary.
		 */
		if (got_sigquit)
			proc_exit(2);
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
	char	*command = "SELECT pg_fdw_resolve()";
	Oid		dbid = DatumGetObjectId(dbid_datum);
	int		ret;

	/*
	 * This background worker does not loop infinitely, so we need handler only
	 * for SIGTERM, in which case the process should just exit quickly.
	 */
	pqsignal(SIGTERM, FDWXactWorker_SIGTERM);
	pqsignal(SIGQUIT, FDWXactWorker_SIGTERM);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);

	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);
	
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
		elog(LOG, "error running pg_fdw_resolve() within database %d",
			 dbid);

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
