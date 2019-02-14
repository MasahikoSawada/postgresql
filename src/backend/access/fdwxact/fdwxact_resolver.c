/*-------------------------------------------------------------------------
 *
 * fdwxact_resolver.c
 *
 * The foreign transaction resolver background worker resolves foreign
 * transactions that participate to a distributed transaction. A resolver
 * process is started by foreign transaction launcher for every databases.
 *
 * A resolver process continues to resolve foreign transactions on a database
 * It resolves two types of foreign transactions: on-line foreign transaction
 * and dangling foreign transaction. The on-line foreign transaction is a
 * foreign transaction that a concurrent backend process is waiting for
 * resolution. The dangling transaction is a foreign transaction that corresponding
 * distributed transaction ended up in in-doubt state. A resolver process
 * doesn' exit as long as there is at least one unresolved foreign transaction
 * on the database even if the timeout has come.
 *
 * Normal termination is by SIGTERM, which instructs the resolver process
 * to exit(0) at the next convenient moment. Emergency  termination is by
 * SIGQUIT; like any backend. The resolver process also terminate by timeouts
 * only if there is no pending foreign transactions on the database waiting
 * to be resolved.
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/fdwxact/fdwxact_resolver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/fdwxact_launcher.h"
#include "access/resolver_internal.h"
#include "access/transam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

/* GUC parameters */
int foreign_xact_resolution_retry_interval;
int foreign_xact_resolver_timeout = 60 * 1000;

FdwXactRslvCtlData *FdwXactRslvCtl;

static void FXRslvLoop(void);
static long FXRslvComputeSleepTime(TimestampTz now, TimestampTz nextResolutionTs);
static void FXRslvCheckTimeout(TimestampTz now);

static void fdwxact_resolver_sighup(SIGNAL_ARGS);
static void fdwxact_resolver_onexit(int code, Datum arg);
static void fdwxact_resolver_detach(void);
static void fdwxact_resolver_attach(int slot);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;

/* true during processing in-doubt transactions */
static bool processing_indoubts = false;

/* Set flag to reload configuration at next convenient time */
static void
fdwxact_resolver_sighup(SIGNAL_ARGS)
{
	int		save_errno = errno;

	got_SIGHUP = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Detach the resolver and cleanup the resolver info.
 */
static void
fdwxact_resolver_detach(void)
{
	/* Block concurrent access */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	MyFdwXactResolver->pid = InvalidPid;
	MyFdwXactResolver->in_use = false;
	MyFdwXactResolver->dbid = InvalidOid;

	LWLockRelease(FdwXactResolverLock);
}

/*
 * Cleanup up foreign transaction resolver info.
 */
static void
fdwxact_resolver_onexit(int code, Datum arg)
{
	fdwxact_resolver_detach();

	/*
	 * If we exits during resolving in-doubt transactions, wake up the launcher
	 * for the retry purpose, meaning re-launching resolvers by intervals.
	 */
	if (processing_indoubts)
		FdwXactLauncherRequestToLaunchForRetry();
	else
		FdwXactLauncherRequestToLaunch();
}

/*
 * Attach to a slot.
 */
static void
fdwxact_resolver_attach(int slot)
{
	/* Block concurrent access */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	Assert(slot >= 0 && slot < max_foreign_xact_resolvers);
	MyFdwXactResolver = &FdwXactRslvCtl->resolvers[slot];

	if (!MyFdwXactResolver->in_use)
	{
		LWLockRelease(FdwXactResolverLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign transaction resolver slot %d is empty, cannot attach",
						slot)));
	}

	MyFdwXactResolver->pid = MyProcPid;
	MyFdwXactResolver->latch = &MyProc->procLatch;
	TIMESTAMP_NOBEGIN(MyFdwXactResolver->last_resolved_time);

	before_shmem_exit(fdwxact_resolver_onexit, (Datum) 0);

	LWLockRelease(FdwXactResolverLock);
}

/* Foreign transaction resolver entry point */
void
FdwXactResolverMain(Datum main_arg)
{
	int slot = DatumGetInt32(main_arg);

	/* Attach to a slot */
	fdwxact_resolver_attach(slot);

	/* Establish signal handlers */
	pqsignal(SIGHUP, fdwxact_resolver_sighup);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnectionByOid(MyFdwXactResolver->dbid, InvalidOid, 0);

	StartTransactionCommand();

	ereport(LOG,
			(errmsg("foreign transaction resolver for database \"%s\" has started",
					get_database_name(MyFdwXactResolver->dbid))));

	CommitTransactionCommand();

	/* Initialize stats to a sanish value */
	MyFdwXactResolver->last_resolved_time = GetCurrentTimestamp();

	/* Run the main loop */
	FXRslvLoop();

	proc_exit(0);
}

/*
 * Fdwxact resolver main loop
 */
static void
FXRslvLoop(void)
{
	MemoryContext resolver_ctx;

	resolver_ctx = AllocSetContextCreate(TopMemoryContext,
										 "Foreign Transaction Resolver",
										 ALLOCSET_DEFAULT_SIZES);

	/* Enter main loop */
	for (;;)
	{
		PGPROC			*waiter = NULL;
		TransactionId	waitXid = InvalidTransactionId;
		TimestampTz		resolutionTs = -1;
		int			rc;
		TimestampTz	now;
		long		sleep_time;

		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(resolver_ctx);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		now = GetCurrentTimestamp();

		/* Get an waiter from the shmem queue */
		waiter = FdwXactGetWaiter(&resolutionTs, &waitXid);

		if (!waiter)
		{
			bool ret;

			ereport(LOG, (errmsg("@@@ no waiters, process indoubts")));

			processing_indoubts = true;

			/* The queue is empty, process in-doubt transactions */
			StartTransactionCommand();
			ret = FdwXactResolveInDoubtTransactions();
			CommitTransactionCommand();

			processing_indoubts = false;

			if (ret)
			{
				/* Update my stats */
				SpinLockAcquire(&(MyFdwXactResolver->mutex));
				MyFdwXactResolver->last_resolved_time = GetCurrentTimestamp();
				SpinLockRelease(&(MyFdwXactResolver->mutex));
			}

			ereport(LOG, (errmsg("@@@ %s sleep %d msec",
								 ret ? "processed some indoubts," : "",
								 foreign_xact_resolution_retry_interval)));

			sleep_time = foreign_xact_resolution_retry_interval;
		}
		else if (resolutionTs < now)
		{
			Assert(TransactionIdIsValid(waitXid));

			ereport(LOG, (errmsg("@@@ get waiter %u, process it", waitXid)));

			/* Resolve the waiting distributed transaction */
			StartTransactionCommand();
			FdwXactResolveTransactionAndReleaseWaiter(waiter, waitXid);
			CommitTransactionCommand();

			/* Update my stats */
			SpinLockAcquire(&(MyFdwXactResolver->mutex));
			MyFdwXactResolver->last_resolved_time = GetCurrentTimestamp();
			SpinLockRelease(&(MyFdwXactResolver->mutex));

			continue;
		}
		else
		{
			ereport(LOG, (errmsg("@@@ get waiter %u, but should wait until %s",
								 waitXid, timestamptz_to_str(resolutionTs))));
			Assert(resolutionTs != 0);
			sleep_time = FXRslvComputeSleepTime(now, resolutionTs);
		}

		FXRslvCheckTimeout(now);

		if (sleep_time == 0)
			continue;

		MemoryContextResetAndDeleteChildren(resolver_ctx);
		MemoryContextSwitchTo(TopMemoryContext);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   sleep_time,
					   WAIT_EVENT_FDW_XACT_RESOLVER_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

/*
 * Check whether there have been foreign transactions by the backend within
 * foreign_xact_resolver_timeout and shutdown if not.
 */
static void
FXRslvCheckTimeout(TimestampTz now)
{
	TimestampTz last_resolved_time;
	TimestampTz timeout;

	if (foreign_xact_resolver_timeout == 0)
		return;

	SpinLockAcquire(&(MyFdwXactResolver->mutex));
	last_resolved_time = MyFdwXactResolver->last_resolved_time;
	SpinLockRelease(&(MyFdwXactResolver->mutex));

	timeout = TimestampTzPlusMilliseconds(last_resolved_time,
										  foreign_xact_resolver_timeout);

	ereport(LOG, (errmsg("@@@ timeout %s, now %s",
						 timestamptz_to_str(timeout),
						 timestamptz_to_str(now))));

	if (now < timeout)
		return;

	/*
	 * Reached to the timeout. We exit if there is no more both pending foreign
	 * transactions.
	 *
	 * @@@ Race condition.
	 */
	if (!fdw_xact_exists(InvalidTransactionId, MyDatabaseId, InvalidOid,
						 InvalidOid))
	{
		StartTransactionCommand();
		ereport(LOG,
				(errmsg("foreign transaction resolver for database \"%s\" will stop because the timeout",
						get_database_name(MyDatabaseId))));
		CommitTransactionCommand();

		fdwxact_resolver_detach();
		proc_exit(0);
	}
}

/*
 * Compute how long we should sleep by the next cycle. Return the sleep time
 * in milliseconds, -1 means that we reached to the timeout and should exits
 */
static long
FXRslvComputeSleepTime(TimestampTz now, TimestampTz nextResolutionTs)
{
	static TimestampTz	wakeup_time = 0;
	long	sleeptime;
	long	sec_to_timeout;
	int		microsec_to_timeout;

	if (wakeup_time <= nextResolutionTs)
		wakeup_time = nextResolutionTs;

	if (now >= wakeup_time)
		return 0;

	/* Compute relative time until wakeup. */
	TimestampDifference(now, wakeup_time,
						&sec_to_timeout, &microsec_to_timeout);

	sleeptime = sec_to_timeout * 1000 + microsec_to_timeout / 1000;

	ereport(LOG, (errmsg("@@@ Sleep %ld msec until %s",
						 sleeptime, timestamptz_to_str(nextResolutionTs))));

	return sleeptime;
}

bool
IsFdwXactResolver(void)
{
	return MyFdwXactResolver != NULL;
}
