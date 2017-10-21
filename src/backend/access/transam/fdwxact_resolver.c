/*-------------------------------------------------------------------------
 *
 * resolver.c
 *
 * PostgreSQL foreign transaction resolver background worker
 *
 * Portions Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/fdwxact_resolver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/resolver_private.h"
#include "access/transam.h"

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
int foreign_xact_resolution_interval;
int foreign_xact_resolver_timeout = 60 * 1000;

FdwXactRslvCtlData *FdwXactRslvCtl;

static long FdwXactRslvComputeSleepTime(TimestampTz now);
static void FdwXactRslvProcessForeignTransactions(void);

static void fdwxact_resolver_sighup(SIGNAL_ARGS);
static void fdwxact_resolver_onexit(int code, Datum arg);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;

/* Report shared memory space needed by FdwXactRsoverShmemInit */
Size
FdwXactResolverShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, mul_size(max_foreign_xact_resolvers,
								   sizeof(FdwXactResolver)));

	return size;
}

/*
 * Allocate and initialize foreign transaction resolver shared
 * memory.
 */
void
FdwXactResolverShmemInit(void)
{
	bool found;

	FdwXactRslvCtl = ShmemInitStruct("Foreign transactions resolvers",
									 FdwXactResolverShmemSize(),
									 &found);

	if (!IsUnderPostmaster)
	{
		int	slot;

		/* First time through, so initialize */
		MemSet(FdwXactRslvCtl, 0, FdwXactResolverShmemSize());

		SHMQueueInit(&(FdwXactRslvCtl->FdwXactQueue));

		for (slot = 0; slot < max_foreign_xact_resolvers; slot++)
		{
			FdwXactResolver *resolver = &FdwXactRslvCtl->resolvers[slot];

			SpinLockInit(&(resolver->mutex));
		}
	}
}

/*
 * Cleanup up foreign transaction resolver info.
 */
static void
fdwxact_resolver_onexit(int code, Datum arg)
{
	MyFdwXactResolver->pid = InvalidPid;
	MyFdwXactResolver->in_use = false;
}

/*
 * Attach to a slot.
 */
void
fdwxact_resolver_attach(int slot)
{
	Assert(slot >= 0 && slot < max_foreign_xact_resolvers);

	/* Block concurrent access */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	MyFdwXactResolver = &FdwXactRslvCtl->resolvers[slot];

	SpinLockAcquire(&(MyFdwXactResolver->mutex));
	MyFdwXactResolver->pid = MyProcPid;
	SpinLockRelease(&(MyFdwXactResolver->mutex));

	MyFdwXactResolver->latch = &MyProc->procLatch;

	if (!MyFdwXactResolver->in_use)
	{
		LWLockRelease(FdwXactResolverLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign transaction resolver slot %d is empty, cannot attach",
						slot)));
	}

	before_shmem_exit(fdwxact_resolver_onexit, (Datum) 0);

	LWLockRelease(FdwXactResolverLock);
}

/* Set flag to reload configuration at next convenient time */
static void
fdwxact_resolver_sighup(SIGNAL_ARGS)
{
	int		save_errno = errno;

	got_SIGHUP = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

/* Foreign transaction resolver entry point */
void
FdwXactRslvMain(Datum main_arg)
{
	int slot = DatumGetInt32(main_arg);

	/* Attach to a slot */
	fdwxact_resolver_attach(slot);

	elog(DEBUG1, "foreign transaciton resolver for database %u started",
		 MyFdwXactResolver->dbid);

	/* Establish signal handlers */
	pqsignal(SIGHUP, fdwxact_resolver_sighup);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Initialize stats to a sanish value */
	MyFdwXactResolver->last_resolution_time = GetCurrentTimestamp();

	/* Establish connection to nailed catalogs */
	BackgroundWorkerInitializeConnectionByOid(MyFdwXactResolver->dbid, InvalidOid);

	for (;;)
	{
		int			rc;
		TimestampTz	now;
		long		sleep_time;

		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Resolve pending transactions if there are */
		FdwXactRslvProcessForeignTransactions();

		now = GetCurrentTimestamp();

		sleep_time = FdwXactRslvComputeSleepTime(now);

		if (sleep_time < 0)
		{
			/*
			 * We reached to the timeout here. We can exit if there is on
			 * pending foreign transactions in the shmem queue. Check it and
			 * then close the business while holding FdwXactResolverLaunchLock.
			 */
			if (!fdw_xact_exists(InvalidTransactionId, MyDatabaseId, InvalidOid,
								 InvalidOid))
			{
				/*
				 * There is no more transactions we need to resolve,
				 * turn off my slot while holding lock so that concurrent
				 * backends cannot register additional entries.
				 */
				SpinLockAcquire(&(MyFdwXactResolver->mutex));
				MyFdwXactResolver->in_use = false;
				SpinLockRelease(&(MyFdwXactResolver->mutex));

				ereport(LOG,
						(errmsg("foreign transaction resolver for database \"%u\" will stop because the timeout",
								MyFdwXactResolver->dbid)));

				proc_exit(0);
			}

			/*
			 * If new foreign transaction came in the meantime of checking, we
			 * handle it before exits. Since we know we have the pending foreign
			 * transaction we don't want to sleep.
			 */
			sleep_time = 0;
		}

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   sleep_time,
					   WAIT_EVENT_FDW_XACT_RESOLVER_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

/*
 * Process all foreign transactions on the database it's connecting to. If we
 * succeeded in resolution we can update the last resolution time. When we resolved
 * no foreign transaction in a cycle we return.
 */
static void
FdwXactRslvProcessForeignTransactions(void)
{
	/*
	 * Loop until there are no more foreign transaction we need to resolve.
	 */
	for (;;)
	{
		int	n_resolved;
		int	n_resolved_dangling;

		StartTransactionCommand();

		/* Resolve all foreign transaction associated with xid */
		n_resolved = FdwXactResolveForeignTransactions(MyFdwXactResolver->dbid);

		/* Resolve dangling transactions if there are */
		n_resolved_dangling = FdwXactResolveDanglingTransactions(MyFdwXactResolver->dbid);

		CommitTransactionCommand();

		if (n_resolved > 0)
		{
			/* Update my state */
			SpinLockAcquire(&(MyFdwXactResolver->mutex));
			MyFdwXactResolver->last_resolution_time = GetCurrentTimestamp();
			SpinLockRelease(&(MyFdwXactResolver->mutex));
		}

		/* Return if we didn't resolve any foreign transactions */
		if (n_resolved == 0 && n_resolved_dangling == 0)
		return;
	}
}

/*
 * Compute how long we should sleep by the next cycle. Return the sleep time
 * in milliseconds, -1 means that we reached to the timeout and should exits
 */
static long
FdwXactRslvComputeSleepTime(TimestampTz now)
{
	static TimestampTz	wakeuptime = 0;
	long	sleeptime;
	long	sec_to_timeout;
	int		microsec_to_timeout;

	if (foreign_xact_resolver_timeout > 0)
	{
		TimestampTz timeout;

		timeout = TimestampTzPlusMilliseconds(MyFdwXactResolver->last_resolution_time,
											  foreign_xact_resolver_timeout);

		/* If we reached to the timeout, exit */
		if (now >= timeout)
			return -1;
	}

	if (now >= wakeuptime)
		wakeuptime = TimestampTzPlusMilliseconds(now,
												 foreign_xact_resolution_interval * 1000);

	/* Compute relative time until wakeup. */
	TimestampDifference(now, wakeuptime,
						&sec_to_timeout, &microsec_to_timeout);

	sleeptime = sec_to_timeout * 1000 + microsec_to_timeout / 1000;

	return sleeptime;
}

/*
 * Launch a new foreign transaction resolver worker if not launched yet.
 * A foreign transaction resolver worker is responsible for the resolution
 * of foreign transactions are registered on one database. So if a resolver
 * worker already is launched by other backend we don't need to launch new
 * one.
 */
void
fdwxact_maybe_launch_resolver(void)
{
	FdwXactResolver *resolver = NULL;
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	int i;
	int	slot;
	bool	found = false;

	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		FdwXactResolver *r = &FdwXactRslvCtl->resolvers[i];

		/*
		 * Found a running resolver that is responsible for the
		 * database "dbid".
		 */
		if (r->in_use && r->pid != InvalidPid && r->dbid == MyDatabaseId)
		{
			found = true;
			resolver = r;
			break;
		}
	}

	/*
	 * If we found the resolver for my database, we don't need to launch new one.
	 * Add a task and wake up it.
	 */
	if (found)
	{
		SetLatch(resolver->latch);
		LWLockRelease(FdwXactResolverLock);

		elog(DEBUG1, "found a running foreign transaction resolver process for database %u",
			 MyDatabaseId);

		return;
	}

	elog(DEBUG1, "starting foreign transaction resolver for datbase ID %u", MyDatabaseId);

	/* Find unused worker slot */
	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		FdwXactResolver *r = &FdwXactRslvCtl->resolvers[i];

		/* Found an used worker slot */
		if (!r->in_use)
		{
			resolver = r;
			slot = i;
			break;
		}
	}

	/*
	 * However if there are no more free worker slots, inform user about it before
	 * exiting.
	 */
	if (resolver == NULL)
	{
		LWLockRelease(FdwXactResolverLock);
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of foreign trasanction resolver slots"),
				 errhint("You might need to increase max_foreign_transaction_resolvers.")));

		return;
	}

	/* Prepare the resolver slot. It's in use but pid is still invalid */
	resolver->dbid = MyDatabaseId;
	resolver->in_use = true;
	resolver->pid = InvalidPid;
	TIMESTAMP_NOBEGIN(resolver->last_resolution_time);

	LWLockRelease(FdwXactResolverLock);

	/* Register the new dynamic worker */
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "FdwXactRslvMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "foreign transaction resolver for database %u", MyDatabaseId);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "foreign transaction resolver");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_main_arg = (Datum) 0;
	bgw.bgw_notify_pid = Int32GetDatum(slot);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		/* Failed to launch, cleanup the worker slot */
		SpinLockAcquire(&(MyFdwXactResolver->mutex));
		resolver->in_use = false;
		SpinLockRelease(&(MyFdwXactResolver->mutex));

		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase max_worker_processes.")));
	}

	/*
	 * We don't need to wait until it attaches here because we're going to wait
	 * until all foreign transactions are resolved.
	 */
}

/*
 * Returns activity of foreign transaction resolvers, including pids, the number
 * of tasks and the last resolution time.
 */
Datum
pg_stat_get_fdwxact_resolver(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_FDWXACT_RESOLVERS_COLS 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		FdwXactResolver	*resolver = &FdwXactRslvCtl->resolvers[i];
		pid_t	pid;
		Oid		dbid;
		TimestampTz last_resolution_time;
		Datum		values[PG_STAT_GET_FDWXACT_RESOLVERS_COLS];
		bool		nulls[PG_STAT_GET_FDWXACT_RESOLVERS_COLS];


		SpinLockAcquire(&(MyFdwXactResolver->mutex));
		if (resolver->pid == 0)
		{
			SpinLockRelease(&(MyFdwXactResolver->mutex));
			continue;
		}

		pid = resolver->pid;
		dbid = resolver->dbid;
		last_resolution_time = resolver->last_resolution_time;
		SpinLockRelease(&(MyFdwXactResolver->mutex));

		memset(nulls, 0, sizeof(nulls));
		/* pid */
		values[0] = Int32GetDatum(pid);

		/* dbid */
		values[1] = ObjectIdGetDatum(dbid);

		/* last_resolution_time */
		if (last_resolution_time == 0)
			nulls[2] = true;
		else
			values[2] = TimestampTzGetDatum(last_resolution_time);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
