/*-------------------------------------------------------------------------
 * xloglevelworker.c
 *		Functionality for controlling 'wal_level' value online.
 *
 * This module implements dynamic WAL level changes via SIGHUP signal,
 * eliminating the need for server restarts. The main idea is to decouple
 * two aspects that were previously controlled by a single wal_level
 * setting: the information included in WAL records and the functionalities
 * available at each WAL level.
 *
 * To increase the WAL level, we first allow processes to write WAL records containing
 * the additional information required by the target functionality, while keeping
 * these functionalities unavailable. Once all processes have synchronized to
 * generate the WAL records, the WAL level is increased further to enable
 * the new functionalities.
 *
 * Decreasing the WAL level follows a similar pattern but requires additional steps.
 * First, any functionality that won't be supported at the lower level must be
 * terminated. For instance, decreasing from 'replica' to 'logical' requires
 * invalidating all logical replication slots. After termination, processes reduce
 * their WAL information content, and once all processes are synchronized, the WAL
 * level is decreased.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/xloglevelworker.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xloglevelworker.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/pgarch.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/guc_hooks.h"

typedef struct WalLevelCtlData
{
	/*
	 * current_wal_level is the value used by all backends to know the active
	 * wal_level, which affects what information is logged in WAL records and
	 * which WAL-related features such as replication, WAL archiving and
	 * logical decoding can be used in the system.
	 *
	 * The process-local 'wal_level' value, which is updated when changing
	 * wal_level GUC parameter, might not represent the up-to-date WAL level.
	 * We need to use GetActiveWalLevel() to get the active WAL level.
	 */
	WalLevel	current_wal_level;

	/*
	 * Different WAL level if we're in the process of changing WAL level
	 * online. Otherwise, same as current_wal_level.
	 */
	WalLevel	target_wal_level;

	/* A valid pid if a worker is working */
	pid_t		worker_pid;
}			WalLevelCtlData;
static WalLevelCtlData * WalLevelCtl = NULL;

typedef void (*wal_level_decrease_action_cb) (void);
typedef void (*wal_level_increase_action_cb) (void);

bool		WalLevelInitialized = false;

/*
 * Both doStandbyInfoLogging and doLogicalInfoLogging are backend-local
 * caches.
 *
 * They can be used to determine if information required by hot standby
 * and/or logical decoding need to be written in WAL records. This local
 * cache is updated when (1) process startup and (2) processing the
 * global barrier.
 */
static bool doStandbyInfoLogging = false;
static bool doLogicalInfoLogging = false;

static void update_wal_logging_info_local(void);
static void update_wal_level(WalLevel new_wal_level, bool notify_all);
static void write_wal_level_change(WalLevel new_wal_level);
static void increase_to_replica_action(void);
static void increase_wal_level(WalLevel next_level, WalLevel target_level,
							   wal_level_increase_action_cb action_cb);
static void decrease_to_replica_action(void);
static void decrease_to_minimal_action(void);
static void decrease_wal_level(WalLevel next_level, WalLevel target_level,
							   wal_level_decrease_action_cb action_cb);
static void wal_level_control_worker_shutdown(int code, Datum arg);
static const char *get_wal_level_string(WalLevel level);

Size
WalLevelCtlShmemSize(void)
{
	return sizeof(WalLevelCtlData);
}

void
WalLevelCtlShmemInit(void)
{
	bool		found;

	WalLevelCtl = ShmemInitStruct("wal_level control",
								  WalLevelCtlShmemSize(),
								  &found);

	if (!found)
	{
		WalLevelCtl->current_wal_level = WAL_LEVEL_REPLICA;
		WalLevelCtl->target_wal_level = WAL_LEVEL_REPLICA;
		WalLevelCtl->worker_pid = InvalidPid;
	}
}

/*
 * Initialize the global wal_level. This function is called after processing
 * the configuration at startup.
 */
void
InitializeWalLevelCtl(void)
{
	Assert(!WalLevelInitialized);

	WalLevelCtl->current_wal_level = wal_level;
	WalLevelCtl->target_wal_level = wal_level;
	WalLevelInitialized = true;
}

/*
 * Update both doStandbyInfoLogging and doLogicalInfoLogging based on the
 * current global WAL level value.
 */
static void
update_wal_logging_info_local(void)
{
	LWLockAcquire(WalLevelControlLock, LW_SHARED);
	doStandbyInfoLogging =
		(WalLevelCtl->current_wal_level >= WAL_LEVEL_STANDBY_INFO_LOGGING);
	doLogicalInfoLogging =
		(WalLevelCtl->current_wal_level >= WAL_LEVEL_LOGICAL_INFO_LOGGING);
	LWLockRelease(WalLevelControlLock);
}

/*
 * Initialize process-local xlog info status. This must be called during the
 * process startup time.
 */
void
InitWalLoggingState(void)
{
	update_wal_logging_info_local();
}

/*
 * This function is called when we are ordered to update the local state
 * by a ProcSignalBarrier.
 */
bool
ProcessBarrierUpdateWalLoggingState(void)
{
	update_wal_logging_info_local();
	return true;
}

/*
 * Return true if the logical info logging is enabled.
 */
bool
LogicalInfoLoggingEnabled(void)
{
	return doLogicalInfoLogging;
}

/*
 * Return true if the standby info logging is enabled.
 */
bool
StandbyInfoLoggingEnabled(void)
{
	return doStandbyInfoLogging;
}

/*
 * Return the active wal_level stored on the shared memory, WalLevelCtl.
 */
int
GetActiveWalLevel(void)
{
	WalLevel	level;

	LWLockAcquire(WalLevelControlLock, LW_SHARED);
	level = WalLevelCtl->current_wal_level;
	LWLockRelease(WalLevelControlLock);

	return level;
}

/*
 * Return true if the logical decoding is ready to use.
 */
bool
LogicalDecodingEnabled(void)
{
	return GetActiveWalLevel() >= WAL_LEVEL_LOGICAL;
}

/*
 * Update WAL level after the recovery if necessary.
 *
 * This function must be called ONCE at the end of the recovery.
 */
void
UpdateWalLevelAfterRecovery(WalLevel level)
{
	if (wal_level == level)
		return;

	WalLevelCtl->current_wal_level = level;
	UpdateWalLevel();

	/* Order all processes to reflect the new WAL level */
	WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_WAL_LOGGING_STATE));
}

void
UpdateWalLevel(void)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;

	if (WalLevelCtl->current_wal_level == wal_level)
		return;

	/*
	 * During recovery, we don't need to any coordination for WAL level
	 * changing with running processes as any writes are not permitted yet.
	 */
	if (RecoveryInProgress())
	{
		WalLevelCtl->current_wal_level = wal_level;
		return;
	}

	LWLockAcquire(WalLevelControlLock, LW_EXCLUSIVE);

	/* Return if a wal-level control worker is already running */
	if (WalLevelCtl->worker_pid != InvalidPid)
	{
		LWLockRelease(WalLevelControlLock);
		return;
	}

	WalLevelCtl->target_wal_level = wal_level;
	LWLockRelease(WalLevelControlLock);

	/* Register the new dynamic worker */
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "WalLevelCtlWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "wal level control worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "wal level control worker");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = MyProcPid;

	/*
	 * XXX: Perhaps it's not okay that we failed to launch a bgworker and give
	 * up wal_level change because we already reported that the change has
	 * been accepted. Do we need to use aux process instead for that purpose?
	 */
	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase \"%s\".", "max_worker_processes")));
}

/*
 * Update the WAL level value on shared memory.
 *
 * If notify_all is true, we order all processes to update their local
 * WAL-logging information cache using global signal barriers, and wait
 * for the complete.
 */
static void
update_wal_level(WalLevel new_wal_level, bool notify_all)
{
	LWLockAcquire(WalLevelControlLock, LW_EXCLUSIVE);
	WalLevelCtl->current_wal_level = new_wal_level;
	LWLockRelease(WalLevelControlLock);

	if (notify_all)
	{
		WaitForProcSignalBarrier(
								 EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_WAL_LOGGING_STATE));
	}
}

/*
 * Write XLOG_WAL_LEVEL_CHANGE record with the given WAL level.
 */
static void
write_wal_level_change(WalLevel new_wal_level)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&new_wal_level), sizeof(int));
	XLogInsert(RM_XLOG_ID, XLOG_WAL_LEVEL_CHANGE);
}

/*
 * Callback function for increasing WAL level to 'replica' from 'minimal'.
 */
static void
increase_to_replica_action(void)
{
	/*
	 * We create a checkpoint to increase wal_level from 'minimal' so that we
	 * can restart from there in case of a server crash.
	 */
	RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
}

/*
 * Function to increase WAL level from 'minimal' to 'replica' or from 'replica'
 * to 'logical'.
 *
 * 'target_level' is the destination WAL level, which must be 'replica' or
 * 'logical'. 'next_level' is the intermediate level for the transition, which
 * must be STANDBY_INFO_LOGGING or LOGICAL_INFO_LOGGING.
 */
static void
increase_wal_level(WalLevel next_level, WalLevel target_level,
				   wal_level_increase_action_cb action_cb)

{
	Assert(next_level == WAL_LEVEL_STANDBY_INFO_LOGGING ||
		   next_level == WAL_LEVEL_LOGICAL_INFO_LOGGING);
	Assert(target_level == WAL_LEVEL_REPLICA ||
		   target_level == WAL_LEVEL_LOGICAL);

	/*
	 * Increase to the 'next_level' so that all process can start to write WAL
	 * records with information required by the 'target_level'. We order all
	 * process to reflect this change.
	 */
	update_wal_level(next_level, true);

	/* Invoke the callback function, if specified */
	if (action_cb)
		action_cb();

	START_CRIT_SECTION();

	/*
	 * Now increase to the 'target_level'.
	 *
	 * It's safe to take increase the WAL level, even when not strictly
	 * required. So first set it true and then write the WAL record.
	 */
	update_wal_level(target_level, false);
	write_wal_level_change(target_level);

	END_CRIT_SECTION();

	ereport(LOG,
			(errmsg("wal_level has been increased to \"%s\"",
					get_wal_level_string(target_level))));
}

/*
 * Callback function for decreasing WAL level to 'logical' from 'replica'.
 */
static void
decrease_to_replica_action(void)
{
	/*
	 * Invalidate all logical replication slots, terminating processes doing
	 * logical decoding (and logical replication) too.
	 */
	InvalidateObsoleteReplicationSlots(RS_INVAL_WAL_LEVEL,
									   0, InvalidOid,
									   InvalidTransactionId);
}

/*
 * Callback function for decreasing WAL level to 'replica' from 'minimal'.
 *
 * We terminate some functionalities that are not available at 'minimal' such
 * as WAL archival and log shipping. The WAL level has already been decreased
 * to WAL_LEVEL_STANDBY_INFO_LOGGING, we don't need to deal with the case
 * where these aux processes are concurrently launched.
 */
static void
decrease_to_minimal_action(void)
{
	/* shutdown archiver */
	PgArchShutdown();

	/* shutdown wal senders */
	WalSndTerminate();
	WalSndWaitStopping();

	/* wait for currently running backups to finish */
	wait_for_backup_finish();
}

/*
 * Function to increase WAL level from 'logical' to 'replica' or from 'replica'
 * to 'minimal'.
 *
 * 'target_level' is the destination WAL level, which must be 'replica' or
 * 'minimal'. 'next_level' is the intermediate level for the transition, which
 * must be STANDBY_INFO_LOGGING or LOGICAL_INFO_LOGGING.
 */
static void
decrease_wal_level(WalLevel next_level, WalLevel target_level,
				   wal_level_decrease_action_cb action_cb)
{
	Assert(next_level == WAL_LEVEL_STANDBY_INFO_LOGGING ||
		   next_level == WAL_LEVEL_LOGICAL_INFO_LOGGING);
	Assert(target_level == WAL_LEVEL_REPLICA ||
		   target_level == WAL_LEVEL_MINIMAL);

	/*
	 * Decrease to the 'next_level' to prevent functionality that require the
	 * current WAL level from being newly started while not affecting the
	 * information contained in WAL records.
	 */
	update_wal_level(next_level, true);

	/* Invoke the callback function, if specified */
	if (action_cb)
		action_cb();

	START_CRIT_SECTION();

	/*
	 * Now increase to the 'target_level'.
	 *
	 * We must not decreasing WAL level before writing the WAL records,
	 * because otherwise we would end up having insufficient information in
	 * WAL records. Therefore, unlike increasing the WAL level, we write the
	 * WAL recordand then set the shared WAL level.
	 */
	write_wal_level_change(target_level);
	update_wal_level(target_level, false);

	END_CRIT_SECTION();

	/* Order all processed to disable logical information WAL-logging */
	WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_WAL_LOGGING_STATE));

	ereport(LOG,
			(errmsg("wal_level has been decreased to \"%s\"",
					get_wal_level_string(target_level))));

}

static void
wal_level_control_worker_shutdown(int code, Datum arg)
{
	WalLevelCtl->worker_pid = InvalidPid;
}

void
WalLevelCtlWorkerMain(Datum main_arg)
{
	WalLevel	current;
	WalLevel	target;

	ereport(LOG,
			(errmsg("wal_level control worker started")));

	LWLockAcquire(WalLevelControlLock, LW_EXCLUSIVE);

	if (WalLevelCtl->worker_pid != InvalidPid)
	{
		LWLockRelease(WalLevelControlLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("wal level worker is already running, cannot start"))));
	}

	if (WalLevelCtl->current_wal_level == WalLevelCtl->target_wal_level)
	{
		LWLockRelease(WalLevelControlLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("no need to change wal_level, exit"))));
	}

	before_shmem_exit(wal_level_control_worker_shutdown, (Datum) 0);
	WalLevelCtl->worker_pid = MyProcPid;

	LWLockRelease(WalLevelControlLock);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	current = WalLevelCtl->current_wal_level;
	target = WalLevelCtl->target_wal_level;

	/* The target WAL level must not be intermediate levels */
	Assert(target == WAL_LEVEL_MINIMAL ||
		   target == WAL_LEVEL_REPLICA ||
		   target == WAL_LEVEL_LOGICAL);

	if (current == target)
		proc_exit(0);

	ereport(LOG,
			(errmsg("changing wal_level from \"%s\" to \"%s\"",
					get_wal_level_string(current), get_wal_level_string(target))));

	if (target > current)
	{
		/* We're increasing the WAL level one by one */

		if (current == WAL_LEVEL_MINIMAL ||
			current == WAL_LEVEL_STANDBY_INFO_LOGGING)
		{
			/* increasing to 'replica' */
			increase_wal_level(WAL_LEVEL_STANDBY_INFO_LOGGING,
							   WAL_LEVEL_REPLICA,
							   increase_to_replica_action);
		}

		if (target == WAL_LEVEL_LOGICAL)
		{
			/* increasing to 'logical' */
			increase_wal_level(WAL_LEVEL_LOGICAL_INFO_LOGGING,
							   WAL_LEVEL_LOGICAL,
							   NULL);
		}
	}
	else
	{
		/* We're decreasing the WAL level one by one */

		if (current == WAL_LEVEL_LOGICAL)
		{
			/* decreasing to 'replica' */
			decrease_wal_level(WAL_LEVEL_LOGICAL_INFO_LOGGING,
							   WAL_LEVEL_REPLICA,
							   decrease_to_replica_action);
		}

		if (target == WAL_LEVEL_MINIMAL)
		{
			/* decreasing to 'replica' */
			decrease_wal_level(WAL_LEVEL_STANDBY_INFO_LOGGING,
							   WAL_LEVEL_MINIMAL,
							   decrease_to_minimal_action);
		}
	}

	ereport(LOG,
			(errmsg("successfully made wal_level \"%s\" effective",
					get_wal_level_string(target))));

	proc_exit(0);
}

/*
 * Find a string representation of wal_level.
 *
 * This function doesn't support deprecated wal_level values such
 * as 'archive'.
 */
static const char *
get_wal_level_string(WalLevel level)
{
	switch (level)
	{
		case WAL_LEVEL_MINIMAL:
			return "minimal";
		case WAL_LEVEL_STANDBY_INFO_LOGGING:
			return "minimal-standby_info_logging";
		case WAL_LEVEL_REPLICA:
			return "replica";
		case WAL_LEVEL_LOGICAL_INFO_LOGGING:
			return "replica-logical_info_logging";
		case WAL_LEVEL_LOGICAL:
			return "logical";
	}

	return "???";
}

const char *
show_wal_level(void)
{
	return get_wal_level_string(GetActiveWalLevel());
}

bool
check_wal_level(int *newval, void **extra, GucSource source)
{
	/* Just accept the value when restoring state in a parallel worker */
	if (InitializingParallelWorker)
		return true;

	if (!WalLevelInitialized)
		return true;

	if (wal_level != *newval)
	{
		if (RecoveryInProgress())
		{
			GUC_check_errmsg("cannot change \"wal_level\" during recovery");
			return false;
		}

		if (WalLevelCtl->current_wal_level != WalLevelCtl->target_wal_level)
		{
			GUC_check_errmsg("cannot change \"wal_level\" while changing level is in-progress");
			GUC_check_errdetail("\"wal_level\" is being changed to \"%s\"",
								get_wal_level_string(WalLevelCtl->target_wal_level));
			return false;
		}
	}

	return true;
}
