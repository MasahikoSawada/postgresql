/*-------------------------------------------------------------------------
 * logicalctl.c
 *		Functionality to control logical decoding status.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/replication/logical/logicalctl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "port/atomics.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/shmem.h"
#include "storage/standby.h"
#include "replication/logicalctl.h"
#include "replication/slot.h"
#include "utils/guc.h"
#include "utils/wait_event_types.h"

LogicalDecodingCtlData *LogicalDecodingCtl = NULL;

/*
 * Process local cache of LogicalDecodingCtl->xlog_logical_info. This is
 * initialized at process startup time, and could be updated when absorbing
 * process barrier in ProcessBarrierUpdateXLogLogicalInfo().
 */
bool		XLogLogicalInfo = false;

Size
LogicalDecodingCtlShmemSize(void)
{
	return sizeof(LogicalDecodingCtlData);
}

void
LogicalDecodingCtlShmemInit(void)
{
	bool		found;

	LogicalDecodingCtl = ShmemInitStruct("Logical information control",
										 LogicalDecodingCtlShmemSize(),
										 &found);

	if (!found)
	{
		LogicalDecodingCtl->transition_in_progress = false;
		ConditionVariableInit(&LogicalDecodingCtl->transition_cv);
		pg_atomic_init_flag(&LogicalDecodingCtl->xlog_logical_info);
		pg_atomic_init_flag(&LogicalDecodingCtl->logical_decoding_enabled);
	}
}

/*
 * Initialize the logical decoding status on shmem at server startup. This
 * must be called ONCE during postmaster or standalone-backend startup,
 * before initializing replication slots.
 */
void
StartupLogicalDecodingStatus(bool status_in_control_file)
{
	if (wal_level == WAL_LEVEL_MINIMAL)
		return;

	/*
	 * If the logical decoding was enabled before the last shutdown, we
	 * continue enabling it as we might have set wal_level='logical' or have a
	 * few logical slots. On primary, wal_level setting can overwrite the
	 * status.
	 */
	if (status_in_control_file ||
		(!StandbyMode && wal_level >= WAL_LEVEL_LOGICAL))
	{
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->xlog_logical_info));
	}
}

/*
 * Update the XLogLogicalInfo cache.
 */
static void
update_xlog_logical_info(void)
{
	XLogLogicalInfo = IsXLogLogicalInfoEnabled();
}

/*
 * Initialize XLogLogicalInfo backend-private cache.
 */
void
InitializeProcessXLogLogicalInfo(void)
{
	update_xlog_logical_info();
}

bool
ProcessBarrierUpdateXLogLogicalInfo(void)
{
	update_xlog_logical_info();
	return true;
}

/*
 * Check the shared memory state and return true if the logical decoding is
 * enabled on the system.
 */
bool
IsLogicalDecodingEnabled(void)
{
	return !pg_atomic_unlocked_test_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
}

/*
 * Check the shared memory state and return true if WAL logging logical
 * information is enabled.
 */
bool
IsXLogLogicalInfoEnabled(void)
{
	return !pg_atomic_unlocked_test_flag(&(LogicalDecodingCtl->xlog_logical_info));
}

/*
 * Enable/Disable both status of logical info WAL logging and logical decoding
 * on shared memory based on the given new status.
 */

void
UpdateLogicalDecodingStatus(bool new_status)
{
	if (new_status)
	{
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->xlog_logical_info));
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
	}
	else
	{
		pg_atomic_clear_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
		pg_atomic_clear_flag(&(LogicalDecodingCtl->xlog_logical_info));
	}

	elog(DEBUG1, "update logical decoding status to %d", new_status);
}

/*
 * A PG_ENSURE_ERROR_CLEANUP callback for making the logical decoding enabled.
 */
static void
abort_enabling_logical_decoding(int code, Datum arg)
{
	Assert(LogicalDecodingCtl->transition_in_progress);

	elog(DEBUG1, "aborting the process of enabling logical decoding");

	pg_atomic_clear_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
	pg_atomic_clear_flag(&(LogicalDecodingCtl->xlog_logical_info));

	/* XXX really no need to wait here? */
	EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO);

	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->transition_in_progress = false;
	LWLockRelease(LogicalDecodingControlLock);

	/* Let waiters know the WAL level change completed */
	ConditionVariableBroadcast(&LogicalDecodingCtl->transition_cv);
}

/*
 * Enable the logical decoding if disabled.
 */
void
EnsureLogicalDecodingEnabled(void)
{
	if (IsLogicalDecodingEnabled())
		return;

	/* Standby cannot change the logical decoding status */
	if (RecoveryInProgress())
		return;

retry:
	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);

	if (LogicalDecodingCtl->transition_in_progress)
	{
		LWLockRelease(LogicalDecodingControlLock);

		/* Wait for someone to complete the transition */
		ConditionVariableSleep(&LogicalDecodingCtl->transition_cv,
							   WAIT_EVENT_LOGICAL_DECODING_STATUS_CHANGE);

		goto retry;
	}

	if (IsLogicalDecodingEnabled())
	{
		LWLockRelease(LogicalDecodingControlLock);
		return;
	}

	LogicalDecodingCtl->transition_in_progress = true;
	LWLockRelease(LogicalDecodingControlLock);

	PG_ENSURE_ERROR_CLEANUP(abort_enabling_logical_decoding, (Datum) 0);
	{
		RunningTransactions running;

		/*
		 * Set logical info WAL logging on the shmem. All process starts after
		 * this point will include the information required by the logical
		 * decoding to WAL records.
		 */
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->xlog_logical_info));

		running = GetRunningTransactionData();
		LWLockRelease(ProcArrayLock);
		LWLockRelease(XidGenLock);

		/*
		 * Order all running processes to reflect the xlog_logical_info
		 * update, and wait.
		 */
		WaitForProcSignalBarrier(
								 EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO));

		/*
		 * Wait for all running transactions to finish as some transaction
		 * might have started with the old state.
		 */
		for (int i = 0; i < running->xcnt; i++)
		{
			TransactionId xid = running->xids[i];

			if (TransactionIdIsCurrentTransactionId(xid))
				continue;

			XactLockTableWait(xid, NULL, NULL, XLTW_None);
		}

		/*
		 * Here, we can ensure that all running transactions are using the new
		 * xlog_logical_info value, writing logical information to WAL
		 * records. So now enable the logical decoding globally.
		 */
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->logical_decoding_enabled));

		LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
		LogicalDecodingCtl->transition_in_progress = false;
		LWLockRelease(LogicalDecodingControlLock);
	}
	PG_END_ENSURE_ERROR_CLEANUP(abort_enabling_logical_decoding, (Datum) 0);

	/* Let waiters know the work finished */
	ConditionVariableBroadcast(&LogicalDecodingCtl->transition_cv);

	if (XLogStandbyInfoActive() && !RecoveryInProgress())
	{
		XLogRecPtr	recptr;
		bool		logical_decoding = true;

		XLogBeginInsert();
		XLogRegisterData(&logical_decoding, sizeof(bool));
		recptr = XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
		XLogFlush(recptr);
	}

	ereport(LOG,
			(errmsg("logical decoding is enabled upon creating a new logical replication slot")));
}

/*
 * Disable the logical decoding if enabled.
 *
 * XXX: This function could write a WAL record in order to tell the standbys
 * know the logical decoding got disabled. However, we need to note that this
 * function could be called during process exits (e.g., by ReplicationSlotCleanup()
 * via before_shmem_exit callbacks), which looks something that we want to
 * avoid.
 */
void
DisableLogicalDecodingIfNecessary(void)
{
	/* Standby cannot change the logical decoding status */
	if (RecoveryInProgress())
		return;

	if (wal_level >= WAL_LEVEL_LOGICAL || !IsLogicalDecodingEnabled())
		return;

	if (pg_atomic_read_u32(&ReplicationSlotCtl->n_inuse_logical_slots) > 0)
		return;

	if (XLogStandbyInfoActive() && !RecoveryInProgress())
	{
		bool		logical_decoding = false;
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterData(&logical_decoding, sizeof(bool));
		recptr = XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
		XLogFlush(recptr);
	}

	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->transition_in_progress = true;
	LWLockRelease(LogicalDecodingControlLock);

	pg_atomic_clear_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
	pg_atomic_clear_flag(&(LogicalDecodingCtl->xlog_logical_info));

	/*
	 * XXX is it okay not to wait for the signal to be absorbed?
	 */
	EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO);

	/* XXX need to wait for transaction finishes? */

	LWLockAcquire(LogicalDecodingControlLock, LW_EXCLUSIVE);
	LogicalDecodingCtl->transition_in_progress = false;
	LWLockRelease(LogicalDecodingControlLock);

	/* Let waiters know the work finished */
	ConditionVariableBroadcast(&LogicalDecodingCtl->transition_cv);

	ereport(LOG,
			(errmsg("logical decoding is disabled because all logical replication slots are removed")));
}

/*
 * Update the logical decoding status at end of the recovery. This function
 * must be called ONCE before accepting writes.
 */
void
UpdateLogicalDecodingStatusEndOfRecovery(void)
{
	bool		new_status = false;

	if (!IsUnderPostmaster)
		return;

	if (wal_level == WAL_LEVEL_MINIMAL)
		return;

#ifdef USE_ASSERT_CHECKING
	{
		bool		xlog_logical_info = IsXLogLogicalInfoEnabled();
		bool		logical_decoding = IsLogicalDecodingEnabled();

		/* Verify we're not in intermediate status */
		Assert((xlog_logical_info && logical_decoding) ||
			   (!xlog_logical_info && !logical_decoding));
	}
#endif

	/*
	 * We can use logical decoding if we're using 'logical' WAL level or there
	 * is at least one logical replication slot.
	 */
	if (wal_level == WAL_LEVEL_LOGICAL ||
		pg_atomic_read_u32(&ReplicationSlotCtl->n_inuse_logical_slots) > 0)
		new_status = true;

	/* Update the status on shmem if needed */
	if (IsLogicalDecodingEnabled() != new_status)
	{
		XLogRecPtr	recptr;

		UpdateLogicalDecodingStatus(new_status);

		XLogBeginInsert();
		XLogRegisterData(&new_status, sizeof(bool));
		recptr = XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
		XLogFlush(recptr);

		elog(DEBUG1, "update logical decoding status to %d at end of recovery",
			 new_status);
	}

	/*
	 * Ensure all running processes have the updated status. We don't need to
	 * wait for running transactions to finish as we don't accept any writes
	 * yet. We need the wait even if we've not updated the status above as the
	 * status have been turned on and off during recovery, having running
	 * processes have different status on their local caches.
	 */
	WaitForProcSignalBarrier(
							 EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO));
}
