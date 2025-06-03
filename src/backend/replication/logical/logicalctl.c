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
	 * We enable both logical info WAL logging and logical decoding if we're
	 * using 'logical' WAL level or the control file tells so.
	 */
	if (wal_level >= WAL_LEVEL_LOGICAL || status_in_control_file)
	{
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->xlog_logical_info));
	}

	/*
	 * ... but in standby mode, if wal_level is 'logical' but the primary
	 * disables logical decoding, we need to disable it also on the standby.
	 *
	 * We don't need to disable logical info WAL logging because we can use
	 * the 'logical' WAL level even after the promotion.
	 */
	if (StandbyMode && wal_level == WAL_LEVEL_LOGICAL && !status_in_control_file)
		pg_atomic_clear_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
}

/*
 * Update the XLogLogicalInfo cache.
 */
static void
update_xlog_logical_info(void)
{
	XLogLogicalInfo = !pg_atomic_unlocked_test_flag(&(LogicalDecodingCtl->xlog_logical_info));
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
 * Is the logical decoding enabled on the system?
 */
bool
IsLogicalDecodingEnabled(void)
{
	return !pg_atomic_unlocked_test_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
}

/*
 * A PG_ENSURE_ERROR_CLEANUP callback for making the logical decoding enabled.
 */
static void
abort_enabling_logical_decoding(int code, Datum arg)
{
	Assert(LogicalDecodingCtl->transition_in_progress);

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

	/* Standby cannot enable the logical decoding */
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
		bool		logical_decoding = true;

		XLogBeginInsert();
		XLogRegisterData(&logical_decoding, sizeof(bool));
		XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
	}
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
	if (wal_level >= WAL_LEVEL_LOGICAL || !IsLogicalDecodingEnabled())
		return;

	if (pg_atomic_read_u32(&ReplicationSlotCtl->n_inuse_logical_slots) > 0)
		return;

	if (XLogStandbyInfoActive() && !RecoveryInProgress())
	{
		bool		logical_decoding = false;

		XLogBeginInsert();
		XLogRegisterData(&logical_decoding, sizeof(bool));
		XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS_CHANGE);
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
}

/*
 * Update the logical decoding status at end of the recovery. This function
 * must be called ONCE before accepting writes.
 */
void
UpdateLogicalDecodingStatusEndOfRecovery(void)
{
	if (wal_level == WAL_LEVEL_MINIMAL)
		return;

	if (wal_level == WAL_LEVEL_LOGICAL)
	{
		/*
		 * Ensure to enable the logical decoding as it might have been
		 * disabled based on the primary's status.
		 *
		 * xlog_logical_info must have been enabled since we've using the
		 * 'logical' WAL level from the server starts.
		 */
		Assert(!pg_atomic_unlocked_test_flag(&(LogicalDecodingCtl->xlog_logical_info)));
		pg_atomic_test_set_flag(&(LogicalDecodingCtl->logical_decoding_enabled));
	}
	else if (wal_level == WAL_LEVEL_REPLICA && IsLogicalDecodingEnabled())
	{
		if (pg_atomic_read_u32(&ReplicationSlotCtl->n_inuse_logical_slots) >= 0)
		{
			/*
			 * if there are in-use logical slots already, we have enabled the
			 * logical decoding during recovery. So we need to enable only
			 * xlog_logical_info and let all processes to reflect it before
			 * the server accepting writes.
			 */
			Assert(IsLogicalDecodingEnabled());
			pg_atomic_test_set_flag(&(LogicalDecodingCtl->xlog_logical_info));

			WaitForProcSignalBarrier(
									 EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATE_XLOG_LOGICAL_INFO));
		}
		else
		{
			/*
			 * This is a situation like where the primary enabled logical
			 * decoding but no slot has been created on the standby. So
			 * disable the logical decoding.
			 */
			pg_atomic_clear_flag(&(LogicalDecodingCtl->xlog_logical_info));
		}
	}
}
