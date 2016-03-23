/*-------------------------------------------------------------------------
 *
 * syncrep.c
 *
 * Synchronous replication is new as of PostgreSQL 9.1.
 *
 * If requested, transaction commits wait until their commit LSN is
 * acknowledged by the synchronous standby.
 *
 * This module contains the code for waiting and release of backends.
 * All code in this module executes on the primary. The core streaming
 * replication transport remains within WALreceiver/WALsender modules.
 *
 * The essence of this design is that it isolates all logic about
 * waiting/releasing onto the primary. The primary defines which standbys
 * it wishes to wait for. The standby is completely unaware of the
 * durability requirements of transactions on the primary, reducing the
 * complexity of the code and streamlining both standby operations and
 * network bandwidth because there is no requirement to ship
 * per-transaction state information.
 *
 * Replication is either synchronous or not synchronous (async). If it is
 * async, we just fastpath out of here. If it is sync, then we wait for
 * the write or flush location on the standby before releasing the waiting
 * backend. Further complexity in that interaction is expected in later
 * releases.
 *
 * The best performing way to manage the waiting backends is to have a
 * single ordered queue of waiting backends, so that we can avoid
 * searching the through all waiters each time we receive a reply.
 *
 * In 9.6 we support multiple synchronous standbys, chosen from a
 * priority list of synchronous_standby_names. Before they can become the
 * synchronous standbys they must have caught up with the primary; that may
 * take some time. Once caught up, the current higher priority standbys
 * which are considered as synchronous at that moment will release
 * waiters from the queue.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xact.h"
#include "miscadmin.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"

/* User-settable parameters for sync rep */
char	   *SyncRepStandbyNames;

#define SyncStandbysDefined() \
	(SyncRepStandbyNames != NULL && SyncRepStandbyNames[0] != '\0')

static bool announce_next_takeover = true;

SyncRepConfigData *SyncRepConfig;
static int	SyncRepWaitMode = SYNC_REP_NO_WAIT;

static void SyncRepQueueInsert(int mode);
static void SyncRepCancelWait(void);
static int	SyncRepWakeQueue(bool all, int mode);

static bool SyncRepGetOldestSyncRecPtr(XLogRecPtr *writePtr,
									   XLogRecPtr *flushPtr, bool *am_sync);
static int	SyncRepGetStandbyPriority(void);

#ifdef USE_ASSERT_CHECKING
static bool SyncRepQueueIsOrderedByLSN(int mode);
#endif

/*
 * ===========================================================
 * Synchronous Replication functions for normal user backends
 * ===========================================================
 */

/*
 * Wait for synchronous replication, if requested by user.
 *
 * Initially backends start in state SYNC_REP_NOT_WAITING and then
 * change that state to SYNC_REP_WAITING before adding ourselves
 * to the wait queue. During SyncRepWakeQueue() a WALSender changes
 * the state to SYNC_REP_WAIT_COMPLETE once replication is confirmed.
 * This backend then resets its state to SYNC_REP_NOT_WAITING.
 */
void
SyncRepWaitForLSN(XLogRecPtr XactCommitLSN)
{
	char	   *new_status = NULL;
	const char *old_status;
	int			mode = SyncRepWaitMode;

	/*
	 * Fast exit if user has not requested sync replication, or there are no
	 * sync replication standby names defined. Note that those standbys don't
	 * need to be connected.
	 */
	if (!SyncRepRequested() || !SyncStandbysDefined())
		return;

	Assert(SHMQueueIsDetached(&(MyProc->syncRepLinks)));
	Assert(WalSndCtl != NULL);

	LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
	Assert(MyProc->syncRepState == SYNC_REP_NOT_WAITING);

	/*
	 * We don't wait for sync rep if WalSndCtl->sync_standbys_defined is not
	 * set.  See SyncRepUpdateSyncStandbysDefined.
	 *
	 * Also check that the standby hasn't already replied. Unlikely race
	 * condition but we'll be fetching that cache line anyway so it's likely
	 * to be a low cost check.
	 */
	if (!WalSndCtl->sync_standbys_defined ||
		XactCommitLSN <= WalSndCtl->lsn[mode])
	{
		LWLockRelease(SyncRepLock);
		return;
	}

	/*
	 * Set our waitLSN so WALSender will know when to wake us, and add
	 * ourselves to the queue.
	 */
	MyProc->waitLSN = XactCommitLSN;
	MyProc->syncRepState = SYNC_REP_WAITING;
	SyncRepQueueInsert(mode);
	Assert(SyncRepQueueIsOrderedByLSN(mode));
	LWLockRelease(SyncRepLock);

	/* Alter ps display to show waiting for sync rep. */
	if (update_process_title)
	{
		int			len;

		old_status = get_ps_display(&len);
		new_status = (char *) palloc(len + 32 + 1);
		memcpy(new_status, old_status, len);
		sprintf(new_status + len, " waiting for %X/%X",
				(uint32) (XactCommitLSN >> 32), (uint32) XactCommitLSN);
		set_ps_display(new_status, false);
		new_status[len] = '\0'; /* truncate off " waiting ..." */
	}

	/*
	 * Wait for specified LSN to be confirmed.
	 *
	 * Each proc has its own wait latch, so we perform a normal latch
	 * check/wait loop here.
	 */
	for (;;)
	{
		int			syncRepState;

		/* Must reset the latch before testing state. */
		ResetLatch(MyLatch);

		/*
		 * Try checking the state without the lock first.  There's no
		 * guarantee that we'll read the most up-to-date value, so if it looks
		 * like we're still waiting, recheck while holding the lock.  But if
		 * it looks like we're done, we must really be done, because once
		 * walsender changes the state to SYNC_REP_WAIT_COMPLETE, it will
		 * never update it again, so we can't be seeing a stale value in that
		 * case.
		 */
		syncRepState = MyProc->syncRepState;
		if (syncRepState == SYNC_REP_WAITING)
			syncRepState = MyProc->syncRepState;
		if (syncRepState == SYNC_REP_WAIT_COMPLETE)
			break;

		/*
		 * If a wait for synchronous replication is pending, we can neither
		 * acknowledge the commit nor raise ERROR or FATAL.  The latter would
		 * lead the client to believe that the transaction aborted, which
		 * is not true: it's already committed locally. The former is no good
		 * either: the client has requested synchronous replication, and is
		 * entitled to assume that an acknowledged commit is also replicated,
		 * which might not be true. So in this case we issue a WARNING (which
		 * some clients may be able to interpret) and shut off further output.
		 * We do NOT reset ProcDiePending, so that the process will die after
		 * the commit is cleaned up.
		 */
		if (ProcDiePending)
		{
			ereport(WARNING,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("canceling the wait for synchronous replication and terminating connection due to administrator command"),
					 errdetail("The transaction has already committed locally, but might not have been replicated to the standby.")));
			whereToSendOutput = DestNone;
			SyncRepCancelWait();
			break;
		}

		/*
		 * It's unclear what to do if a query cancel interrupt arrives.  We
		 * can't actually abort at this point, but ignoring the interrupt
		 * altogether is not helpful, so we just terminate the wait with a
		 * suitable warning.
		 */
		if (QueryCancelPending)
		{
			QueryCancelPending = false;
			ereport(WARNING,
					(errmsg("canceling wait for synchronous replication due to user request"),
					 errdetail("The transaction has already committed locally, but might not have been replicated to the standby.")));
			SyncRepCancelWait();
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
			SyncRepCancelWait();
			break;
		}

		/*
		 * Wait on latch.  Any condition that should wake us up will set the
		 * latch, so no need for timeout.
		 */
		WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1);
	}

	/*
	 * WalSender has checked our LSN and has removed us from queue. Clean up
	 * state and leave.  It's OK to reset these shared memory fields without
	 * holding SyncRepLock, because any walsenders will ignore us anyway when
	 * we're not on the queue.
	 */
	Assert(SHMQueueIsDetached(&(MyProc->syncRepLinks)));
	MyProc->syncRepState = SYNC_REP_NOT_WAITING;
	MyProc->waitLSN = 0;

	if (new_status)
	{
		/* Reset ps display */
		set_ps_display(new_status, false);
		pfree(new_status);
	}
}

/*
 * Insert MyProc into the specified SyncRepQueue, maintaining sorted invariant.
 *
 * Usually we will go at tail of queue, though it's possible that we arrive
 * here out of order, so start at tail and work back to insertion point.
 */
static void
SyncRepQueueInsert(int mode)
{
	PGPROC	   *proc;

	Assert(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);
	proc = (PGPROC *) SHMQueuePrev(&(WalSndCtl->SyncRepQueue[mode]),
								   &(WalSndCtl->SyncRepQueue[mode]),
								   offsetof(PGPROC, syncRepLinks));

	while (proc)
	{
		/*
		 * Stop at the queue element that we should after to ensure the queue
		 * is ordered by LSN.
		 */
		if (proc->waitLSN < MyProc->waitLSN)
			break;

		proc = (PGPROC *) SHMQueuePrev(&(WalSndCtl->SyncRepQueue[mode]),
									   &(proc->syncRepLinks),
									   offsetof(PGPROC, syncRepLinks));
	}

	if (proc)
		SHMQueueInsertAfter(&(proc->syncRepLinks), &(MyProc->syncRepLinks));
	else
		SHMQueueInsertAfter(&(WalSndCtl->SyncRepQueue[mode]), &(MyProc->syncRepLinks));
}

/*
 * Acquire SyncRepLock and cancel any wait currently in progress.
 */
static void
SyncRepCancelWait(void)
{
	LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
	if (!SHMQueueIsDetached(&(MyProc->syncRepLinks)))
		SHMQueueDelete(&(MyProc->syncRepLinks));
	MyProc->syncRepState = SYNC_REP_NOT_WAITING;
	LWLockRelease(SyncRepLock);
}

void
SyncRepCleanupAtProcExit(void)
{
	if (!SHMQueueIsDetached(&(MyProc->syncRepLinks)))
	{
		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
		SHMQueueDelete(&(MyProc->syncRepLinks));
		LWLockRelease(SyncRepLock);
	}
}

/*
 * ===========================================================
 * Synchronous Replication functions for wal sender processes
 * ===========================================================
 */

/*
 * Take any action required to initialise sync rep state from config
 * data. Called at WALSender startup and after each SIGHUP.
 */
void
SyncRepInitConfig(void)
{
	int			priority;

	/* Update the config data of synchronous replication */
	SyncRepFreeConfig(SyncRepConfig);
	SyncRepUpdateConfig();

	/*
	 * Determine if we are a potential sync standby and remember the result
	 * for handling replies from standby.
	 */
	priority = SyncRepGetStandbyPriority();
	if (MyWalSnd->sync_standby_priority != priority)
	{
		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
		MyWalSnd->sync_standby_priority = priority;
		LWLockRelease(SyncRepLock);
		ereport(DEBUG1,
			(errmsg("standby \"%s\" now has synchronous standby priority %u",
					application_name, priority)));
	}
}

/*
 * Update the LSNs on each queue based upon our latest state. This
 * implements a simple policy of first-valid-sync-standby-releases-waiter.
 *
 * Other policies are possible, which would change what we do here and
 * perhaps also which information we store as well.
 */
void
SyncRepReleaseWaiters(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	XLogRecPtr	writePtr;
	XLogRecPtr	flushPtr;
	bool		got_oldest;
	bool		am_sync;
	int			numwrite = 0;
	int			numflush = 0;

	/*
	 * If this WALSender is serving a standby that is not on the list of
	 * potential sync standbys then we have nothing to do. If we are still
	 * starting up, still running base backup or the current flush position
	 * is still invalid, then leave quickly also.
	 */
	if (MyWalSnd->sync_standby_priority == 0 ||
		MyWalSnd->state < WALSNDSTATE_STREAMING ||
		XLogRecPtrIsInvalid(MyWalSnd->flush))
		return;

	/*
	 * We're a potential sync standby. Release waiters if there are
	 * enough sync standbys and we are considered as sync.
	 */
	LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

	/*
	 * Check whether we are a sync standby or not, and calculate
	 * the oldest positions among all sync standbys.
	 */
	got_oldest = SyncRepGetOldestSyncRecPtr(&writePtr, &flushPtr, &am_sync);

	/*
	 * If we are managing the sync standby, though we weren't
	 * prior to this, then announce we are now the sync standby.
	 */
	if (announce_next_takeover && am_sync)
	{
		announce_next_takeover = false;
		ereport(LOG,
				(errmsg("standby \"%s\" is now the synchronous standby with priority %u",
						application_name, MyWalSnd->sync_standby_priority)));
	}

	/*
	 * If the number of sync standbys is less than requested or we aren't
	 * managing the sync standby then just leave.
	 */
	if (!got_oldest || !am_sync)
	{
		LWLockRelease(SyncRepLock);
		announce_next_takeover = !am_sync;
		return;
	}

	/*
	 * Set the lsn first so that when we wake backends they will release up to
	 * this location.
	 */
	if (walsndctl->lsn[SYNC_REP_WAIT_WRITE] < writePtr)
	{
		walsndctl->lsn[SYNC_REP_WAIT_WRITE] = writePtr;
		numwrite = SyncRepWakeQueue(false, SYNC_REP_WAIT_WRITE);
	}
	if (walsndctl->lsn[SYNC_REP_WAIT_FLUSH] < flushPtr)
	{
		walsndctl->lsn[SYNC_REP_WAIT_FLUSH] = flushPtr;
		numflush = SyncRepWakeQueue(false, SYNC_REP_WAIT_FLUSH);
	}

	LWLockRelease(SyncRepLock);

	elog(DEBUG3, "released %d procs up to write %X/%X, %d procs up to flush %X/%X",
		 numwrite, (uint32) (writePtr >> 32), (uint32) writePtr,
		 numflush, (uint32) (flushPtr >> 32), (uint32) flushPtr);
}

/*
 * Calculate the oldest Write and Flush positions among sync standbys.
 *
 * Return false if the number of sync standbys is less than
 * synchronous_standby_names specifies. Otherwise return true and
 * store the oldest positions into *writePtr and *flushPtr.
 *
 * On return, *am_sync is set to true if this walsender is connecting to
 * sync standby. Otherwise it's set to false.
 */
static bool
SyncRepGetOldestSyncRecPtr(XLogRecPtr *writePtr, XLogRecPtr *flushPtr,
						   bool *am_sync)
{
	List		*sync_standbys;
	ListCell	*cell;

	*writePtr = InvalidXLogRecPtr;
	*flushPtr = InvalidXLogRecPtr;
	*am_sync = false;

	/* Get standbys that are considered as synchronous at this moment */
	sync_standbys = SyncRepGetSyncStandbys();

	/* Quick exit if there are not enough synchronous standbys */
	if (list_length(sync_standbys) < SyncRepConfig->num_sync)
	{
		*am_sync = list_member_int(sync_standbys, MyWalSnd->slotno);
		list_free(sync_standbys);
		return false;
	}

	/*
	 * Scan through all sync standbys and calculate the oldest
	 * Write and Flush positions.
	 */
	foreach (cell, sync_standbys)
	{
		WalSnd *walsnd = &WalSndCtl->walsnds[lfirst_int(cell)];
		XLogRecPtr	write;
		XLogRecPtr	flush;

		SpinLockAcquire(&walsnd->mutex);
		write = walsnd->write;
		flush = walsnd->flush;
		SpinLockRelease(&walsnd->mutex);

		if (XLogRecPtrIsInvalid(*writePtr) || *writePtr > write)
			*writePtr = write;
		if (XLogRecPtrIsInvalid(*flushPtr) || *flushPtr > flush)
			*flushPtr = flush;
		if (walsnd == MyWalSnd)
			*am_sync = true;
	}

	list_free(sync_standbys);
	return true;
}

/*
 * Return the list of sync standbys, or NIL if no sync standby is connected.
 *
 * If there are multiple standbys with the same priority,
 * the first one found is considered as higher priority.
 * The caller must hold SyncRepLock.
 */
List *
SyncRepGetSyncStandbys(void)
{
	List	*result = NIL;
	List	*pending = NIL;
	int	lowest_priority;
	int	next_highest_priority;
	int	this_priority;
	int	priority;
	int	i;
	WalSnd	*walsnd;

	/* Quick exit if sync replication is not requested */
	if (SyncRepConfig == NULL)
		return NIL;

	lowest_priority = list_length(SyncRepConfig->members);
	next_highest_priority = lowest_priority + 1;

	/*
	 * Find the sync standbys which have the highest priority (i.e, 1).
	 * Also store all the other potential sync standbys into the pending list,
	 * in order to scan it later and find other sync standbys from it quickly.
	 */
	for (i = 0; i < max_wal_senders; i++)
	{
		walsnd = &WalSndCtl->walsnds[i];

		/* Must be active */
		if (walsnd->pid == 0)
			continue;

		/* Must be streaming */
		if (walsnd->state != WALSNDSTATE_STREAMING)
			continue;

		/* Must be synchronous */
		this_priority = walsnd->sync_standby_priority;
		if (this_priority == 0)
			continue;

		/* Must have a valid flush position */
		if (XLogRecPtrIsInvalid(walsnd->flush))
			continue;

		/*
		 * If the priority is equal to 1, consider this standby as sync
		 * and append it to the result. Otherwise append this standby
		 * to the pending list to check if it's actually sync or not later.
		 */
		if (this_priority == 1)
		{
			result = lappend_int(result, i);
			if (list_length(result) == SyncRepConfig->num_sync)
			{
				list_free(pending);
				return result;		/* Exit if got enough sync standbys */
			}
		}
		else
		{
			pending = lappend_int(pending, i);

			/*
			 * Track the highest priority among the standbys in the pending
			 * list, in order to use it as the starting priority for later scan
			 * of the list. This is useful to find quickly the sync standbys
			 * from the pending list later because we can skip unnecessary
			 * scans for the unused priorities.
			 */
			if (this_priority < next_highest_priority)
				next_highest_priority = this_priority;
		}
	}

	/*
	 * Consider all pending standbys as sync if the number of them plus
	 * already-found sync ones is lower than the configuration requests.
	 */
	if (list_length(result) + list_length(pending) <= SyncRepConfig->num_sync)
		return list_concat(result, pending);

	/*
	 * Find the sync standbys from the pending list.
	 */
	priority = next_highest_priority;
	while (priority <= lowest_priority)
	{
		ListCell	*cell;
		ListCell	*prev = NULL;
		ListCell	*next;

		next_highest_priority = lowest_priority + 1;

		foreach (cell, pending)
		{
			i = lfirst_int(cell);
			walsnd = &WalSndCtl->walsnds[i];

			next = lnext(cell);

			this_priority = walsnd->sync_standby_priority;
			if (this_priority == priority)
			{
				result = lappend_int(result, i);
				if (list_length(result) == SyncRepConfig->num_sync)
				{
					list_free(pending);
					return result;		/* Exit if got enough sync standbys */
				}

				/*
				 * Remove the entry for this sync standby from the list
				 * to prevent us from looking at the same entry again.
				 */
				pending = list_delete_cell(pending, cell, prev);

				continue;
			}

			if (this_priority < next_highest_priority)
				next_highest_priority = this_priority;

			prev = cell;
		}

		priority = next_highest_priority;
	}

	return result;
}

/*
 * Check if we are in the list of sync standbys, and if so, determine
 * priority sequence. Return priority if set, or zero to indicate that
 * we are not a potential sync standby.
 *
 * Compare the parameter SyncRepStandbyNames against the application_name
 * for this WALSender, or allow any name if we find a wildcard "*".
 */
static int
SyncRepGetStandbyPriority(void)
{
	List	   *members;
	ListCell   *l;
	int			priority = 0;
	bool		found = false;

	/*
	 * Since synchronous cascade replication is not allowed, we always set the
	 * priority of cascading walsender to zero.
	 */
	if (am_cascading_walsender)
		return 0;

	if (!SyncStandbysDefined())
		return 0;

	members = SyncRepConfig->members;
	foreach(l, members)
	{
		char	   *standby_name = (char *) lfirst(l);

		priority++;

		if (pg_strcasecmp(standby_name, application_name) == 0 ||
			pg_strcasecmp(standby_name, "*") == 0)
		{
			found = true;
			break;
		}
	}

	return (found ? priority : 0);
}

/*
 * Walk the specified queue from head.  Set the state of any backends that
 * need to be woken, remove them from the queue, and then wake them.
 * Pass all = true to wake whole queue; otherwise, just wake up to
 * the walsender's LSN.
 *
 * Must hold SyncRepLock.
 */
static int
SyncRepWakeQueue(bool all, int mode)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	PGPROC	   *proc = NULL;
	PGPROC	   *thisproc = NULL;
	int			numprocs = 0;

	Assert(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);
	Assert(SyncRepQueueIsOrderedByLSN(mode));

	proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->SyncRepQueue[mode]),
								   &(WalSndCtl->SyncRepQueue[mode]),
								   offsetof(PGPROC, syncRepLinks));

	while (proc)
	{
		/*
		 * Assume the queue is ordered by LSN
		 */
		if (!all && walsndctl->lsn[mode] < proc->waitLSN)
			return numprocs;

		/*
		 * Move to next proc, so we can delete thisproc from the queue.
		 * thisproc is valid, proc may be NULL after this.
		 */
		thisproc = proc;
		proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->SyncRepQueue[mode]),
									   &(proc->syncRepLinks),
									   offsetof(PGPROC, syncRepLinks));

		/*
		 * Set state to complete; see SyncRepWaitForLSN() for discussion of
		 * the various states.
		 */
		thisproc->syncRepState = SYNC_REP_WAIT_COMPLETE;

		/*
		 * Remove thisproc from queue.
		 */
		SHMQueueDelete(&(thisproc->syncRepLinks));

		/*
		 * Wake only when we have set state and removed from queue.
		 */
		SetLatch(&(thisproc->procLatch));

		numprocs++;
	}

	return numprocs;
}

/*
 * The checkpointer calls this as needed to update the shared
 * sync_standbys_defined flag, so that backends don't remain permanently wedged
 * if synchronous_standby_names is unset.  It's safe to check the current value
 * without the lock, because it's only ever updated by one process.  But we
 * must take the lock to change it.
 */
void
SyncRepUpdateSyncStandbysDefined(void)
{
	bool		sync_standbys_defined = SyncStandbysDefined();

	if (sync_standbys_defined != WalSndCtl->sync_standbys_defined)
	{
		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

		/*
		 * If synchronous_standby_names has been reset to empty, it's futile
		 * for backends to continue to waiting.  Since the user no longer
		 * wants synchronous replication, we'd better wake them up.
		 */
		if (!sync_standbys_defined)
		{
			int			i;

			for (i = 0; i < NUM_SYNC_REP_WAIT_MODE; i++)
				SyncRepWakeQueue(true, i);
		}

		/*
		 * Only allow people to join the queue when there are synchronous
		 * standbys defined.  Without this interlock, there's a race
		 * condition: we might wake up all the current waiters; then, some
		 * backend that hasn't yet reloaded its config might go to sleep on
		 * the queue (and never wake up).  This prevents that.
		 */
		WalSndCtl->sync_standbys_defined = sync_standbys_defined;

		LWLockRelease(SyncRepLock);
	}
}

/*
 * Parse synchronous_standby_names and update the config data
 * of synchronous standbys.
 */
void
SyncRepUpdateConfig(void)
{
	bool	parse_res;

	if (!SyncStandbysDefined())
		return;

	/*
	 * check_synchronous_standby_names() verifies the setting value of
	 * synchronous_standby_names before this function is called. So
	 * syncrep_yyparse() must not cause an error here.
	 */
	parse_res = syncrep_scanstr(SyncRepStandbyNames);
	Assert(parse_res);

	SyncRepConfig = syncrep_parse_result;
	syncrep_parse_result = NULL;
}

/*
 * Free a previously-allocated config data of synchronous replication.
 */
void
SyncRepFreeConfig(SyncRepConfigData *config)
{
	if (!config)
		return;

	list_free_deep(config->members);
	pfree(config);
}

#ifdef USE_ASSERT_CHECKING
static bool
SyncRepQueueIsOrderedByLSN(int mode)
{
	PGPROC	   *proc = NULL;
	XLogRecPtr	lastLSN;

	Assert(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);

	lastLSN = 0;

	proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->SyncRepQueue[mode]),
								   &(WalSndCtl->SyncRepQueue[mode]),
								   offsetof(PGPROC, syncRepLinks));

	while (proc)
	{
		/*
		 * Check the queue is ordered by LSN and that multiple procs don't
		 * have matching LSNs
		 */
		if (proc->waitLSN <= lastLSN)
			return false;

		lastLSN = proc->waitLSN;

		proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->SyncRepQueue[mode]),
									   &(proc->syncRepLinks),
									   offsetof(PGPROC, syncRepLinks));
	}

	return true;
}
#endif

/*
 * ===========================================================
 * Synchronous Replication functions executed by any process
 * ===========================================================
 */

bool
check_synchronous_standby_names(char **newval, void **extra, GucSource source)
{
	if (*newval != NULL && (*newval)[0] != '\0')
	{
		/* Parse value */
		if (!(syncrep_scanstr(*newval)))
		{
			GUC_check_errcode(ERRCODE_SYNTAX_ERROR);
			return false;
		}

		/*
		 * syncrep_yyparse sets the global syncrep_parse_result as side effect.
		 * But this function is required to just check, so frees it
		 * once parsing parameter.
		 */
		SyncRepFreeConfig(syncrep_parse_result);
	}

	return true;
}

void
assign_synchronous_commit(int newval, void *extra)
{
	switch (newval)
	{
		case SYNCHRONOUS_COMMIT_REMOTE_WRITE:
			SyncRepWaitMode = SYNC_REP_WAIT_WRITE;
			break;
		case SYNCHRONOUS_COMMIT_REMOTE_FLUSH:
			SyncRepWaitMode = SYNC_REP_WAIT_FLUSH;
			break;
		default:
			SyncRepWaitMode = SYNC_REP_NO_WAIT;
			break;
	}
}
