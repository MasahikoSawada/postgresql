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
 * In 9.1 we support only a single synchronous standby, chosen from a
 * priority list of synchronous_standby_names. Before it can become the
 * synchronous standby it must have caught up with the primary; that may
 * take some time. Once caught up, the current highest priority standby
 * will release waiters from the queue.
 *
 * Portions Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

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
#include "utils/json.h"

#define DEBUG_QUORUM 1

/* User-settable parameters for sync rep */
char	   *SyncRepStandbyNames;
SyncInfoNode *SyncRepStandbyInfo;

#define SyncStandbysDefined() \
	(SyncRepStandbyNames != NULL && SyncRepStandbyNames[0] != '\0')

#define DefaultSyncGroupName "Default Group"

static SyncInfoNode *init_node(SyncInfoNodeType type);

typedef struct SyncInfoParseState
{
	SyncParseStateName state_name;
	struct SyncInfoParseState *next;
}	SyncInfoParseState;

typedef struct SyncInfoState
{
	int			array_size;
	char	   *key_name;
	SyncInfoNode *array_first_node;
	SyncInfoNode *cur_node;
	SyncInfoParseState *parse_state;
}	SyncInfoState;

static char *read_sync_file(int *length);
static void sync_info_object_start(void *pstate);
static void sync_info_array_start(void *pstate);
static void sync_info_array_end(void *pstate);
static void sync_info_object_field_start(void *pstate, char *fname, bool isnull);
static void sync_info_object_field_end(void *pstate, char *fname, bool isnull);
static void sync_info_scalar(void *pstate, char *token, JsonTokenType tokentype);
static void add_node(SyncInfoState * state, SyncInfoNodeType ntype);
bool		populate_group(SyncInfoNode * expr, SyncInfoState *state, bool found);
static bool announce_next_takeover = true;

static int	SyncRepWaitMode = SYNC_REP_NO_WAIT;

static void SyncRepQueueInsert(int mode);
static void SyncRepCancelWait(void);
static int	SyncRepWakeQueue(bool all, int mode);

static int	SyncRepGetStandbyPriority(void);

#ifdef DEBUG_QUORUM
void print_structure(SyncInfoNode *expr, int level);
#endif

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
		 * lead the client to believe that that the transaction aborted, which
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
 * Find the WAL sender servicing the synchronous standby with the lowest
 * priority value, or NULL if no synchronous standby is connected. If there
 * are multiple standbys with the same lowest priority value, the first one
 * found is selected. The caller must hold SyncRepLock.
 */
bool
CheckNameList(SyncInfoNode * expr, char *name, bool found)
{
	if (expr->gtype == GNODE_NAME)
	{
		if (strcmp(expr->name, name) == 0)
			found = true;
	}
	else if (GNODE_GROUP)
	{
		if (expr->group != NULL)
			found = CheckNameList(expr->group, name, found);
	}

	if (!found && expr->next)
		found = CheckNameList(expr->next, name, found);

	return found;
}

bool
populate_group(SyncInfoNode *expr, SyncInfoState *state, bool found)
{
	if (GNODE_GROUP)
	{
		if (strcmp(expr->name, state->key_name) == 0)
		{
			expr->group = state->array_first_node;
			expr->ngroups = state->array_size;

			/*
			 * Check if the priority/quorum number exceeds against the number
			 * of node of group.
			 */
			if (expr->count > expr->ngroups)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("The not-existing order is specified in group \"%s\", The max number is \"%d\"",
								expr->name, expr->ngroups)));

			found = true;
		}

		else if (expr->group != NULL)
			found = populate_group(expr->group, state, found);
	}

	if (expr->next)
		found = populate_group(expr->next, state, found);

	return found;
}

/* Decide LSN in acordance with status of sync standbys at this time */
XLogRecPtr *
SyncRepGetQuorumRecPtr(SyncInfoNode *node, List **lsnlist, bool priority_group)
{
	int			i;
	XLogRecPtr *lsn;
	ListCell   *cell;

	if (node->gtype == GNODE_NAME)
	{
		XLogRecPtr *tmplsn = (XLogRecPtr *) palloc(sizeof(XLogRecPtr) * NUM_SYNC_REP_WAIT_MODE);

		tmplsn[SYNC_REP_WAIT_WRITE] = InvalidXLogRecPtr;
		tmplsn[SYNC_REP_WAIT_FLUSH] = InvalidXLogRecPtr;

		/*
		 * Get write/flush LSN from corresponding active wal sender using
		 * WalSnd->name. If there are wal senders which has same name, we
		 * select higher.
		 */
		for (i = 0; i < max_wal_senders; i++)
		{
			volatile WalSnd *walsnd = &WalSndCtl->walsnds[i];

			/* Must bev active */
			if (walsnd->pid == 0)
				continue;
			/* Must be streaming */
			if (walsnd->state != WALSNDSTATE_STREAMING)
				continue;

			/* Must be synchronous */
			if (walsnd->sync_standby_priority == 0)
				continue;

			/* Must have a valid flush position */
			if (XLogRecPtrIsInvalid(walsnd->flush))
				continue;


			if (strcmp(node->name, (char *) walsnd->name) == 0)
			{
				/* Found same name node. Get highest lsn */
				if (tmplsn[SYNC_REP_WAIT_WRITE] < walsnd->write &&
					tmplsn[SYNC_REP_WAIT_FLUSH] < walsnd->flush)
				{
					tmplsn[SYNC_REP_WAIT_WRITE] = walsnd->write;
					tmplsn[SYNC_REP_WAIT_FLUSH] = walsnd->flush;
				}
#ifdef DEBUG_QUORUM
				/* debug print */
				elog(WARNING, "[%d]---- NAME [%s] : write : %X/%X, flush : %X/%X",
					 MyProcPid,
					 node->name,
					 (uint32) (tmplsn[SYNC_REP_WAIT_WRITE] >> 32), (uint32) tmplsn[SYNC_REP_WAIT_WRITE],
					 (uint32) (tmplsn[SYNC_REP_WAIT_FLUSH] >> 32), (uint32) tmplsn[SYNC_REP_WAIT_FLUSH]
					);
#endif
			}
		}

		lsn = tmplsn;

	}
	else if (node->gtype == GNODE_GROUP)
	{
		List	   *new_lsnlist = NIL;

		/* Get list of whole group's lsn */
		if (node->group != NULL)
		{
			SyncRepGetQuorumRecPtr(node->group, &new_lsnlist, node->priority_group);
			Assert(new_lsnlist);

			/*
			 * Decide group's lsn using by quorum number Assume the list is
			 * order by LSN in desc.
			 */
			lsn = (XLogRecPtr *) list_nth(new_lsnlist, node->count - 1);

#ifdef DEBUG_QUORUM
			/* Debug print */
			i = 0;
			elog(WARNING, "[%d]---- %s GROUP [%s:%d] decided LSN : write = %X/%X, flush = %X/%X",
				 MyProcPid,
				 (node->priority_group == true) ? "PRIORITY" : "QUORUM",
				 node->name,
				 node->count,
				 (uint32) (lsn[SYNC_REP_WAIT_WRITE] >> 32), (uint32) lsn[SYNC_REP_WAIT_WRITE],
				 (uint32) (lsn[SYNC_REP_WAIT_FLUSH] >> 32), (uint32) lsn[SYNC_REP_WAIT_FLUSH]
				);
/*             elog(WARNING, "[%d]---- [%s] : write : %X/%X, flush : %X/%X",
                        MyProcPid,
                        "G",
                        (uint32) (lsn[SYNC_REP_WAIT_WRITE] >> 32), (uint32) lsn[SYNC_REP_WAIT_WRITE],
                        (uint32) (lsn[SYNC_REP_WAIT_FLUSH] >> 32), (uint32) lsn[SYNC_REP_WAIT_FLUSH]
                       );*/
#endif

		}
	}


	if (node->next)
	{
		SyncRepGetQuorumRecPtr(node->next, lsnlist, node->priority_group);
	}
	/* For root call */
	if (lsnlist == NULL)
		return lsn;

	/*
	 * At last, Insert lsn[] into list. We're asuumed that list is descending
	 * order.
	 */
	if (*lsnlist == NIL)
	{
		*lsnlist = lappend(*lsnlist, lsn);
	}
	else if (priority_group)
	{
		if (lsn[SYNC_REP_WAIT_WRITE] != InvalidXLogRecPtr &&
			lsn[SYNC_REP_WAIT_FLUSH] != InvalidXLogRecPtr)
			*lsnlist = lcons(lsn, *lsnlist);	/* Prepend lsn to list */
	}
	else
	{
		bool		inserted = false;

		foreach(cell, *lsnlist)
		{
			XLogRecPtr *cur_lsn = (XLogRecPtr *) lfirst(cell);
			XLogRecPtr *next_lsn;

			/* If list has only one lsn element, just compare two LSNs. */
			if (cell->next == NULL)
			{
				inserted = true;
				if (
					cur_lsn[SYNC_REP_WAIT_WRITE] < lsn[SYNC_REP_WAIT_WRITE] &&
					cur_lsn[SYNC_REP_WAIT_FLUSH] < lsn[SYNC_REP_WAIT_FLUSH])
					*lsnlist = lcons(lsn, *lsnlist);	/* Prepend lsn to list */
				else
					*lsnlist = lappend(*lsnlist, lsn);	/* Append lsn to list */
				break;
			}

			/* Get next lsn in advance to insert lsn immidiately after cur_lsn */
			next_lsn = (XLogRecPtr *) lfirst(cell->next);

			/* Found lsn */
			if (next_lsn[SYNC_REP_WAIT_WRITE] < lsn[SYNC_REP_WAIT_WRITE] &&
				next_lsn[SYNC_REP_WAIT_FLUSH] < lsn[SYNC_REP_WAIT_FLUSH])
			{
				inserted = true;
				lappend_cell(*lsnlist, cell, lsn);
				break;
			}
		}
		/* If there is not cell which is smaller than me, append lsn into tail */
		if (!inserted)
			*lsnlist = lappend(*lsnlist, lsn);
	}

	return lsn;
}

WalSnd *
SyncRepGetSynchronousStandby(void)
{
	WalSnd	   *result = NULL;
	int			result_priority = 0;
	int			i;

	for (i = 0; i < max_wal_senders; i++)
	{
		/* Use volatile pointer to prevent code rearrangement */
		volatile WalSnd *walsnd = &WalSndCtl->walsnds[i];
		int			this_priority;

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

		/* Must have a lower priority value than any previous ones */
		if (result != NULL && result_priority <= this_priority)
			continue;

		/* Must have a valid flush position */
		if (XLogRecPtrIsInvalid(walsnd->flush))
			continue;

		result = (WalSnd *) walsnd;
		result_priority = this_priority;

		/*
		 * If priority is equal to 1, there cannot be any other WAL senders
		 * with a lower priority, so we're done.
		 */
		if (this_priority == 1)
			return result;
	}

	return result;
}

/*
 * Update the LSNs on each queue based upon our latest state. This
 * implements a simple policy of first-valid-standby-releases-waiter.
 *
 * Other policies are possible, which would change what we do here and what
 * perhaps also which information we store as well.
 */
void
SyncRepReleaseWaiters(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	int			numwrite = 0;
	int			numflush = 0;
	XLogRecPtr *lsn;

	/*
	 * If this WALSender is serving a standby that is not on the list of
	 * potential standbys then we have nothing to do. If we are still starting
	 * up, still running base backup or the current flush position is still
	 * invalid, then leave quickly also.
	 */
	if (MyWalSnd->sync_standby_priority == 0 ||
		MyWalSnd->state < WALSNDSTATE_STREAMING ||
		XLogRecPtrIsInvalid(MyWalSnd->flush))
		return;

	/*
	 * We're a potential sync standby. Release waiters if we are the highest
	 * priority standby.
	 */
	LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

	lsn = SyncRepGetQuorumRecPtr(SyncRepStandbyInfo, NULL, SyncRepStandbyInfo->priority_group);

	Assert(lsn);

#ifdef DEBUG_QUORUM
	/* Debug print */
	elog(WARNING, "====== CONCLUSION write = %X/%X, flush = %X/%X ======",
		 (uint32) (lsn[SYNC_REP_WAIT_WRITE] >> 32), (uint32) lsn[SYNC_REP_WAIT_WRITE],
		 (uint32) (lsn[SYNC_REP_WAIT_FLUSH] >> 32), (uint32) lsn[SYNC_REP_WAIT_FLUSH]);
#endif


	/*
	 * Set the lsn first so that when we wake backends they will release up to
	 * this location.
	 */
	if (walsndctl->lsn[SYNC_REP_WAIT_WRITE] < lsn[SYNC_REP_WAIT_WRITE])
	{
		walsndctl->lsn[SYNC_REP_WAIT_WRITE] = MyWalSnd->write;
		numwrite = SyncRepWakeQueue(false, SYNC_REP_WAIT_WRITE);
	}
	if (walsndctl->lsn[SYNC_REP_WAIT_FLUSH] < lsn[SYNC_REP_WAIT_FLUSH])
	{
		walsndctl->lsn[SYNC_REP_WAIT_FLUSH] = MyWalSnd->flush;
		numflush = SyncRepWakeQueue(false, SYNC_REP_WAIT_FLUSH);
	}

	LWLockRelease(SyncRepLock);

	elog(DEBUG3, "released %d procs up to write %X/%X, %d procs up to flush %X/%X",
		 numwrite, (uint32) (MyWalSnd->write >> 32), (uint32) MyWalSnd->write,
	   numflush, (uint32) (MyWalSnd->flush >> 32), (uint32) MyWalSnd->flush);

	/*
	 * If we are managing the highest priority standby, though we weren't
	 * prior to this, then announce we are now the sync standby.
	 */
	if (announce_next_takeover)
	{
		announce_next_takeover = false;
		ereport(LOG,
				(errmsg("standby \"%s\" is now the synchronous standby with priority %u",
						application_name, MyWalSnd->sync_standby_priority)));
	}
}

/*
 * Check if we are in the list of sync standbys, and if so, determine
 * priority sequence. Return priority if set, or zero to indicate that
 * we are not a potential sync standby.
 *
 * Compare the parameter SyncRepStandbyInfo against the application_name
 * for this WALSender, or allow any name if we find a wildcard "*".
 */
static int
SyncRepGetStandbyPriority(void)
{
	/*
	 * Since synchronous cascade replication is not allowed, we always set the
	 * priority of cascading walsender to zero.
	 */
	if (am_cascading_walsender)
		return 0;

	/*
	 * There are not any synchronous standbys, return 0.
	 */
	if (SyncRepStandbyInfo == NULL)
		return 0;

	if (CheckNameList(SyncRepStandbyInfo, application_name, false))
		return 1;

	return 0;
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

static SyncInfoNode *
init_node(SyncInfoNodeType gtype)
{
	SyncInfoNode *expr = malloc(sizeof(SyncInfoNode));

	expr->gtype = gtype;
	expr->next = NULL;
	expr->name = NULL;
	expr->count = -1;
	expr->group = NULL;

	return expr;
}

static void
add_node(SyncInfoState *state, SyncInfoNodeType ntype)
{
	SyncInfoParseState *pstate = state->parse_state;
	SyncInfoNode *new_node = init_node(ntype);

	switch (pstate->state_name)
	{
		case SYNC_INFO_MAIN:
			Assert(!SyncRepStandbyInfo);
			new_node->name = DefaultSyncGroupName;
			SyncRepStandbyInfo = new_node;
			break;
		case SYNC_INFO_NODES:
		case SYNC_INFO_GROUP_DETAIL:
			if ((state->array_size)++ == 0)
				state->array_first_node = new_node;
			else if (state->array_size > 0)
				state->cur_node->next = new_node;
			break;
		default:
			/* do nothing */
			break;
	}
	state->cur_node = new_node;
}

static SyncInfoParseState *
pushState(SyncInfoParseState **pstate, SyncParseStateName state)
{
	SyncInfoParseState *ns = palloc(sizeof(SyncInfoParseState));
	

	if (state != SYNC_INFO_MAIN_OBJECT_START && state < (*pstate)->state_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("Incorrect order of keys in synchronous_standby_names")));

	ns->state_name = state;
	ns->next = *pstate;

#ifdef DEBUG_QUORUM
	SyncInfoParseState *tmp = ns;

	elog(WARNING, "====== STACK =====");
	elog(WARNING, "%d", tmp->state_name);
	while(tmp->next)
	{
		tmp = tmp->next;
		elog(WARNING, "|");
		elog(WARNING, "%d", tmp->state_name);
	}
#endif

	return ns;
}

static SyncInfoParseState *
popState(SyncInfoParseState **pstate)
{
	SyncInfoParseState *ps = (*pstate)->next;

	pfree(*pstate);
	return ps;
}

static void
sync_info_object_start(void *istate)
{
	SyncInfoState *state = (SyncInfoState *) istate;

	if (state->parse_state == NULL)
		state->parse_state = pushState(&(state->parse_state), SYNC_INFO_MAIN_OBJECT_START);
	else
		add_node(state, GNODE_GROUP);
}

static void
sync_info_array_start(void *istate)
{
	SyncInfoState *state = (SyncInfoState *) istate;

	SyncParseStateName state_name = state->parse_state->state_name;

	if (state_name == SYNC_INFO_GROUP_DETAIL || state_name == SYNC_INFO_NODES)
		state->array_size = 0;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("synchronous_standby_names : key \"%s\" value cannot be array",
						state->key_name)));
}

static void
sync_info_array_end(void *istate)
{
	SyncInfoState *state = (SyncInfoState *) istate;

	switch (state->parse_state->state_name)
	{
		case SYNC_INFO_GROUP_DETAIL:
			if (!populate_group(SyncRepStandbyInfo, state, false))
				ereport(LOG,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("synchronous_standby_names : group \"%s\" defined but not used",
								state->key_name)));
			break;
		case SYNC_INFO_NODES:
			state->cur_node = SyncRepStandbyInfo;
			SyncRepStandbyInfo->group = state->array_first_node;
			break;
		default:
			/* do nothing */
			return;
	}
	state->array_size = -1;
	state->key_name = NULL;
	state->array_first_node = NULL;
}

static void
sync_info_object_field_start(void *istate, char *fname, bool isnull)
{
	SyncInfoState *state = (SyncInfoState *) istate;
	SyncInfoParseState *pstate;

	pstate = state->parse_state;

	if (!fname)
	{
		SyncRepStandbyInfo = NULL;
		ereport(ERROR, (errmsg("The token field name is NULL")));
		return;
	}
	state->key_name = fname;

	switch (pstate->state_name)
	{
		case SYNC_INFO_MAIN_OBJECT_START:
			if (strcmp(fname, "sync_info") == 0)
				state->parse_state = pushState(&(state->parse_state), SYNC_INFO_MAIN);
			else if (strcmp(fname, "groups") == 0)
			{
				if (SyncRepStandbyInfo == NULL)
					ereport(ERROR, (errmsg("'sync_info' should be defined before 'groups'")));

				state->parse_state = pushState(&(state->parse_state), SYNC_INFO_GROUPS);
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("Unrecognised key \"%s\" in synchronous_standby_names",
								fname)));
			break;

		case SYNC_INFO_GROUPS:
			state->parse_state = pushState(&(state->parse_state), SYNC_INFO_GROUP_DETAIL);
			break;

		default:
			if (strcmp(fname, "priority") == 0)
			{
				state->cur_node->priority_group = true;
				state->parse_state = pushState(&(state->parse_state), SYNC_INFO_COUNT);
			}
			else if (strcmp(fname, "quorum") == 0)
			{
				state->cur_node->priority_group = false;
				state->parse_state = pushState(&(state->parse_state), SYNC_INFO_COUNT);
			}
			else if (strcmp(fname, "nodes") == 0)
				state->parse_state = pushState(&(state->parse_state), SYNC_INFO_NODES);
			else if (strcmp(fname, "group") == 0)
				state->parse_state = pushState(&(state->parse_state), SYNC_INFO_GROUP_NAME);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("Unrecognised key \"%s\" in synchronous_standby_names",
								fname)));
	}
}

static void
sync_info_object_field_end(void *istate, char *fname, bool isnull)
{
	SyncInfoState *state = (SyncInfoState *) istate;

	state->parse_state = popState(&(state->parse_state));
}

static void
sync_info_scalar(void *istate, char *token, JsonTokenType tokentype)
{
	SyncInfoState *state = (SyncInfoState *) istate;
	SyncInfoParseState *pstate;

	pstate = state->parse_state;
	Assert(token != NULL);

	switch (tokentype)
	{
		case JSON_TOKEN_NUMBER:
			if (pstate->state_name == SYNC_INFO_COUNT)
			{
				state->cur_node->count = atoi(token);
				break;
			}
		case JSON_TOKEN_STRING:
			{
				if (pstate->state_name == SYNC_INFO_NODES || pstate->state_name == SYNC_INFO_GROUP_DETAIL)
				{
					if (state->array_size > -1)
						add_node(state, GNODE_NAME);
					else
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("synchronous_standby_names: key \"%s\" value must be array",
									state->key_name)));
				}

				state->cur_node->name = pstrdup(token);
				break;
			}

		default:
			ereport(LOG,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("Unrecognized token \"%s\" in synchronous_standby_names",
							token)));
			break;
	}

}

bool
check_synchronous_standby_names(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *elemlist;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	if (SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/*
		 * Any additional validation of standby names should go here.
		 *
		 * Don't attempt to set WALSender priority because this is executed by
		 * postmaster at startup, not WALSender, so the application_name is not
		 * yet correctly set.
		 */

		pfree(rawstring);
		list_free(elemlist);
		SyncRepStandbyInfo = NULL;

		return true;
	}
	else
	{
		/* Do check something to validate JSON */
		SyncRepStandbyInfo = NULL;
	}

	return true;
}

void
assign_synchronous_standby_names(char *newval, void *extra)
{
	List	*elemlist;
	char	*rawstring = pstrdup(newval);

	/* First, we try to parse string as conma-sepalated list. */
	if (SplitIdentifierString(rawstring, ',', &elemlist))
	{
		ListCell   *l;
		SyncInfoNode *temp = NULL;

		SyncRepStandbyInfo = init_node(GNODE_GROUP);
		SyncRepStandbyInfo->name = DefaultSyncGroupName;
		SyncRepStandbyInfo->priority_group = true;
		SyncRepStandbyInfo->count = 1;

		foreach(l, elemlist)
		{
			char	   *standby_name = (char *) lfirst(l);

			if (temp != NULL)
			{
				temp->next = init_node(GNODE_NAME);
				temp = temp->next;
			}
			else
			{
				temp = init_node(GNODE_NAME);
				SyncRepStandbyInfo->group = temp;
			}
			temp->name = pstrdup(standby_name);
		}
	}
	else /* Otherwise, JSON format */
	{
		text	   *sync_info = cstring_to_text(rawstring);
		JsonLexContext *lex;
		JsonSemAction sem;
		SyncInfoState state;

		lex = makeJsonLexContext(sync_info, true);
		memset(&sem, 0, sizeof(JsonSemAction));
		memset(&state, 0, sizeof(SyncInfoState));

		state.array_size = -1;
		sem.semstate = (void *) &state;
		sem.object_start = sync_info_object_start;
		sem.array_start = sync_info_array_start;
		sem.array_end = sync_info_array_end;
		sem.scalar = sync_info_scalar;
		sem.object_field_start = sync_info_object_field_start;
		sem.object_field_end = sync_info_object_field_end;

		pg_parse_json(lex, &sem);
	}

	pfree(rawstring);
	list_free(elemlist);

#ifdef DEBUG_QUORUM
	print_structure(SyncRepStandbyInfo, 0);
#endif
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

#ifdef DEBUG_QUORUM
void
print_structure(SyncInfoNode * expr, int level)
{
	char       *blank = (char *) palloc(sizeof(char) * ((level * 4) + 1));
	
	memset(blank, '-', level * 4);
	blank[level * 4] = '\0';
	
	if (expr->gtype == GNODE_NAME)
		elog(WARNING, "[NAME] : %s name = %s", blank, expr->name);
	else
	{
		elog(WARNING, "[GROUP]: %s %s = %d name = %s",
			 blank,
			 (expr->priority_group == true) ? "priority" : "quorum",
			 expr->count,
			 expr->name);
		level++;
		if (expr->group)
			print_structure(expr->group, level);
		level--;
	}
	if (expr->next)
		print_structure(expr->next, level);
}
#endif
