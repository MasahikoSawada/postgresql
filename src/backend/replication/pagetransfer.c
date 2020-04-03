/*-------------------------------------------------------------------------
 *
 * pagetransfer.c
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/pagetransfer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/pagetransfer.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"

static void PageTxfCancelWait(void);
static bool page_verify_checksum(Page page, BlockNumber blknum);

void
PageTxfReleaseWaiter(RelFileNode rnode, ForkNumber forknum, BlockNumber blknum,
					 Page page)
{
	PGPROC *proc = NULL;
	PGPROC *thisproc = NULL;
	bool	invalid_remote_page = false;
	bool	restored = false;
	int		numprocs = 0;

	if (PageIsNew(page) || !page_verify_checksum(page, blknum))
		invalid_remote_page = true;

	LWLockAcquire(PageTxfLock, LW_EXCLUSIVE);

	proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->pageTxfQueue),
								   &(WalSndCtl->pageTxfQueue),
								   offsetof(PGPROC, pageTxfLinks));

	while (proc)
	{
		thisproc = proc;
		proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->pageTxfQueue),
									   &(proc->pageTxfLinks),
									   offsetof(PGPROC, pageTxfLinks));

		/*
		 * Check if there is the process who waiting for the given page. Since it's
		 * possible that multiple backends are waiting for the same page, we scan all
		 * waiting processes.
		 */
		if (thisproc->pageTxfState == PAGE_TXF_BEING_PROCESSED &&
			RelFileNodeEquals(thisproc->pageTxfNode, rnode) &&
			thisproc->pageTxfForkNum == forknum &&
			thisproc->pageTxfBlknum == blknum)
		{
			numprocs++;

			/*
			 * Found the waiting process but unfortunately the transferred page also
			 * has corruption. Release the waiter and mark it as falied.
			 */
			if (invalid_remote_page)
			{
				SHMQueueDelete(&(thisproc->pageTxfLinks));
				pg_write_barrier();
				thisproc->pageTxfState = PAGE_TXF_FAILED;
				SetLatch(&(thisproc->procLatch));
				continue;
			}

			/*
			 * Restore the transferred page if not done yet.
			 */
			if (!restored)
			{
				WalSndRestorePage(rnode, forknum, blknum, page);
				restored = true;
			}

			/* Wake up the waiter */
			SHMQueueDelete(&(thisproc->pageTxfLinks));
			pg_write_barrier();
			thisproc->pageTxfState = PAGE_TXF_COMPLETE;
			SetLatch(&(thisproc->procLatch));
		}
	}

	LWLockRelease(PageTxfLock);

	elog(DEBUG3, "release %d procs waiting for rel (%u,%u,%u), fork %u, block %u",
		 numprocs,
		 rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blknum);
}

static void
PageTxfCancelWait(void)
{
	if (!SHMQueueIsDetached(&(MyProc->pageTxfLinks)))
		SHMQueueDelete(&(MyProc->pageTxfLinks));
	MyProc->pageTxfState = PAGE_TXF_NOT_WAITING;
}

void
PageTxfWaitForRequestedPage(RelFileNode rnode, ForkNumber forknum,
							BlockNumber blknum)
{
	char		*new_status = NULL;
	const char	*old_status;
	bool		found = false;
	int			i;

	/*
	 * Check if there is an active physical replication slot. If no, we cannot
	 * request the page transfer.
	 *
	 * XXX: we also need to check walsenders that is active but not using
	 * replication slot.
	 */
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

		if (slot->in_use && slot->active_pid != 0 && SlotIsPhysical(slot))
		{
			found = true;
			break;
		}
	}
	LWLockRelease(ReplicationSlotControlLock);

	if (!found)
		ereport(ERROR,
				(errmsg("there is no physical standby server to request page")));

	LWLockAcquire(PageTxfLock, LW_EXCLUSIVE);

	/* Set our request information and add ourselves to the queue */
	MyProc->pageTxfLSN = GetXLogWriteRecPtr();
	memcpy(&MyProc->pageTxfNode, &rnode, sizeof(RelFileNode));
	MyProc->pageTxfForkNum = forknum;
	MyProc->pageTxfBlknum = blknum;
	MyProc->pageTxfState = PAGE_TXF_WAITING;
	SHMQueueInsertBefore(&(WalSndCtl->pageTxfQueue), &(MyProc->pageTxfLinks));
	WalSndWakeup();

	LWLockRelease(PageTxfLock);

	/* Alter ps display to show waiting for sync rep. */
	if (update_process_title)
	{
		int			len;

		old_status = get_ps_display(&len);
		new_status = (char *) palloc(len + 17 + 1);
		memcpy(new_status, old_status, len);
		sprintf(new_status + len, " waiting for page");
		set_ps_display(new_status);
		new_status[len] = '\0'; /* truncate off " waiting ..." */
	}

	/*
	 * Wait for the requested page is restored.
	 */
	for (;;)
	{
		int rc;

		elog(NOTICE, "waiting state %d", MyProc->pageTxfState);

		ResetLatch(MyLatch);

		/* The page is successfully restored */
		if (MyProc->pageTxfState == PAGE_TXF_COMPLETE)
			break;

		/*
		 * The page restore failed because the transferred page also has corruption.
		 */
		if (MyProc->pageTxfState == PAGE_TXF_FAILED)
			ereport(ERROR,
					(errmsg("failed to repair page")));

		if (ProcDiePending)
		{
			ereport(WARNING,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("canceling the wait for page repair via replication and terminating connection due to administrator command")));
			PageTxfCancelWait();
			break;
		}

		if (QueryCancelPending)
		{
			QueryCancelPending = false;
			ereport(WARNING,
					(errmsg("canceling wait for page repair via replication due to user request")));
			PageTxfCancelWait();
			break;
		}

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
					   WAIT_EVENT_PAGE_TRANSFER);

		if (rc & WL_POSTMASTER_DEATH)
		{
			ProcDiePending = true;
			PageTxfCancelWait();
			break;
		}
	}

	pg_read_barrier();
	Assert(SHMQueueIsDetached(&(MyProc->pageTxfLinks)));
	MyProc->pageTxfState = PAGE_TXF_NOT_WAITING;
	MyProc->pageTxfLSN = 0;

	if (new_status)
	{
		/* Reset ps display */
		set_ps_display(new_status);
		pfree(new_status);
	}
}

/*
 * Return one backend process waiting for page transfer.
 */
PGPROC *
PageTxfGetWaitingRequest(void)
{
	PGPROC	   *proc = NULL;

	Assert(LWLockHeldByMe(PageTxfLock));

	proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->pageTxfQueue),
								   &(WalSndCtl->pageTxfQueue),
								   offsetof(PGPROC, pageTxfLinks));

	while (proc)
	{
		if (proc->pageTxfState == PAGE_TXF_WAITING)
			return proc;

		proc = (PGPROC *) SHMQueueNext(&(WalSndCtl->pageTxfQueue),
									   &(proc->pageTxfLinks),
									   offsetof(PGPROC, pageTxfLinks));
	}

	return NULL;
}

/*
 * Verify the given page's checksum. Unlike PageIsVerified this function assumes that
 * the given page is neither a new or empty page, and doesn't report checksum failure.
 */
static bool
page_verify_checksum(Page page, BlockNumber blknum)
{
	uint16 checksum;
	PageHeader p = (PageHeader) page;

	Assert(!PageIsNew(page));
	Assert(DataChecksumsEnabled());

	checksum = pg_checksum_page((char *) page, blknum);

	return (checksum == p->pd_checksum);
}

Datum
repair_page(PG_FUNCTION_ARGS)
{
	Oid oid = PG_GETARG_OID(0);
	BlockNumber blkno = PG_GETARG_UINT32(1);
	Relation rel;

	rel = relation_open(oid, AccessExclusiveLock);

	PageTxfWaitForRequestedPage(rel->rd_node, MAIN_FORKNUM, blkno);

	relation_close(rel, NoLock);

	PG_RETURN_INT32(1);
}
