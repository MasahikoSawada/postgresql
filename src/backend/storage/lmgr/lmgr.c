/*-------------------------------------------------------------------------
 *
 * lmgr.c
 *	  POSTGRES lock manager code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/lmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/inval.h"

/*
 * Compute the hash code associated with a RELEXTLOCK.
 *
 * To avoid unnecessary recomputations of the hash code, we try to do this
 * just once per function, and then pass it around as needed.  Aside from
 * passing the hashcode to hash_search_with_hash_value(), we can extract
 * the lock partition number from the hashcode.
 */
#define RelExtLockTargetTagHashCode(relextlocktargettag) \
	get_hash_value(RelExtLockHash, (const void *) relextlocktargettag)

/*
 * The lockmgr's shared hash tables are partitioned to reduce contention.
 * To determine which partition a given relid belongs to, compute the tag's
 * hash code with ExtLockTagHashCode(), then apply one of these macros.
 * NB: NUM_RELEXTENSIONLOCK_PARTITIONS must be a power of 2!
 */
#define RelExtLockHashPartition(hashcode) \
	((hashcode) % NUM_RELEXTLOCK_PARTITIONS)
#define RelExtLockHashPartitionLock(hashcode) \
	(&MainLWLockArray[RELEXTLOCK_MANAGER_LWLOCK_OFFSET + \
					  LockHashPartition(hashcode)].lock)
#define RelExtLockHashPartitionLockByIndex(i) \
	(&MainLWLockArray[RELEXTLOCK_MANAGER_LWLOCK_OFFSET + (i)].lock


/*
 * Per-backend counter for generating speculative insertion tokens.
 *
 * This may wrap around, but that's OK as it's only used for the short
 * duration between inserting a tuple and checking that there are no (unique)
 * constraint violations.  It's theoretically possible that a backend sees a
 * tuple that was speculatively inserted by another backend, but before it has
 * started waiting on the token, the other backend completes its insertion,
 * and then performs 2^32 unrelated insertions.  And after all that, the
 * first backend finally calls SpeculativeInsertionLockAcquire(), with the
 * intention of waiting for the first insertion to complete, but ends up
 * waiting for the latest unrelated insertion instead.  Even then, nothing
 * particularly bad happens: in the worst case they deadlock, causing one of
 * the transactions to abort.
 */
static uint32 speculativeInsertionToken = 0;


/*
 * Struct to hold context info for transaction lock waits.
 *
 * 'oper' is the operation that needs to wait for the other transaction; 'rel'
 * and 'ctid' specify the address of the tuple being waited for.
 */
typedef struct XactLockTableWaitInfo
{
	XLTW_Oper	oper;
	Relation	rel;
	ItemPointer ctid;
} XactLockTableWaitInfo;

static void XactLockTableWaitErrorCb(void *arg);
static bool CreateRelExtLock(const RELEXTLOCKTAG *targettag, uint32 hashcode,
							 LWLockMode lockmode, bool conditional);
static void DeleteRelExtLock(const RELEXTLOCKTAG *targettag, uint32 hashcode);
static bool RelExtLockExists(const RELEXTLOCKTAG *targettag);

/*
 * Pointers to hash tables containing lock state
 *
 * The RelExtLockHash hash table is in shared memory; LocalRelExtLockHash
 * hashtable is local to each backend.
 */
static HTAB *RelExtLockHash;
static HTAB *LocalRelExtLockHash;


/*
 * InitRelExtLock
 *      Initialize the relation extension lock manager's data structures.
 */
void
InitRelExtLock(long max_table_size)
{
	HASHCTL	info;
	long		init_table_size;

	/*
	 * Compute init/max size to request for lock hashtables.  Note these
	 * calculations must agree with LockShmemSize!
	 */
	init_table_size = max_table_size / 2;

	/*
	 * Allocate hash table for RELEXTLOCK structs. This stores per-relation
	 * lock.
	 */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(RELEXTLOCK);
	info.num_partitions = NUM_RELEXTLOCK_PARTITIONS;

	RelExtLockHash = ShmemInitHash("EXTRELLOCK Hash",
								   init_table_size,
								   max_table_size,
								   &info,
								   HASH_ELEM | HASH_BLOBS | HASH_PARTITION);

	if (LocalRelExtLockHash)
		hash_destroy(LocalRelExtLockHash);

	/*
	 * Allocate non-shared hash table for RELEXTLOCK structs.  This stores
	 * per-relation extension lock and holding information.
	 */
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(LOCALRELEXTLOCK);

	LocalRelExtLockHash = hash_create("LOCALRELEXTLOCK hash",
									  16,
									  &info,
									  HASH_ELEM | HASH_BLOBS);
}

/*
 * RelationInitLockInfo
 *		Initializes the lock information in a relation descriptor.
 *
 *		relcache.c must call this during creation of any reldesc.
 */
void
RelationInitLockInfo(Relation relation)
{
	Assert(RelationIsValid(relation));
	Assert(OidIsValid(RelationGetRelid(relation)));

	relation->rd_lockInfo.lockRelId.relId = RelationGetRelid(relation);

	if (relation->rd_rel->relisshared)
		relation->rd_lockInfo.lockRelId.dbId = InvalidOid;
	else
		relation->rd_lockInfo.lockRelId.dbId = MyDatabaseId;
}

/*
 * SetLocktagRelationOid
 *		Set up a locktag for a relation, given only relation OID
 */
static inline void
SetLocktagRelationOid(LOCKTAG *tag, Oid relid)
{
	Oid			dbid;

	if (IsSharedRelation(relid))
		dbid = InvalidOid;
	else
		dbid = MyDatabaseId;

	SET_LOCKTAG_RELATION(*tag, dbid, relid);
}

/*
 *		LockRelationOid
 *
 * Lock a relation given only its OID.  This should generally be used
 * before attempting to open the relation's relcache entry.
 */
void
LockRelationOid(Oid relid, LOCKMODE lockmode)
{
	LOCKTAG		tag;
	LockAcquireResult res;

	SetLocktagRelationOid(&tag, relid);

	res = LockAcquire(&tag, lockmode, false, false);

	/*
	 * Now that we have the lock, check for invalidation messages, so that we
	 * will update or flush any stale relcache entry before we try to use it.
	 * RangeVarGetRelid() specifically relies on us for this.  We can skip
	 * this in the not-uncommon case that we already had the same type of lock
	 * being requested, since then no one else could have modified the
	 * relcache entry in an undesirable way.  (In the case where our own xact
	 * modifies the rel, the relcache update happens via
	 * CommandCounterIncrement, not here.)
	 */
	if (res != LOCKACQUIRE_ALREADY_HELD)
		AcceptInvalidationMessages();
}

/*
 *		ConditionalLockRelationOid
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 *
 * NOTE: we do not currently need conditional versions of all the
 * LockXXX routines in this file, but they could easily be added if needed.
 */
bool
ConditionalLockRelationOid(Oid relid, LOCKMODE lockmode)
{
	LOCKTAG		tag;
	LockAcquireResult res;

	SetLocktagRelationOid(&tag, relid);

	res = LockAcquire(&tag, lockmode, false, true);

	if (res == LOCKACQUIRE_NOT_AVAIL)
		return false;

	/*
	 * Now that we have the lock, check for invalidation messages; see notes
	 * in LockRelationOid.
	 */
	if (res != LOCKACQUIRE_ALREADY_HELD)
		AcceptInvalidationMessages();

	return true;
}

/*
 *		UnlockRelationId
 *
 * Unlock, given a LockRelId.  This is preferred over UnlockRelationOid
 * for speed reasons.
 */
void
UnlockRelationId(LockRelId *relid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

	LockRelease(&tag, lockmode, false);
}

/*
 *		UnlockRelationOid
 *
 * Unlock, given only a relation Oid.  Use UnlockRelationId if you can.
 */
void
UnlockRelationOid(Oid relid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SetLocktagRelationOid(&tag, relid);

	LockRelease(&tag, lockmode, false);
}

/*
 *		LockRelation
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
void
LockRelation(Relation relation, LOCKMODE lockmode)
{
	LOCKTAG		tag;
	LockAcquireResult res;

	SET_LOCKTAG_RELATION(tag,
						 relation->rd_lockInfo.lockRelId.dbId,
						 relation->rd_lockInfo.lockRelId.relId);

	res = LockAcquire(&tag, lockmode, false, false);

	/*
	 * Now that we have the lock, check for invalidation messages; see notes
	 * in LockRelationOid.
	 */
	if (res != LOCKACQUIRE_ALREADY_HELD)
		AcceptInvalidationMessages();
}

/*
 *		ConditionalLockRelation
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
bool
ConditionalLockRelation(Relation relation, LOCKMODE lockmode)
{
	LOCKTAG		tag;
	LockAcquireResult res;

	SET_LOCKTAG_RELATION(tag,
						 relation->rd_lockInfo.lockRelId.dbId,
						 relation->rd_lockInfo.lockRelId.relId);

	res = LockAcquire(&tag, lockmode, false, true);

	if (res == LOCKACQUIRE_NOT_AVAIL)
		return false;

	/*
	 * Now that we have the lock, check for invalidation messages; see notes
	 * in LockRelationOid.
	 */
	if (res != LOCKACQUIRE_ALREADY_HELD)
		AcceptInvalidationMessages();

	return true;
}

/*
 *		UnlockRelation
 *
 * This is a convenience routine for unlocking a relation without also
 * closing it.
 */
void
UnlockRelation(Relation relation, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_RELATION(tag,
						 relation->rd_lockInfo.lockRelId.dbId,
						 relation->rd_lockInfo.lockRelId.relId);

	LockRelease(&tag, lockmode, false);
}

/*
 *		LockHasWaitersRelation
 *
 * This is a function to check whether someone else is waiting for a
 * lock which we are currently holding.
 */
bool
LockHasWaitersRelation(Relation relation, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_RELATION(tag,
						 relation->rd_lockInfo.lockRelId.dbId,
						 relation->rd_lockInfo.lockRelId.relId);

	return LockHasWaiters(&tag, lockmode, false);
}

/*
 *		LockRelationIdForSession
 *
 * This routine grabs a session-level lock on the target relation.  The
 * session lock persists across transaction boundaries.  It will be removed
 * when UnlockRelationIdForSession() is called, or if an ereport(ERROR) occurs,
 * or if the backend exits.
 *
 * Note that one should also grab a transaction-level lock on the rel
 * in any transaction that actually uses the rel, to ensure that the
 * relcache entry is up to date.
 */
void
LockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

	(void) LockAcquire(&tag, lockmode, true, false);
}

/*
 *		UnlockRelationIdForSession
 */
void
UnlockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

	LockRelease(&tag, lockmode, true);
}

/*
 *		LockRelationForExtension
 *
 * This lock is used to interlock addition of pages to relations.
 * We need such locking because bufmgr/smgr definition of P_NEW is not
 * race-condition-proof.
 *
 * We assume the caller is already holding some type of regular lock on
 * the relation, so no AcceptInvalidationMessages call is needed here.
 */
void
LockRelationForExtension(Relation relation, LWLockMode lockmode)
{
	RELEXTLOCKTAG	locktag;
	LOCALRELEXTLOCK	*local_lock;
	bool	found;
	uint32	hashcode;

	locktag.relid = relation->rd_id;
	locktag.mode = lockmode;

	/* Do we have the lock already? */
	if (RelExtLockExists(&locktag))
		return;

	hashcode = RelExtLockTargetTagHashCode(&locktag);

	/* Acquire lock in local hash table */
	local_lock = (LOCALRELEXTLOCK *) hash_search_with_hash_value(LocalRelExtLockHash,
																 (void *) &locktag,
																 hashcode,
																 HASH_ENTER, &found);
	local_lock->held = true;

	/* Actually create the lock in shared hash table */
	CreateRelExtLock(&locktag, hashcode, lockmode, false);
}

/*
 *		ConditionalLockRelationForExtension
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool
ConditionalLockRelationForExtension(Relation relation, LWLockMode lockmode)
{
	RELEXTLOCKTAG	locktag;
	LOCALRELEXTLOCK	*local_lock;
	bool	found;
	uint32	hashcode;
	bool	ret;

	locktag.relid = relation->rd_id;
	locktag.mode = lockmode;

	/* Do we have the lock already? */
	if (RelExtLockExists(&locktag))
		return true;

	hashcode = RelExtLockTargetTagHashCode(&locktag);

	/* Acquire lock in local hash table, but we're not sure the result of acquire yet */
	local_lock = (LOCALRELEXTLOCK *) hash_search_with_hash_value(LocalRelExtLockHash,
																 (void *) &locktag,
																 hashcode,
																 HASH_ENTER, &found);
	ret = CreateRelExtLock(&locktag, hashcode, lockmode, true);
	local_lock->held = ret;

	return ret;
}

/*
 *		RelationExtensionLockWaiterCount
 *
 * Count the number of processes waiting for the given relation extension lock.
 * NOte that this routine doesn't acquire the partition lock. Please make sure
 * that the caller must acquire partitionlock in exclusive mode or we must call
 * this routine after acquired the relation extension lock of this relation.
 */
int
RelationExtensionLockWaiterCount(Relation relation)
{
	RELEXTLOCKTAG	locktag;
	RELEXTLOCK	*ext_lock;
	bool	found;
	int		nwaiters;
	uint32	hashcode;

	locktag.relid = relation->rd_id;
	locktag.mode = LW_EXCLUSIVE;
	hashcode = RelExtLockTargetTagHashCode(&locktag);

	ext_lock = (RELEXTLOCK *) hash_search_with_hash_value(RelExtLockHash,
														  (void *) &locktag,
														  hashcode,
														  HASH_FIND, &found);
	/* We assume that we already acquire this lock */
	Assert(found);

	nwaiters = LWLockWaiterCount(&(ext_lock->lock));

	return nwaiters;
}

/*
 *		UnlockRelationForExtension
 */
void
UnlockRelationForExtension(Relation relation, LWLockMode lockmode)
{
	RELEXTLOCKTAG	locktag;
	uint32	hashcode;
	bool	found;

	locktag.relid = relation->rd_id;
	locktag.mode = lockmode;

	/* Quick exit, if we don't acquire lock */
	if (!RelExtLockExists(&locktag))
		return;

	/* Remove hash entry from local hash table */
	hashcode = RelExtLockTargetTagHashCode(&locktag);
	hash_search_with_hash_value(LocalRelExtLockHash,
								(void *) &locktag,
								hashcode, HASH_REMOVE,
								&found);

	Assert(found);

	/* Actually remove the lock in shared hash table */
	DeleteRelExtLock(&locktag, hashcode);
}

/*
 *		LockPage
 *
 * Obtain a page-level lock.  This is currently used by some index access
 * methods to lock individual index pages.
 */
void
LockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 blkno);

	(void) LockAcquire(&tag, lockmode, false, false);
}

/*
 *		ConditionalLockPage
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool
ConditionalLockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 blkno);

	return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		UnlockPage
 */
void
UnlockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 blkno);

	LockRelease(&tag, lockmode, false);
}

/*
 *		LockTuple
 *
 * Obtain a tuple-level lock.  This is used in a less-than-intuitive fashion
 * because we can't afford to keep a separate lock in shared memory for every
 * tuple.  See heap_lock_tuple before using this!
 */
void
LockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_TUPLE(tag,
					  relation->rd_lockInfo.lockRelId.dbId,
					  relation->rd_lockInfo.lockRelId.relId,
					  ItemPointerGetBlockNumber(tid),
					  ItemPointerGetOffsetNumber(tid));

	(void) LockAcquire(&tag, lockmode, false, false);
}

/*
 *		ConditionalLockTuple
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool
ConditionalLockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_TUPLE(tag,
					  relation->rd_lockInfo.lockRelId.dbId,
					  relation->rd_lockInfo.lockRelId.relId,
					  ItemPointerGetBlockNumber(tid),
					  ItemPointerGetOffsetNumber(tid));

	return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		UnlockTuple
 */
void
UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_TUPLE(tag,
					  relation->rd_lockInfo.lockRelId.dbId,
					  relation->rd_lockInfo.lockRelId.relId,
					  ItemPointerGetBlockNumber(tid),
					  ItemPointerGetOffsetNumber(tid));

	LockRelease(&tag, lockmode, false);
}

/*
 *		XactLockTableInsert
 *
 * Insert a lock showing that the given transaction ID is running ---
 * this is done when an XID is acquired by a transaction or subtransaction.
 * The lock can then be used to wait for the transaction to finish.
 */
void
XactLockTableInsert(TransactionId xid)
{
	LOCKTAG		tag;

	SET_LOCKTAG_TRANSACTION(tag, xid);

	(void) LockAcquire(&tag, ExclusiveLock, false, false);
}

/*
 *		XactLockTableDelete
 *
 * Delete the lock showing that the given transaction ID is running.
 * (This is never used for main transaction IDs; those locks are only
 * released implicitly at transaction end.  But we do use it for subtrans IDs.)
 */
void
XactLockTableDelete(TransactionId xid)
{
	LOCKTAG		tag;

	SET_LOCKTAG_TRANSACTION(tag, xid);

	LockRelease(&tag, ExclusiveLock, false);
}

/*
 *		XactLockTableWait
 *
 * Wait for the specified transaction to commit or abort.  If an operation
 * is specified, an error context callback is set up.  If 'oper' is passed as
 * None, no error context callback is set up.
 *
 * Note that this does the right thing for subtransactions: if we wait on a
 * subtransaction, we will exit as soon as it aborts or its top parent commits.
 * It takes some extra work to ensure this, because to save on shared memory
 * the XID lock of a subtransaction is released when it ends, whether
 * successfully or unsuccessfully.  So we have to check if it's "still running"
 * and if so wait for its parent.
 */
void
XactLockTableWait(TransactionId xid, Relation rel, ItemPointer ctid,
				  XLTW_Oper oper)
{
	LOCKTAG		tag;
	XactLockTableWaitInfo info;
	ErrorContextCallback callback;

	/*
	 * If an operation is specified, set up our verbose error context
	 * callback.
	 */
	if (oper != XLTW_None)
	{
		Assert(RelationIsValid(rel));
		Assert(ItemPointerIsValid(ctid));

		info.rel = rel;
		info.ctid = ctid;
		info.oper = oper;

		callback.callback = XactLockTableWaitErrorCb;
		callback.arg = &info;
		callback.previous = error_context_stack;
		error_context_stack = &callback;
	}

	for (;;)
	{
		Assert(TransactionIdIsValid(xid));
		Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

		SET_LOCKTAG_TRANSACTION(tag, xid);

		(void) LockAcquire(&tag, ShareLock, false, false);

		LockRelease(&tag, ShareLock, false);

		if (!TransactionIdIsInProgress(xid))
			break;
		xid = SubTransGetParent(xid);
	}

	if (oper != XLTW_None)
		error_context_stack = callback.previous;
}

/*
 *		ConditionalXactLockTableWait
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE if the lock was acquired.
 */
bool
ConditionalXactLockTableWait(TransactionId xid)
{
	LOCKTAG		tag;

	for (;;)
	{
		Assert(TransactionIdIsValid(xid));
		Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

		SET_LOCKTAG_TRANSACTION(tag, xid);

		if (LockAcquire(&tag, ShareLock, false, true) == LOCKACQUIRE_NOT_AVAIL)
			return false;

		LockRelease(&tag, ShareLock, false);

		if (!TransactionIdIsInProgress(xid))
			break;
		xid = SubTransGetParent(xid);
	}

	return true;
}

/*
 *		SpeculativeInsertionLockAcquire
 *
 * Insert a lock showing that the given transaction ID is inserting a tuple,
 * but hasn't yet decided whether it's going to keep it.  The lock can then be
 * used to wait for the decision to go ahead with the insertion, or aborting
 * it.
 *
 * The token is used to distinguish multiple insertions by the same
 * transaction.  It is returned to caller.
 */
uint32
SpeculativeInsertionLockAcquire(TransactionId xid)
{
	LOCKTAG		tag;

	speculativeInsertionToken++;

	/*
	 * Check for wrap-around. Zero means no token is held, so don't use that.
	 */
	if (speculativeInsertionToken == 0)
		speculativeInsertionToken = 1;

	SET_LOCKTAG_SPECULATIVE_INSERTION(tag, xid, speculativeInsertionToken);

	(void) LockAcquire(&tag, ExclusiveLock, false, false);

	return speculativeInsertionToken;
}

/*
 *		SpeculativeInsertionLockRelease
 *
 * Delete the lock showing that the given transaction is speculatively
 * inserting a tuple.
 */
void
SpeculativeInsertionLockRelease(TransactionId xid)
{
	LOCKTAG		tag;

	SET_LOCKTAG_SPECULATIVE_INSERTION(tag, xid, speculativeInsertionToken);

	LockRelease(&tag, ExclusiveLock, false);
}

/*
 *		SpeculativeInsertionWait
 *
 * Wait for the specified transaction to finish or abort the insertion of a
 * tuple.
 */
void
SpeculativeInsertionWait(TransactionId xid, uint32 token)
{
	LOCKTAG		tag;

	SET_LOCKTAG_SPECULATIVE_INSERTION(tag, xid, token);

	Assert(TransactionIdIsValid(xid));
	Assert(token != 0);

	(void) LockAcquire(&tag, ShareLock, false, false);
	LockRelease(&tag, ShareLock, false);
}

/*
 * XactLockTableWaitErrorContextCb
 *		Error context callback for transaction lock waits.
 */
static void
XactLockTableWaitErrorCb(void *arg)
{
	XactLockTableWaitInfo *info = (XactLockTableWaitInfo *) arg;

	/*
	 * We would like to print schema name too, but that would require a
	 * syscache lookup.
	 */
	if (info->oper != XLTW_None &&
		ItemPointerIsValid(info->ctid) && RelationIsValid(info->rel))
	{
		const char *cxt;

		switch (info->oper)
		{
			case XLTW_Update:
				cxt = gettext_noop("while updating tuple (%u,%u) in relation \"%s\"");
				break;
			case XLTW_Delete:
				cxt = gettext_noop("while deleting tuple (%u,%u) in relation \"%s\"");
				break;
			case XLTW_Lock:
				cxt = gettext_noop("while locking tuple (%u,%u) in relation \"%s\"");
				break;
			case XLTW_LockUpdated:
				cxt = gettext_noop("while locking updated version (%u,%u) of tuple in relation \"%s\"");
				break;
			case XLTW_InsertIndex:
				cxt = gettext_noop("while inserting index tuple (%u,%u) in relation \"%s\"");
				break;
			case XLTW_InsertIndexUnique:
				cxt = gettext_noop("while checking uniqueness of tuple (%u,%u) in relation \"%s\"");
				break;
			case XLTW_FetchUpdated:
				cxt = gettext_noop("while rechecking updated tuple (%u,%u) in relation \"%s\"");
				break;
			case XLTW_RecheckExclusionConstr:
				cxt = gettext_noop("while checking exclusion constraint on tuple (%u,%u) in relation \"%s\"");
				break;

			default:
				return;
		}

		errcontext(cxt,
				   ItemPointerGetBlockNumber(info->ctid),
				   ItemPointerGetOffsetNumber(info->ctid),
				   RelationGetRelationName(info->rel));
	}
}

/*
 * WaitForLockersMultiple
 *		Wait until no transaction holds locks that conflict with the given
 *		locktags at the given lockmode.
 *
 * To do this, obtain the current list of lockers, and wait on their VXIDs
 * until they are finished.
 *
 * Note we don't try to acquire the locks on the given locktags, only the VXIDs
 * of its lock holders; if somebody grabs a conflicting lock on the objects
 * after we obtained our initial list of lockers, we will not wait for them.
 */
void
WaitForLockersMultiple(List *locktags, LOCKMODE lockmode)
{
	List	   *holders = NIL;
	ListCell   *lc;

	/* Done if no locks to wait for */
	if (list_length(locktags) == 0)
		return;

	/* Collect the transactions we need to wait on */
	foreach(lc, locktags)
	{
		LOCKTAG    *locktag = lfirst(lc);

		holders = lappend(holders, GetLockConflicts(locktag, lockmode));
	}

	/*
	 * Note: GetLockConflicts() never reports our own xid, hence we need not
	 * check for that.  Also, prepared xacts are not reported, which is fine
	 * since they certainly aren't going to do anything anymore.
	 */

	/* Finally wait for each such transaction to complete */
	foreach(lc, holders)
	{
		VirtualTransactionId *lockholders = lfirst(lc);

		while (VirtualTransactionIdIsValid(*lockholders))
		{
			VirtualXactLock(*lockholders, true);
			lockholders++;
		}
	}

	list_free_deep(holders);
}

/*
 * WaitForLockers
 *
 * Same as WaitForLockersMultiple, for a single lock tag.
 */
void
WaitForLockers(LOCKTAG heaplocktag, LOCKMODE lockmode)
{
	List	   *l;

	l = list_make1(&heaplocktag);
	WaitForLockersMultiple(l, lockmode);
	list_free(l);
}


/*
 *		LockDatabaseObject
 *
 * Obtain a lock on a general object of the current database.  Don't use
 * this for shared objects (such as tablespaces).  It's unwise to apply it
 * to relations, also, since a lock taken this way will NOT conflict with
 * locks taken via LockRelation and friends.
 */
void
LockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
				   LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_OBJECT(tag,
					   MyDatabaseId,
					   classid,
					   objid,
					   objsubid);

	(void) LockAcquire(&tag, lockmode, false, false);

	/* Make sure syscaches are up-to-date with any changes we waited for */
	AcceptInvalidationMessages();
}

/*
 *		UnlockDatabaseObject
 */
void
UnlockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
					 LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_OBJECT(tag,
					   MyDatabaseId,
					   classid,
					   objid,
					   objsubid);

	LockRelease(&tag, lockmode, false);
}

/*
 *		LockSharedObject
 *
 * Obtain a lock on a shared-across-databases object.
 */
void
LockSharedObject(Oid classid, Oid objid, uint16 objsubid,
				 LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_OBJECT(tag,
					   InvalidOid,
					   classid,
					   objid,
					   objsubid);

	(void) LockAcquire(&tag, lockmode, false, false);

	/* Make sure syscaches are up-to-date with any changes we waited for */
	AcceptInvalidationMessages();
}

/*
 *		UnlockSharedObject
 */
void
UnlockSharedObject(Oid classid, Oid objid, uint16 objsubid,
				   LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_OBJECT(tag,
					   InvalidOid,
					   classid,
					   objid,
					   objsubid);

	LockRelease(&tag, lockmode, false);
}

/*
 *		LockSharedObjectForSession
 *
 * Obtain a session-level lock on a shared-across-databases object.
 * See LockRelationIdForSession for notes about session-level locks.
 */
void
LockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid,
						   LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_OBJECT(tag,
					   InvalidOid,
					   classid,
					   objid,
					   objsubid);

	(void) LockAcquire(&tag, lockmode, true, false);
}

/*
 *		UnlockSharedObjectForSession
 */
void
UnlockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid,
							 LOCKMODE lockmode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_OBJECT(tag,
					   InvalidOid,
					   classid,
					   objid,
					   objsubid);

	LockRelease(&tag, lockmode, true);
}


/*
 * Append a description of a lockable object to buf.
 *
 * Ideally we would print names for the numeric values, but that requires
 * getting locks on system tables, which might cause problems since this is
 * typically used to report deadlock situations.
 */
void
DescribeLockTag(StringInfo buf, const LOCKTAG *tag)
{
	switch ((LockTagType) tag->locktag_type)
	{
		case LOCKTAG_RELATION:
			appendStringInfo(buf,
							 _("relation %u of database %u"),
							 tag->locktag_field2,
							 tag->locktag_field1);
			break;
		case LOCKTAG_PAGE:
			appendStringInfo(buf,
							 _("page %u of relation %u of database %u"),
							 tag->locktag_field3,
							 tag->locktag_field2,
							 tag->locktag_field1);
			break;
		case LOCKTAG_TUPLE:
			appendStringInfo(buf,
							 _("tuple (%u,%u) of relation %u of database %u"),
							 tag->locktag_field3,
							 tag->locktag_field4,
							 tag->locktag_field2,
							 tag->locktag_field1);
			break;
		case LOCKTAG_TRANSACTION:
			appendStringInfo(buf,
							 _("transaction %u"),
							 tag->locktag_field1);
			break;
		case LOCKTAG_VIRTUALTRANSACTION:
			appendStringInfo(buf,
							 _("virtual transaction %d/%u"),
							 tag->locktag_field1,
							 tag->locktag_field2);
			break;
		case LOCKTAG_SPECULATIVE_TOKEN:
			appendStringInfo(buf,
							 _("speculative token %u of transaction %u"),
							 tag->locktag_field2,
							 tag->locktag_field1);
			break;
		case LOCKTAG_OBJECT:
			appendStringInfo(buf,
							 _("object %u of class %u of database %u"),
							 tag->locktag_field3,
							 tag->locktag_field2,
							 tag->locktag_field1);
			break;
		case LOCKTAG_USERLOCK:
			/* reserved for old contrib code, now on pgfoundry */
			appendStringInfo(buf,
							 _("user lock [%u,%u,%u]"),
							 tag->locktag_field1,
							 tag->locktag_field2,
							 tag->locktag_field3);
			break;
		case LOCKTAG_ADVISORY:
			appendStringInfo(buf,
							 _("advisory lock [%u,%u,%u,%u]"),
							 tag->locktag_field1,
							 tag->locktag_field2,
							 tag->locktag_field3,
							 tag->locktag_field4);
			break;
		default:
			appendStringInfo(buf,
							 _("unrecognized locktag type %d"),
							 (int) tag->locktag_type);
			break;
	}
}

/*
 * GetLockNameFromTagType
 *
 *	Given locktag type, return the corresponding lock name.
 */
const char *
GetLockNameFromTagType(uint16 locktag_type)
{
	if (locktag_type > LOCKTAG_LAST_TYPE)
		return "???";
	return LockTagTypeNames[locktag_type];
}


/*
 * Check whether a particular relation extension lock is held by this transaction.
 *
 * Note that this function may return false even when it fhe lock exists in local
 * hash table, because the conditional relation extension lock doesn't remove the
 * local hash entry even when failed to acquire lock.
 */
static bool
RelExtLockExists(const RELEXTLOCKTAG *targettag)
{
	LOCALRELEXTLOCK	*lock;
	uint32	hashcode;

	hashcode = RelExtLockTargetTagHashCode(targettag);

	lock = (LOCALRELEXTLOCK *) hash_search_with_hash_value(LocalRelExtLockHash,
														   (void *) targettag,
														   hashcode, HASH_FIND, NULL);

	if (!lock)
		return false;

	/*
	 * Found entry in the table, but still need to check whether it's actually
	 * held -- it could be just created when acquiring conditional lock.
	 */
	return lock->held;
}

/*
 * Create RELEXTLOCK hash entry on shared hash table. To avoid dead-lock with
 * partition lock and LWLock, we acquire them but don't release it here. The
 * caller must call DeleteRelExtLock later to release these locks.
 */
static bool
CreateRelExtLock(const RELEXTLOCKTAG *targettag, uint32 hashcode, LWLockMode lockmode,
				 bool conditional)
{
	RELEXTLOCK	*ext_lock;
	LWLock	*partitionLock;
	bool	found;
	bool	ret = false;

	partitionLock = RelExtLockHashPartitionLock(hashcode);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	ext_lock = (RELEXTLOCK *) hash_search_with_hash_value(RelExtLockHash,
														  (void * ) targettag,
														  hashcode, HASH_ENTER, &found);

	if (!ext_lock)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory"),
				 errhint("You might need to increase max_pred_locks_per_transaction.")));

	/* This is a new hash entry, initialize it */
	if (!found)
		LWLockInitialize(&(ext_lock->lock), LWTRANCHE_RELEXT_LOCK_MANAGER);

	if (conditional)
		ret = LWLockConditionalAcquire(&(ext_lock->lock), lockmode);
	else
		ret = LWLockAcquire(&(ext_lock->lock), lockmode);

	/* Always return true if not conditional lock */
	return ret;
}

/*
 * Remove RELEXTLOCK from shared RelExtLockHash hash table. Since other backends
 * might be acquiring it or waiting for this lock, we can delete it only if there
 * is no longer backends who are interested in it.
 *
 * Note that we assume partition lock for hash table is already acquired when
 * acquiring the lock. This routine should release partition lock as well after
 * released LWLock.
 */
static void
DeleteRelExtLock(const RELEXTLOCKTAG *targettag, uint32 hashcode)
{
	RELEXTLOCK	*ext_lock;
	LOCALRELEXTLOCK	*lock;
	LWLock	*partitionLock;
	bool	found;

	partitionLock = RelExtLockHashPartitionLock(hashcode);

	ext_lock = (RELEXTLOCK *) hash_search_with_hash_value(RelExtLockHash,
														  (void * ) targettag,
														  hashcode,
														  HASH_FIND, &found);

	if (!found)
		return;

	/*
	 * Remove this hash entry if there is no longer someone who is interested
	 * in extension lock of this relation.
	 */
	if (LWLockCheckForCleanup(&(ext_lock->lock)))
		hash_search_with_hash_value(RelExtLockHash, (void *) targettag,
									hashcode, HASH_REMOVE, &found);

	LWLockRelease(&(ext_lock->lock));
	LWLockRelease(partitionLock);
}
