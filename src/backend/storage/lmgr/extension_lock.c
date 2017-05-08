/*-------------------------------------------------------------------------
 *
 * extension_lock.c
 *	  Relation extension lock manager
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * src/backend/storage/lmgr/extension_lock.c
 *
 * NOTES:
 *
 * This lock manager is specialized in relation extension locks; light
 * weight and interruptible lock manager. It's similar to heavy-weight
 * lock but doesn't have dead lock detection mechanism, group locking
 * mechanism and multiple lock modes.
 *
 * The entries for relation extension locks are allocated on the shared
 * memory as an array. The pair of database id and relation id maps to
 * one of them by hashing.
 *
 * For lock acquisition we use an atomic compare-and-exchange on the
 * state variable. When a process tries to acquire a lock that conflicts
 * with existing lock, it is put to sleep using condition variables
 * if not conditional locking. When release the lock, we use an atomic
 * decrement to release the lock.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "storage/extension_lock.h"
#include "utils/rel.h"

/* The total entries of relation extension lock on shared memory */
#define N_RELEXTLOCK_ENTS 1024

#define RELEXT_LOCK_BIT		((uint32) ((1 << 25)))

/* Must be greater than MAX_BACKENDS - which is 2^23-1, so we're fine. */
#define RELEXT_WAIT_COUNT_MASK	((uint32) ((1 << 24) - 1))

/* This tag maps to one of entries on the RelExtLockArray array by hashing */
typedef struct RelExtLockTag
{
	Oid		dbid;
	Oid		relid;
} RelExtLockTag;

typedef struct RelExtLock
{
	pg_atomic_uint32	state; 	/* state of exclusive lock */
	ConditionVariable	cv;
} RelExtLock;

/*
 * This structure holds information per-object relation extension
 * lock. "lock" variable represents the RelExtLockArray we are
 * holding, waiting for or had been holding before. If we're holding
 * a relation extension lock on a relation, nLocks > 0. nLocks == 0
 * means that we don't hold any locks. We use this structure to keep
 * track of holding relation extension locks, and to also store it
 * as a cache. So when releasing the lock we don't invalidate the lock
 * variable. We check the cache first, and then use it without touching
 * RelExtLockArray if the relation extension lock is the same as what
 * we just touched.
 *
 * At most one lock can be held at once. Note that sometimes we
 * could try to acquire a lock for the additional forks while holding
 * the lock for the main fork; for example, adding extra relation
 * blocks for both relation and its free space map. But since this
 * lock manager doesn't distinguish between the forks, we just
 * increment nLocks in the case.
 */
typedef	struct relextlock_handle
{
	Oid				relid;
	RelExtLock		*lock;
	int				nLocks;		/* > 0 means holding it */
	bool			waiting;	/* true if we're waiting it */
} relextlock_handle;

static relextlock_handle held_relextlock;

/* Pointer to array containing relation extension lock states */
static RelExtLock *RelExtLockArray;

static bool RelExtLockAcquire(Oid relid, bool conditional);
static void RelExtLockRelease(Oid rleid, bool force);
static bool RelExtLockAttemptLock(RelExtLock *relextlock);
static inline uint32 RelExtLockTargetTagToIndex(RelExtLockTag *locktag);

Size
RelExtLockShmemSize(void)
{
	/* Relation extension locks array */
	return mul_size(N_RELEXTLOCK_ENTS, sizeof(RelExtLock));
}

/*
 * InitRelExtLock
 *      Initialize the relation extension lock manager's data structures.
 */
void
InitRelExtLocks(void)
{
	Size	size;
	bool	found;
	int		i;

	size = mul_size(N_RELEXTLOCK_ENTS, sizeof(RelExtLock));
	RelExtLockArray = (RelExtLock *)
		ShmemInitStruct("Relation Extension Lock", size, &found);

	/* we're the first - initialize */
	if (!found)
	{
		for (i = 0; i < N_RELEXTLOCK_ENTS; i++)
		{
			RelExtLock *relextlock = &RelExtLockArray[i];

			pg_atomic_init_u32(&(relextlock->state), 0);
			ConditionVariableInit(&(relextlock->cv));
		}
	}
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
LockRelationForExtension(Relation relation)
{
	RelExtLockAcquire(relation->rd_id, false);
}

/*
 *		ConditionalLockRelationForExtension
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool
ConditionalLockRelationForExtension(Relation relation)
{
	return RelExtLockAcquire(relation->rd_id, true);
}

/*
 *		RelationExtensionLockWaiterCount
 *
 * Count the number of processes waiting for the given relation extension
 * lock. Note that since the lock for multiple relations uses the same
 * RelExtLock entry, the return value might not be accurate.
 */
int
RelationExtensionLockWaiterCount(Relation relation)
{
	RelExtLockTag tag;
	RelExtLock	*relextlock;
	uint32		state;

	/* Make a lock tag */
	tag.dbid = MyDatabaseId;
	tag.relid = RelationGetRelid(relation);

	relextlock = &RelExtLockArray[RelExtLockTargetTagToIndex(&tag)];
	state = pg_atomic_read_u32(&(relextlock->state));

	return (state & RELEXT_WAIT_COUNT_MASK);
}

/*
 *		UnlockRelationForExtension
 */
void
UnlockRelationForExtension(Relation relation)
{
	RelExtLockRelease(relation->rd_id, false);
}

/*
 *		RelationExtensionLockReleaseAll
 *
 * release all currently-held relation extension locks
 */
void
RelExtLockReleaseAll(void)
{
	if (held_relextlock.nLocks > 0)
		RelExtLockRelease(held_relextlock.relid, true);
	else if (held_relextlock.waiting)
	{
		/*
		 * Decrement the ref counts if we don't hold the lock but
		 * was waiting for the lock.
		 */
		pg_atomic_sub_fetch_u32(&(held_relextlock.lock->state), 1);
	}
}

/*
 *		IsAnyRelationExtensionLockHeld
 *
 * Return true if we're holding relation extension locks.
 */
bool
IsAnyRelationExtensionLockHeld(void)
{
	return held_relextlock.nLocks > 0;
}

/*
 *		WaitForRelationExtensionLockToBeFree
 *
 * Wait for the relation extension lock on the given relation to
 * be free without acquiring it.
 */
void
WaitForRelationExtensionLockToBeFree(Relation relation)
{
	RelExtLock	*relextlock;
	Oid		relid;

	relid = RelationGetRelid(relation);

	/* If we already hold the lock, no need to wait */
	if (held_relextlock.nLocks > 0 && relid == held_relextlock.relid)
		return;

	/*
	 * If the last relation extension lock we touched is the same
	 * one for which we now need to wait, we can use our cached
	 * pointer to the lock instead of recomputing it.
	 */
	if (relid == held_relextlock.relid)
		relextlock = held_relextlock.lock;
	else
	{
		RelExtLockTag tag;

		/* Make a lock tag */
		tag.dbid = MyDatabaseId;
		tag.relid = relid;

		relextlock = &RelExtLockArray[RelExtLockTargetTagToIndex(&tag)];

		/* Remember the lock we're interested in */
		held_relextlock.relid = relid;
		held_relextlock.lock = relextlock;
	}

	for (;;)
	{
		uint32	state;

		state = pg_atomic_read_u32(&(relextlock)->state);

		/* Break if nobody is holding the lock on this relation */
		if ((state & RELEXT_LOCK_BIT) == 0)
			break;

		/* Could not get the lock, prepare to wait */
		if (!held_relextlock.waiting)
		{
			pg_atomic_add_fetch_u32(&(relextlock->state), 1);
			held_relextlock.waiting = true;
		}

		/* Sleep until the lock is released */
		ConditionVariableSleep(&(relextlock->cv),
							   WAIT_EVENT_RELATION_EXTENSION_LOCK);
	}

	ConditionVariableCancelSleep();

	/* Release any wait count we hold */
	if (held_relextlock.waiting)
	{
		pg_atomic_sub_fetch_u32(&(relextlock->state), 1);
		held_relextlock.waiting = false;
	}

	return;
}

/*
 * Compute the hash code associated with a RelExtLock.
 *
 * To avoid unnecessary recomputations of the hash code, we try to do this
 * just once per function, and then pass it around as needed.  we can
 * extract the index number of RelExtLockArray.
 */
static inline uint32
RelExtLockTargetTagToIndex(RelExtLockTag *locktag)
{
	return (tag_hash((const void *) locktag, sizeof(RelExtLockTag))
			% N_RELEXTLOCK_ENTS);
}

/*
 * Acquire a relation extension lock.
 */
static bool
RelExtLockAcquire(Oid relid, bool conditional)
{
	RelExtLock	*relextlock;
	bool	mustwait;

	/*
	 * If we already hold the lock, we can just increase the count locally.
	 * Since we don't do deadlock detection, caller must not try to take a
	 * new relation extension lock while already holding them.
	 */
	if (held_relextlock.nLocks > 0)
	{
		if (relid != held_relextlock.relid)
			elog(ERROR,
				 "cannot acquire relation extension locks for multiple relations at the same");

		held_relextlock.nLocks++;
		return true;
	}

	/*
	 * If the last relation extension lock we touched is the same one for
	 * we now need to acquire, we can use our cached pointer to the lock
	 * instead of recomputing it.  This is likely to be a common case in
	 * practice.
	 */
	if (relid == held_relextlock.relid)
		relextlock = held_relextlock.lock;
	else
	{
		RelExtLockTag tag;

		/* Make a lock tag */
		tag.dbid = MyDatabaseId;
		tag.relid = relid;

		relextlock = &RelExtLockArray[RelExtLockTargetTagToIndex(&tag)];

		/* Remeber the lock we're interested in */
		held_relextlock.relid = relid;
		held_relextlock.lock = relextlock;
	}

	held_relextlock.waiting = false;
	for (;;)
	{
		mustwait = RelExtLockAttemptLock(relextlock);

		if (!mustwait)
			break;	/* got the lock */

		/* Could not got the lock, return iff in locking conditionally */
		if (conditional)
			return false;

		/* Could not get the lock, prepare to wait */
		if (!held_relextlock.waiting)
		{
			pg_atomic_add_fetch_u32(&(relextlock->state), 1);
			held_relextlock.waiting = true;
		}

		/* Sleep until the lock is released */
		ConditionVariableSleep(&(relextlock->cv),
							   WAIT_EVENT_RELATION_EXTENSION_LOCK);
	}

	ConditionVariableCancelSleep();

	/* Release any wait count we hold */
	if (held_relextlock.waiting)
	{
		pg_atomic_sub_fetch_u32(&(relextlock->state), 1);
		held_relextlock.waiting = false;
	}

	Assert(!mustwait);

	/* Remember lock held by this backend */
	held_relextlock.relid = relid;
	held_relextlock.lock = relextlock;
	held_relextlock.nLocks = 1;

	/* We got the lock! */
	return true;
}

/*
 * RelExtLockRelease
 *
 * Release a previously acquired relation extension lock. If force is
 * true, we release the all holding locks on the given relation.
 */
static void
RelExtLockRelease(Oid relid, bool force)
{
	RelExtLock	*relextlock;
	uint32	state;
	uint32	wait_counts;

	/* We should have acquired a lock before releasing */
	Assert(held_relextlock.nLocks > 0);

	if (relid != held_relextlock.relid)
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("relation extension lock for %u is not held",
						relid)));

	/* If force releasing, release all locks we're holding */
	if (force)
		held_relextlock.nLocks = 0;
	else
		held_relextlock.nLocks--;

	Assert(held_relextlock.nLocks >= 0);

	/* Return if we're still holding the lock even after computation */
	if (held_relextlock.nLocks > 0)
		return;

	relextlock = held_relextlock.lock;

	/* Release the lock */
	state = pg_atomic_sub_fetch_u32(&(relextlock->state), RELEXT_LOCK_BIT);

	/* If there may be waiters, wake them up */
	wait_counts = state & RELEXT_WAIT_COUNT_MASK;

	if (wait_counts > 0)
		ConditionVariableBroadcast(&(relextlock->cv));
}

/*
 * Internal function that attempts to atomically acquire the relation
 * extension lock.
 *
 * Returns true if the lock isn't free and we need to wait.
 */
static bool
RelExtLockAttemptLock(RelExtLock *relextlock)
{
	uint32	oldstate;

	oldstate = pg_atomic_read_u32(&relextlock->state);

	while (true)
	{
		uint32	desired_state;
		bool	lock_free;

		desired_state = oldstate;

		lock_free = (oldstate & RELEXT_LOCK_BIT) == 0;
		if (lock_free)
			desired_state += RELEXT_LOCK_BIT;

		if (pg_atomic_compare_exchange_u32(&relextlock->state,
										   &oldstate, desired_state))
		{
			if (lock_free)
				return false;
			else
				return true;
		}
	}
	pg_unreachable();
}
