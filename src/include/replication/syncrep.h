/*-------------------------------------------------------------------------
 *
 * syncrep.h
 *	  Exports from replication/syncrep.c.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/replication/syncrep.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SYNCREP_H
#define _SYNCREP_H

#include "access/xlogdefs.h"
#include "utils/guc.h"

#define SyncRepRequested() \
	(max_wal_senders > 0 && synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)

/* SyncRepWaitMode */
#define SYNC_REP_NO_WAIT		-1
#define SYNC_REP_WAIT_WRITE		0
#define SYNC_REP_WAIT_FLUSH		1

#define NUM_SYNC_REP_WAIT_MODE	2

/* syncRepState */
#define SYNC_REP_NOT_WAITING		0
#define SYNC_REP_WAITING			1
#define SYNC_REP_WAIT_COMPLETE		2

/* SyncRepMethod */
#define SYNC_REP_METHOD_1_PRIORITY	0
#define SYNC_REP_METHOD_PRIORITY	1

/* user-settable parameters for synchronous replication */
extern char *SyncRepStandbyNames;
extern int	synchronous_replication_method;
extern int	synchronous_standby_num;

/* called by user backend */
extern void SyncRepWaitForLSN(XLogRecPtr XactCommitLSN);

/* called at backend exit */
extern void SyncRepCleanupAtProcExit(void);

/* called by wal sender */
extern void SyncRepInitConfig(void);
extern void SyncRepReleaseWaiters(void);

/* called by checkpointer */
extern void SyncRepUpdateSyncStandbysDefined(void);

/* forward declaration to avoid pulling in walsender_private.h */
struct WalSnd;

extern int SyncRepGetSyncStandbys(int *sync_standbys);
extern bool SyncRepSyncedLsnAdvancedTo(XLogRecPtr *write_pos, XLogRecPtr *flush_pos);
extern bool SyncRepActiveListedWalSender(int num);

/* '1-priority' and 'priority' method */
extern int SyncRepGetSyncStandbysPriority(int *sync_standbys);
extern bool	SyncRepGetSyncLsnsPriority(XLogRecPtr *write_pos, XLogRecPtr *flush_pos);

extern bool check_synchronous_standby_names(char **newval, void **extra, GucSource source);
extern void assign_synchronous_commit(int newval, void *extra);

/* Process configuration parameter related to synchronous replication */
extern void ProcessSynchronousReplicationConfig(void);

#endif   /* _SYNCREP_H */
