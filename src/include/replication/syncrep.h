/*-------------------------------------------------------------------------
 *
 * syncrep.h
 *	  Exports from replication/syncrep.c.
 *
 * Portions Copyright (c) 2010-2015, PostgreSQL Global Development Group
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
#include "utils/jsonapi.h"

#define SyncRepRequested() \
	(max_wal_senders > 0 && synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)

/* SyncRepFile */
#define SYNC_FILENAME   "pg_syncinfo.conf"

/* SyncRepWaitMode */
#define SYNC_REP_NO_WAIT		-1
#define SYNC_REP_WAIT_WRITE		0
#define SYNC_REP_WAIT_FLUSH		1

#define NUM_SYNC_REP_WAIT_MODE	2

/* syncRepState */
#define SYNC_REP_NOT_WAITING		0
#define SYNC_REP_WAITING			1
#define SYNC_REP_WAIT_COMPLETE		2

/* user-settable parameters for synchronous replication */
extern char *SyncRepStandbyNames;

typedef enum SyncInfoNodeType
{
   GNODE_NAME,
   GNODE_GROUP
}  SyncInfoNodeType;

//typedef struct SyncInfoNode SyncInfoNode;

typedef struct SyncInfoNode
{
   SyncInfoNodeType gtype;
   char       *name;
   bool        priority_group;
   int         count;
   int         ngroups;
   struct SyncInfoNode  *group;
   struct SyncInfoNode  *next;
} SyncInfoNode;

typedef enum
{
	SYNC_INFO_MAIN_OBJECT_START,
	SYNC_INFO_MAIN,
	SYNC_INFO_NODES,
	SYNC_INFO_OBJECT_START,
	SYNC_INFO_COUNT,
	SYNC_INFO_GROUP_NAME,
	SYNC_INFO_GROUPS,
	SYNC_INFO_ARRAY_START,
	SYNC_INFO_GROUP_INFO
}SyncParseStateName;

extern SyncInfoNode *SyncRepStandbyInfo;

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
extern struct WalSnd *SyncRepGetSynchronousStandby(void);

extern bool CheckNameList(SyncInfoNode *expr, char *name, bool found);
extern XLogRecPtr *SyncRepGetQuorumRecPtr(SyncInfoNode *node, List **lsnlist, bool set_prioirty);

extern bool check_synchronous_standby_names(char **newval, void **extra, GucSource source);
extern void assign_synchronous_commit(int newval, void *extra);

#endif   /* _SYNCREP_H */
