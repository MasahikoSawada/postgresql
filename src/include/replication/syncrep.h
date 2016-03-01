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
#define SYNC_REP_METHOD_PRIORITY	0

/* SyncGroupNode */
#define SYNC_REP_GROUP_NAME			0x01
#define SYNC_REP_GROUP_GROUP		0x02

#define SyncStandbysDefined() \
	(SyncRepStandbyNamesString != NULL && SyncRepStandbyNamesString[0] != '\0')

struct SyncGroupNode;
typedef struct SyncGroupNode SyncGroupNode;

struct	SyncGroupNode
{
	/* Common information */
	int		type;
	char	*name;
	SyncGroupNode	*next; /* Same group, next name node */

	/* For group ndoe */
	int sync_method; /* priority */
	int	sync_num;
	int	member_num;
	SyncGroupNode	*members; /* member of its group */
	bool (*SyncRepGetSyncedLsnsFn) (SyncGroupNode *group, XLogRecPtr *write_pos,
									XLogRecPtr *flush_pos);
	int (*SyncRepGetSyncStandbysFn) (SyncGroupNode *group, int *list);
};

/* user-settable parameters for synchronous replication */
extern SyncGroupNode *SyncRepStandbys;
extern char	*SyncRepStandbyNamesString;

/* called by user backend */
extern void SyncRepWaitForLSN(XLogRecPtr XactCommitLSN);

/* called at backend exit */
extern void SyncRepCleanupAtProcExit(void);

/* called by wal sender */
extern void SyncRepInitConfig(void);
extern void SyncRepReleaseWaiters(void);

/* called by checkpointer */
extern void SyncRepUpdateSyncStandbysDefined(void);

extern bool check_synchronous_standby_names(char **newval, void **extra, GucSource source);
extern void assign_synchronous_standby_names(const char *newval, void *extra);
extern void assign_synchronous_commit(int newval, void *extra);

/*
 * Internal functions for parsing the replication grammar, in syncgroup_gram.y and
 * syncgroup_scanner.l
 */
extern int  syncgroup_yyparse(void);
extern int  syncgroup_yylex(void);
extern void syncgroup_yyerror(const char *str);
extern void syncgroup_scanner_init(const char *query_string);
extern void syncgroup_scanner_finish(void);
extern void SyncRepClearStandbyGroupList(SyncGroupNode *group);

/* function for synchronous replication group */
extern bool SyncRepGetSyncedLsnsUsingPriority(SyncGroupNode *group,
											  XLogRecPtr *write_pos, XLogRecPtr *flush_pos);
extern int SyncRepGetSyncStandbysUsingPriority(SyncGroupNode *group, int *sync_list);

#endif   /* _SYNCREP_H */
