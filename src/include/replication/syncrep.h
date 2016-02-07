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

/* SyncRepGroupNodeType */

/* user-settable parameters for synchronous replication */
extern char *SyncRepStandbyNames;

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

extern bool check_synchronous_standby_names(char **newval, void **extra, GucSource source);
extern void assign_synchronous_commit(int newval, void *extra);
extern bool	check_synchronous_standby_group(char **newval, void **extra, GucSource source);

/*
 * Internal functions for parsing the replication grammar, in syncgroup_gram.y and
 * syncgroup_scanner.l
 */
extern int  syncgroup_yyparse(void);
extern int  syncgroup_yylex(void);
extern void syncgroup_yyerror(const char *str) pg_attribute_noreturn();
extern void syncgroup_scanner_init(const char *query_string);
extern void syncgroup_scanner_finish(void);

extern bool SyncRepSyncedLsnAdvancedTo(XLogRecPtr *write_pos, XLogRecPtr *flush_pos);
extern int *SyncRepGetSyncedLsns(XLogRecPtr *write_pos, XLogRecPtr *flush_pos);

typedef enum SyncGroupNodeType
{
	SYNCGROUP_NAME,
	SYNCGROUP_GROUP
}	SyncGroupNodeType;

struct SyncGroupNode;
typedef struct SyncGroupNode SyncGroupNode;

struct	SyncGroupNode
{
	SyncGroupNodeType	type;
	char	*name;

	/* For name node */
	SyncGroupNode *next;

	/* For group ndoe */
	int	wait_num;
	SyncGroupNode	*member;
	int *(*SyncRepGetSyncedLsnsFn) (XLogRecPtr *write_pos, XLogRecPtr *flush_pos);
	/* XXX : Do group node need to have a funciton to get priority? */
};

extern SyncGroupNode *SyncRepStandbyGroup;
extern char *SyncRepStandbyGroupString;

#endif   /* _SYNCREP_H */
