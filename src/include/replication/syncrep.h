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

/* user-settable parameters for synchronous replication */
/* extern char *SyncRepStandbyNames; */

/*
 * Internal functions for parsing the replication grammar, in guc_gram.y and
 * guc_scanner.l
  */
extern int  repl_guc_yyparse(void);
extern int  repl_guc_yylex(void);
extern void repl_guc_yyerror(const char *str) pg_attribute_noreturn();
extern void repl_guc_scanner_init(const char *query_string);
extern void repl_guc_scanner_finish(void);


typedef enum GroupNodeType
{
	GNODE_NAME,
	GNODE_GROUP
}	GroupNodeType;

struct GroupNode;
typedef struct GroupNode GroupNode;


extern GroupNode *SyncRepStandbyNames;
extern char *SyncRepStandbyNameString;

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
extern XLogRecPtr *GetSyncStandbysRecPtr(GroupNode *node, List **xlogrecptr);
extern bool check_synchronous_standby_names(char **newval, void **extra, GucSource source);
extern void assign_synchronous_commit(int newval, void *extra);
extern bool CheckNameList(GroupNode *expr, char *name, bool found);

struct GroupNode
{
	/* For Common */
	GroupNodeType gtype;
	XLogRecPtr write;
	XLogRecPtr flush;

	/* For NAME */
	char *name;
	struct WalSnd	*walsnd;
	GroupNode	*next;
	
	/* For GROUP */
	int			quorum;
	int			ngroups;
	GroupNode	*group;
};


#endif   /* _SYNCREP_H */
