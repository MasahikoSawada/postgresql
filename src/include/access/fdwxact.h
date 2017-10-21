/*
 * fdwxact.h
 *
 * PostgreSQL distributed transaction manager
 *
 * Portions Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact.h
 */
#ifndef FDW_XACT_H
#define FDW_XACT_H

#include "access/xlogreader.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/backendid.h"
#include "storage/shmem.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

#define	FDW_XACT_NOT_WAITING		0
#define	FDW_XACT_WAITING			1
#define	FDW_XACT_WAIT_COMPLETE		2

#define FDW_XACT_ID_LEN (2 + 1 + 8 + 1 + 8 + 1 + 8)
#define FdwXactEnabled() (max_prepared_foreign_xacts > 0)

/* Shared memory entry for a prepared or being prepared foreign transaction */
typedef struct FdwXactData *FdwXact;

/* Enum to track the status of prepared foreign transaction */
typedef enum
{
	FDW_XACT_PREPARING,			/* foreign transaction is (being) prepared */
	FDW_XACT_COMMITTING_PREPARED,		/* foreign prepared transaction is to
										 * be committed */
	FDW_XACT_ABORTING_PREPARED, /* foreign prepared transaction is to be
								 * aborted */
	FDW_XACT_RESOLVED
} FdwXactStatus;

typedef struct FdwXactData
{
	FdwXact		fx_free_next;	/* Next free FdwXact entry */
	FdwXact		fx_next;		/* Next FdwXact entry accosiated with the same
								   transaction */
	Oid			dboid;			/* database oid where to find foreign server
								 * and user mapping */
	TransactionId local_xid;	/* XID of local transaction */
	Oid			serverid;		/* foreign server where transaction takes
								 * place */
	Oid			userid;			/* user who initiated the foreign transaction */
	Oid			umid;			/* user mapping id for connection key */
	FdwXactStatus status;		/* The state of the foreign
								 * transaction. This doubles as the
								 * action to be taken on this entry. */

	/*
	 * Note that we need to keep track of two LSNs for each FdwXact. We keep
	 * track of the start LSN because this is the address we must use to read
	 * state data back from WAL when committing a FdwXact. We keep track of
	 * the end LSN because that is the LSN we need to wait for prior to
	 * commit.
	 */
	XLogRecPtr	fdw_xact_start_lsn;		/* XLOG offset of inserting this entry start */
	XLogRecPtr	fdw_xact_end_lsn;		/* XLOG offset of inserting this entry end */

	bool		valid; /* Has the entry been complete and written to file? */
	BackendId	locking_backend;	/* Backend working on this entry */
	bool		ondisk;			/* TRUE if prepare state file is on disk */
	bool		inredo;			/* TRUE if entry was added via xlog_redo */
	char		fdw_xact_id[FDW_XACT_ID_LEN];		/* prepared transaction identifier */
}	FdwXactData;

/* Shared memory layout for maintaining foreign prepared transaction entries. */
typedef struct
{
	/* Head of linked list of free FdwXactData structs */
	FdwXact		freeFdwXacts;

	/* Number of valid foreign transaction entries */
	int			numFdwXacts;

	/* Upto max_prepared_foreign_xacts entries in the array */
	FdwXact		fdw_xacts[FLEXIBLE_ARRAY_MEMBER];		/* Variable length array */
}	FdwXactCtlData;

/* Pointer to the shared memory holding the foreign transactions data */
 FdwXactCtlData *FdwXactCtl;

/*
 * On disk file structure
 */
typedef struct
{
	Oid			dboid;			/* database oid where to find foreign server
								 * and user mapping */
	TransactionId local_xid;
	Oid			serverid;		/* foreign server where transaction takes
								 * place */
	Oid			userid;			/* user who initiated the foreign transaction */
	Oid			umid;
	char		fdw_xact_id[FDW_XACT_ID_LEN]; /* foreign txn prepare id */
}	FdwXactOnDiskData;

typedef struct
{
	TransactionId xid;
	Oid			serverid;
	Oid			userid;
	Oid			dbid;
}	FdwRemoveXlogRec;

/* GUC parameters */
extern int	max_prepared_foreign_xacts;
extern int	max_foreign_xact_resolvers;
extern int	foreign_xact_resolution_interval;
extern int	foreign_xact_resolver_timeout;

/* Info types for logs related to FDW transactions */
#define XLOG_FDW_XACT_INSERT	0x00
#define XLOG_FDW_XACT_REMOVE	0x10

extern Size FdwXactShmemSize(void);
extern void FdwXactShmemInit(void);
extern void RecoverFdwXacts(void);
extern void FdwXactRegisterForeignServer(Oid serverid, Oid userid, bool can_prepare,
										 bool modify);
extern TransactionId PrescanFdwXacts(TransactionId oldestActiveXid);
extern bool fdw_xact_has_usermapping(Oid serverid, Oid userid);
extern bool fdw_xact_has_server(Oid serverid);
extern void AtEOXact_FdwXacts(bool is_commit);
extern void AtPrepare_FdwXacts(void);
extern bool fdw_xact_exists(TransactionId xid, Oid dboid, Oid serverid,
				Oid userid);
extern void CheckPointFdwXacts(XLogRecPtr redo_horizon);
extern bool FdwTwoPhaseNeeded(void);
extern void PreCommit_FdwXacts(void);
extern void FdwXactRedoAdd(XLogReaderState *record);
extern void FdwXactRedoRemove(TransactionId xid, Oid serverid, Oid userid);
extern void KnownFdwXactRecreateFiles(XLogRecPtr redo_horizon);
extern void FdwXactWaitForResolution(TransactionId wait_xid, bool commit);
extern int FdwXactResolveForeignTransactions(Oid dbid);
extern int FdwXactResolveDanglingTransactions(Oid dbid);
extern bool TwoPhaseCommitRequired(void);

extern void fdw_xact_redo(XLogReaderState *record);
extern void fdw_xact_desc(StringInfo buf, XLogReaderState *record);
extern const char *fdw_xact_identify(uint8 info);

#endif   /* FDW_XACT_H */
