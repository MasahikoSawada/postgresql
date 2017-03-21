/*
 * fdw_xact.h
 *
 * PostgreSQL distributed transaction manager
 *
 * Portions Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * src/include/access/fdw_xact.h
 */
#ifndef FDW_XACT_H
#define FDW_XACT_H

#include "storage/backendid.h"
#include "foreign/foreign.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

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
	uint32		fdw_xact_id_len;/* Length of the value stored in the next
								 * field */
	/* This should always be the last member */
	char		fdw_xact_id[FLEXIBLE_ARRAY_MEMBER];		/* variable length array
														 * to store foreign
														 * transaction
														 * information. */
}	FDWXactOnDiskData;

typedef struct
{
	TransactionId xid;
	Oid			serverid;
	Oid			userid;
	Oid			dbid;
}	FdwRemoveXlogRec;

extern int	max_prepared_foreign_xacts;

/* Info types for logs related to FDW transactions */
#define XLOG_FDW_XACT_INSERT	0x00
#define XLOG_FDW_XACT_REMOVE	0x10

extern Size FDWXactShmemSize(void);
extern void FDWXactShmemInit(void);
extern void RecoverFDWXacts(void);
extern TransactionId PrescanFDWXacts(TransactionId oldestActiveXid);
extern bool fdw_xact_has_usermapping(Oid serverid, Oid userid);
extern bool fdw_xact_has_server(Oid serverid);
extern void fdw_xact_redo(XLogReaderState *record);
extern void fdw_xact_desc(StringInfo buf, XLogReaderState *record);
extern const char *fdw_xact_identify(uint8 info);
extern void AtEOXact_FDWXacts(bool is_commit);
extern void AtPrepare_FDWXacts(void);
extern void FDWXactTwoPhaseFinish(bool isCommit, TransactionId xid);
extern bool fdw_xact_exists(TransactionId xid, Oid dboid, Oid serverid,
				Oid userid);
extern void CheckPointFDWXact(XLogRecPtr redo_horizon);
extern void RegisterXactForeignServer(Oid serverid, Oid userid, bool can_prepare);
extern bool FdwTwoPhaseNeeded(void);
extern void PreCommit_FDWXacts(void);
extern void FDWXactRedoAdd(XLogReaderState *record);
extern void FDWXactRedoRemove(TransactionId xid, Oid serverid, Oid userid);
extern void KnownFDWXactRecreateFiles(XLogRecPtr redo_horizon);

#endif   /* FDW_XACT_H */
