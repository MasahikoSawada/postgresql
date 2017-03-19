/*-------------------------------------------------------------------------
 *
 * fdw_xactdesc.c
 *		PostgreSQL distributed transaction manager for foreign server.
 *
 * This module describes the WAL records for foreign transaction manager.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/fdw_xactdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/fdw_xact.h"
#include "access/xloginsert.h"
#include "lib/stringinfo.h"

extern void
fdw_xact_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_FDW_XACT_INSERT)
	{
		FDWXactOnDiskData *fdw_insert_xlog = (FDWXactOnDiskData *) rec;

		appendStringInfo(buf, "Foreign server oid: %u", fdw_insert_xlog->serverid);
		appendStringInfo(buf, " user oid: %u", fdw_insert_xlog->userid);
		appendStringInfo(buf, " database id: %u", fdw_insert_xlog->dboid);
		appendStringInfo(buf, " local xid: %u", fdw_insert_xlog->local_xid);
		/* TODO: This should be really interpreted by each FDW */

		/*
		 * TODO: we also need to assess whether we want to add this
		 * information
		 */
		appendStringInfo(buf, " foreign transaction info: %s",
						 fdw_insert_xlog->fdw_xact_id);
	}
	else
	{
		FdwRemoveXlogRec *fdw_remove_xlog = (FdwRemoveXlogRec *) rec;

		appendStringInfo(buf, "Foreign server oid: %u", fdw_remove_xlog->serverid);
		appendStringInfo(buf, " user oid: %u", fdw_remove_xlog->userid);
		appendStringInfo(buf, " database id: %u", fdw_remove_xlog->dbid);
		appendStringInfo(buf, " local xid: %u", fdw_remove_xlog->xid);
	}

}

extern const char *
fdw_xact_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_FDW_XACT_INSERT:
			return "NEW FOREIGN TRANSACTION";
		case XLOG_FDW_XACT_REMOVE:
			return "REMOVE FOREIGN TRANSACTION";
	}
	/* Keep compiler happy */
	return NULL;
}
