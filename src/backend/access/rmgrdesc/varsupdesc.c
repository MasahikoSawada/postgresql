/*-------------------------------------------------------------------------
 *
 * varsupdesc.c
 *	  rmgr descriptor routines for access/transam/varsup.c
 *
 * Portions Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/varsupdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/varsup_internal.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"

void
varsup_desc(StringInfo buf, XLogReaderState *record)
{
	//char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_VARSUP_XID_LSN_RANGES)
	{

	}
}

const char *
varsup_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_VARSUP_XID_LSN_RANGES:
			id = "XID_LSN_RANGES";
			break;
	}

	return id;
}
