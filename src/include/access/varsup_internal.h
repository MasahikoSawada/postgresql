/*
 * varsup_internal.h
 *
 * varsup WAL routines. Internal to varsup.c and varsupdesc.c
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/varsup_internal.h
 */
#ifndef VARSUP_INTERNAL_H
#define VARSUP_INTERNAL_H

#include "access/xlog.h"
#include "access/xlogdefs.h"

/* XLOG info values for varsup rmgr */
#define XLOG_VARSUP_XID_LSN_RANGES			0x10

typedef struct xl_varsup_xid_lsn_ranges
{
	int		numranges;
	XidLSNRange	ranges[FLEXIBLE_ARRAY_MEMBER];
} xl_varsup_xid_lsn_ranges;

/* in varsup.c */
extern void varsup_redo(XLogReaderState *record);

/* in varsupdesc.c */
extern void varsup_desc(StringInfo buf, XLogReaderState *record);
extern const char *varsup_identify(uint8 info);

#endif   /* VARSUP_INTERNAL_H */
