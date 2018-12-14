#ifndef KMGR_XLOG_H
#define KMGR_XLOG

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"

#define XLOG_ENCKEY 0x00
extern void enckey_redo(XLogReaderState *record);
extern void enckey_desc(StringInfo buf, XLogReaderState *record);
extern const char *enckey_identify(uint8 info);

#endif
