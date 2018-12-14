#include "postgres.h"

#include "storage/kmgr_xlog.h"

void
enckey_desc(StringInfo buf, XLogReaderState *record)
{

}

const char *
enckey_identify(uint8 info)
{
	return "EncKeyRotation";
}
