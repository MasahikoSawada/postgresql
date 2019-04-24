#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"

#include "storage/keyring.h"
#include "storage/encryption.h"

PG_MODULE_MAGIC;

#define INIT() \
	do { \
		if (!initialized) \
		{ \
			KeyringSetup(); \
			initialized = true; \
		} \
	} while(0)

static bool initialized = false;

PG_FUNCTION_INFO_V1(generate_tblsp_key);
PG_FUNCTION_INFO_V1(update_tblsp_keyring_file);
PG_FUNCTION_INFO_V1(load_tblsp_keyring_file);
PG_FUNCTION_INFO_V1(dump_local_tblsp_keyring);

Datum
generate_tblsp_key(PG_FUNCTION_ARGS)
{
	Oid oid = (Oid) PG_GETARG_INT32(0);

	INIT();

	test_generateTblspKey(oid);
	test_dumpLocalTblspKeyring();
	PG_RETURN_BOOL(true);
}

Datum
update_tblsp_keyring_file(PG_FUNCTION_ARGS)
{
	INIT();

	test_updateTblspKeyringFile();
	PG_RETURN_BOOL(true);
}

Datum
load_tblsp_keyring_file(PG_FUNCTION_ARGS)
{
	INIT();

	PG_RETURN_BOOL(true);
}

Datum
dump_local_tblsp_keyring(PG_FUNCTION_ARGS)
{
	INIT();

	test_dumpLocalTblspKeyring();
	PG_RETURN_BOOL(true);
}
