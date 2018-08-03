/*-------------------------------------------------------------------------
 *
 * kmgrfunc.c
 *	  PostgreSQL key manager SQL interface
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/kmgr/kmgrfunc.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/kmgr.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

extern void _PG_key_management_plugin_init(KeyMgrPluginCallbacks *cb);

static void test_startupkey(void);
static bool test_getkey (char *keyid, Oid userid,
						 char **key, int *keylen);
static bool test_generatekey (char *keyid, Oid userid);
static bool test_removekey (char *keyid, Oid userid);

/*
 * Specify output plugin callbacks
 */
void
_PG_key_management_plugin_init(KeyMgrPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_key_management_plugin_init, KeyMgrPluginInit);

	cb->startup_cb = test_startupkey;
	cb->getkey_cb = test_getkey;
	cb->generatekey_cb = test_generatekey;
	cb->removekey_cb = test_removekey;
}

static void
test_startupkey(void)
{
	elog(NOTICE, "test_key: startup");
}

static bool
test_getkey(char *keyid, Oid userid, char **key, int *keylen)
{
	elog(NOTICE, "test_key: getkey");
	return true;
}

static bool
test_generatekey(char *keyid, Oid userid)
{
	elog(NOTICE, "test_key: generatekey");
	return true;

}

static bool
test_removekey(char *keyid, Oid userid)
{
	elog(NOTICE, "test_key: removekey");
	return true;
}

