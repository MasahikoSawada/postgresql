/*-------------------------------------------------------------------------
 *
 * plugin.c
 *	 This module manages the key managment plugin
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATIONf
 *	  src/backend/storage/kmgr/plugin.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "fmgr.h"

#include "storage/kmgr_api.h"
#include "storage/kmgr_plugin.h"
#include "utils/memutils.h"

static MemoryContext KmgrPluginCtx;
static KmgrPluginCallbacks callbacks;

static void load_kmgr_plugin(const char *libraryname);

char *
KmgrPluginGetKey(const char *id)
{
	return callbacks.getkey_cb(id);
}

void
KmgrPluginGenerateKey(const char *id)
{
	callbacks.generatekey_cb(id);
}

void
KmgrPluginRemoveKey(const char *id)
{
	callbacks.removekey_cb(id);
}

bool
KmgrPluginIsExist(const char *id)
{
	return callbacks.isexistkey_cb(id);
}

void
KmgrPluginStartup(void)
{
	if (callbacks.startup_cb)
		callbacks.startup_cb();
}

/* Load keyring plugin */
void
startupKmgrPlugin(const char *libraryname)
{
	MemoryContext old_ctx;

	if (libraryname == NULL || libraryname[0] == '\0')
		return;

	ereport(LOG,
			(errmsg("loading keyring plugin \"%s\"", libraryname)));

	if (!KmgrPluginCtx)
		KmgrPluginCtx = AllocSetContextCreate(TopMemoryContext,
											  "Key manager plugin",
											  ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(KmgrPluginCtx);

	/* Load keyring plugins and call the startup callback */
	load_kmgr_plugin(libraryname);

	MemoryContextSwitchTo(old_ctx);
}

/* Load keyring plugin and check callbacks */
static void
load_kmgr_plugin(const char *libraryname)
{
	KmgrPluginInit plugin_init;

	plugin_init = (KmgrPluginInit)
		load_external_function(libraryname,
							   "_PG_kmgr_init", false, NULL);

	if (plugin_init == NULL)
		elog(ERROR, "key management plugin have to declare the _PG_keyring_init symbol");

	/* Call initialize function */
	plugin_init(&callbacks);

	if (callbacks.getkey_cb == NULL)
		elog(ERROR, "key management plugin have to register a get key callback");
	if (callbacks.generatekey_cb == NULL)
		elog(ERROR, "key management plugin have to register a generate key callback");
	if (callbacks.removekey_cb == NULL)
		elog(ERROR, "key management plugin have to register a remove key callback");
}
