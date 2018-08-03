/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	  PostgreSQL symmetric key manager
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/kmgr/kmgr.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "fmgr.h"

#include "utils/kmgr.h"
#include "utils/kmgrapi.h"
#include "utils/memutils.h"

typedef struct Key
{
	/* Key header */
	char				*keyid;
	char				*type;
	KeyGeneration		generation;
} Key;

typedef struct KeyMgrContext
{
	KeyMgrPluginCallbacks callbacks;
	bool isReady;
} KeyMgrContext;

/* GUC parameter */
char *key_management_provider;

static KeyMgrContext *KmgrContext = NULL;

static bool provider_successfully_loaded = false;
static KeyGeneration TDECurrentKeyGeneration;

static void LoadKeyMgrPlugin(void);
static void KeyMgrProviderStartup(void);

void
KeyMgrInit(void)
{
	if (key_management_provider == NULL || key_management_provider[0] == '\0')
		elog(ERROR, "cannot initialize key manager without key_management_provider");

	KmgrContext = (KeyMgrContext *) MemoryContextAlloc(TopMemoryContext,
													   sizeof(KeyMgrContext));
	KmgrContext->isReady = false;
	TDECurrentKeyGeneration = 0;

	LoadKeyMgrPlugin();
}

static void
KeyMgrProviderStartup(void)
{
	Assert(KmgrContext);

	/* Startup key management API */
	if (provider_successfully_loaded && KmgrContext->callbacks.startup_cb)
		KmgrContext->callbacks.startup_cb();

	KmgrContext->isReady = true;
}

bool
GenerateKey(char *keyid, char *keytype)
{
	Assert(KmgrContext);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

	elog(NOTICE, "generate key : keyid = %s, keytype = %s",keyid, keytype);
	KmgrContext->callbacks.generatekey_cb(keyid, GetUserId());

	return true;
}

bool
GetKey(char *keyid, KeyGeneration generation, char **key, int *keylen)
{
	Assert(KmgrContext);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

	elog(NOTICE, "get key : keyid = %s, generation = %u", keyid, generation);
	KmgrContext->callbacks.getkey_cb(keyid, GetUserId(), NULL, NULL);
	return true;
}

bool
RemoveKey(char *keyid, KeyGeneration generation)
{
	Assert(KmgrContext);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

	elog(NOTICE, "remove key : keyid = %s, generation = %u", keyid, generation);
	KmgrContext->callbacks.removekey_cb(keyid, GetUserId());
	return true;
}

KeyGeneration
RotateKey(char *keyid)
{
	Assert(KmgrContext);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

	elog(NOTICE, "rotate key : keyid = %s", keyid);
	return 0;
}

KeyGeneration
TDEGetCurrentKeyGeneration(void)
{
	Assert(KmgrContext);

	return TDECurrentKeyGeneration;
}

/*
 * Load the key management plubing, lookup and execute its plugin init function,
 * and chechk that it provides the required callbacks.
 */
static void
LoadKeyMgrPlugin(void)
{
	KeyMgrPluginInit plugin_init;

	if (provider_successfully_loaded)
		return;

	plugin_init = (KeyMgrPluginInit)
		load_external_function(key_management_provider,
							   "_PG_key_management_plugin_init", false, NULL);

	if (plugin_init == NULL)
		elog(ERROR, "key management plugins have to declare the _PG_key_management_plugin_init symbol");

	plugin_init(&KmgrContext->callbacks);

	if (KmgrContext->callbacks.getkey_cb == NULL)
		elog(ERROR, "key management plugin have to register a get key callback");
	if (KmgrContext->callbacks.generatekey_cb == NULL)
		elog(ERROR, "key management plugin have to register a generate key callback");
	if (KmgrContext->callbacks.removekey_cb == NULL)
		elog(ERROR, "key management plugin have to register a remove key callback");

	provider_successfully_loaded = true;
}

void
AtEOXact_KMgr(bool is_commit)
{
	provider_successfully_loaded = false;
}
