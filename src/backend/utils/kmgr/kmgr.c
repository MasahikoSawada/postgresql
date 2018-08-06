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
#include "utils/keyring_api.h"
#include "utils/memutils.h"

#define ConstructKeyId(keyid, gen, dst) \
	sprintf((dst), "%s-%u", (keyid), (gen))

typedef struct Key
{
	/* Key header */
	char				*keyid;
	char				*type;
	KeyGeneration		generation;
} Key;

typedef struct KeyMgrContext
{
	KeyringPluginCallbacks callbacks;
	bool isReady;

	Key keys;
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
	MemoryContext old_cxt;

	if (key_management_provider == NULL || key_management_provider[0] == '\0')
		elog(ERROR, "cannot initialize key manager without key_management_provider");


	/* Quick exit, if already loaded */
	if (KmgrContext)
		return;

	/* Load the plugin and initialize key management context */
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	KmgrContext = (KeyMgrContext *) MemoryContextAlloc(TopMemoryContext,
													   sizeof(KeyMgrContext));
	KmgrContext->isReady = false;
	TDECurrentKeyGeneration = 0;
	LoadKeyMgrPlugin();

	MemoryContextSwitchTo(old_cxt);
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
	char *key_with_gen = palloc(sizeof(char) * MAX_KEY_ID_LEN);

	Assert(KmgrContext);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

	ConstructKeyId(keyid, 0, key_with_gen);
	KmgrContext->callbacks.generatekey_cb(key_with_gen, GetUserId());

	return true;
}

bool
GetKey(char *keyid, KeyGeneration generation, char **key, int *keylen)
{
	char *key_with_gen = palloc(sizeof(char) * MAX_KEY_ID_LEN);
	bool ret;

	Assert(KmgrContext);
	Assert(*key && keylen);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

	ConstructKeyId(keyid, generation, key_with_gen);
	ret = KmgrContext->callbacks.getkey_cb(key_with_gen,
										   GetUserId(),
										   key,
										   keylen);
	return ret;
}

bool
RemoveKey(char *keyid, KeyGeneration generation)
{
	Assert(KmgrContext);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

	KmgrContext->callbacks.removekey_cb(keyid, GetUserId());
	return true;
}

KeyGeneration
RotateKey(char *keyid)
{
	Assert(KmgrContext);

	if (!KmgrContext->isReady)
		KeyMgrProviderStartup();

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
	KeyringPluginInit plugin_init;

	if (provider_successfully_loaded)
		return;

	plugin_init = (KeyringPluginInit)
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
