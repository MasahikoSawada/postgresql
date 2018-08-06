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

	/* Load theplugin */
	LoadKeyMgrPlugin();

	/* Invoke startup callback if provided */
	KeyMgrProviderStartup();

	MemoryContextSwitchTo(old_cxt);
}

/*
 * Invoke startup callback if defined.
 */
static void
KeyMgrProviderStartup(void)
{
	Assert(KmgrContext);

	if (!provider_successfully_loaded)
	{
		KmgrContext->isReady = false;
		return;
	}

	/* Startup key management API */
	if (KmgrContext->callbacks.startup_cb)
		KmgrContext->callbacks.startup_cb();

	KmgrContext->isReady = true;
}

/*
 * Generat key from the keyring.
 *
 * The initial key generation is always 0.
 */
bool
GenerateKey(char *keyid, char *keytype)
{
	char *key_with_gen = palloc(sizeof(char) * MAX_KEY_ID_LEN);

	Assert(KmgrContext);

	ConstructKeyId(keyid, 0, key_with_gen);
	PG_TRY();
	{
		KmgrContext->callbacks.generatekey_cb(key_with_gen, GetUserId());
	}
	PG_CATCH();
	{
		errcontext("generating the new key in \"%s\" plugin",
				   key_management_provider);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return true;
}

/*
 * Get key from the keyring.
 *
 * Since the given keyid is a raw key identifier we construct
 * an key id combine with its generation for searching.
 */
bool
GetKey(char *keyid, KeyGeneration generation, char **key, int *keylen)
{
	char *key_with_gen = palloc(sizeof(char) * MAX_KEY_ID_LEN);
	bool ret;

	Assert(KmgrContext);
	Assert(*key && keylen);

	ConstructKeyId(keyid, generation, key_with_gen);

	PG_TRY();
	{
		ret = KmgrContext->callbacks.getkey_cb(key_with_gen,
											   GetUserId(),
											   key,
											   keylen);
	}
	PG_CATCH();
	{
		errcontext("getting the key from \"%s\" plugin",
				   key_management_provider);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Key not found, emit an error */
	if (!ret)
		ereport(ERROR,
				(errmsg("key \"%s\" does not exist", keyid),
				 errdetail("key generation is %d", generation)));

	return ret;
}

/*
 * Remove key from the keyring.
 *
 * Since the given keyid is a raw key identifier we construct
 * an key id combine with its generation for searching.
 */
bool
RemoveKey(char *keyid, KeyGeneration generation)
{
	Assert(KmgrContext);

	KmgrContext->callbacks.removekey_cb(keyid, GetUserId());
	return true;
}

/*
 * Rotate key in the keyring.
 *
 * Since the given keyid is a raw key identifier we construct
 * an key id combine with its generation for searching.
 */
KeyGeneration
RotateKey(char *keyid)
{
	Assert(KmgrContext);

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
