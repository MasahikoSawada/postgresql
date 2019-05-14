/*-------------------------------------------------------------------------
 *
 * keyring_file.c
 *	  Test module for Key ring plugin
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/test_keyringfile/keyring_file.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/hsearch.h"
#include "storage/kmgr.h"
#include "storage/kmgr_api.h"
#include "utils/memutils.h"

#define KEYRING_MAX_KYES 128

PG_MODULE_MAGIC;

typedef char KeyId[KEYRING_MASTER_KEY_ID_SIZE];

typedef struct EncKey
{
	KeyId keyid;
	char  key[256];
} EncKey;

typedef struct KeyCtlData
{
	HTAB *keys;
} KeyCtlData;
typedef KeyCtlData *KeyCtl;

static KeyCtl MyKeyCtl;

/* function prototypes */
extern void _PG_keyring_init(KeyringPluginCallbacks *cb);

static void test_startupkey(void);
static bool test_getkey(char *keyid, Oid userid,
						char **key, int *keylen);
static bool test_generatekey(char *keyid, Oid userid);
static bool test_removekey(char *keyid, Oid userid);

/*
 * Specify output plugin callbacks
 */
void
_PG_keyring_init(KeyringPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_keyring_init, KeyringPluginInit);

	cb->startup_cb = test_startupkey;
	cb->getkey_cb = test_getkey;
	cb->generatekey_cb = test_generatekey;
	cb->removekey_cb = test_removekey;
}

static void
test_startupkey(void)
{
	HASHCTL ctl;
	MemoryContext old_cxt;

	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	MyKeyCtl = (KeyCtl) palloc(sizeof(KeyCtlData));

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(KeyId);
	ctl.entrysize = sizeof(EncKey);
	ctl.hcxt = TopMemoryContext;

	MyKeyCtl->keys = hash_create("test_keyring key map",
								 KEYRING_MAX_KYES,
								 &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	MemoryContextSwitchTo(old_cxt);
}

static bool
test_getkey(char *keyid, Oid userid, char **key, int *keylen)
{
	KeyId id;
	EncKey *mykey;
	bool found;

	Assert(*key && keylen);

	/* Construct search key id */
	MemSet(id, 0, sizeof(id));
	strncpy(id, keyid, strlen(keyid));

	mykey = hash_search(MyKeyCtl->keys,
						&id,
						HASH_FIND,
						&found);

	/* Not found */
	if (!found)
		return false;

	/* Set to output parameter */
	memcpy(*key, mykey->key, KEYRING_MASTER_KEY_ID_SIZE);
	*keylen = sizeof(mykey->keyid);

	return true;
}

static bool
test_generatekey(char *keyid, Oid userid)
{
	KeyId id;
	EncKey *mykey;
	bool found;

	Assert(*keyid);

	/* Construct search key id */
	MemSet(id, 0, sizeof(id));
	strncpy(id, keyid, strlen(keyid));

	/* Check if the key duplication */
	mykey = hash_search(MyKeyCtl->keys,
						&id,
						HASH_FIND,
						&found);
	if (found)
		ereport(ERROR, (errmsg("key \"%s\" is already registered",
							   keyid)));

	/* Register the key */
	mykey = hash_search(MyKeyCtl->keys,
						&id,
						HASH_ENTER,
						&found);
	snprintf(mykey->key, sizeof(mykey->key), "hoge my key");

	return true;

}

static bool
test_removekey(char *keyid, Oid userid)
{
	return true;
}
