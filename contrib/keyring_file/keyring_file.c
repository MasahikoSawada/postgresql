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

#include <unistd.h>

#include "fmgr.h"
#include "utils/hsearch.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/kmgr_api.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#define KEYRING_MAX_KYES 128
#define TEST_MATERKEY_PATH  "global/kmgr_test"


PG_MODULE_MAGIC;

typedef char KeyId[MAX_MASTER_KEY_ID_LEN];

typedef struct MyKey
{
	char	id[MAX_MASTER_KEY_ID_LEN];
	char	key[ENCRYPTION_KEY_SIZE];
} MyKey;

static HTAB *MyKeys;

/* function prototypes */
extern void _PG_kmgr_init(KmgrPluginCallbacks *cb);
static void test_startup(void);
static void test_getkey(const char *keyid, char **key);
static void test_generatekey(const char *keyid);
static void test_removekey(const char *keyid);
static bool test_isexistkey(const char *keyid);
static void load_all_keys(void);
static void save_all_keys(void);

/*
 * Specify output plugin callbacks
 */
void
_PG_kmgr_init(KmgrPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_kmgr_init, KmgrPluginInit);

	cb->startup_cb = test_startup;
	cb->getkey_cb = test_getkey;
	cb->generatekey_cb = test_generatekey;
	cb->isexistkey_cb = test_isexistkey;
	cb->removekey_cb = test_removekey;
}

/*
 * Read all keys in my keyfile(kmgr_test.kr) and build hash map for keys.
 */
static void
load_all_keys(void)
{
	char path[MAXPGPATH];
	int fd;

	sprintf(path, TEST_MATERKEY_PATH);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (errno == ENOENT)
			return;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	for (;;)
	{
		MyKey *key = palloc(sizeof(MyKey));
		MyKey *keycache;
		int read_len;

		read_len = read(fd, key, sizeof(MyKey));

		if (read_len < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", path))));
		else if (read_len == 0) /* EOF */
			break;
		else if (read_len != sizeof(MyKey))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": read %d instead of %d bytes",
							path, read_len, (int32) sizeof(MyKey))));

		keycache = hash_search(MyKeys, (void *) &key->id, HASH_ENTER, NULL);
		memcpy(keycache->key, key->key, ENCRYPTION_KEY_SIZE);
#ifdef DEBUG_TDE
		ereport(LOG, (errmsg("keyring_file: load mkid = \"%s\", mk = \"%s\"",
							 keycache->id, keycache->key)));
#endif
	}

	CloseTransientFile(fd);
}

/*
 * Persistent all keys in my key file(kmgr_test).
 */
static void
save_all_keys(void)
{
	HASH_SEQ_STATUS status;
	char path[MAXPGPATH];
	MyKey *key;
	FILE *fpout;
	int	rc;

	sprintf(path, TEST_MATERKEY_PATH);
	fpout = AllocateFile(path, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary keyring file \"%s\": %m",
						path)));
		return;
	}

	/* Iterate all keys while writing them to the file */
	hash_seq_init(&status, MyKeys);
	while ((key = (MyKey *) hash_seq_search(&status)) != NULL)
	{
#ifdef DEBUG_TDE
		ereport(LOG, (errmsg("keyring_file: save mkid = \"%s\", mk = \"%s\"",
							 key->id, key->key)));
#endif
		rc = fwrite(key, sizeof(MyKey), 1, fpout);
	}

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary keyring file \"%s\": %m",
						path)));
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary keyring file \"%s\": %m",
						path)));
	}
}

static void
test_startup(void)
{
	HASHCTL ctl;
	MemoryContext old_cxt;

	ereport(LOG, (errmsg("keyring_file: starting up")));

	old_cxt = MemoryContextSwitchTo(TopMemoryContext);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MAX_MASTER_KEY_ID_LEN);
	ctl.entrysize = sizeof(MyKey);
	ctl.hcxt = TopMemoryContext;

	MyKeys = hash_create("test_kmgr key hash map",
						 KEYRING_MAX_KYES,
						 &ctl,
						 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	load_all_keys();

	MemoryContextSwitchTo(old_cxt);
}

static void
test_getkey(const char *keyid, char **key)
{
	MyKey *mykey;
	bool found;

	Assert(keyid != NULL && key != NULL);

	mykey = hash_search(MyKeys, &keyid, HASH_FIND,	&found);

	if (!found)
		ereport(ERROR,
				(errmsg("keyring_file: could not get master key \"%s\"",
						keyid)));

	/* Set master key */
	*key = palloc0(ENCRYPTION_KEY_SIZE);
	memcpy(*key, mykey->key, ENCRYPTION_KEY_SIZE);

#ifdef DEBUG_TDE
	ereport(LOG, (errmsg("keyring_file: get master key, keyid = \"%s\", key = \"%s\"",
						 keyid, *key)));
#endif
}

static bool
test_isexistkey(const char *keyid)
{
	bool found;

	Assert(keyid != NULL);

	(void *) hash_search(MyKeys, &keyid, HASH_FIND,	&found);

	return found;
}

static void
test_generatekey(const char *keyid)
{
#define MATERKEYDATA "0987654321098765432109876543210987654321098765432109876543210987"
	MyKey *mykey;
	bool found;

	Assert(keyid);

	/* Duplication check */
	mykey = hash_search(MyKeys, &keyid, HASH_FIND,	&found);

	if (found)
		ereport(ERROR,
				(errmsg("key \"%s\" is already registered", keyid)));

	/* Ok, register the key */
	mykey = hash_search(MyKeys, &keyid,	HASH_ENTER,	&found);
	Assert(!found);

	/* Set master key */
	MemSet(mykey->key, 0, ENCRYPTION_KEY_SIZE);
	memcpy(mykey->key, MATERKEYDATA, ENCRYPTION_KEY_SIZE);

	/* debug log */
#ifdef DEBUG_TDE
	ereport(LOG, (errmsg("keyring_file: genearte key id = \"%s\", key = \"%s\"",
						 keyid, mykey->key)));
#endif
	/* update key file */
	save_all_keys();
}

static void
test_removekey(const char *keyid)
{
	/* Not implemented yet */
	return;
}
