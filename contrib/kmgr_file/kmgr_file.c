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
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/hashutils.h"
#include "utils/guc.h"
#include "miscadmin.h"

#define KEYRING_MAX_KEYS 128
#define TEST_MASTERKEY_FILENAME "kmgr_test"


PG_MODULE_MAGIC;

typedef char KeyId[MASTER_KEY_ID_LEN];

typedef struct MyKey
{
	char	id[MASTER_KEY_ID_LEN];
	char	key[ENCRYPTION_KEY_SIZE];
} MyKey;

static HTAB *MyKeys;
static LWLock *lock;
static char *masterkey_filepath;

/* function prototypes */
extern void _PG_kmgr_init(KmgrPluginCallbacks *cb);
static void test_startup(void);
static char *test_getkey(const char *keyid);
static void test_generatekey(const char *keyid);
static void test_removekey(const char *keyid);
static bool test_isexistkey(const char *keyid);
static void load_all_keys(void);
static void save_all_keys(void);
static Size test_memsize(void);

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

	DefineCustomStringVariable("kmgr_file.masterkey_filepath",
							   "file location of key file",
							   NULL,
							   &masterkey_filepath,
							   "global/",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	RequestAddinShmemSpace(test_memsize());
	RequestNamedLWLockTranche("kmgr_test", 1);
}

static Size
test_memsize(void)
{
	Size size;

	size = MAXALIGN(sizeof(lock));
	size = add_size(size, hash_estimate_size(KEYRING_MAX_KEYS, sizeof(MyKey)));

	return size;
}

/*
 * Read all keys in my keyfile(kmgr_test.kr) and build hash map for keys.
 */
static void
load_all_keys(void)
{
	char path[MAXPGPATH];
	int fd;

	LWLockAcquire(lock, LW_EXCLUSIVE);

	sprintf(path, "%s/%s", masterkey_filepath, TEST_MASTERKEY_FILENAME);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (errno == ENOENT)
		{
			LWLockRelease(lock);
			return;
		}

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

		keycache = hash_search(MyKeys, (void *) key->id, HASH_ENTER, NULL);

		memcpy(keycache->key, key->key, ENCRYPTION_KEY_SIZE);
	}

	CloseTransientFile(fd);
	LWLockRelease(lock);
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

	sprintf(path, "%s/%s", masterkey_filepath, TEST_MASTERKEY_FILENAME);
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
		rc = fwrite(key, sizeof(MyKey), 1, fpout);
		(void) rc;
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

	ereport(LOG, (errmsg("keyring_file: starting up")));

	//LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MASTER_KEY_ID_LEN;
	ctl.entrysize = sizeof(MyKey);

	MyKeys = ShmemInitHash("test_kmgr key shared hash map",
						   KEYRING_MAX_KEYS, KEYRING_MAX_KEYS,
						   &ctl,
						   HASH_ELEM | HASH_BLOBS);

	lock = &(GetNamedLWLockTranche("kmgr_test"))->lock;

	//LWLockRelease(AddinShmemInitLock);

	load_all_keys();
}

static char *
test_getkey(const char *keyid)
{
	char *key;
	MyKey *mykey;
	bool found;

	LWLockAcquire(lock, LW_SHARED);

	mykey = hash_search(MyKeys, (void *) keyid, HASH_FIND,	&found);

	if (!found)
		ereport(ERROR,
				(errmsg("kmgr_file: could not get master key \"%s\"",
						keyid)));

	/* Set master key */
	key = palloc0(ENCRYPTION_KEY_SIZE);
	memcpy(key, mykey->key, ENCRYPTION_KEY_SIZE);

	LWLockRelease(lock);

	return key;
}

static bool
test_isexistkey(const char *keyid)
{
	bool found;

	Assert(keyid != NULL);

	LWLockAcquire(lock, LW_SHARED);

	(void *) hash_search(MyKeys, (void *) keyid, HASH_FIND,	&found);

	LWLockRelease(lock);

	return found;
}

static void
test_generatekey(const char *keyid)
{
	MyKey *mykey;
	bool found;
	char *newkey;
	int ret;

	Assert(keyid);

	LWLockAcquire(lock, LW_EXCLUSIVE);

	/* Set master key */
	newkey = (char *) palloc0(ENCRYPTION_KEY_SIZE);
	ret = pg_strong_random(newkey, ENCRYPTION_KEY_SIZE);
	if (!ret)
		ereport(ERROR,
				(errmsg("failed to generate tablespace encryption key")));

	/* Duplication check */
	mykey = hash_search(MyKeys, (void *) keyid, HASH_FIND,	&found);

	if (found)
		ereport(ERROR,
				(errmsg("key \"%s\" is already registered", keyid)));

	/* Ok, register the key */
	mykey = hash_search(MyKeys, (void *) keyid,	HASH_ENTER,	&found);
	Assert(!found);

	memcpy(mykey->key, newkey, ENCRYPTION_KEY_SIZE);

	/* update key file */
	save_all_keys();

	LWLockRelease(lock);
}

static void
test_removekey(const char *keyid)
{
	/* Not implemented yet */
	return;
}
