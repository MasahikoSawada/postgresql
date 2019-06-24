/*-------------------------------------------------------------------------
 *
 * keyring.c
 *	 This module manages tablespace keys.
 *
 * Keyring is a module to manage all per tablespaces encryption keys. It
 * persists all tablespace keys on the disk (global/pg_tblsp.kr) and updates
 * the file each time when tablespace are either created or dropped. All
 * processes who want encrypt or decrypt a database object such as tables,
 * indexes and WAL need to obtain the tablespace key that the object pertains,
 * and the master key because all tablespace keys on the disk are encrypted
 * with the master key. Once got all tablespace keys we get the master key
 * via a kmgr plugin library and decrypt all of them. In memory all tablespace
 * keys are stored with the key decrypted.
 *
 * LOCKING:
 * All process read keyring file at the first time when accessing to the
 * encrypted tablespace and cache the keyring in the local memory. Concurrent
 * process who created, removed and rotated keys sends cache invalidation.
 * KeyringControlLock is used to prevent keyring file from being modified by
 * concurrent processes. So we need to acquire it before we could read the
 * keyrig file and then possibly we udpate our own local cache while holding
 * the lock.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/kmgr/keyring.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/syscache.h"

#define KEYRING_TBLSP_FILE "pg_tblsp.kr"

PG_FUNCTION_INFO_V1(pg_get_tablespace_keys);

/* Struct for one tablespace key */
typedef struct TblspKeyData
{
	Oid		spcoid;		/* hash key; must be first */
	char	tblspkey[ENCRYPTION_KEY_SIZE];
} TblspKeyData;

/* The master key written in the keyring file */
static char	currentMasterKeyId[MAX_MASTER_KEY_ID_LEN];
static bool currentMasterKeyId_initialize = false;

/* Tablespace keys */
static HTAB *tblspKeyring;

static MemoryContext tblspKeyringContext;

static bool keyring_invalid = true;

static void initialize_keyring(void);
static void reload_keyring_file(void);
static List *read_keyring_file(void);
static void update_keyring_file(const char *masterkey_id,
								const char *masterkey);
static TblspKeyData *get_keyring_entry(Oid spcOid, bool *found);
static void invalidate_keyring(Datum arg, int cacheid, uint32 hashvalue);
static void key_encryption_tweak(char *tweak, Oid spcoid);
static void encrypt_tblsp_key(Oid spcoid, char *tblspkey, const char *masterkey);
static void decrypt_tblsp_key(Oid spcoid, char *tblspkey, const char *masterkey);

/*
 * Register kerying invalidation callback.
 */
void
KeyringSetup(void)
{
	if (!TransparentEncryptionEnabled())
		return;

#ifdef DEBUG_TDE
	ereport(LOG,
			(errmsg("keyring::setup pid = %d\n", MyProcPid)));
#endif
	CacheRegisterSyscacheCallback(TABLESPACEOID,
								  invalidate_keyring,
								  (Datum) 0);
}

/*
 * Initialize keyring memory context and local keyring hash table.
 */
static void
initialize_keyring(void)
{
	HASHCTL hash_ctl;

	/* Destory old keyring if exists */
	if (tblspKeyring)
	{
		hash_destroy(tblspKeyring);
		tblspKeyring = NULL;
	}

	if (!tblspKeyringContext)
		tblspKeyringContext = AllocSetContextCreate(TopMemoryContext,
													"Tablespace keys",
													ALLOCSET_DEFAULT_SIZES);

	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TblspKeyData);
	hash_ctl.hcxt = tblspKeyringContext;

	tblspKeyring = hash_create("tablespace key ring",
							   1000, &hash_ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Routine to get tablespace key identified by the given tablespace oid.
 * Return the tablespace key or NULL if not found. For processes who just
 * read and write buffer data on shared memory without using relation cache
 * such as checkpointer and bgwriter, it's quite possible that the local cache
 * is out of date because such processes doesn't get cache invalidations. So
 * we need to check the keyring file again when we could not find the key in
 * the local cache.
 */
static TblspKeyData *
get_keyring_entry(Oid spcOid, bool *found)
{
	TblspKeyData *key;

	LWLockAcquire(KeyringControlLock, LW_SHARED);

	/* if the local cache is out of date, update it */
	if (keyring_invalid)
	{
		reload_keyring_file();
		keyring_invalid = false;
	}

	/* quick return if the keyring is empty */
	if (!tblspKeyring)
	{
		LWLockRelease(KeyringControlLock);
		*found = false;
		return NULL;
	}

	key = hash_search(tblspKeyring, (void *) &spcOid, HASH_FIND, found);

	if (!(*found))
	{
		/*
		 * It's very optimistic. Since it's possible that the local cache
		 * is out of date we reload the up-to-date keyring and try to find
		 * again.
		 *
		 * @@@ : perhaps this is required only by checkpointer and bgwriter.
		 */
		read_keyring_file();
		key = hash_search(tblspKeyring, (void *) &spcOid, HASH_FIND, found);
	}

	LWLockRelease(KeyringControlLock);

	return key;
}

/*
 * Return the tablespace key string of the given tablespace, or NULL if not
 * found. Returned key string is ENCRYPTION_KEY_SIZE byte.
 */
char *
KeyringGetKey(Oid spcOid)
{
	TblspKeyData *tskey;
	bool		found;
	char		*keystr;

	if (!TransparentEncryptionEnabled())
		return NULL;

	if (!OidIsValid(spcOid))
		return NULL;

	tskey = get_keyring_entry(spcOid, &found);

	if (!found)
		return NULL;

	keystr = (char *) palloc(ENCRYPTION_KEY_SIZE);
	memcpy(keystr, tskey->tblspkey, ENCRYPTION_KEY_SIZE);

	return keystr;
}

/*
 * Check the tablespace key is exists. Having tablespace key means that
 * the tablespace is encrypted.
 */
bool
KeyringKeyExists(Oid spcOid)
{
	bool		found;

	if (!TransparentEncryptionEnabled())
		return false;

	if (!OidIsValid(spcOid))
		return false;

	(void) get_keyring_entry(spcOid, &found);

	return found;
}

/*
 * Generate new tablespace key and update the keyring file. Return encrypted
 * new tablespace key string.
 */
char *
KeyringCreateKey(Oid spcOid)
{
	TblspKeyData *key;
	char		*masterkey;
	char		*retkey;
	bool		found;
	bool		ret;

	if (!TransparentEncryptionEnabled())
		return false;

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	/* if the local cache is out of date, update it */
	if (keyring_invalid)
	{
		reload_keyring_file();
		keyring_invalid = false;
	}

	if (!tblspKeyring)
		initialize_keyring();

	key = hash_search(tblspKeyring, (void *) &spcOid, HASH_ENTER, &found);

	/*
	 * Since tablespace creation can only be done by the backend processes,
	 * we don't need to check the keyring file again unlike get_keyring_entry.
	 */
	if (found)
		ereport(ERROR,
				(errmsg("found duplicate tablespace encryption key for tablespace %u",
						key->spcoid)));

	/* Generate a random tablespace key */
	retkey = (char *) palloc0(ENCRYPTION_KEY_SIZE);
	ret = pg_strong_random(retkey, ENCRYPTION_KEY_SIZE);
	if (!ret)
		ereport(ERROR,
				(errmsg("failed to generate tablespace encryption key")));

	memcpy(key->tblspkey, retkey, ENCRYPTION_KEY_SIZE);

	/*
	 * If the current master key is not initialized, we fetch the current
	 * id and set to the cache.
	 */
	if (!currentMasterKeyId_initialize)
	{
		GetCurrentMasterKeyId(currentMasterKeyId);
		currentMasterKeyId_initialize = true;
	}

	/* Update tablespace key file */
	update_keyring_file(currentMasterKeyId,
						GetMasterKey(currentMasterKeyId));

	LWLockRelease(KeyringControlLock);

	/* Encrypt tablespace key with the master key */
	masterkey = GetMasterKey(currentMasterKeyId);
	encrypt_tblsp_key(spcOid, retkey, masterkey);

	return retkey;
}

/*
 * Drop one tablespace key from the local cache as well as the keyring file.
 */
void
KeyringDropKey(Oid spcOid)
{
	TblspKeyData *key;
	bool found;

	if (!TransparentEncryptionEnabled())
		return;

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	if (keyring_invalid)
	{
		reload_keyring_file();
		keyring_invalid = false;
	}

	key = hash_search(tblspKeyring, (void *) &spcOid, HASH_REMOVE, &found);

	if (!found)
		ereport(ERROR,
				(errmsg("could not find tablespace encryption key for tablespace %u",
						spcOid)));

	/* Update tablespace key file */
	update_keyring_file(currentMasterKeyId,
						GetMasterKey(currentMasterKeyId));

	LWLockRelease(KeyringControlLock);
}

/*
 * Load the keyring file into the local cache.
 */
static void
reload_keyring_file(void)
{
	List *keylist;
	ListCell *lc;
	char *masterkey = NULL;

	keylist = read_keyring_file();

	/* There is no key in the file */
	if (keylist == NIL)
	{
#ifdef DEBUG_TDE
		fprintf(stderr, "tblsp_key::reload loaded 0 keys by pid %d\n",
				MyProcPid);
#endif
		tblspKeyring = NULL;
		return;
	}

	/* cleanup the existing keyring */
	initialize_keyring();

	/* Get the master key by identifier */
	masterkey = GetMasterKey(currentMasterKeyId);

#ifdef DEBUG_TDE
	fprintf(stderr, "keyring:: reload get master key id %s, key %s\n",
			currentMasterKeyId, dk(masterkey));
#endif

	foreach (lc, keylist)
	{
		TblspKeyData *key = (TblspKeyData *) lfirst(lc);
		TblspKeyData *cache_key;

		cache_key = hash_search(tblspKeyring, (void *) &key->spcoid,
								HASH_ENTER, NULL);

		/* Decyrpt tablespace key by the master key before caching */
		decrypt_tblsp_key(key->spcoid, key->tblspkey, masterkey);

#ifdef DEBUG_TDE
		fprintf(stderr, "keyring::reload load oid = %u, mkid = %s, mk = %s, dk = %s\n",
				key->spcoid, currentMasterKeyId, dk(masterkey), dk(key->tblspkey));
#endif
		memcpy(cache_key, key, sizeof(TblspKeyData));
	}

#ifdef DEBUG_TDE
	fprintf(stderr, "    keyring::reload loaded %d keys by pid %d\n",
			list_length(keylist), MyProcPid);
#endif
	list_free_deep(keylist);
}

/*
 * Read the keyring file and return the list of tablespace keys.
 */
static List *
read_keyring_file(void)
{
	char *path = "global/"KEYRING_TBLSP_FILE;
	List *key_list = NIL;
	int read_len;
	int fd;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (errno == ENOENT)
			return NIL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	/* Read and set the current master key id */
	if ((read_len = read(fd, currentMasterKeyId, MAX_MASTER_KEY_ID_LEN)) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", path))));

	for (;;)
	{
		TblspKeyData *key = palloc(sizeof(TblspKeyData));

		read_len = read(fd, key, sizeof(TblspKeyData));

		if (read_len < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", path))));
		else if (read_len == 0) /* EOF */
			break;
		else if (read_len != sizeof(TblspKeyData))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": read %d instead of %d bytes",
							path, read_len, (int32) sizeof(TblspKeyData))));
		key_list = lappend(key_list, key);
	}

	CloseTransientFile(fd);

	return key_list;
}

/*
 * Update the keyring file based on the local cache. It the master key is
 * NULL, we get the master key corresponding to each tablespace keys to
 * encrypt them. On the other hand, if specified, we encrypt tablespace keys
 * with it regardless of the master key ID of each tablespace keys.
 */
static void
update_keyring_file(const char *masterkey_id, const char *masterkey)
{
	HASH_SEQ_STATUS status;
	TblspKeyData *key;
	char path[MAXPGPATH];
	char tmppath[MAXPGPATH];
	FILE *fpout;
	int	rc;

	sprintf(path, "global/"KEYRING_TBLSP_FILE);
	sprintf(tmppath, "global/"KEYRING_TBLSP_FILE".tmp");

	fpout = AllocateFile(tmppath, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary keyring file \"%s\": %m",
						tmppath)));
		return;
	}

	/* Write the master key id first */
	rc = fwrite(masterkey_id, MAX_MASTER_KEY_ID_LEN, 1, fpout);

	/* Write tablespace key to the file */
	hash_seq_init(&status, tblspKeyring);
	while ((key = (TblspKeyData *) hash_seq_search(&status)) != NULL)
	{
		TblspKeyData k;

		/* Copy to work buffer */
		memcpy(&k, key, sizeof(TblspKeyData));

		/* Prepare tablespace key and master key to write */
		encrypt_tblsp_key(key->spcoid, (char *) &k.tblspkey, masterkey);

#ifdef DEBUG_TDE
		fprintf(stderr, "keyring::udpate file::reenc tblspkey oid %u, mkid %s, mk %s, dk %s, edk %s\n",
				key->spcoid, masterkey_id, dk(masterkey),
				dk(key->tblspkey), dk(k.tblspkey));
#endif

		rc = fwrite(&k, sizeof(TblspKeyData), 1, fpout);
		(void) rc; /* will check for error with ferror */
	}

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary keyring file \"%s\": %m",
						tmppath)));
		FreeFile(fpout);
		unlink(tmppath);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary keyring file \"%s\": %m",
						tmppath)));
		unlink(tmppath);
	}
	else if (durable_rename(tmppath, path, ERROR) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary keyring file \"%s\" to \"%s\": %m",
						tmppath, path)));
		unlink(tmppath);
	}
}

/*
 * Reencrypt all tablespace keys with the given key. The caller must
 * holt KeyringControlLock in exclusive mode.
 */
void
reencryptKeyring(const char *masterkey_id, const char *masterkey)
{
	if (keyring_invalid)
	{
		reload_keyring_file();
		keyring_invalid = false;
	}

	update_keyring_file(masterkey_id, masterkey);
}

/* Invalidation callback to clear all buffered tablespace keys */
static void
invalidate_keyring(Datum arg, int cacheid, uint32 hashvalue)
{
	elog(DEBUG1, "invalidate tablespace keyring caches");

#ifdef DEBUG_TDE
	fprintf(stderr, "tblsp_key::invalid tblspkeys %d\n", MyProcPid);
#endif
	keyring_invalid = true;
}

/*
 * Encrypt and decrypt routine for tablespace key
 */
static void
encrypt_tblsp_key(Oid spcoid, char *tblspkey, const char *masterkey)
{
	char tweak[ENCRYPTION_TWEAK_SIZE];

	/* And encrypt tablespace key before writing */
	key_encryption_tweak(tweak, spcoid);
	encrypt_block(tblspkey, tblspkey, ENCRYPTION_KEY_SIZE,
				  masterkey, tweak, false);

}

static void
decrypt_tblsp_key(Oid spcoid, char *tblspkey, const char *masterkey)
{
	char tweak[ENCRYPTION_TWEAK_SIZE];

	/* And encrypt tablespace key before writing */
	key_encryption_tweak(tweak, spcoid);
	decrypt_block(tblspkey, tblspkey, ENCRYPTION_KEY_SIZE,
				  masterkey, tweak, false);

}

static void
key_encryption_tweak(char *tweak, Oid spcoid)
{
	memset(tweak, 0, ENCRYPTION_TWEAK_SIZE);
	memcpy(tweak, &spcoid, sizeof(Oid));
}

Datum
pg_get_tablespace_keys(PG_FUNCTION_ARGS)
{
#define PG_TABLESPACE_KEYS_COLS 2
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS status;
	TblspKeyData *key;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* if the local cache is out of date, update it */
	if (keyring_invalid)
	{
		reload_keyring_file();
		keyring_invalid = false;
	}

	/* Get the master key and encrypt the tablespace key with it */

	/* Get all tablespace keys */
	hash_seq_init(&status, tblspKeyring);
	while ((key = (TblspKeyData *) hash_seq_search(&status)) != NULL)
	{
		Datum	values[PG_TABLESPACE_KEYS_COLS];
		bool	nulls[PG_TABLESPACE_KEYS_COLS];
		char	buf[ENCRYPTION_KEY_SIZE + 1];
		char	*masterkey;
		bytea	*data_bytea;

		memcpy(buf, key->tblspkey, ENCRYPTION_KEY_SIZE);

		/* Encrypt tablespace key */
		masterkey = GetMasterKey(currentMasterKeyId);
		encrypt_tblsp_key(key->spcoid, buf, masterkey);
		buf[ENCRYPTION_KEY_SIZE] = '\0';

		memset(nulls, 0, 2);
		values[0] = ObjectIdGetDatum(key->spcoid);

		data_bytea = (bytea *) palloc(ENCRYPTION_KEY_SIZE + VARHDRSZ);
		SET_VARSIZE(data_bytea, ENCRYPTION_KEY_SIZE + VARHDRSZ);
		memcpy(VARDATA(data_bytea), buf, ENCRYPTION_KEY_SIZE);
		values[1] = PointerGetDatum(data_bytea);

#ifdef DEBUG_TDE
		fprintf(stderr, "keyring::dump oid %u, mkid %s, mk %s, dk %s, edk %s\n",
				key->spcoid, currentMasterKeyId, dk(masterkey),
				dk(key->tblspkey), dk(buf));
#endif

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupestore);

	return (Datum) 0;
}
