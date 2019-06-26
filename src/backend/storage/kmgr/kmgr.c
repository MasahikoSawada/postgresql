/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Encryption key management module.
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
 *	  src/backend/storage/kmgr/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"

#include "access/xlog.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/kmgr_plugin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/syscache.h"

#define KEYRING_TBLSPC_FILE "pg_tblspc.kr"

#define FIRST_MASTER_KEY_SEQNO	0

/* GUC variable */
char *kmgr_plugin_library = NULL;

/* Struct for one tablespace key */
typedef struct TblspcKeyData
{
	Oid		spcoid;		/* hash key; must be first */
	char	tblspckey[ENCRYPTION_KEY_SIZE];
} TblspcKeyData;

/* The master key id and seqno written in the keyring file */
static char	masterKeyId_cache[MASTER_KEY_ID_LEN];
static MasterKeySeqNo masterKeySeqNo_cache;

/* Local cache for tablespace keys */
static HTAB *tblspcKeyring;

static MemoryContext tblspcKeyringContext;
static bool keyring_invalid = true;

PG_FUNCTION_INFO_V1(pg_rotate_encryption_key);
PG_FUNCTION_INFO_V1(pg_get_tablespace_keys);

static void reset_keyring_cache(void);
static void reload_keyring_file(void);
static char *read_keyring_file(List **keylist_p);
static void update_keyring_file(const char *masterkey_id,
								const char *masterkey);
static void reencrypt_keyring_file(const char *masterkey_id,
								   const char *masterkrey);
static TblspcKeyData *get_keyring_entry(Oid spcOid, bool *found);
static void invalidate_keyring(Datum arg, int cacheid, uint32 hashvalue);
static void key_encryption_tweak(char *tweak, Oid spcoid);
static void encrypt_tblspc_key(Oid spcoid, char *tblspckey, const char *masterkey);
static void decrypt_tblspc_key(Oid spcoid, char *tblspckey, const char *masterkey);
static void ensure_latest_keyring(bool need_lock);
static MasterKeySeqNo get_seqno_from_keyid(const char *masterkeyid);
static bool get_masterkey_id_from_file(char *masterkeyid_p,
									   MasterKeySeqNo *seqno_p);

/*
 * Get the master key via kmgr plugin, and store both key and id to the
 * shared memory. This function must be used at postmaster startup time
 * but after created shared memory.
 */
void
InitializeKmgr(void)
{
	MasterKeySeqNo seqno;
	char *key = NULL;

	if (!TransparentEncryptionEnabled())
		return;

	/* Invoke startup callback */
	KmgrPluginStartup();

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::initialize trying to get latest info from file\n");
#endif

	/* Read keyring file and get the master key id */
	if (!get_masterkey_id_from_file(masterKeyId_cache, &seqno))
	{
		/* First time, create initial keyring file */
		seqno = FIRST_MASTER_KEY_SEQNO;
		snprintf(masterKeyId_cache, MASTER_KEY_ID_LEN, MASTER_KEY_ID_FORMAT,
				 GetSystemIdentifier(), seqno);

#ifdef DEBUG_TDE
		fprintf(stderr, "kmgr::initialize initialize keyring file id %s\n",
				masterKeyId_cache);
#endif
		/* Create keyring file */
		update_keyring_file(masterKeyId_cache, NULL);
	}
#ifdef DEBUG_TDE
	else
		fprintf(stderr, "kmgr::initialize found keyring file, id %s seqno %u\n",
				masterKeyId_cache, seqno);
#endif

	Assert(seqno >= 0);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::initialize startup mkid %s, seqno %u\n",
			masterKeyId_cache, seqno);
#endif

	if (!KmgrPluginIsExist(masterKeyId_cache))
		KmgrPluginGenerateKey(masterKeyId_cache);

	/* Get the master key from plugin */
	key = KmgrPluginGetKey(masterKeyId_cache);

	if (key == NULL)
		elog(ERROR, "could not get the encryption master key via kmgr plugin during startup");

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::initialize set id %s, key %s, seq %u\n",
			masterKeyId_cache, dk(key), seqno);
#endif
}


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

/* process and load kmgr_plugin_library plugin */
void
processKmgrPlugin(void)
{
	process_shared_preload_libraries_in_progress = true;
	startupKmgrPlugin(kmgr_plugin_library);
	process_shared_preload_libraries_in_progress = false;
}

/*
 * Return the tablespace key string of the given tablespace, or NULL if not
 * found. Returned key string is ENCRYPTION_KEY_SIZE byte.
 */
char *
KeyringGetKey(Oid spcOid)
{
	TblspcKeyData *tskey;
	bool		found;
	char		*keystr;

	if (!TransparentEncryptionEnabled())
		return NULL;

	if (!OidIsValid(spcOid))
		return NULL;

#ifdef DEBUG_TDE
	fprintf(stderr, "keyring::get key oid %u\n", spcOid);
#endif

	tskey = get_keyring_entry(spcOid, &found);

	if (!found)
		return NULL;

	keystr = (char *) palloc(ENCRYPTION_KEY_SIZE);
	memcpy(keystr, tskey->tblspckey, ENCRYPTION_KEY_SIZE);

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
	TblspcKeyData *key;
	char		*masterkey;
	char		*retkey;
	bool		found;
	bool		ret;

	if (!TransparentEncryptionEnabled())
		return false;

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	ensure_latest_keyring(false);

	if (!tblspcKeyring)
		reset_keyring_cache();

	key = hash_search(tblspcKeyring, (void *) &spcOid, HASH_ENTER, &found);

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
	memcpy(key->tblspckey, retkey, ENCRYPTION_KEY_SIZE);

	/* Update tablespace key file */
	masterkey = KmgrPluginGetKey(masterKeyId_cache);
	update_keyring_file(masterKeyId_cache, masterkey);

	LWLockRelease(KeyringControlLock);

	/* The returned key also must be encrypted */
	encrypt_tblspc_key(spcOid, retkey, masterkey);

	return retkey;
}

/*
 * Drop one tablespace key from the local cache as well as the keyring file.
 */
void
KeyringDropKey(Oid spcOid)
{
	bool found;

	if (!TransparentEncryptionEnabled())
		return;

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	ensure_latest_keyring(false);

	hash_search(tblspcKeyring, (void *) &spcOid, HASH_REMOVE, &found);

	if (!found)
		ereport(ERROR,
				(errmsg("could not find tablespace encryption key for tablespace %u",
						spcOid)));

	/* Update tablespace key file */
	update_keyring_file(masterKeyId_cache, KmgrPluginGetKey(masterKeyId_cache));

	LWLockRelease(KeyringControlLock);
}

void
KeyringAddKey(Oid spcOid, char *encrypted_key, const char *masterkeyid)
{
	TblspcKeyData *key;
	char	buf[ENCRYPTION_KEY_SIZE];
	char	*masterkey;
	bool	found;

	/* Copy to work buffer */
	memcpy(buf, encrypted_key, ENCRYPTION_KEY_SIZE);

	masterkey = KmgrPluginGetKey(masterkeyid);

	/* Decrypt tablespace key with the master key */
	decrypt_tblspc_key(spcOid, buf, masterkey);

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

#ifdef DEBUG_TDE
	fprintf(stderr, "keyring::add during recov oid %u, dke %s, mkid %s\n",
			spcOid, dk(encrypted_key), masterkeyid);
#endif

	key = hash_search(tblspcKeyring, (void *) &spcOid, HASH_ENTER, &found);

	if (found)
	{
		LWLockRelease(KeyringControlLock);
		return;
	}
//		ereport(ERROR,
//				(errmsg("encryption key for tablespace %u already exists",
//						spcOid)));

	/* Store the raw key to the hash */
	memcpy(key->tblspckey, buf, ENCRYPTION_KEY_SIZE);

	/* Update keyring file */
	update_keyring_file(masterKeyId_cache, KmgrPluginGetKey(masterKeyId_cache));

	LWLockRelease(KeyringControlLock);
}

/*
 * Initialize keyring memory context and local keyring hash table.
 */
static void
reset_keyring_cache(void)
{
	HASHCTL hash_ctl;

	/* Destory old keyring if exists */
	if (tblspcKeyring)
	{
		hash_destroy(tblspcKeyring);
		tblspcKeyring = NULL;
	}

	if (!tblspcKeyringContext)
		tblspcKeyringContext = AllocSetContextCreate(TopMemoryContext,
													"Tablespace keys",
													ALLOCSET_DEFAULT_SIZES);

	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TblspcKeyData);
	hash_ctl.hcxt = tblspcKeyringContext;

	tblspcKeyring = hash_create("tablespace key ring",
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
static TblspcKeyData *
get_keyring_entry(Oid spcOid, bool *found)
{
	TblspcKeyData *key;

	ensure_latest_keyring(true);

	/* quick return if the keyring is empty */
	if (!tblspcKeyring)
	{
		*found = false;
		return NULL;
	}

	key = hash_search(tblspcKeyring, (void *) &spcOid, HASH_FIND, found);

	if (!(*found) &&
		(AmCheckpointerProcess() || AmBackgroundWriterProcess()))
	{
#ifdef DEBUG_TDE
	fprintf(stderr, "keyring::not found key, so reload and retry oid %u\n", spcOid);
#endif

	/*
		 * It's very optimistic. Since it's possible that the local cache
		 * is out of date we reload the up-to-date keyring and try to find
		 * again.
		 *
		 * XXX : perhaps this is required only by checkpointer and bgwriter.
		 */
		LWLockAcquire(KeyringControlLock, LW_SHARED);
		reload_keyring_file();
		LWLockRelease(KeyringControlLock);

		key = hash_search(tblspcKeyring, (void *) &spcOid, HASH_FIND, found);
	}

	return key;
}

/*
 * Load the keyring file into the local cache.
 */
static void
reload_keyring_file(void)
{
	List *keylist = NIL;
	ListCell *lc;
	char *masterkeyid;
	char *masterkey;

	masterkeyid = read_keyring_file(&keylist);

	/* There is no key in the file */
	if (masterkeyid == NULL || keylist == NIL)
	{
#ifdef DEBUG_TDE
		fprintf(stderr, "tblspc_key::reload loaded 0 keys by pid %d\n",
				MyProcPid);
#endif
		tblspcKeyring = NULL;
		return;
	}

	/* cleanup the existing keyring */
	reset_keyring_cache();

	/* Update local master key id */
	memcpy(masterKeyId_cache, masterkeyid, MASTER_KEY_ID_LEN);

	/* Get the master key by identifier */
	masterkey = KmgrPluginGetKey(masterKeyId_cache);

#ifdef DEBUG_TDE
	fprintf(stderr, "keyring::reload get master key id %s, key %s\n",
			masterKeyId_cache, dk(masterkey));
#endif

	/* Update local keyring cache */
	foreach (lc, keylist)
	{
		TblspcKeyData *key = (TblspcKeyData *) lfirst(lc);
		TblspcKeyData *cache_key;

		cache_key = hash_search(tblspcKeyring, (void *) &key->spcoid,
								HASH_ENTER, NULL);

		/* Decyrpt tablespace key by the master key before caching */
		decrypt_tblspc_key(key->spcoid, key->tblspckey, masterkey);

#ifdef DEBUG_TDE
		fprintf(stderr, "    keyring::reload load oid = %u, mkid = %s, mk = %s, dk = %s\n",
				key->spcoid, masterKeyId_cache, dk(masterkey), dk(key->tblspckey));
#endif
		memcpy(cache_key, key, sizeof(TblspcKeyData));
	}

#ifdef DEBUG_TDE
	fprintf(stderr, "    keyring::reload loaded %d keys by pid %d\n",
			list_length(keylist), MyProcPid);
#endif

	/* Update sequence number */
	masterKeySeqNo_cache = get_seqno_from_keyid(masterKeyId_cache);

	list_free_deep(keylist);
}

/*
 * Read the keyring file and set *masterkeyid to the master key id.
 */
static bool
get_masterkey_id_from_file(char *masterkeyid_p, MasterKeySeqNo *seqno_p)
{
	List *dummy = NIL;
	char *masterkeyid;

	/* Read the latest keyring file */
	masterkeyid = read_keyring_file(&dummy);

	if (!masterkeyid)
		return false;

	/* Success */
	memcpy(masterkeyid_p, masterkeyid, MASTER_KEY_ID_LEN);
	*seqno_p = get_seqno_from_keyid(masterkeyid);

	return true;
}

/*
 * Read the keyring file and return the list of tablespace keys.
 */
static char *
read_keyring_file(List **keylist_p)
{
	char *path = "global/"KEYRING_TBLSPC_FILE;
	char *mkeyid;
	int read_len;
	int fd;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (errno == ENOENT)
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	/* Read */
	mkeyid = palloc0(MASTER_KEY_ID_LEN);
	if ((read_len = read(fd, mkeyid, MASTER_KEY_ID_LEN)) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", path))));

	for (;;)
	{
		TblspcKeyData *key = palloc(sizeof(TblspcKeyData));

		read_len = read(fd, key, sizeof(TblspcKeyData));

		if (read_len < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", path))));
		else if (read_len == 0) /* EOF */
			break;
		else if (read_len != sizeof(TblspcKeyData))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": read %d instead of %d bytes",
							path, read_len, (int32) sizeof(TblspcKeyData))));
		*keylist_p = lappend(*keylist_p, key);
	}

	CloseTransientFile(fd);

	return mkeyid;
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
	TblspcKeyData *key;
	char path[MAXPGPATH];
	char tmppath[MAXPGPATH];
	FILE *fpout;
	int	rc;

	sprintf(path, "global/"KEYRING_TBLSPC_FILE);
	sprintf(tmppath, "global/"KEYRING_TBLSPC_FILE".tmp");

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
	rc = fwrite(masterkey_id, MASTER_KEY_ID_LEN, 1, fpout);

	/* If we have any tablespace keys, write them to the file.  */
	if (tblspcKeyring)
	{
		/* Write tablespace key to the file */
		hash_seq_init(&status, tblspcKeyring);
		while ((key = (TblspcKeyData *) hash_seq_search(&status)) != NULL)
		{
			TblspcKeyData k;

			/* Copy to work buffer */
			memcpy(&k, key, sizeof(TblspcKeyData));

			/* Prepare tablespace key and master key to write */
			encrypt_tblspc_key(key->spcoid, (char *) &k.tblspckey, masterkey);

#ifdef DEBUG_TDE
			fprintf(stderr, "keyring::udpate file::reenc tblspckey oid %u, mkid %s, mk %s, dk %s, edk %s\n",
					key->spcoid, masterkey_id, dk(masterkey),
					dk(key->tblspckey), dk(k.tblspckey));
#endif

			rc = fwrite(&k, sizeof(TblspcKeyData), 1, fpout);
			(void) rc; /* will check for error with ferror */
		}
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
static void
reencrypt_keyring_file(const char *masterkey_id, const char *masterkey)
{
	update_keyring_file(masterkey_id, masterkey);
}

/* Invalidation callback to clear all buffered tablespace keys */
static void
invalidate_keyring(Datum arg, int cacheid, uint32 hashvalue)
{
	elog(DEBUG1, "invalidate tablespace keyring caches");

#ifdef DEBUG_TDE
	fprintf(stderr, "tblspc_key::invalid tblspckeys %d\n", MyProcPid);
#endif
	keyring_invalid = true;
}

/*
 * Encrypt and decrypt routine for tablespace key
 */
static void
encrypt_tblspc_key(Oid spcoid, char *tblspckey, const char *masterkey)
{
	char tweak[ENCRYPTION_TWEAK_SIZE];

	/* And encrypt tablespace key before writing */
	key_encryption_tweak(tweak, spcoid);
	encrypt_block(tblspckey, tblspckey, ENCRYPTION_KEY_SIZE,
				  masterkey, tweak, false);

}

static void
decrypt_tblspc_key(Oid spcoid, char *tblspckey, const char *masterkey)
{
	char tweak[ENCRYPTION_TWEAK_SIZE];

	/* And encrypt tablespace key before writing */
	key_encryption_tweak(tweak, spcoid);
	decrypt_block(tblspckey, tblspckey, ENCRYPTION_KEY_SIZE,
				  masterkey, tweak, false);

}

static void
key_encryption_tweak(char *tweak, Oid spcoid)
{
	memset(tweak, 0, ENCRYPTION_TWEAK_SIZE);
	memcpy(tweak, &spcoid, sizeof(Oid));
}

static MasterKeySeqNo
get_seqno_from_keyid(const char *masterkeyid)
{
	MasterKeySeqNo seqno;
	char	seqno_str[5];
	uint64	dummy;

	/* Got the maste key id, got sequence number */
	sscanf(masterkeyid, MASTER_KEY_ID_FORMAT_SCAN,  &dummy, seqno_str);
	seqno = atoi(seqno_str);

	Assert(seqno >= 0);

	return seqno;
}

static void
ensure_latest_keyring(bool need_lock)
{
	if (need_lock)
		LWLockAcquire(KeyringControlLock, LW_SHARED);

	AcceptInvalidationMessages();

	if (keyring_invalid)
	{
#ifdef DEBUG_TDE
		fprintf(stderr, "keyring::ensure cache is invalid, so reload\n");
#endif
		reload_keyring_file();
		keyring_invalid = false;
	}
//#ifdef DEBUG_TDE
//	else
//		fprintf(stderr, "keyring::ensure no need reload\n");
//#endif

	if (need_lock)
		LWLockRelease(KeyringControlLock);
}

/*
 * Rotate the master key and reencrypt all tablespace keys with new one.
 */
Datum
pg_rotate_encryption_key(PG_FUNCTION_ARGS)
{
	char masterkeyid[MASTER_KEY_ID_LEN];
	char newid[MASTER_KEY_ID_LEN + 1] = {0};
	char *newkey;
	MasterKeySeqNo seqno;

	/* prevent concurrent process trying key rotation */
	LWLockAcquire(MasterKeyRotationLock, LW_EXCLUSIVE);

	ensure_latest_keyring(true);

	/* Get latest master key and seqno from file, not cache */
	if (!get_masterkey_id_from_file(masterkeyid, &seqno))
		elog(ERROR, "invalid keyring file");

	/* Craft the new master key id */
	sprintf(newid, MASTER_KEY_ID_FORMAT, GetSystemIdentifier(), seqno + 1);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::rotate new id id %s, oldseq %u\n",
			newid, seqno);
#endif

	/* Get new master key */
	KmgrPluginGenerateKey(newid);
	newkey = KmgrPluginGetKey(newid);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::rotate generated new id id %s, key %s\n",
			newid, dk(newkey));
#endif

	/* Block concurrent processes are about to read the keyring file */
	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	/*
	 * Reencrypt all tablespace keys with the new master key, and update
	 * the keyring file.
	 */
	reencrypt_keyring_file(newid, newkey);

	/* Invalidate keyring caches before releasing the lock */
	SysCacheInvalidate(TABLESPACEOID, (Datum) 0);

	/* Ok allows processes to read the keyring file */
	LWLockRelease(KeyringControlLock);

	LWLockRelease(MasterKeyRotationLock);

	PG_RETURN_TEXT_P(cstring_to_text(newid));
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
	TblspcKeyData *key;

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

	ensure_latest_keyring(true);

	/* Get the master key and encrypt the tablespace key with it */

	/* Get all tablespace keys */
	hash_seq_init(&status, tblspcKeyring);
	while ((key = (TblspcKeyData *) hash_seq_search(&status)) != NULL)
	{
		Datum	values[PG_TABLESPACE_KEYS_COLS];
		bool	nulls[PG_TABLESPACE_KEYS_COLS];
		char	buf[ENCRYPTION_KEY_SIZE + 1];
		char	*masterkey;
		bytea	*data_bytea;

		memcpy(buf, key->tblspckey, ENCRYPTION_KEY_SIZE);

		/* Encrypt tablespace key */
		masterkey = KmgrPluginGetKey(masterKeyId_cache);
		encrypt_tblspc_key(key->spcoid, buf, masterkey);
		buf[ENCRYPTION_KEY_SIZE] = '\0';

		memset(nulls, 0, 2);
		values[0] = ObjectIdGetDatum(key->spcoid);

		data_bytea = (bytea *) palloc(ENCRYPTION_KEY_SIZE + VARHDRSZ);
		SET_VARSIZE(data_bytea, ENCRYPTION_KEY_SIZE + VARHDRSZ);
		memcpy(VARDATA(data_bytea), buf, ENCRYPTION_KEY_SIZE);
		values[1] = PointerGetDatum(data_bytea);

#ifdef DEBUG_TDE
		fprintf(stderr, "keyring::dump oid %u, mkid %s, mk %s, dk %s, edk %s\n",
				key->spcoid, masterKeyId_cache, dk(masterkey),
				dk(key->tblspckey), dk(buf));
#endif

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupestore);

	return (Datum) 0;
}
