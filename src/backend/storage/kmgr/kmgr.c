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

/* Filename of tablespace keyring */
#define KEYRING_TBLSPC_FILE "pg_tblspc.kr"

/*
 * Since we have encryption keys per tablspace, we expect this value is enough
 * for most usecase.
 */
#define KMGR_KEYRING_SIZE 128

/* Struct for one tablespace key, and hash entry */
typedef struct TblspcKeyData
{
	Oid		spcoid;		/* hash key; must be first */
	char	tblspckey[ENCRYPTION_KEY_SIZE];	/* non-encrypted key */
} TblspcKeyData;

/*
 * Shared struct to save master key information written in the keyring file.
 */
typedef struct KmgrCtlData
{
	char			masterKeyId[MASTER_KEY_ID_LEN];
	MasterKeySeqNo	masterKeySeqNo;
} KmgrCtlData;

/* GUC variable */
char *kmgr_plugin_library = NULL;

/*
 * Shared variables.  The following shared variables are protected by
 * KeyringControlLock.
*/
static KmgrCtlData *KmgrCtl;
static HTAB			*TblspcKeyring;

PG_FUNCTION_INFO_V1(pg_rotate_encryption_key);
PG_FUNCTION_INFO_V1(pg_get_tablespace_keys);

static bool load_keyring_file(void);
static char *read_keyring_file(List **keylist_p);
static void update_keyring_file(void);
static void update_keyring_file_extended(const char *masterkey_id,
										 const char *masterkey);
static void key_encryption_tweak(char *tweak, Oid spcoid);
static void encrypt_tblspc_key(Oid spcoid, char *tblspckey, const char *masterkey);
static void decrypt_tblspc_key(Oid spcoid, char *tblspckey, const char *masterkey);
static MasterKeySeqNo get_seqno_from_master_key_id(const char *masterkeyid);
static void create_master_key_id(char *id, MasterKeySeqNo seqno);

Size
KmgrShmemSize(void)
{
	Size size;

	size = MAXALIGN(sizeof(KmgrCtlData));
	size = add_size(size,
					hash_estimate_size(KMGR_KEYRING_SIZE, sizeof(TblspcKeyData)));

	return size;
}

void
KmgrShmemInit(void)
{
	HASHCTL hash_ctl;
	bool	found;

	KmgrCtl = ShmemInitStruct("KmgrCtl shared",
							  KmgrShmemSize(),
							  &found);

	if (!found)
		MemSet(KmgrCtl, 0, sizeof(KmgrCtlData));

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TblspcKeyData);

	TblspcKeyring = ShmemInitHash("kmgr keyring hash",
								  KMGR_KEYRING_SIZE, KMGR_KEYRING_SIZE,
								  &hash_ctl,
								  HASH_ELEM | HASH_BLOBS);
}

/*
 * Initialize Kmgr and kmgr plugin. We load the keyring file and set up both
 * KmgrCtl and TblspcKeyring hash table on shared memory. When first time to
 * access the keyring file, ie the keyring file does not exist, we create it
 * with the initial master key id. If the keyring file exists, we load it to
 * the shared structs. This function must be called by postmaster at startup
 * time.
 */
void
InitializeKmgr(void)
{
	char *key = NULL;

	if (!TransparentEncryptionEnabled())
		return;

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::initialize trying to get latest info from file\n");
#endif

	/*
	 * Invoke kmgr plugin startup callback. Since we could get the master key
	 * during loading the keyring file we have to startup the plugin beforehand.
	 */
	ereport(DEBUG1, (errmsg("invoking kmgr plugin startup callback")));
	KmgrPluginStartup();

	/* Load keyring file and update shmem structs */
	if (!load_keyring_file())
	{
		/*
		 * If the keyring doesn't exist this is the first time to access the
		 * keyring file. We create it with the initial master key id.
		 */
		create_master_key_id(KmgrCtl->masterKeyId, FIRST_MASTER_KEY_SEQNO);
		KmgrCtl->masterKeySeqNo = FIRST_MASTER_KEY_SEQNO;

#ifdef DEBUG_TDE
		fprintf(stderr, "kmgr::initialize initialize keyring file id %s\n",
				KmgrCtl->masterKeyId);
#endif

		/* Create keyring file only with only master key id */
		LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);
		update_keyring_file_extended(KmgrCtl->masterKeyId, NULL);
		LWLockRelease(KeyringControlLock);
	}
#ifdef DEBUG_TDE
	else
		fprintf(stderr, "kmgr::initialize found keyring file, id %s seqno %u\n",
				KmgrCtl->masterKeyId, KmgrCtl->masterKeySeqNo);
#endif

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::initialize startup mkid %s, seqno %u\n",
			KmgrCtl->masterKeyId, KmgrCtl->masterKeySeqNo);
#endif

	/* Create the master key if not exists */
	if (!KmgrPluginIsExist(KmgrCtl->masterKeyId))
		KmgrPluginGenerateKey(KmgrCtl->masterKeyId);

	/* Get the master key */
	key = KmgrPluginGetKey(KmgrCtl->masterKeyId);

	Assert(key != NULL);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::initialize set id %s, key %s, seq %u\n",
			KmgrCtl->masterKeyId, dk(key), KmgrCtl->masterKeySeqNo);
#endif
}

/* process and load kmgr plugin given by kmgr_plugin_library */
void
processKmgrPlugin(void)
{
	process_shared_preload_libraries_in_progress = true;
	startupKmgrPlugin(kmgr_plugin_library);
	process_shared_preload_libraries_in_progress = false;
}

/* Set the encryption key of the given tablespace to *key */
void
KeyringGetKey(Oid spcOid, char *key)
{
	TblspcKeyData *tskey;
	bool		found;

	Assert(OidIsValid(spcOid));

#ifdef DEBUG_TDE
	fprintf(stderr, "keyring::get key oid %u\n", spcOid);
#endif

	LWLockAcquire(KeyringControlLock, LW_SHARED);

	tskey = hash_search(TblspcKeyring, (void *) &spcOid,
						HASH_FIND, &found);

	LWLockRelease(KeyringControlLock);

	if (!found)
		ereport(ERROR, (errmsg("could not find encryption key for tablespace %u",
							   spcOid)));

	/* Set encryption key */
	memcpy(key, tskey->tblspckey, ENCRYPTION_KEY_SIZE);
}

/*
 * Check the tablespace key is exists. Since encrypted tablespaces has its
 * encryption key this function can be used to check if the tablespace is
 * encrypted.
 */
bool
KeyringKeyExists(Oid spcOid)
{
	bool		found;

	Assert(OidIsValid(spcOid));

	LWLockAcquire(KeyringControlLock, LW_SHARED);

	(void *) hash_search(TblspcKeyring, (void *) &spcOid, HASH_FIND, &found);

	LWLockRelease(KeyringControlLock);

	return found;
}

/*
 * Generate new tablespace key and update the keyring file. Return encrypted
 * key of new tablespace key.
 */
char *
KeyringCreateKey(Oid spcOid)
{
	TblspcKeyData *key;
	char		*masterkey;
	char		*retkey;
	bool		found;
	bool		ret;

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	key = hash_search(TblspcKeyring, (void *) &spcOid,
					  HASH_ENTER, &found);

	if (found)
		elog(ERROR, "found duplicate tablespace encryption key for tablespace %u",
			 spcOid);

	/* Generate a random tablespace key */
	retkey = (char *) palloc0(ENCRYPTION_KEY_SIZE);
	ret = pg_strong_random(retkey, ENCRYPTION_KEY_SIZE);
	if (!ret)
		ereport(ERROR,
				(errmsg("failed to generate tablespace encryption key")));
	memcpy(key->tblspckey, retkey, ENCRYPTION_KEY_SIZE);

	/* Update tablespace key file */
	update_keyring_file();

	LWLockRelease(KeyringControlLock);

	/* The returned key must be encrypted */
	masterkey = KmgrPluginGetKey(KmgrCtl->masterKeyId);
	encrypt_tblspc_key(spcOid, retkey, masterkey);

	return retkey;
}

/*
 * Drop one tablespace key from the keyring hash table and update the keyring
 * file.
 */
void
KeyringDropKey(Oid spcOid)
{
	bool found;

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	hash_search(TblspcKeyring, (void *) &spcOid, HASH_REMOVE, &found);

	if (!found)
		elog(ERROR, "could not find tablespace encryption key for tablespace %u",
			 spcOid);

	LWLockRelease(KeyringControlLock);
	/* Update tablespace key file */
	update_keyring_file();
}

/*
 * Add a tablespace key of given tablespace to the keyring hash table.
 * *encrrypted_key is encrypted with the encryption key identified by
 * masterkeyid. If the encryption key of the tablespace already exists,
 * we check if these keys are the same.
 */
void
KeyringAddKey(Oid spcOid, char *encrypted_key, const char *masterkeyid)
{
	TblspcKeyData *key;
	char	buf[ENCRYPTION_KEY_SIZE];
	char	*masterkey;
	bool	found;

	/* Copy to work buffer */
	memcpy(buf, encrypted_key, ENCRYPTION_KEY_SIZE);

	/* Get the master key */
	masterkey = KmgrPluginGetKey(masterkeyid);

	/* Decrypt tablespace key with the master key */
	decrypt_tblspc_key(spcOid, buf, masterkey);

	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

#ifdef DEBUG_TDE
	fprintf(stderr, "keyring::add during recov oid %u, dke %s, mkid %s\n",
			spcOid, dk(encrypted_key), masterkeyid);
#endif

	key = hash_search(TblspcKeyring, (void *) &spcOid, HASH_ENTER,
					  &found);

	if (found)
	{
		LWLockRelease(KeyringControlLock);

		if (strncmp(key->tblspckey, buf, ENCRYPTION_KEY_SIZE) != 0)
			elog(ERROR, "adding encryption key for tablespace %u does not match the exsiting one",
				spcOid);

		/* The existing key is the same, return */
		return;
	}

	/* Store the raw key to the hash */
	memcpy(key->tblspckey, buf, ENCRYPTION_KEY_SIZE);

	/* Update keyring file */
	update_keyring_file();

	LWLockRelease(KeyringControlLock);
}

/*
 * Load the keyring file and update the shared variables. This function is
 * intended to be used by postmaster at startup time , so lockings are not
 * needed.
 */
static bool
load_keyring_file(void)
{
	List *keylist = NIL;
	ListCell *lc;
	char *masterkeyid;
	char *masterkey;

	/* Read keyring file */
	masterkeyid = read_keyring_file(&keylist);

	/* There is no keyring file */
	if (masterkeyid == NULL)
	{
		/* We must not get any keys as well */
		Assert(keylist == NIL);
#ifdef DEBUG_TDE
		fprintf(stderr, "kmgr::reload loaded 0 keys by pid %d\n",
				MyProcPid);
#endif
		return false;
	}

	/* We got keyring file. Update the master key id and sequence number */
	memcpy(KmgrCtl->masterKeyId, masterkeyid, MASTER_KEY_ID_LEN);
	KmgrCtl->masterKeySeqNo = get_seqno_from_master_key_id(masterkeyid);

	/* Get the master key */
	masterkey = KmgrPluginGetKey(masterkeyid);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::reload get master key id %s, key %s\n",
			masterkeyid, dk(masterkey));
#endif

	/* Loading tablespace keys to shared keyring hash table */
	foreach (lc, keylist)
	{
		TblspcKeyData *key_infile = (TblspcKeyData *) lfirst(lc);
		TblspcKeyData *key;

		key = hash_search(TblspcKeyring, (void *) &key_infile->spcoid,
						  HASH_ENTER, NULL);

		/* Decyrpt tablespace key by the master key */
		decrypt_tblspc_key(key->spcoid, key_infile->tblspckey, masterkey);

		/* Set unencrypted tablespace key to the keyring hash table */
		memcpy(key->tblspckey, key_infile->tblspckey, ENCRYPTION_KEY_SIZE);

#ifdef DEBUG_TDE
		fprintf(stderr, "    kmgr::reload load oid = %u, mkid = %s, mk = %s, dk = %s\n",
				key->spcoid, masterkeyid, dk(masterkey), dk(key->tblspckey));
#endif
	}

#ifdef DEBUG_TDE
	fprintf(stderr, "    kmgr::reload loaded %d keys by pid %d\n",
			list_length(keylist), MyProcPid);
#endif

	list_free_deep(keylist);

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
 * Update the keyring file with the current master key */
static void
update_keyring_file(void)
{
	update_keyring_file_extended(KmgrCtl->masterKeyId,
								 KmgrPluginGetKey(KmgrCtl->masterKeyId));
}

/*
 * Update the keyring file with the specified key and id. *masterkey_id will
 * be written to the beginning of keyring file and tablespace keys encrypted
 * with *masterkey follows. The caller must hold KeyringControlLock in
 * exclusive mode to prevent keyring file from concurrent update.
 */
static void
update_keyring_file_extended(const char *masterkey_id, const char *masterkey)
{
	HASH_SEQ_STATUS status;
	TblspcKeyData *key;
	char path[MAXPGPATH];
	char tmppath[MAXPGPATH];
	FILE *fpout;
	int	rc;

	Assert(LWLockHeldByMeInMode(KeyringControlLock, LW_EXCLUSIVE));

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
	if (hash_get_num_entries(TblspcKeyring) > 0)
	{
		/* Write tablespace key to the file */
		hash_seq_init(&status, TblspcKeyring);
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

static void
create_master_key_id(char *id, MasterKeySeqNo seqno)
{
	 char sysid[32];
	 Assert(id != NULL);

	 snprintf(sysid, sizeof(sysid), UINT64_FORMAT, GetSystemIdentifier());
	 snprintf(id, MASTER_KEY_ID_LEN, MASTER_KEY_ID_FORMAT,
			  sysid, seqno);
}

/* Return the sequence number written in the *masterkeyid */
static MasterKeySeqNo
get_seqno_from_master_key_id(const char *masterkeyid)
{
	MasterKeySeqNo seqno;
	uint32	dummy;

	/* Get the sequence number */
	sscanf(masterkeyid, MASTER_KEY_ID_FORMAT_SCAN, &dummy, &seqno);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::extract mkeyid seqno %u, dummy %u\n",
			seqno, dummy);
#endif
	Assert(seqno >= 0);

	return seqno;
}

/*
 * Rotate the master key. This function generate new master key id and
 * require the kmgr plugin to generate the corresponding key. And then, using
 * the new master key we update keyring file while encrypt all tablespace keys.
 */
Datum
pg_rotate_encryption_key(PG_FUNCTION_ARGS)
{
	char newid[MASTER_KEY_ID_LEN] = {0};
	char retid[MASTER_KEY_ID_LEN + 1];
	char *newkey;
	MasterKeySeqNo current_seqno;

	/* prevent concurrent process trying key rotation */
	LWLockAcquire(MasterKeyRotationLock, LW_EXCLUSIVE);

	/* Get the current sequence number */
	LWLockAcquire(KeyringControlLock, LW_SHARED);
	current_seqno = KmgrCtl->masterKeySeqNo;
	LWLockRelease(KeyringControlLock);

	/* Craft the new master key id */
	create_master_key_id(newid, current_seqno + 1);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::rotate new id id %s, oldseq %u\n",
			newid, current_seqno);
#endif

	/* Generate the new master key by the new id */
	KmgrPluginGenerateKey(newid);
	newkey = KmgrPluginGetKey(newid);

	Assert(newkey);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::rotate generated new id id %s, key %s\n",
			newid, dk(newkey));
#endif

	/*
	 * Update share memory information with the next id and sequence number.
	 */
	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	KmgrCtl->masterKeySeqNo = current_seqno + 1;
	memcpy(KmgrCtl->masterKeyId, newid, MASTER_KEY_ID_LEN);

	/*
	 * Reencrypt all tablespace keys with the new master key, and update
	 * the keyring file.
	 */
	update_keyring_file_extended(newid, newkey);

	LWLockRelease(KeyringControlLock);

	LWLockRelease(MasterKeyRotationLock);

	/* Craft key id for the return value */
	memcpy(retid, newid, MASTER_KEY_ID_LEN);
	retid[MASTER_KEY_ID_LEN] = '\0';

	PG_RETURN_TEXT_P(cstring_to_text(retid));
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

	/* Get the master key and encrypt the tablespace key with it */

	/* Get all tablespace keys */
	hash_seq_init(&status, TblspcKeyring);
	while ((key = (TblspcKeyData *) hash_seq_search(&status)) != NULL)
	{
		Datum	values[PG_TABLESPACE_KEYS_COLS];
		bool	nulls[PG_TABLESPACE_KEYS_COLS];
		char	buf[ENCRYPTION_KEY_SIZE + 1];
		char	*masterkey;
		bytea	*data_bytea;

		memcpy(buf, key->tblspckey, ENCRYPTION_KEY_SIZE);

		/* Encrypt tablespace key */
		masterkey = KmgrPluginGetKey(KmgrCtl->masterKeyId);
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
				key->spcoid, KmgrCtl->masterKeyId, dk(masterkey),
				dk(key->tblspckey), dk(buf));
#endif

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupestore);

	return (Datum) 0;
}
