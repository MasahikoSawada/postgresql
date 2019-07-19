/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Encryption key management module.
 *
 * This module manages both all tablespace keys and the master key identifer,
 * for transparent data encryption. TDE uses two types of keys: master key and
 * tablespace key. We have tablespace key for each tablespace whose encryption
 * is enabled and encrypt relation data and WAL with the corresponding
 * tablespace key. The bunch of tablespace keys are called 'keyring'. The
 * keyring is loaded to shared memory from the keyring file located on
 * global/pg_tblspc.kr at postmaster startup, and is modified each time when
 * encrypted tablespace is either created or dropped. The master key is used to
 * encrypt tablespace keys. On the shared memory we have non-encrypted
 * tablespace keys in the keyring but when we persist them to the disk we must
 * encrypt each of them with the master key.
 *
 * We also write the master key identifier that used to encrypt the persisted
 * tablespace keys. The master key identifier is passed to kmgr plugin callbacks.
 * The master key identifer consists of the system identifer and the master key
 * sequence number, which is unsigned integer starting from
 * FIRST_MASTER_KEY_SEQNO. The sequence number is incremented whenever key
 * rotation. When key rotation, we generates a new master key and then update
 * keyring file while reencrypting all  tablespace keys with the new master key.
 * We don't need to reencrypt relation data itself.
 *
 * LOCKING:
 * After loaded the keyring to the shared memory, we have the hash table which
 * maps between tablespace oid and encryption key, and have the master key
 * identifer and sequence number. These fields are protected by
 * KeyringControlLock. Also when updating the keyring file we have to be holding
 * KeyringControlLock in exclusive mode to prevent the keyring file from being
 * updated by concurrent processes. The process who want to rotate the master key
 * needs to hold MasterKeyRotationLock in exclusive mode until end of all
 * operation as well as KeyringControlLock during updating the keyring file.
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
 * KmgrCtl and TblspcKeyring are protected by KeyringControlFile lightweight
 * lock. If we want to increment master key sequence number when key rotation,
 * we need to acquire MasterKeyRotationLock in addition.
 */
typedef struct KmgrCtlData
{
	char			masterKeyId[MASTER_KEY_ID_LEN];
	MasterKeySeqNo	masterKeySeqNo;
} KmgrCtlData;
static KmgrCtlData *KmgrCtl;
static HTAB			*TblspcKeyring;

/* GUC variable */
char *database_encryption_key_passphrase_command = NULL;

PG_FUNCTION_INFO_V1(pg_rotate_encryption_key);
PG_FUNCTION_INFO_V1(pg_get_tablespace_keys);

static void run_database_encryption_key_passpharse_command(const char *prompt,
														   char *buf, int size);
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
 * Run database_encryption_key_passphrase_command
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf, which is of size size.  The return
 * value is the length of the actual result.
 */
static void
run_database_encryption_key_passpharse_command(const char *prompt,
											   char *buf, int size)
{
	StringInfoData command;
	char	   *p;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	Assert(prompt);
	Assert(size > 0);
	buf[0] = '\0';

	initStringInfo(&command);

	for (p = database_encryption_key_passphrase_command; *p; p++)
	{
		if (p[0] == '%')
		{
			switch (p[1])
			{
				case 'p':
					appendStringInfoString(&command, prompt);
					p++;
					break;
				case '%':
					appendStringInfoChar(&command, '%');
					p++;
					break;
				default:
					appendStringInfoChar(&command, p[0]);
			}
		}
		else
			appendStringInfoChar(&command, p[0]);
	}

	fh = OpenPipeStream(command.data, "r");
	if (fh == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command.data)));

	if (!fgets(buf, size, fh))
	{
		if (ferror(fh))
		{
			pfree(command.data);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from command \"%s\": %m",
							command.data)));
		}
	}

	pclose_rc = ClosePipeStream(fh);
	if (pclose_rc == -1)
	{
		pfree(command.data);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
	}
	else if (pclose_rc != 0)
	{
		pfree(command.data);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("command \"%s\" failed",
						command.data),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
	}

	/* strip trailing newline */
	len = strlen(buf);
	if (len > 0 && buf[len - 1] == '\n')
		buf[--len] = '\0';

	pfree(command.data);
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
	char passphrase[ENCRYPTION_MAX_PASSPHASE];
	const char *prompt = "SSL_CTX_set_default_passwd_cb:";

	if (!TransparentEncryptionEnabled())
		return;

	/* Get encryption key passphrase */
	run_database_encryption_key_passpharse_command(prompt,
												   passphrase,
												   ENCRYPTION_MAX_PASSPHASE);

	/* Load keyring file and update shmem structs */
	if (!load_keyring_file())
	{
		/*
		 * If the keyring doesn't exist this is the first time to access the
		 * keyring file. We create it with the initial master key id.
		 */
		create_master_key_id(KmgrCtl->masterKeyId, FIRST_MASTER_KEY_SEQNO);
		KmgrCtl->masterKeySeqNo = FIRST_MASTER_KEY_SEQNO;

		/* Create keyring file only with only master key id */
		LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);
		update_keyring_file_extended(KmgrCtl->masterKeyId, NULL);
		LWLockRelease(KeyringControlLock);

		ereport(DEBUG3, (errmsg("create initial keyring file with master key id")));
	}

	/* Create the master key if not exists */
	if (!KmgrPluginIsExist(KmgrCtl->masterKeyId))
		KmgrPluginGenerateKey(KmgrCtl->masterKeyId);

	/* Get the master key */
	key = KmgrPluginGetKey(KmgrCtl->masterKeyId);

	Assert(key != NULL);
}

/* Set the encryption key of the given tablespace to *key */
void
KeyringGetKey(Oid spcOid, char *key)
{
	TblspcKeyData *tskey;
	bool		found;

	Assert(OidIsValid(spcOid));

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
		return false;
	}

	/* We got keyring file. Update the master key id and sequence number */
	memcpy(KmgrCtl->masterKeyId, masterkeyid, MASTER_KEY_ID_LEN);
	KmgrCtl->masterKeySeqNo = get_seqno_from_master_key_id(masterkeyid);

	/* Get the master key */
	masterkey = KmgrPluginGetKey(masterkeyid);

	/* Loading tablespace keys to shared keyring hash table */
	foreach (lc, keylist)
	{
		TblspcKeyData *key_infile = (TblspcKeyData *) lfirst(lc);
		TblspcKeyData *key;

		key = hash_search(TblspcKeyring, (void *) &key_infile->spcoid,
						  HASH_ENTER, NULL);

		/* Decrypt tablespace key by the master key */
		decrypt_tblspc_key(key->spcoid, key_infile->tblspckey, masterkey);

		/* Set unencrypted tablespace key to the keyring hash table */
		memcpy(key->tblspckey, key_infile->tblspckey, ENCRYPTION_KEY_SIZE);
	}

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

	/* Read the master key id */
	mkeyid = palloc0(MASTER_KEY_ID_LEN);
	if ((read_len = read(fd, mkeyid, MASTER_KEY_ID_LEN)) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", path))));

	/* Tablespace keys follows */
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

/*
 * Craft the master key identifier using the given sequence number and system
 * identifier.
 */
static void
create_master_key_id(char *id, MasterKeySeqNo seqno)
{
	 char sysid[32];

	 Assert(id != NULL);
	 snprintf(sysid, sizeof(sysid), UINT64_FORMAT, GetSystemIdentifier());
	 snprintf(id, MASTER_KEY_ID_LEN, MASTER_KEY_ID_FORMAT,
			  sysid, seqno);
}

/* Extract the sequence number from *masterkeyid */
static MasterKeySeqNo
get_seqno_from_master_key_id(const char *masterkeyid)
{
	MasterKeySeqNo seqno;
	uint32	dummy;

	/* Get the sequence number */
	sscanf(masterkeyid, MASTER_KEY_ID_FORMAT_SCAN, &dummy, &seqno);
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

	/* Protect sequence number increment from concurrent processes */
	LWLockAcquire(MasterKeyRotationLock, LW_EXCLUSIVE);

	/* Get the current sequence number */
	LWLockAcquire(KeyringControlLock, LW_SHARED);
	current_seqno = KmgrCtl->masterKeySeqNo;
	LWLockRelease(KeyringControlLock);

	/* Craft the new master key id */
	create_master_key_id(newid, current_seqno + 1);

	/* Generate the new master key by the new id */
	KmgrPluginGenerateKey(newid);
	newkey = KmgrPluginGetKey(newid);
	Assert(newkey);

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
	char			*masterkey;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	HASH_SEQ_STATUS	status;
	TblspcKeyData	*key;

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
	masterkey = KmgrPluginGetKey(KmgrCtl->masterKeyId);

	/* Get all tablespace keys */
	hash_seq_init(&status, TblspcKeyring);
	while ((key = (TblspcKeyData *) hash_seq_search(&status)) != NULL)
	{
		Datum	values[PG_TABLESPACE_KEYS_COLS];
		bool	nulls[PG_TABLESPACE_KEYS_COLS];
		char	buf[ENCRYPTION_KEY_SIZE + 1];
		bytea	*data_bytea;

		memcpy(buf, key->tblspckey, ENCRYPTION_KEY_SIZE);

		/* Encrypt tablespace key */
		encrypt_tblspc_key(key->spcoid, buf, masterkey);
		buf[ENCRYPTION_KEY_SIZE] = '\0';

		memset(nulls, 0, 2);
		values[0] = ObjectIdGetDatum(key->spcoid);

		data_bytea = (bytea *) palloc(ENCRYPTION_KEY_SIZE + VARHDRSZ);
		SET_VARSIZE(data_bytea, ENCRYPTION_KEY_SIZE + VARHDRSZ);
		memcpy(VARDATA(data_bytea), buf, ENCRYPTION_KEY_SIZE);
		values[1] = PointerGetDatum(data_bytea);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupestore);

	return (Datum) 0;
}
