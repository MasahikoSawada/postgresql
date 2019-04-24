/*-------------------------------------------------------------------------
 *
 * keyring.c
 *	 This module manages table space keys and master key for data encryption.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/keyring/keyring.c
 *
 * NOTES
 *		This file is compiled as both front-end and backend code, so it
 *		may not use ereport, server-defined static variables, etc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/keyring.h"
#include "storage/shmem.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/syscache.h"

#define KEYRING_TBLSP_FILE "pg_tblsp.kr"


HTAB *tblspKeyring;
MemoryContext keyringContext;

static List *readTblspKeryingFile(void);
static void reloadTblspKeyring(void);
static void invalidateTblspKeyring(Datum arg, int cacheid,	uint32 hashvalue);
static TblspKeyData *generateTblspKey(Oid spcOid);
static void storeTblspKey(TblspKeyData *key);

void
KeyringSetup(void)
{

	/*
	if (!data_encryption)
		return;
	*/

	if (!tblspKeyring)
	{
		MemoryContext old_ctx;
		HASHCTL hash_ctl;

		if (!keyringContext)
			keyringContext = AllocSetContextCreate(TopMemoryContext,
												   "Tablespace keys",
												   ALLOCSET_DEFAULT_SIZES);

		old_ctx = MemoryContextSwitchTo(keyringContext);

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(TblspKeyData);
		hash_ctl.hcxt = keyringContext;

		tblspKeyring = hash_create("tablespace key ring",
								   1000, &hash_ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		MemoryContextSwitchTo(old_ctx);

		CacheRegisterSyscacheCallback(TABLESPACEOID,
									  invalidateTblspKeyring,
									  (Datum) 0);
	}

	reloadTblspKeyring();
}

/*
char *
KeyringGetTblspKey(Oid spcOid)
{

}

void
KeyringStoreTblspKey(const char *key)
{

}
*/

static TblspKeyData *
generateTblspKey(Oid spcOid)
{
	TblspKeyData *key;
	bool		found;

	key = hash_search(tblspKeyring, (void *) &spcOid, HASH_ENTER, &found);

	if (found)
		ereport(ERROR,
				(errmsg("there is already key for tablespace %u",
						key->spcOid)));

	/*
	KeyringGetCurrentMasterKeyId(&key->masterKeyID);
	KeryingGetMasterKey(key->masterKeyID, &master_key);
	*/
	key->masterKeyID[0] = 'M';

	/* Generate random tablespace key */
	key->plainKey[0] = 'T';

	return key;
}

/*
 * Reload tablespace keyring based on the tablespace keyring file
 * This function intended to be used for loading the tablespace keys to the
 * empty cache. So the caller must ensure the tblspKeyring doesn't have
 * any entries.
 */
static void
reloadTblspKeyring(void)
{
	List *key_list;
	ListCell *lc;

	key_list = readTblspKeryingFile();

	foreach (lc, key_list)
	{
		TblspKeyData *key = (TblspKeyData *) lfirst(lc);
		TblspKeyData *cache_key;
		const char *master_key;

		/*
		 * Decrypt tablespace key using the master key before caching.
		 */
		/*
		master_key = KeyringGetMasterKey(key->masterKeyId);
		KeyringEnryptionTweak(key->tblspKey, tweak);
		DecryptTblspKey(key->spckey, key->spckey, tweak);
		*/

		cache_key = hash_search(tblspKeyring, (void *) &key->spcOid,
								HASH_ENTER, NULL);

		memcpy(cache_key, key, sizeof(TblspKeyData));
	}

	list_free_deep(key_list);
}

/*
 * Read and return the list of tablespace keys.
 */
static List *
readTblspKeryingFile(void)
{
	char *path = "global/"KEYRING_TBLSP_FILE;
	List *key_list = NIL;
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

	for (;;)
	{
		TblspKeyData *key = palloc(sizeof(TblspKeyData));
		int read_len;

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

static void
updateTblspKeyringFile(void)
{
	HASH_SEQ_STATUS status;
	TblspKeyData *key;
	char path[MAXPGPATH];
	char tmppath[MAXPGPATH];
	FILE *fpout;
	int	rc;

	/* lock */

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

	/* Write tablespace key to the file */
	hash_seq_init(&status, tblspKeyring);
	while ((key = (TblspKeyData *) hash_seq_search(&status)) != NULL)
	{
		rc = fwrite(key, sizeof(TblspKeyData), 1, fpout);
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

/* Invalidation callback to clear all buffered tablespace keys */
static void
invalidateTblspKeyring(Datum arg, int cacheid, uint32 hashvalue)
{
	hash_destroy(tblspKeyring);
	tblspKeyring = NULL;
}


/************************************************************************/
TblspKeyData *
test_generateTblspKey(Oid oid)
{
	TblspKeyData *key;

	key = generateTblspKey(oid);

	elog(NOTICE, "oid = %u, mk id = %s, dk key = %s",
		 key->spcOid,
		 key->masterKeyID,
		 key->plainKey);

	return key;
}

void
test_dumpLocalTblspKeyring(void)
{
	HASH_SEQ_STATUS status;
	TblspKeyData *key;

	if (!tblspKeyring)
	{
		elog(NOTICE, "---- no tblsp keys ----");
		return;
	}

	elog(NOTICE, "---- dump local tblsp keys ----");
	hash_seq_init(&status, tblspKeyring);
	while ((key = (TblspKeyData *) hash_seq_search(&status)) != NULL)
	{
		elog(NOTICE, "oid = %u, mk id = %s, dk key = %s",
			 key->spcOid,
			 key->masterKeyID,
			 key->plainKey);
	}
}

void
test_updateTblspKeyringFile(void)
{
	updateTblspKeyringFile();
	elog(NOTICE, "wrote %ld entries", hash_get_num_entries(tblspKeyring));
}
