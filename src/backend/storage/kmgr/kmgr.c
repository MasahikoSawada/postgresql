/*
 * Symmetric key manager
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "catalog/pg_tablespace.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/encryption.h"
#include "storage/kmgr.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/hsearch.h"
#include "utils/spccache.h"
#include "utils/syscache.h"

#define PG_TBLSP_KEY_DIR "pg_tblspc_key"

#define TBLSP_ENCRYPTION_KEY_LEN 64
#define MAX_KEY_GENERATION 32

#define transparent_encryption_is_enabled() \
	(MasterKeyGetCommand != NULL && MasterKeyGetCommand[0] != '\0')

typedef struct TblspKey
{
	Oid	oid;
	char key[TBLSP_ENCRYPTION_KEY_LEN];
} TblspKey;

typedef struct TblspKeyMap
{
	KeyGeneration generation;
	HTAB *map;
} TblspKeyMap;

/* Global variables */
MasterKeyCtlData	*MasterKeyCtl = NULL;
char				*MasterKeyGetCommand = NULL;

static TblspKeyMap *MyTblspKeyMap = NULL;
static char	*MasterKey_cached = NULL;
static KeyGeneration MasterKey_gen;

static MemoryContext kmgr_ctx = NULL;
static bool cache_invalidated = false;

static char tblsp_tweak[TWEAK_SIZE];

static void InvalidateTablespaceKeyCallback(Datum arg, int cacheid, uint32 hashvalue);

static void generate_tablesapce_key(char *key);
static TblspKeyMap *get_keymap(void);
static void update_tablespace_key_file(void);
static void update_tablespace_key_file_extended(TblspKeyMap *keymap,
												char masterkey[MASTER_ENCRYPTION_KEY_LEN]);
static void reload_tblsp_keys(bool missing_ok);
static void reload_tblsp_keys_common(KeyGeneration generation, bool missing_ok);
static List *read_tablespace_key_file(KeyGeneration gen, bool missing_ok);
static void execute_get_masterkey_command(char *masterkey, KeyGeneration generation);
static char *get_tablespace_key(Oid tablespaceoid, bool missing_ok);
static void register_tablespace_key(Oid tablespaceoid, const char *key, bool overwrite_ok);
static void remove_tablespace_key(Oid tablespaceoid);

static void
dump_all_key_caches(void)
{
#ifdef DEBUG_TDE
 	TblspKeyMap *keymap = get_keymap();
	HASH_SEQ_STATUS	status;
	TblspKey *k;

	if (MyTblspKeyMap != NULL)
	{
		elog(NOTICE, " -- No keymaps loaded --");
		return;
	}

	elog(NOTICE, "== CACHED KEY STATS ==");
	elog(NOTICE, "-- master key   : \"%s\"", GetEncryptionMasterKey());
	elog(NOTICE, "-- Keymap generation %d --", keymap->generation);

	hash_seq_init(&status, keymap->map);
	while ((k = (TblspKey *) hash_seq_search(&status)) != NULL)
		elog(NOTICE, "  - oid %d, key \"%s\"", k->oid, k->key);

	elog(NOTICE, "======================");
#endif
}

void
KmgrShmemInit(void)
{
	bool found;

	MasterKeyCtl = ShmemInitStruct("Encryption Master Key",
								   KmgrShmemSize(),
								   &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		pg_atomic_init_u32(&(MasterKeyCtl->cur_generation), InvalidKeyGeneration);
		SpinLockInit(&(MasterKeyCtl->mutex));
	}
	else
		Assert(found);
}
Size
KmgrShmemSize(void)
{
	return sizeof(MasterKeyCtlData);
}

char *
GetEncryptionMasterKey(void)
{
 	KeyGeneration gen;

	if (!MasterKeyCtl)
		return NULL;

	/* Get current generation */
	gen = pg_atomic_read_u32(&(MasterKeyCtl->cur_generation));

	if (MasterKey_cached != NULL)
	{
		/* The current cached master key is the latest */
		if (gen == MasterKey_gen)
			return MasterKey_cached;

		/* There is a cahce but too old, update it and fall through */
		MasterKey_gen = gen;
	}
	else
		MasterKey_cached = MemoryContextAlloc(TopMemoryContext,
											  MASTER_ENCRYPTION_KEY_LEN);

	SpinLockAcquire(&(MasterKeyCtl->mutex));
	memcpy(MasterKey_cached, MasterKeyCtl->key, MASTER_ENCRYPTION_KEY_LEN);
	SpinLockRelease(&(MasterKeyCtl->mutex));

	MasterKey_gen = gen;

	return MasterKey_cached;
}

/*
 * Does the given tablespace have encryption key?
 */
bool
TablespaceHasEncryptionKey(Oid tablespaceoid)
{
	return (get_tablespace_key(tablespaceoid, true) != NULL);
}

/*
 * Return tablespace encryption key of the current generation */
char *
GetTablespaceKey(Oid tablespaceoid)
{
	return get_tablespace_key(tablespaceoid, true);
}

void
SetKeyGeneration(KeyGeneration generation)
{
	if (MasterKeyCtl != NULL)
		pg_atomic_write_u32(&(MasterKeyCtl->cur_generation), (uint32) generation);
}

/*
 * This function is used before starting redo by startup process. So no need
 * locks to set the master key.
 */
void
StartupKmgr(KeyGeneration generation)
{
	char masterkey[MASTER_ENCRYPTION_KEY_LEN] = {0};

	Assert(MasterKeyCtl != NULL);

	pg_atomic_write_u32(&(MasterKeyCtl->cur_generation), (uint32) generation);

	/*
	 * Even if we don't have any tablespace keys for now, we fetch the master
	 * key of the first generation in advance.
	 */
	if (generation == InvalidKeyGeneration)
		generation = FirstKeyGeneration;

	/* Get the encryption master key at startup */
	execute_get_masterkey_command(masterkey, generation);

	/*
	 * Copy the master key to shmem. There is no need to lock as only the postmaster
	 * does it.
	 */
	MemSet(&(MasterKeyCtl->key), 0, MASTER_ENCRYPTION_KEY_LEN);
	memcpy(&(MasterKeyCtl->key), masterkey, MASTER_ENCRYPTION_KEY_LEN);

	ereport(LOG, (errmsg("starting up key manager, current key generation is %d", generation)));
}

/*
 * Execute get_masterkey_command shell command to get the master key and return it.
 * The length of the master key is always MASTER_ENCRYPTION_KEY_LEN.
 */
static void
execute_get_masterkey_command(char *masterkey, KeyGeneration generation)
{
	char	command[MAXPGPATH];
	const char *sp;
	char *cp, *endp;
	char gen_str[8];
	int	save_errno;
	FILE	*fp;

	if (!transparent_encryption_is_enabled())
		return;

	Assert(MasterKeyCtl);

	sprintf(gen_str, "%d", generation);

	cp = command;
	endp = command + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = MasterKeyGetCommand; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'g':
					sp++;
					/* Set key generation */
					strlcpy(cp, gen_str, endp - cp);
					cp += strlen(gen_str);
					break;
				case '%':
					sp++;
					if (cp < endp)
						*cp++ = *sp;
				default:
					*cp++ = *sp;
					break;
			}
		}
		else
		{
			if (cp < endp)
				*cp++ = *sp;
		}
	}

	*cp = '\0';

	ereport(LOG, (errmsg("executing getting master key command  \"%s\"",
						 command)));

	errno = 0;
	fp = popen(command, PG_BINARY_R);
	save_errno = errno;
	pqsignal(SIGPIPE, SIG_IGN);
	errno = save_errno;

	if (errno == EMFILE || errno == ENFILE)
		ereport(ERROR,
				(errmsg("out of file descriptors: %m")));

	fread(masterkey, MASTER_ENCRYPTION_KEY_LEN / 2, 1, fp);

	{
		char mk_str[MASTER_ENCRYPTION_KEY_LEN + 1] = {0};
		memcpy(mk_str, masterkey, MASTER_ENCRYPTION_KEY_LEN);
		mk_str[MASTER_ENCRYPTION_KEY_LEN] = '\0';
		ereport(LOG, (errmsg("Get master key \"%s\"", mk_str)));
	}
}

/* Create a tablespace encryption key and register it */
void
CreateTablespaceKey(Oid tablespaceoid)
{
	char key[TBLSP_ENCRYPTION_KEY_LEN];

	if (!transparent_encryption_is_enabled())
		ereport(ERROR,
				(errmsg("cannot create encrypted tablespace when transparent encryption is disabled")));

	LWLockAcquire(EncryptionKeyCreateLock, LW_EXCLUSIVE);

	/*
	 * Initialize key generation when the first time to create tablespace key.
	 */
	if (GetCurrentKeyGeneration() == InvalidKeyGeneration)
		pg_atomic_write_u32(&(MasterKeyCtl->cur_generation), FirstKeyGeneration);

	generate_tablesapce_key(key);
	register_tablespace_key(tablespaceoid, key, false);

	/*
	 * Update file.
	 * We need to get the current key generation again here because the key generation
	 * might have been initialized just before.
	 */
	update_tablespace_key_file();

	LWLockRelease(EncryptionKeyCreateLock);
}

/* Drop a tablespace key */
void
DropTablespaceKey(Oid tablespaceoid)
{
	LWLockAcquire(EncryptionKeyCreateLock, LW_EXCLUSIVE);

	remove_tablespace_key(tablespaceoid);

	/* Update file */
	update_tablespace_key_file();

	LWLockRelease(EncryptionKeyCreateLock);
}

static void
update_tablespace_key_file(void)
{
	update_tablespace_key_file_extended(get_keymap(), GetEncryptionMasterKey());
}


static void
update_tablespace_key_file_extended(TblspKeyMap *keymap, char masterkey[MASTER_ENCRYPTION_KEY_LEN])
{
	KeyGeneration generation = keymap->generation;
	HASH_SEQ_STATUS status;
	char	path[MAXPGPATH];
	int		fd;
	TblspKey	*tskey;

	/* Prepare the tablespace key file */
	sprintf(path, PG_TBLSP_KEY_DIR"/%d", generation);
	fd = OpenTransientFile(path, O_CREAT | O_WRONLY | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	/* Truncate the current all contents */
	if (ftruncate(fd, 0) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not reset file \"%s\": %m", path)));

#ifdef DEBUG_TDE
	elog(NOTICE, "writing keys of generation %d : path %s",
		 generation, path);
#endif
	/* Write each tablespace keys */
	hash_seq_init(&status, keymap->map);
	while ((tskey = (TblspKey *) hash_seq_search(&status)) != NULL)
	{
		TblspKey *k = palloc(sizeof(TblspKey));

		k->oid = tskey->oid;

		/* Encrypt tablespace key before writing */
		TablespaceKeyEncryptionTweak(tblsp_tweak, k->oid);
		encrypt_data(tskey->key, k->key, TBLSP_ENCRYPTION_KEY_LEN, masterkey, tblsp_tweak);

#ifdef DEBUG_TDE
		elog(NOTICE, "  - write (encrypted) oid %d, key \"%s\", plain \"%s\"",
			 k->oid, k->key, tskey->key);
#endif
		if (write(fd, k, sizeof(TblspKey)) != sizeof(TblspKey))
		{
			int save_errno = errno;

			CloseTransientFile(fd);

			errno = save_errno ? save_errno : ENOSPC;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m", path)));
		}
	}

	pg_fsync(fd);
	CloseTransientFile(fd);
}

static void
register_tablespace_key(Oid tablespaceoid, const char *key, bool overwrite_ok)
{
	TblspKey *tskey;
	TblspKeyMap *keymap;
	bool		found;

	keymap = get_keymap();

	if (!keymap)
		return;

	tskey = hash_search(keymap->map, (void *) &tablespaceoid, HASH_ENTER, &found);

	if (found && overwrite_ok)
		ereport(ERROR, (errmsg("the tablespace key for tablespace %d already exists",
							   tablespaceoid)));

	/*
	 * Store the tablespace key. We have plain key data in the memory. When
	 * dumping the keys we encrypt them.
	 */
	MemSet(tskey->key, 0, TBLSP_ENCRYPTION_KEY_LEN);
	memcpy(tskey->key, key, TBLSP_ENCRYPTION_KEY_LEN);
}

static void
remove_tablespace_key(Oid tablespaceoid)
{
	TblspKey *tskey;
	TblspKeyMap *keymap;

	keymap = get_keymap();

	if (!keymap)
		return;

	tskey = hash_search(keymap->map, (void *) &tablespaceoid, HASH_REMOVE, NULL);
}

static char *
get_tablespace_key(Oid tablespaceoid, bool missing_ok)
{
	TblspKey *tskey;
	TblspKeyMap *keymap;
	bool found;

	keymap = get_keymap();

	if (!keymap)
		return NULL;

	tskey = hash_search(keymap->map, (void *) &tablespaceoid, HASH_FIND, &found);

	if (!found)
	{
		if (!missing_ok)
			ereport(ERROR, (errmsg("could not find tablespace key for %d", tablespaceoid)));

		return NULL;
	}

	return tskey->key;
}

static TblspKeyMap *
get_keymap(void)
{
	KeyGeneration gen = GetCurrentKeyGeneration();

	if (gen != InvalidKeyGeneration && MyTblspKeyMap == NULL)
	{
		HASHCTL hash_ctl;
		MemoryContext old_ctx;

		if (!kmgr_ctx)
			kmgr_ctx = AllocSetContextCreate(TopMemoryContext,
											 "Tablespace keys",
											 ALLOCSET_DEFAULT_SIZES);
		old_ctx = MemoryContextSwitchTo(kmgr_ctx);

		MyTblspKeyMap = (TblspKeyMap *) palloc(sizeof(TblspKeyMap));

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(TblspKey);
		hash_ctl.hcxt = kmgr_ctx;

		MyTblspKeyMap->map = hash_create("My Tablespace Key Map", 1000, &hash_ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		//elog(NOTICE, "Create keymap cache with gen %d", gen);
		MyTblspKeyMap->generation = gen;

		MemoryContextSwitchTo(old_ctx);

		CacheRegisterSyscacheCallback(TABLESPACEOID,
									  InvalidateTablespaceKeyCallback,
									  (Datum) 0);
		reload_tblsp_keys(true);
	}

	/* Ensure tablespace keys are loaded */
	if (cache_invalidated)
		reload_tblsp_keys(false);

	return MyTblspKeyMap;
}

static void
generate_tablesapce_key(char *key)
{
	char tblspkey[TBLSP_ENCRYPTION_KEY_LEN] = {0};
	pg_strong_random(tblspkey, TBLSP_ENCRYPTION_KEY_LEN / 2);
	MemSet(key, 0, TBLSP_ENCRYPTION_KEY_LEN);
	memcpy(key, tblspkey, TBLSP_ENCRYPTION_KEY_LEN);
}

static void
reload_tblsp_keys(bool missing_ok)
{
	reload_tblsp_keys_common(GetCurrentKeyGeneration(), missing_ok);
}

/*
 * Load tablespace keys from the file.
 * Ignore NOENT error is missing_ok is ture.
 */
static void
reload_tblsp_keys_common(KeyGeneration generation, bool missing_ok)
{
	TblspKeyMap *keymap;
	List		*tskeys;
	ListCell	*lc;
	TblspKey *cached_key;
	int		nkeys_cache;
	int		nkeys_file;
	HASH_SEQ_STATUS	status;
	KeyGeneration current_gen = GetCurrentKeyGeneration();

	if (!transparent_encryption_is_enabled())
		return;

	/*
	 * Quick exit if the current key generation is an invalid value. When
	 * there have been no tablepsace keys the current geneartion is invalid.
	 */
	if (current_gen == InvalidKeyGeneration)
		return;

	if (MyTblspKeyMap->generation != current_gen)
	{
		hash_destroy(MyTblspKeyMap->map);
		MyTblspKeyMap->map = NULL;
	}

	cache_invalidated = false;

	/* Get the current tablespace key cache */
	keymap = get_keymap();
	Assert(keymap != NULL);
	nkeys_cache = hash_get_num_entries(keymap->map);

	/* Read the latest information from the file */
	tskeys = read_tablespace_key_file(generation, missing_ok);

	if (tskeys == NIL)
		return;

	nkeys_file = list_length(tskeys);

#ifdef DEBUG_TDE
	fprintf(stderr, "loading keys of generation %d\n", generation);
#endif
	foreach (lc, tskeys)
	{
		TblspKey *tskey_in_file = (TblspKey *) lfirst(lc);
		TblspKey *tskey;
		bool	found;

		tskey = hash_search(keymap->map, (void *) &(tskey_in_file->oid),
							HASH_ENTER, &found);

		/* We have already this key, skip */
		if (found)
			continue;

		/* Found new key we don't have yet, decrypt and store it */
		TablespaceKeyEncryptionTweak(tblsp_tweak, tskey->oid);
		decrypt_data(tskey_in_file->key, tskey->key, TBLSP_ENCRYPTION_KEY_LEN,
					 GetEncryptionMasterKey(), tblsp_tweak);

#ifdef DEBUG_TDE
		fprintf(stderr, "  - loaded (decrypted) oid %d, orig-key \"%s\", dec-key \"%s\"\n",
				tskey_in_file->oid, tskey_in_file->key, tskey->key);
//		elog(NOTICE, "  - loaded (decrypted) oid %d, key \"%s\"", keys[i].oid, keys[i].key);
#endif
	}

	/* Update key generation of cached tablespace keys */
	if (keymap->generation < current_gen)
		keymap->generation = current_gen;

	/* Switch over removing tablespace key cache if there is a removed key */
	if (nkeys_cache <= nkeys_file)
		return;

	/* Some keys are removed, remove them from the cache */
	hash_seq_init(&status, keymap->map);
	while ((cached_key = (TblspKey *) hash_seq_search(&status)) != NULL)
	{
		bool found = false;

		foreach (lc, tskeys)
		{
			TblspKey *tskey_in_file = (TblspKey *) lfirst(lc);

			if (cached_key->oid == tskey_in_file->oid)
			{
				found = true;
				break;
			}
		}

		if (found)
			hash_search(keymap->map, (void *) &cached_key->oid, HASH_REMOVE, NULL);
	}
}

/* Invalidation callback to clear all buffered tablespace keys */
static void
InvalidateTablespaceKeyCallback(Datum arg, int cacheid, uint32 hashvalue)
{
#ifdef DEBUG_TDE
	elog(NOTICE, "invalidate all keys");
#endif
	cache_invalidated = true;
}

/*
 * Read the tablespace key file of the given generation and return the list of
 * TblspKey
 */
static List*
read_tablespace_key_file(KeyGeneration gen, bool missing_ok)
{
	char path[MAXPGPATH];
	List	 *tblsp_keys = NIL;
	int	fd;
	int readBytes;

	sprintf(path, PG_TBLSP_KEY_DIR"/%d", gen);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (missing_ok)
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	/* Read tablespace key file until EOF */
	while (true)
	{
		TblspKey *key = palloc(sizeof(TblspKey));

		readBytes = read(fd, key, sizeof(TblspKey));

		if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": %m", path)));
		else if (readBytes == 0)		/* EOF */
			break;
		else if (readBytes != sizeof(TblspKey))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": read %d instead of %d bytes",
							path, readBytes, (int32) sizeof(TblspKey))));

		/* Get it, append to the list */
		tblsp_keys = lappend(tblsp_keys, key);
	}

	CloseTransientFile(fd);

	return tblsp_keys;
}

Datum
pg_rotate_encryption_key(PG_FUNCTION_ARGS)
{
	KeyGeneration generation;
	TblspKeyMap *keymap;
	char		masterkey[MASTER_ENCRYPTION_KEY_LEN] = {0};

	LWLockAcquire(EncryptionKeyRotateLock, LW_EXCLUSIVE);

	/* Get the current generation keymap */
	generation = GetCurrentKeyGeneration();
	keymap = get_keymap();

	/* Get the new master key via get_masterkey_command */
	execute_get_masterkey_command(masterkey, generation + 1);

	/* Increment the key generation locally */
	keymap->generation++;

	SpinLockAcquire(&(MasterKeyCtl->mutex));
	MemSet(&(MasterKeyCtl->key), 0, MASTER_ENCRYPTION_KEY_LEN);
	memcpy(MasterKeyCtl->key, masterkey, MASTER_ENCRYPTION_KEY_LEN);
	SpinLockRelease(&(MasterKeyCtl->mutex));

	/* Dump tablespace keys with the new enryption keys */
	update_tablespace_key_file_extended(keymap, masterkey);

	/*
	 * After we incremented the key generation, subsequent processes will try
	 * to see the tablespace keys encrypted with the new mater key.
	 */
	pg_atomic_write_u32(&(MasterKeyCtl->cur_generation), generation + 1);

	LWLockRelease(EncryptionKeyRotateLock);

	dump_all_key_caches();

	PG_RETURN_BOOL(true);
}

void
enckey_redo(XLogReaderState *record)
{

}
