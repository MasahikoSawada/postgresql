/*-------------------------------------------------------------------------
 *
 * encryption.c
 *  This code handles encryption and decryption of data.
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *  src/backend/storage/encryption/encryption.c
 *
 * TODO
 * * Master key management code should be separated from encryption code?
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "common/string.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/ipc.h"
#include "storage/encryption.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#define MASTER_KEY_DIR "pg_keys"

int max_master_keys = 0;

static void PersistMasterKey(MasterKey *key);
static void encrypt_block(const char *input, char *output, Size size, char *key);
static void decrypt_block(const char *input, char *output, Size size, char *key);

Size
MasterKeyShmemSize(void)
{
	Size size = 0;

	if (max_master_keys == 0)
		return size;

	size = add_size(size, sizeof(MasterKeyCtl));
	size = add_size(size, sizeof(MasterKey) * max_master_keys);

	return size;
}

void
MasterKeyShmemInit(void)
{
	bool found;

	if (max_master_keys == 0)
		return;

	MasterKeyCtl = (MasterKeyCtlData *)
		ShmemInitStruct("Mater key management", MasterKeyShmemSize(), &found);

	if (!found)
		MemSet(MasterKeyCtl, 0, MasterKeyShmemSize());
}

void
CheckPointMasterKeys(void)
{
	int i;

	LWLockAcquire(MasterKeyLock, LW_SHARED);
	for (i = 0; i < max_master_keys; i++)
	{
		MasterKey *key = &MasterKeyCtl->masterkeys[i];

		if (!OidIsValid(key->dbid))
			continue;

		PersistMasterKey(key);
	}
	LWLockRelease(MasterKeyLock);
}

/*
 * Persist master key data to disk
 */
static void
PersistMasterKey(MasterKey *key)
{
	char	path[MAXPGPATH];
	int		fd;

	if (!key->is_dirty)
		return;

	snprintf(path, sizeof(path), MASTER_KEY_DIR "/%u", key->dbid);
	fd = OpenTransientFile(path, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						path)));
		return;
	}

	/* XXX : CRC check reuiqred, and maybe in CRIT_SECTION? */

	if (write(fd, key, sizeof(key)) != sizeof(key))
	{
		int	save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						path)));
		return;
	}

	if (pg_fsync(fd) != 0)
	{
		int save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						path)));
		return;
	}

	CloseTransientFile(fd);

	key->is_dirty = false;
}

/*
 * Encrypt one block data.
 */
char *
EncryptOneBuffer(char *raw_buf, char *key)
{
	static char *bufCopy = NULL;

	if (bufCopy == NULL)
		bufCopy = MemoryContextAlloc(TopMemoryContext, BLCKSZ);

	encrypt_block(raw_buf, bufCopy, BLCKSZ, key);
	return bufCopy;
}

/*
 * Decrypt one block data.
 *
 * XXX: original buffer data should not be changed
 */
char *
DecryptOneBuffer(char *encrypted_buf, char *key)
{
	static char *bufCopy = NULL;

	if (bufCopy == NULL)
		bufCopy = MemoryContextAlloc(TopMemoryContext, BLCKSZ);

	decrypt_block(encrypted_buf, encrypted_buf, BLCKSZ, key);
	return encrypted_buf;

}
/*
 * Encrypt data.
 */
static void
encrypt_block(const char *input, char *output, Size size, char *key)
{
	int i;

	for (i = 0; i < size; i++)
		output[i] = input[i] + 1;
}

/*
 * Decrypts data.
 */
static void
decrypt_block(const char *input, char *output, Size size, char *key)
{
	int i;

	for (i = 0; i < size; i++)
		output[i] = input[i] - 1;
}
