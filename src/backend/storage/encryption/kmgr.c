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
 * keyring file while reencrypting all	tablespace keys with the new master key.
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
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/syscache.h"

/* Filename of tablespace keyring */
#define KMGR_FILENAME "global/pg_kmgr"

/*
 * Since we have encryption keys per tablspace, we expect this value is enough
 * for most usecase.
 */
#define TDE_KEYRING_SIZE 1024

typedef unsigned char keydata_t;

typedef struct KmgrFileData
{
	/* salt used for KEK derivation */
	keydata_t	kek_salt[TDE_KEK_DEVIRATION_SALT_SIZE];

	/* HMAC for KEK */
	keydata_t	kek_hmac[TDE_KEK_HMAC_SIZE];

	/* MDEK in encrypted state, NULL-terminated */
	keydata_t	mdek[TDE_MDEK_SIZE];

	/* CRC of all above ... MUST BE LAST! */
	pg_crc32c	crc;
} KmgrFileData;

typedef struct KmgrCtlData
{
	keydata_t			mdek[TDE_MDEK_SIZE];
} KmgrCtlData;
static KmgrCtlData		*KmgrCtl;

typedef struct TblkeyEntry
{
	RelFileNode rnode;

	/* The size of TDEK is either 16 or 32 depending on configuration */
	keydata_t		tdek[TDE_MAX_ENCRYPTION_KEY_SIZE];
} TblkeyEntry;

static MemoryContext	KmgrContext;

/* Per table encryption key hash table */
static HTAB				*TblKeyring;

/* GUC variable */
char *database_encryption_key_passphrase_command = NULL;

PG_FUNCTION_INFO_V1(pg_rotate_encryption_key);

static int run_database_encryption_key_passpharse_command(const char *prompt,
														  char *buf, int size);
static KmgrFileData *read_kmgr_file(void);
static void write_kmgr_file(KmgrFileData *filedata);
static void get_kek_and_hmackey_from_passphrase(char *passphrase, char pplen,
												keydata_t salt[TDE_KEK_DEVIRATION_SALT_SIZE],
												keydata_t kek[TDE_KEK_SIZE],
												keydata_t hmackey[TDE_KEK_HMAC_KEY_SIZE]);
static bool vefiry_passphrase(KmgrFileData *kmgfile,
							  char passphrase[TDE_MAX_PASSPHRASE_LEN],
							  int passlen, keydata_t kek[TDE_KEK_SIZE]);
static TblkeyEntry* derive_tdek(RelFileNode rnode);

/*
 * bootstrapping kmgr. derive KEK, generate MDEK and salt, compute hmac,
 write kmgr file etc.
 */
void
BootstrapKmgr(void)
{
	const char *prompt = "Enter database encryption pass phrase:";
	KmgrFileData kmgrfile;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t kek_salt[TDE_KEK_DEVIRATION_SALT_SIZE];
	keydata_t kek_hmac[TDE_KEK_HMAC_SIZE];
	keydata_t mdek[TDE_MDEK_SIZE];
	keydata_t kek[TDE_KEK_SIZE];
	keydata_t hmackey[TDE_KEK_HMAC_KEY_SIZE];
	int len;
	int ret;

	 /* Get encryption key passphrase */
	len = run_database_encryption_key_passpharse_command(prompt,
														 passphrase,
														 TDE_MAX_PASSPHRASE_LEN);

	/* Generate salt for KEK derivation */
	ret = pg_strong_random(kek_salt, TDE_KEK_DEVIRATION_SALT_SIZE);
	if (!ret)
		ereport(ERROR,
				(errmsg("failed to generate random salt for key encryption key")));

	/* Get KEK and HMAC key */
	get_kek_and_hmackey_from_passphrase(passphrase, len, kek_salt, kek, hmackey);

	/* Generate salt for KEK */
	ret = pg_strong_random(mdek, TDE_MDEK_SIZE);
	if (!ret)
		ereport(ERROR,
				(errmsg("failed to generate the master encryption key")));

	/* HHMAC */
	ComputeHMAC(hmackey, TDE_KEK_HMAC_SIZE, mdek,
				TDE_MDEK_SIZE, kek_hmac);

	/* Encrypt MDEK with KEK */
	WrapEncrytionKey(kek, mdek, TDE_MDEK_SIZE, mdek);

	/* Fill out the kmgr file contents */
	memcpy(kmgrfile.kek_salt, kek_salt, TDE_KEK_DEVIRATION_SALT_SIZE);
	memcpy(kmgrfile.kek_hmac, kek_hmac, TDE_KEK_HMAC_SIZE);
	memcpy(kmgrfile.mdek, mdek, TDE_MDEK_SIZE);

	/* write kmgr file to the disk */
	write_kmgr_file(&kmgrfile);
}

Size
KmgrShmemSize(void)
{
	Size size;

	size = MAXALIGN(sizeof(KmgrCtlData));

	return size;
}

void
KmgrShmemInit(void)
{
	bool	found;

	KmgrCtl = ShmemInitStruct("KmgrCtl shared",
							  KmgrShmemSize(),
							  &found);

	if (!found)
		MemSet(KmgrCtl, 0, sizeof(KmgrCtlData));
}

/*
 * Run database_encryption_key_passphrase_command
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf, which is of size size.	 The return
 * value is the length of the actual result.
 */
static int
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

	return len;
}

/*
 * Get encryption key passphrase and verify it, then get the un-encrypted
 * MDEK. This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	const char *prompt = "Enter database encryption pass phrase:";
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t kek[TDE_KEK_SIZE];
	keydata_t mdek[TDE_MDEK_SIZE];
	KmgrFileData *kmgrfile;
	int	len;

	/* Get contents of kmgr file */
	kmgrfile = read_kmgr_file();

	/* Get encryption key passphrase */
	len = run_database_encryption_key_passpharse_command(prompt,
														 passphrase,
														 TDE_MAX_PASSPHRASE_LEN);

	/* Verify the correctness of KEK */
	if (!vefiry_passphrase(kmgrfile, passphrase, len, kek))
		ereport(ERROR,
				(errmsg("invalid passphrase")));

	/* Derive KEK + HMAC key from passphrase */
	DeriveKeyFromPassphrase(passphrase, len, kmgrfile->kek_salt,
							TDE_KEK_DEVIRATION_SALT_SIZE,
							TDE_KEK_DEVIRATION_ITER_COUNT,
							TDE_KEK_DERIVED_KEY_SIZE, kek);

	/* Unwrap MDEK with KEK */
	UnwrapEncrytionKey(kek, kmgrfile->mdek, TDE_MDEK_SIZE, mdek);

	/* Copy MDEK to shread memory struct */
	memcpy(KmgrCtl->mdek, mdek, TDE_MDEK_SIZE);
}

/*
 * Set up per backend key manager
 */
void
SetupKmgr(void)
{
	HASHCTL ctl;

	if (KmgrContext == NULL)
		KmgrContext = AllocSetContextCreate(TopMemoryContext,
											"TDE per table encryption key context",
											ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RelFileNode);
	ctl.entrysize = sizeof(TblkeyEntry);
	ctl.hcxt = KmgrContext;
	TblKeyring = hash_create("local per table key hash table",
							 TDE_KEYRING_SIZE, &ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 *
 * If 'create' is true, we derive TDEK if not exist on the keyring instead
 * of raising an error.
 */
void
GetRelEncKey(RelFileNode rnode, char *key, bool create)
{
	TblkeyEntry *tkey;
	bool		found;

	tkey = hash_search(TblKeyring, (void *) &rnode, HASH_FIND, &found);

	if (!found)
	{
		if (!create)
			ereport(ERROR,
					(errmsg("could not find encryption key for relation %u/%u/%u",
							rnode.dbNode, rnode.spcNode, rnode.relNode)));

		/* derive new TDEK for this relation and register to the keyring */
		tkey = derive_tdek(rnode);
	}

	/* Set encryption key */
	memcpy(key, tkey->tdek, EncryptionKeySize);
}

/*
 * Drop one tablespace key from the keyring hash table and update the keyring
 * file.
 */
void
DropRelEncKey(RelFileNode rnode)
{
	bool found;

	hash_search(TblKeyring, (void *) &rnode, HASH_REMOVE, &found);

	if (!found)
		elog(ERROR, "could not find encryption key for relation %u/%u/%u",
			 rnode.dbNode, rnode.spcNode, rnode.relNode);
}

/*
 * Read kmgr file, and return palloc'd file data.
 */
static KmgrFileData *
read_kmgr_file(void)
{
	KmgrFileData *kmgrfile;
	pg_crc32c	crc;
	int read_len;
	int fd;

	fd = OpenTransientFile(KMGR_FILENAME, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (errno == ENOENT)
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", KMGR_FILENAME)));
	}

	/* Read data */
	kmgrfile = (KmgrFileData *) palloc(sizeof(KmgrFileData));

	/* @@@: report wait event */

	if ((read_len = read(fd, kmgrfile, sizeof(KmgrFileData))) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", KMGR_FILENAME))));

	/* @@@ : end wait event */

	if (CloseTransientFile(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", KMGR_FILENAME)));

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) kmgrfile, offsetof(KmgrFileData, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, kmgrfile->crc))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						KMGR_FILENAME)));

	return kmgrfile;
}
/*
 * @@@ : currently this can be called only when bootstrapping. postmaster and
 * backend never modify kmgr file unless kek rotation.
 */
static void
write_kmgr_file(KmgrFileData *filedata)
{
	int				fd;

	INIT_CRC32C(filedata->crc);
	COMP_CRC32C(filedata->crc, filedata, offsetof(KmgrFileData, crc));
	FIN_CRC32C(filedata->crc);

	fd = OpenTransientFile(KMGR_FILENAME, PG_BINARY | O_CREAT | O_RDWR);

	if (fd < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						KMGR_FILENAME)));
		return;
	}

	/* @@@: report wait event */

	if (write(fd, filedata, sizeof(KmgrFileData)) != sizeof(KmgrFileData))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write kmgr file \"%s\": %m",
						KMGR_FILENAME)));

	/* @@@ : end wait event */

	if (CloseTransientFile(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", KMGR_FILENAME)));

	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not sync file \"%s\": %m",
						 KMGR_FILENAME))));
}

static void
get_kek_and_hmackey_from_passphrase(char *passphrase, char pplen,
									keydata_t salt[TDE_KEK_DEVIRATION_SALT_SIZE],
									keydata_t kek[TDE_KEK_SIZE],
									keydata_t hmackey[TDE_KEK_HMAC_KEY_SIZE])
{
	keydata_t enckey_and_hmackey[TDE_KEK_DERIVED_KEY_SIZE];

	/* Derive kek and hmac key from passphrase, or error */
	DeriveKeyFromPassphrase(passphrase, pplen,
							salt, TDE_KEK_DEVIRATION_SALT_SIZE,
							TDE_KEK_DEVIRATION_ITER_COUNT,
							TDE_KEK_SIZE + TDE_KEK_HMAC_KEY_SIZE,
							enckey_and_hmackey);

	/* Extract KEK and HMAC key for IV+MDEK */
	memcpy(kek, enckey_and_hmackey, sizeof(TDE_KEK_SIZE));
	memcpy(hmackey, enckey_and_hmackey + TDE_KEK_SIZE,
		   sizeof(TDE_KEK_HMAC_KEY_SIZE));
}

static bool
vefiry_passphrase(KmgrFileData *kmgrfile, char passphrase[TDE_MAX_PASSPHRASE_LEN],
				  int passlen, keydata_t kek[TDE_KEK_SIZE])
{
	keydata_t hmac[TDE_KEK_HMAC_SIZE];
	keydata_t kek_tmp[TDE_KEK_SIZE];
	keydata_t hmackey_tmp[TDE_KEK_HMAC_KEY_SIZE];

	get_kek_and_hmackey_from_passphrase(passphrase, passlen,
										kmgrfile->kek_salt, kek_tmp,
										hmackey_tmp);

	/* Compute HMAC for IV+MDEK using the HMAC key derived from passpharse */
	ComputeHMAC(hmackey_tmp, TDE_KEK_HMAC_KEY_SIZE, kmgrfile->mdek,
				TDE_MDEK_SIZE, hmac);

	/* Check HMAC */
	if (memcmp(kmgrfile->kek_hmac, hmac, TDE_KEK_HMAC_SIZE) != 0)
		return false;

	/* user-provided passphrase is correct, set KEK */
	memcpy(kek, kek_tmp, TDE_KEK_SIZE);

	return true;
}

/*
 * Derive TDEK and regsiter to the local keyring, and then return it
 */
static TblkeyEntry*
derive_tdek(RelFileNode rnode)
{
	TblkeyEntry *tkey;
	bool		found;
	char tdek_len = EncryptionKeySize;


	tkey = hash_search(TblKeyring, (void *) &rnode, HASH_ENTER, &found);

	if (found)
		return NULL;

	/* derive new TDEK that length is EncryptionKeySize from MDEK */
	DeriveNewKey(KmgrCtl->mdek, TDE_MDEK_SIZE, rnode, tkey->tdek, tdek_len);

	return tkey;
}
