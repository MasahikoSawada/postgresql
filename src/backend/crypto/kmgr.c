/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Key manager routines
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * Key manager is enabled if user requests during initdb.  We have one key
 * encryption key (KEK) and one internal key: SQL key.  During bootstrap,
 * we generate internal keys (currently only one), wrap them by KEK which
 * is derived from the user-provided passphrase and store them into each
 * file located at KMGR_DIR.  Once generated, these are not changed.
 * During startup, we unwrap all internal keys and load them to the shared
 * memory space.  Internal keys on the shared memory are read-only.
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "common/sha2.h"
#include "common/kmgr_utils.h"
#include "crypto/kmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* Struct stores internal keys in plaintext format */
typedef struct KmgrShmemData
{
	/*
	 * Internal cryptographic keys. Keys are stored at its ID'th.
	 */
	CryptoKey	intlKeys[KMGR_MAX_INTERNAL_KEYS];
} KmgrShmemData;
static KmgrShmemData *KmgrShmem;

/* Key lengths of available internal keys */
static int internalKeyLengths[KMGR_MAX_INTERNAL_KEYS] =
{
	KMGR_KEY_LEN	/* KMGR_SQL_KEY_ID */
};

static MemoryContext KmgrCtx = NULL;
static bool	kmgr_initialized = false;

/* GUC variables */
bool		key_management_enabled = false;;
char	   *cluster_passphrase_command = NULL;

/* Key wrap context initialized with the SQL key */
static KeyWrapCtx *WrapCtx = NULL;

static void InitKmgr(void);
static void ShutdownKmgr(int code, Datum arg);
static void KmgrSaveCryptoKeys(const char *dir, CryptoKey *keys);
static CryptoKey *generate_crypto_key(int len);
static void recoverIncompleteRotation(void);
static bool pg_wrap_internal(uint8 *in, int inlen, uint8 *out, int *outlen,
							 bool for_wrap);

/*
 * This function must be called ONCE on system install.
 */
void
BootStrapKmgr(void)
{
	KeyWrapCtx	*ctx;
	CryptoKey	keys_wrap[KMGR_MAX_INTERNAL_KEYS] = {0};
	char		passphrase[KMGR_MAX_PASSPHRASE_LEN];
	uint8		kekenc[KMGR_ENCKEY_LEN];
	uint8		kekhmac[KMGR_HMACKEY_LEN];
	int			passlen;

	/*
	 * Requirement check. We need openssl library to enable key management
	 * because all encryption and decryption calls happen via openssl function
	 * calls.
	 */
#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use cluster encryption."))));
#endif

	/* Get key encryption key from the passphrase command */
	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase, KMGR_MAX_PASSPHRASE_LEN);
	if (passlen < KMGR_MIN_PASSPHRASE_LEN)
		ereport(ERROR,
				(errmsg("passphrase must be more than %d bytes",
						KMGR_MIN_PASSPHRASE_LEN)));

	/* Get key encryption key and HMAC key from passphrase */
	kmgr_derive_keys(passphrase, passlen, kekenc, kekhmac);

	/* Create keywrap contextd temporarily */
	ctx = create_keywrap_ctx(kekenc, kekhmac);
	if (!ctx)
		elog(ERROR, "could not initialize key wrap contect");

	/* Wrap all internal keys by key encryption key */
	for (int id = 0; id < KMGR_MAX_INTERNAL_KEYS; id++)
	{
		CryptoKey *key;

		/* generate an internal key */
		key = generate_crypto_key(internalKeyLengths[id]);

		if (!kmgr_wrap_key(ctx, key, &(keys_wrap[id])))
		{
			free_keywrap_ctx(ctx);
			elog(ERROR, "failed to wrap cluster encryption key");
		}
	}

	/* Save internal keys to the disk */
	KmgrSaveCryptoKeys(KMGR_DIR, keys_wrap);

	free_keywrap_ctx(ctx);
}

/* Report shared-memory space needed by KmgrShmem */
Size
KmgrShmemSize(void)
{
	if (!key_management_enabled)
		return 0;

	return MAXALIGN(sizeof(KmgrShmemData));
}

/* Allocate and initialize key manager memory */
void
KmgrShmemInit(void)
{
	bool	found;

	if (!key_management_enabled)
		return;

	KmgrShmem = (KmgrShmemData *) ShmemInitStruct("Key manager",
												  KmgrShmemSize(), &found);

	if (!found)
		memset(KmgrShmem, 0, KmgrShmemSize());
}

/*
 * Get encryption key passphrase and verify it, then get the internal keys.
 * This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	CryptoKey	*keys_wrap;
	char		passphrase[KMGR_MAX_PASSPHRASE_LEN];
	int			passlen;
	int			nkeys;

	if (!key_management_enabled)
		return;

	elog(DEBUG1, "starting up key management system");

	/* Recover the failure of the last passphrase rotation if necessary */
	recoverIncompleteRotation();

	/* Get the crypto keys from the file */
	keys_wrap = kmgr_get_cryptokeys(KMGR_DIR, &nkeys);
	Assert(nkeys == KMGR_MAX_INTERNAL_KEYS);

	/* Get cluster passphrase */
	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase, KMGR_MAX_PASSPHRASE_LEN);

	/*
	 * Verify passphrase and preapre an internal key in plaintext on shared memory.
	 *
	 * XXX: do we need to prevent internal keys from being swapped out using
	 * mlock?
	 */
	if (!kmgr_verify_passphrase(passphrase, passlen, keys_wrap, KmgrShmem->intlKeys,
								KMGR_MAX_INTERNAL_KEYS))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster passphrase does not match expected passphrase")));
}

/* Initialize kmgr context in backend process */
static void
InitKmgr(void)
{
	CryptoKey	*sqlkey;
	uint8		*sql_enckey;
	uint8		*sql_hmackey;
	MemoryContext oldctx;

	Assert(KmgrShmem != NULL);
	Assert(KmgrCtx == NULL);

	on_shmem_exit(ShutdownKmgr, 0);

	KmgrCtx = AllocSetContextCreate(TopMemoryContext,
									"Key manager context",
									ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(KmgrCtx);

	/*
	 * Prepare key wrap context with SQL internal key for pg_wrap and pg_unwrap
	 * SQL functions.
	 */
	sqlkey = &(KmgrShmem->intlKeys[KMGR_SQL_KEY_ID]);
	sql_enckey = (uint8 *) sqlkey->key;
	sql_hmackey = (uint8 *) ((char *) sqlkey->key + KMGR_ENCKEY_LEN);
	WrapCtx = create_keywrap_ctx(sql_enckey, sql_hmackey);
	if (!WrapCtx)
		elog(ERROR, "could not initialize key wrap contect");

	MemoryContextSwitchTo(oldctx);
	kmgr_initialized = true;
}

/* Callback function to cleanup keywrap context */
static void
ShutdownKmgr(int code, Datum arg)
{
	if (WrapCtx)
		free_keywrap_ctx(WrapCtx);
}

/* Generate an empty CryptoKey */
static CryptoKey *
generate_crypto_key(int len)
{
	CryptoKey *newkey;

	Assert(len < KMGR_MAX_KEY_LEN);
	newkey = (CryptoKey *) palloc0(sizeof(CryptoKey));

	if (!pg_strong_random(newkey->key, len))
		elog(ERROR, "failed to generate new crypto key");

	newkey->klen = len;

	return newkey;
}

/*
 * Save the given crypto keys to the disk. We don't need CRC check for crypto
 * keys because these keys have HMAC which is used for integrity check
 * during unwrapping.
 */
static void
KmgrSaveCryptoKeys(const char *dir, CryptoKey *keys)
{
	elog(DEBUG2, "saving all cryptographic keys");

	for (int i = 0; i < KMGR_MAX_INTERNAL_KEYS; i++)
	{
		int			fd;
		char		path[MAXPGPATH];

		CryptoKeyFilePath(path, dir, i);

		if ((fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY)) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m",
							path)));

		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_WRITE);
		if (write(fd, &(keys[i]), sizeof(CryptoKey)) != sizeof(CryptoKey))
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							path)));
		}
		pgstat_report_wait_end();

		pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_SYNC);
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							path)));
		pgstat_report_wait_end();

		if (close(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							path)));
	}
}

/* Internal function to wrap or unwrap the given data */
static bool
pg_wrap_internal(uint8 *in, int inlen, uint8 *out, int *outlen, bool for_wrap)
{
	CryptoKey *ikey;
	CryptoKey *okey;
	bool		ret;

	if (!kmgr_initialized)
		InitKmgr();

	Assert(WrapCtx != NULL);

	ikey = (CryptoKey *) palloc0(sizeof(CryptoKey));
	okey = (CryptoKey *) palloc0(sizeof(CryptoKey));

	/* Prepare key for input */
	memcpy(ikey->key, in, inlen);
	ikey->klen = inlen;

	if (for_wrap)
		ret =  kmgr_wrap_key(WrapCtx, ikey, okey);
	else
		ret =  kmgr_unwrap_key(WrapCtx, ikey, okey);

	if (!ret)
	{
		pfree(ikey);
		pfree(okey);
		return false;
	}

	/* Set result */
	memcpy(out, okey->key, okey->klen);
	*outlen = okey->klen;

	pfree(ikey);
	pfree(okey);
	return true;
}

/*
 * Check the last passphrase rotation was completed. If not, we decide which wrapped
 * keys will be used according to the status of temporary directory and its wrapped
 * keys.
 */
static void
recoverIncompleteRotation(void)
{
	struct stat st;
	struct stat st_tmp;
	CryptoKey *keys;
	int			nkeys_tmp;

	/* The cluster passphrase rotation was completed, nothing to do */
	if (stat(KMGR_TMP_DIR, &st_tmp) != 0)
		return;

	/*
	 * If there is only temporary directory, it means that the previous
	 * rotation failed after wrapping the all internal keys by the new
	 * passphrase.  Therefore we use the new cluster passphrase.
	 */
	if (stat(KMGR_DIR, &st) != 0)
	{
		ereport(DEBUG1,
				(errmsg("there is only temporary directory, use the newly wrapped keys")));

		if (rename(KMGR_TMP_DIR, KMGR_DIR) != 0)
			ereport(ERROR,
					errmsg("could not rename directory \"%s\" to \"%s\": %m",
						   KMGR_TMP_DIR, KMGR_DIR));
		ereport(LOG,
				errmsg("cryptographic keys wrapped by new passphrase command are chosen"),
				errdetail("last cluster passphrase rotation failed in the middle"));
		return;
	}

	/*
	 * In case where both the original directory and temporary directory
	 * exist, there are two possibilities: (a) the all internal keys are
	 * wrapped by the new passphrase but rotation failed before removing the
	 * original directory, or (b) the rotation failed during wrapping internal
	 * keys by the new passphrase.  In case of (a) we need to use the wrapped
	 * keys in the temporary directory as rotation is essentially completed,
	 * but in case of (b) we use the wrapped keys in the original directory.
	 *
	 * To check the possibility of (b) we validate the wrapped keys in the
	 * temporary directory by checking the number of wrapped keys.  Since the
	 * wrapped key length is smaller than one disk sector, which is 512 bytes
	 * on common hardware, saving wrapped key is atomic write. So we can
	 * ensure that the all wrapped keys are valid if the number of wrapped
	 * keys in the temporary directory is KMGR_MAX_INTERNAL_KEYS.
	 */
	keys = kmgr_get_cryptokeys(KMGR_TMP_DIR, &nkeys_tmp);

	if (nkeys_tmp == KMGR_MAX_INTERNAL_KEYS)
	{
		/*
		 * This is case (a), the all wrapped keys in temporary directory are
		 * valid. Remove the original directory and rename.
		 */
		ereport(DEBUG1,
				(errmsg("last passphrase rotation failed before renaming direcotry name, use the newly wrapped keys")));

		if (!rmtree(KMGR_DIR, true))
			ereport(ERROR,
					(errmsg("could not remove directory \"%s\"",
							KMGR_DIR)));
		if (rename(KMGR_TMP_DIR, KMGR_DIR) != 0)
			ereport(ERROR,
					errmsg("could not rename directory \"%s\" to \"%s\": %m",
						   KMGR_TMP_DIR, KMGR_DIR));

		ereport(LOG,
				errmsg("cryptographic keys wrapped by new passphrase command are chosen"),
				errdetail("last cluster passphrase rotation failed in the middle"));
	}
	else
	{
		/*
		 * This is case (b), the last passphrase rotation failed during
		 * wrapping keys. Remove the keys in the temporary directory and use
		 * keys in the original keys.
		 */
		ereport(DEBUG1,
				(errmsg("last passphrase rotation failed during wrapping keys, use the old wrapped keys")));

		if (!rmtree(KMGR_TMP_DIR, true))
			ereport(ERROR,
					(errmsg("could not remove directory \"%s\"",
							KMGR_DIR)));
		ereport(LOG,
				errmsg("cryptographic keys wrapped by old passphrase command are chosen"),
				errdetail("last cluster passphrase rotation failed in the middle"));
	}

	pfree(keys);
}

/*
 * SQL function to wrap the given data by the user key
 */
Datum
pg_wrap(PG_FUNCTION_ARGS)
{
	text	   *data = PG_GETARG_TEXT_PP(0);
	bytea	   *res;
	int			datalen;
	int			reslen;
	int			len;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not wrap key because key management is not supported")));

	datalen = VARSIZE_ANY_EXHDR(data);

	if (datalen >= KMGR_MAX_KEY_LEN)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid input key size")));

	reslen = VARHDRSZ + SizeOfWrappedKey(datalen);
	res = palloc(reslen);

	if (!pg_wrap_internal((uint8 *) VARDATA_ANY(data), datalen,
						  (uint8 *) VARDATA(res), &len, true))
		elog(ERROR, "could not wrap the given secret");

	SET_VARSIZE(res, reslen);

	PG_RETURN_TEXT_P(res);
}

/*
 * SQL function to unwrap the given data by the user key
 */
Datum
pg_unwrap(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_PP(0);
	text	   *res;
	int			datalen;
	int			buflen;
	int			len;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not wrap key because key management is not supported")));

	datalen = VARSIZE_ANY_EXHDR(data);

	/* Check if the input length is more than minimum length of wrapped key */
	if (datalen < SizeOfWrappedKey(0) || datalen >= KMGR_MAX_WRAPPED_KEY_LEN )
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid input wrapped key size")));

	buflen = VARHDRSZ + SizeOfUnwrappedKey(datalen);
	res = palloc(buflen);

	if (!pg_wrap_internal((uint8 *) VARDATA_ANY(data), datalen,
						  (uint8 *) VARDATA(res), &len, false))
		elog(ERROR, "could not unwrap the given secret");

	/*
	 * The size of unwrapped key can be smaller than the size estimated before
	 * unwrapping since the padding is removed during unwrapping.
	 */
	SET_VARSIZE(res, VARHDRSZ + len);

	PG_RETURN_TEXT_P(res);
}

/*
 * SQL function to rotate the cluster passphrase. This function assumes that
 * the cluster_passphrase_command is already reloaded to the new value.
 * All internal keys are wrapped by the new passphrase and saved to the disk.
 * To update all crypto keys atomically we save the newly wrapped keys to the
 * temporary directory, pg_cryptokeys_tmp, and remove the original directory,
 * pg_cryptokeys, and rename it. These operation is performed without the help
 * of WAL.  In the case of failure during rotationpg_cryptokeys directory and
 * pg_cryptokeys_tmp directory can be left in incomplete status.  We recover
 * the incomplete situation by checkIncompleteRotation.
 */
Datum
pg_rotate_cluster_passphrase(PG_FUNCTION_ARGS)
{
	KeyWrapCtx	*ctx;
	CryptoKey	newkeys[KMGR_MAX_INTERNAL_KEYS] = {0};
	char		passphrase[KMGR_MAX_PASSPHRASE_LEN];
	uint8		new_kekenc[KMGR_ENCKEY_LEN];
	uint8		new_kekhmac[KMGR_HMACKEY_LEN];
	int			passlen;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not rotate cluster passphrase because key management is not supported")));

	/* Recover the failure of the last passphrase rotation if necessary */
	recoverIncompleteRotation();

	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase,
												  KMGR_MAX_PASSPHRASE_LEN);
	if (passlen < KMGR_MIN_PASSPHRASE_LEN)
		ereport(ERROR,
				(errmsg("passphrase must be more than %d bytes",
						KMGR_MIN_PASSPHRASE_LEN)));

	/* Get new key encryption key and wrap context */
	kmgr_derive_keys(passphrase, passlen, new_kekenc, new_kekhmac);
	ctx = create_keywrap_ctx(new_kekenc, new_kekhmac);
	if (!ctx)
		elog(ERROR, "could not initialize key wrap contect");

	for (int id = 0; id < KMGR_MAX_INTERNAL_KEYS; id++)
	{
		if (!kmgr_wrap_key(ctx, &(KmgrShmem->intlKeys[id]), &(newkeys[id])))
			elog(ERROR, "failed to wrap key");
	}

	/* Create temporary directory */
	if (MakePGDirectory(KMGR_TMP_DIR) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create temporary directory \"%s\": %m",
						KMGR_TMP_DIR)));
	fsync_fname(KMGR_TMP_DIR, true);

	/* Prevent concurrent key rotation */
	LWLockAcquire(KmgrFileLock, LW_EXCLUSIVE);

	/* Save the key wrapped by the new passphrase to the temporary directory */
	KmgrSaveCryptoKeys(KMGR_TMP_DIR, newkeys);

	/* Remove the original directory */
	if (!rmtree(KMGR_DIR, true))
		ereport(ERROR,
				(errmsg("could not remove directory \"%s\"",
						KMGR_DIR)));

	/* Rename to the original directory */
	if (rename(KMGR_TMP_DIR, KMGR_DIR) != 0)
		ereport(ERROR,
				(errmsg("could not rename directory \"%s\" to \"%s\": %m",
						KMGR_TMP_DIR, KMGR_DIR)));
	fsync_fname(KMGR_DIR, true);

	LWLockRelease(KmgrFileLock);

	PG_RETURN_BOOL(true);
}
