/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Encryption key management module.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xlog.h"
#include "common/sha2.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define KMGR_PROMPT_MSG "Enter database encryption pass phrase:"

/*
 * Key encryption key. This variable is set during verification
 * of user given passphrase. After verified, the plain key data
 * is set to this variable.
 */
static keydata_t keyEncKey[TDE_KEK_SIZE];

/*
 * Relation encryption key and WAL encryption key.  Similar to
 * key encryption key, these variables store the plain key data.
 */
static keydata_t relEncKey[TDE_MAX_DEK_SIZE];
static keydata_t walEncKey[TDE_MAX_DEK_SIZE];

/* GUC variable */
char *cluster_passphrase_command = NULL;

static int run_cluster_passphrase_command(char *buf, int size);
static void get_kek_and_hmackey_from_passphrase(char *passphrase,
												Size passlen,
												keydata_t kek[TDE_KEK_SIZE],
												keydata_t hmackey[TDE_HMAC_KEY_SIZE]);
static bool verify_passphrase(char *passphrase, int passlen,
							  WrappedEncKeyWithHmac **keys, int nkeys);
static bool kmgr_wrap_key(keydata_t *key, keydata_t *in, keydata_t *out);
static bool kmgr_unwrap_key(keydata_t *key, keydata_t *in, keydata_t *out);
static void kmgr_compute_hmac(keydata_t *hmackey, keydata_t *key,
							  keydata_t *hmac);

/*
 * This func must be called ONCE on system install. we retrive KEK,
 * generate RDEK and WDEK etc.
 */
KmgrBootstrapInfo *
BootStrapKmgr(int bootstrap_data_encryption_cipher)
{
	KmgrBootstrapInfo *kmgrinfo;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t *rdek_enc;
	keydata_t *wdek_enc;
	keydata_t *rdek_hmac;
	keydata_t *wdek_hmac;
	int	wrapped_keysize;
	int	len;

	if (bootstrap_data_encryption_cipher == TDE_ENCRYPTION_OFF)
		return NULL;

#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use cluster encryption."))));
#endif

	kmgrinfo = palloc0(sizeof(KmgrBootstrapInfo));
	rdek_enc = kmgrinfo->relEncKey.key;
	rdek_hmac = kmgrinfo->relEncKey.hmac;
	wdek_enc = kmgrinfo->walEncKey.key;
	wdek_hmac = kmgrinfo->walEncKey.hmac;

	/*
	 * Set data encryption cipher so that subsequent bootstrapping process
	 * can proceed.
	 */
	SetConfigOption("data_encryption_cipher",
					EncryptionCipherString(bootstrap_data_encryption_cipher),
					PGC_INTERNAL, PGC_S_OVERRIDE);

	/* Get key encryption key fro command */
	len = run_cluster_passphrase_command(passphrase, TDE_MAX_PASSPHRASE_LEN);

	/* Get key encryption key and HMAC key from passphrase */
	get_kek_and_hmackey_from_passphrase(passphrase, len, keyEncKey,
										hmackey);

	/*
	 * Generate relation encryption key and WAL encryption key.
	 * The generated two keys must be stored in relEncKey and
	 * walEncKey that can be used by other modules since even
	 * during bootstrapping we need to encrypt both systemcatalogs
	 * and WAL.
	 */
	if (!pg_strong_random(relEncKey, EncryptionKeySize))
		ereport(PANIC,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to generate relation encryption key")));
	if (!pg_strong_random(walEncKey, EncryptionKeySize))
		ereport(PANIC,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to generate WAL encryption key")));

	/* Wrap both keys by KEK */
	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;
	if (!kmgr_wrap_key(keyEncKey, relEncKey, rdek_enc))
		ereport(PANIC,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to wrap relation encryption key")));

	if (!kmgr_wrap_key(keyEncKey, walEncKey, wdek_enc))
		ereport(PANIC,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to wrap WAL encryption key")));

	/* Compute both HMAC */
	kmgr_compute_hmac(hmackey, rdek_enc, rdek_hmac);
	kmgr_compute_hmac(hmackey, wdek_enc, wdek_hmac);

	/* return keys and HMACs generated during bootstrap */
	return kmgrinfo;
}

/*
 * Run cluster_passphrase_command
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf, which is of size size.
 * The return value is the length of the actual result.
 */
static int
run_cluster_passphrase_command(char *buf, int size)
{
	StringInfoData command;
	char	   *p;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	Assert(size > 0);
	buf[0] = '\0';

	initStringInfo(&command);

	for (p = cluster_passphrase_command; *p; p++)
	{
		if (p[0] == '%')
		{
			switch (p[1])
			{
				case 'p':
					appendStringInfoString(&command, KMGR_PROMPT_MSG);
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
 * RDEK and WDEK. This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	WrappedEncKeyWithHmac *wrapped_keys[2];
	char	passphrase[TDE_MAX_PASSPHRASE_LEN];
	int		len;

	if (!DataEncryptionEnabled())
		return;

	/* Get cluster passphrase */
	len = run_cluster_passphrase_command(passphrase,
										 TDE_MAX_PASSPHRASE_LEN);

	/* Get two wrapped keys stored in control file */
	wrapped_keys[0] = GetTDERelationEncryptionKey();;
	wrapped_keys[1] = GetTDEWALEncryptionKey();

	/* Verify the correctness of given passphrase */
	if (!verify_passphrase(passphrase, len, wrapped_keys, 2))
		ereport(ERROR,
				(errmsg("cluster passphrase does not match expected passphrase")));

	/* The passphrase is correct, unwrap both RDEK and WDEK */
	if (!kmgr_unwrap_key(keyEncKey, wrapped_keys[0]->key, relEncKey))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to unwrapped relation encryption key")));

	if (!kmgr_unwrap_key(keyEncKey, wrapped_keys[1]->key, walEncKey))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to unwrapped WAL encryptoin key")));
}

 /*
  * Hash the given passphrase and extract it into KEK and HMAC
  * key.
  */
static void
get_kek_and_hmackey_from_passphrase(char *passphrase, Size passlen,
									keydata_t kek_out[TDE_KEK_SIZE],
									keydata_t hmackey_out[TDE_HMAC_KEY_SIZE])
{
	keydata_t enckey_and_hmackey[PG_SHA512_DIGEST_LENGTH];
	pg_sha512_ctx ctx;

	pg_sha512_init(&ctx);
	pg_sha512_update(&ctx, (const uint8 *) passphrase, passlen);
	pg_sha512_final(&ctx, enckey_and_hmackey);

	/*
	 * SHA-512 results 64 bytes. We extract it into two keys for
	 * each 32 bytes: one for key encryption and another one for
	 * HMAC.
	 */
	memcpy(kek_out, enckey_and_hmackey, TDE_KEK_SIZE);
	memcpy(hmackey_out, enckey_and_hmackey + TDE_KEK_SIZE, TDE_HMAC_KEY_SIZE);
}

/*
 * Verify the correctness of the given passphrase. We compute HMACs of the
 * wrapped keys (RDEK and WDEK) using the HMAC key retrived from the user
 * provided passphrase. And then we compare it with the HMAC stored alongside
 * the controlfile. Return true if both HMACs are matched, meaning the given
 * passphrase is correct. Otherwise return false.
 */
static bool
verify_passphrase(char *passphrase, int passlen, WrappedEncKeyWithHmac **keys,
				  int nkeys)
{
	keydata_t user_kek[TDE_KEK_SIZE];
	keydata_t user_hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t result_hmac[TDE_HMAC_SIZE];
	int	i;

	get_kek_and_hmackey_from_passphrase(passphrase, passlen,
										user_kek, user_hmackey);

	/* Verify both HMACs of RDEK and WDEK */
	for (i = 0; i < nkeys; i++)
	{
		kmgr_compute_hmac(user_hmackey, keys[i]->key,
						  result_hmac);
		if (memcmp(result_hmac, keys[i]->hmac, TDE_HMAC_SIZE) != 0)
			return false;
	}

	/* The passphrase is verified. Save the key encryption key */
	memcpy(keyEncKey, user_kek, TDE_KEK_SIZE);

	return true;
}

/* key wrapping routine for key manager */
static bool
kmgr_wrap_key(keydata_t *key, keydata_t *in, keydata_t *out)
{
	int	out_size;

	pg_wrap_key(key, TDE_KEK_SIZE,
				in, EncryptionKeySize,
				out, &out_size);
	if (out_size != (EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE))
		return false;

	return true;
}

/* key unwrapping routine for key manager */
static bool
kmgr_unwrap_key(keydata_t *key, keydata_t *in, keydata_t *out)
{
	int	out_size;

	pg_unwrap_key(key, TDE_KEK_SIZE,
				  in, (EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE),
				  out, &out_size);
	if (out_size != EncryptionKeySize)
		return false;

	return true;
}

/* Compute HMAC of the given wrapped key */
static void
kmgr_compute_hmac(keydata_t *hmackey, keydata_t *key,
				  keydata_t *hmac)
{
	pg_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					key, EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE,
					hmac);
}

/* Return plain relation encryption key */
void
KmgrGetRelationEncryptionKey(char *key_out)
{
	Assert(DataEncryptionEnabled());
	Assert(key_out);

	MemSet(key_out, 0, TDE_MAX_DEK_SIZE);
	memcpy(key_out, relEncKey, EncryptionKeySize);
}

/* Return plain WAL encryption key */
void
KmgrGetWALEncryptionKey(char *key_out)
{
	Assert(DataEncryptionEnabled());
	Assert(key_out);

	MemSet(key_out, 0, TDE_MAX_DEK_SIZE);
	memcpy(key_out, walEncKey, EncryptionKeySize);
}

/*
 * SQL function to rotate the cluster encryption key. This function
 * assumes that the cluster_passphrase_command is already reloaded
 * to the new value.
 */
Datum
pg_rotate_encryption_key(PG_FUNCTION_ARGS)
{
	WrappedEncKeyWithHmac new_keys[2] = {0};
	char	passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t	new_kek[TDE_KEK_SIZE];
	keydata_t	new_hmackey[TDE_HMAC_KEY_SIZE];
	int	len;
	int	i;

	len = run_cluster_passphrase_command(passphrase,
										 TDE_MAX_PASSPHRASE_LEN);

	get_kek_and_hmackey_from_passphrase(passphrase, len,
										new_kek, new_hmackey);

	/* Copy the current two encrpytion keys */
	memcpy(&(new_keys[0].key), relEncKey, EncryptionKeySize);
	memcpy(&(new_keys[1].key), walEncKey, EncryptionKeySize);

	/*
	 * Wrap and compute HMAC of both relation encryption key and
	 * wal encryption key by the new key encryption key.
	 */
	for (i = 0; i < 2; i++)
	{
		if (!kmgr_wrap_key(new_kek, new_keys[i].key, new_keys[i].key))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to wrapped encryption key")));

		kmgr_compute_hmac(new_hmackey, new_keys[i].key,
						  new_keys[i].hmac);
	}

	/* Update control file */
	LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
	SetTDERelationEncryptionKey(&(new_keys[0]));
	SetTDEWALEncryptionKey(&(new_keys[1]));
	UpdateControlFile();
	LWLockRelease(ControlFileLock);

	PG_RETURN_BOOL(true);
}
