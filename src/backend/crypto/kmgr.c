/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Encryption key management module.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/kms/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/err.h>
#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xlog.h"
#include "common/sha2.h"
#include  "catalog/pg_control.h"
#include "crypto/kmgr.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define KMGR_PROMPT_MSG "Enter database encryption pass phrase:"
#define EncryptionKeySize 256
#define KEY_TDE_MAX_DEK_SIZE	256

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
static keydata_t relEncKey[KEY_TDE_MAX_DEK_SIZE];
static keydata_t walEncKey[TDE_MAX_DEK_SIZE];

/* GUC variable */
char *cluster_passphrase_command = NULL;

/*
 * This function must be called ONCE on system install. we retrive KEK,
 * generate RDEK and WDEK etc.
 */
KmgrBootstrapInfo *
BootStrapKmgr(int bootstrap_data_encryption_cipher)
{
	KmgrBootstrapInfo	*kmgrinfo;
	char 			passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t 		hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t 		*rdek_enc;
	keydata_t 		*wdek_enc;
	keydata_t 		*rdek_hmac;
	keydata_t 		*wdek_hmac;
	int			wrapped_keysize;
	int			len;
	int 			size;

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

	/* Get key encryption key for command */
	len = kms_run_passphrase_command(passphrase);

	/* Get key encryption key and HMAC key from passphrase */
	kms_get_kek_and_hmackey(passphrase, len, keyEncKey,
										hmackey);

	/*
	 * Generate relation encryption key and WAL encryption key.
	 * The generated two keys must be stored in relEncKey and
	 * walEncKey that can be used by other modules since even
	 * during bootstrapping we need to encrypt both systemcatalogs
	 * and WAL.
	 */
	if (!pg_strong_random(relEncKey, EncryptionKeySize))
		ereport(ERROR,
				(errmsg("failed to generate relation encryption key")));
	if (!pg_strong_random(walEncKey, EncryptionKeySize))
		ereport(ERROR,
				(errmsg("failed to generate WAL encryption key")));

	/* Wrap both keys by KEK */
	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;
	kms_wrap_key(keyEncKey, TDE_KEK_SIZE,
				relEncKey, EncryptionKeySize,
				rdek_enc, &size);
	if (size != wrapped_keysize)
		elog(ERROR, "wrapped relation encryption key size is invalid, got %d expected %d",
			 size, wrapped_keysize);

	kms_wrap_key(keyEncKey, TDE_KEK_SIZE,
				walEncKey, EncryptionKeySize,
				wdek_enc, &size);
	if (size != wrapped_keysize)
		elog(ERROR, "wrapped WAL encryption key size is invalid, got %d expected %d",
			 size, wrapped_keysize);

	/* Compute both HMAC */
	kms_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					rdek_enc, wrapped_keysize,
					rdek_hmac);
	kms_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					wdek_enc, wrapped_keysize,
					wdek_hmac);

	/* return keys and HMACs generated during bootstrap */
	return kmgrinfo;
}

/*
 * Get encryption key passphrase and verify it, then get the un-encrypted
 * RDEK and WDEK. This function is called by postmaster at startup time.
 */
void
kmgr_init(void)
{
	WrappedEncKeyWithHmac	*master_dek;
	char			passphrase[TDE_MAX_PASSPHRASE_LEN];
	int			len;
	int			wrapped_keysize;
	int			unwrapped_size;

	/* Get cluster passphrase */
	len = kms_run_passphrase_command(passphrase);
	if(len <= 0)	
	{
		ereport(WARNING,
			(errmsg("cluster initialized without key managment system")));
		return;
	}
	master_dek = GetMaterKey();

	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;

	/* Verify the correctness of given passphrase */
	if (!kms_verify_passphrase(passphrase, len, master_dek))
		ereport(ERROR,
				(errmsg("cluster passphrase does not match expected passphrase")));

	/* The passphrase is correct, unwrap both RDEK and WDEK */
	kms_unwrap_key(keyEncKey, TDE_KEK_SIZE,
				  master_dek->key, wrapped_keysize,
				  relEncKey, &unwrapped_size);

	if (unwrapped_size != EncryptionKeySize)
		elog(ERROR, "unwrapped relation encryption key size is invalid, got %d expected %d",
			 unwrapped_size, EncryptionKeySize);
}

