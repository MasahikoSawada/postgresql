/*-------------------------------------------------------------------------
 *
 * keyapi.c
 *	 Encryption key management module.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/kms/keyapi.c
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
#include "crypto/kmgr.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define EncryptionKeySize 256


static EVP_CIPHER_CTX *
kms_create_ctx(int klen, bool isenc, bool iswrap);

void
kms_wrap_key(const unsigned char *key, int key_size, unsigned char *in,
		int in_size, unsigned char *out, int *out_size)
{
	EVP_CIPHER_CTX *ctx = kms_create_ctx(EncryptionKeySize, true, false);

	if (EVP_EncryptInit_ex(ctx, NULL, NULL, key, NULL) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during unwrapping key"),
					(errdetail("openssl error string: %s",
						ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_CIPHER_CTX_set_key_length(ctx, key_size))
		ereport(ERROR,
			(errmsg("openssl encountered setting key length error during wrapping key"),
				(errdetail("openssl error string: %s",
					ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_EncryptUpdate(ctx, out, out_size, in, in_size))
		ereport(ERROR,
			(errmsg("openssl encountered error during wrapping key"),
				(errdetail("openssl error string: %s",
					ERR_error_string(ERR_get_error(), NULL)))));
}

void
kms_unwrap_key(const unsigned char *key, int key_size, unsigned char *in,
				int in_size, unsigned char *out, int *out_size)
{
	EVP_CIPHER_CTX *ctx = kms_create_ctx(EncryptionKeySize, true, false);

	if (EVP_DecryptInit_ex(ctx, NULL, NULL, key, NULL) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during unwrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_CIPHER_CTX_set_key_length(ctx, key_size))
		ereport(ERROR,
				(errmsg("openssl encountered setting key length error during unwrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (EVP_DecryptUpdate(ctx, out, out_size, in, in_size) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered error during unwrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));
}

void
kms_compute_hmac(const unsigned char *hmac_key, int key_size,
				  unsigned char *data, int data_size, unsigned char *hmac)
{
	unsigned char 	*h;
	uint32		hmac_size;

	Assert(hmac != NULL);

	h = HMAC(EVP_sha256(), hmac_key, key_size, data, data_size, hmac, &hmac_size);

	if (h == NULL)
		ereport(ERROR,
				(errmsg("could not compute HMAC"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	memcpy(hmac, h, hmac_size);
}

static EVP_CIPHER_CTX *
kms_create_ctx(int klen, bool isenc, bool iswrap)
{
	EVP_CIPHER_CTX *ctx;
	int 		ret;

	/* Create new openssl cipher context */
	ctx = EVP_CIPHER_CTX_new();

	/* Enable key wrap algorithm */
	if (iswrap)
		EVP_CIPHER_CTX_set_flags(ctx, EVP_CIPHER_CTX_FLAG_WRAP_ALLOW);

	if (ctx == NULL)
		ereport(ERROR,
				(errmsg("openssl encountered error during creating context"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (isenc)
		ret = EVP_EncryptInit_ex(ctx, (const EVP_CIPHER *) EVP_aes_256_ctr, NULL,
								 NULL, NULL);
	else
		ret = EVP_DecryptInit_ex(ctx, (const EVP_CIPHER *) EVP_aes_256_ctr, NULL,
								 NULL, NULL);

	if (ret != 1)
			ereport(ERROR,
					(errmsg("openssl encountered error during initializing context"),
					 (errdetail("openssl error string: %s",
								ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_CIPHER_CTX_set_key_length(ctx, klen))
		ereport(ERROR,
				(errmsg("openssl encountered error during setting key length"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	return ctx;
}
