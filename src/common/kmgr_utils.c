/*-------------------------------------------------------------------------
 *
 * kmgr_utils.c
 *	  Shared frontend/backend for cryptographic key management
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/kmgr_utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/kmgr_utils.h"
#include "common/sha2.h"

static pg_cipher_ctx *wrapctx = NULL;
static pg_cipher_ctx *unwrapctx = NULL;
static bool keywrap_initialized = false;

static bool
initialize_keywrap_ctx(void)
{
	wrapctx = pg_cipher_ctx_create();
	if (wrapctx == NULL)
		return false;

	unwrapctx = pg_cipher_ctx_create();
	if (unwrapctx == NULL)
		return false;

	if (!pg_aes256_ctr_wrap_init(wrapctx))
		return false;

	if (!pg_aes256_ctr_wrap_init(unwrapctx))
		return false;

	keywrap_initialized = true;
	return true;
}

/*
 * Hash the given passphrase and extract it into KEK and HMAC
 * key.
 */
void
kmgr_derive_keys(char *passphrase, Size passlen,
				 uint8 kek[KMGR_KEK_LEN],
				 uint8 hmackey[KMGR_HMAC_KEY_LEN])
{
	uint8 keys[PG_SHA512_DIGEST_LENGTH];
	pg_sha512_ctx ctx;

	pg_sha512_init(&ctx);
	pg_sha512_update(&ctx, (const uint8 *) passphrase, passlen);
	pg_sha512_final(&ctx, keys);

	/*
	 * SHA-512 results 64 bytes. We extract it into two keys for
	 * each 32 bytes.
	 */
	if (kek)
		memcpy(kek, keys, KMGR_KEK_LEN);
	if (hmackey)
		memcpy(hmackey, keys + KMGR_KEK_LEN, KMGR_HMAC_KEY_LEN);
}

/*
 * Verify the correctness of the given passphrase. We compute HMACs of the
 * wrapped keys (RDEK and WDEK) using the HMAC key retrived from the user
 * provided passphrase. And then we compare it with the HMAC stored alongside
 * the controlfile. Return true if both HMACs are matched, meaning the given
 * passphrase is correct. Otherwise return false.
 */
bool
kmgr_verify_passphrase(char *passphrase, int passlen,
					   WrappedEncKeyWithHmac *kh, int keylen)
{
	uint8 user_kek[KMGR_KEK_LEN];
	uint8 user_hmackey[KMGR_HMAC_KEY_LEN];
	uint8 result_hmac[KMGR_HMAC_LEN];

	kmgr_derive_keys(passphrase, passlen, user_kek, user_hmackey);

	/* Verify both HMAC */
	kmgr_compute_HMAC(user_hmackey, kh->key, keylen, result_hmac);

	if (memcmp(result_hmac, kh->hmac, KMGR_HMAC_LEN) != 0)
		return false;

	return true;
}

bool
kmgr_wrap_key(uint8 *key, const uint8 *in, int insize, uint8 *out)
{
	int outsize;

	if (!keywrap_initialized)
		if (!initialize_keywrap_ctx())
			return false;

	return pg_cipher_encrypt(wrapctx, key, in , insize,
							 NULL, out, &outsize);
}

bool
kmgr_unwrap_key(uint8 *key, const uint8 *in, int insize, uint8 *out)
{
	int outsize;

	if (!keywrap_initialized)
		if (!initialize_keywrap_ctx())
			return false;

	return pg_cipher_decrypt(unwrapctx, key, in, insize,
							 NULL, out, &outsize);
}

bool
kmgr_compute_HMAC(uint8 *key, const uint8 *data, int size,
				  uint8 *result)
{
	int resultsize;

	return pg_compute_HMAC(key, data, size, result, &resultsize);
}

/* Convert cipher name string to integer value */
int
kmgr_cipher_value(const char *name)
{
	if (strcasecmp(name, "aes-128") == 0)
		return KMGR_ENCRYPTION_AES128;

	if (strcasecmp(name, "aes-256") == 0)
		return KMGR_ENCRYPTION_AES256;

	return KMGR_ENCRYPTION_OFF;
}

/* Convert integer value to cipher name string */
char *
kmgr_cipher_string(int value)
{
	switch (value)
	{
		case KMGR_ENCRYPTION_OFF :
			return "off";
		case KMGR_ENCRYPTION_AES128:
			return "aes-128";
		case KMGR_ENCRYPTION_AES256:
			return "aes-256";
		default:
			return "unknown";
	}
	return "unknown";
}

