/*-------------------------------------------------------------------------
 *
 * aead.c
 *	  Shared frontend/backend code for AEAD
 *
 * This contains the common low-level functions needed in both frontend and
 * backend, for implement the Authenticated Encryption with Associated
 * Data (AEAD) that are based on the composition of the Advanced Encryption
 * Standard (AES) in the Cipher Block Chaining (CBC) mode of operation for
 * encryption, and the HMAC-SHA message authentication code (MAC).
 *
 * This AEAD algorithm is derived from the specification draft of
 * Authenticated Encryption with AES-CBC and HMAC-SHA[1].  It uses an
 * Authenticated Encryption with Associated Data, following an
 * Encrypt-then-MAC approach. Our AEAD algorithm uses AES-256 encryption
 * in the CBC mode of operation for encryption to encrypt data with a random
 * initialization vector (IV), and then compute HMAC-SHA512 of encrypted data.
 * The cipher text consists of the HMAC, IV and encrypted data.
 *
 * [1] https://tools.ietf.org/html/draft-mcgrew-aead-aes-cbc-hmac-sha2-05
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/aead.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/aead.h"
#include "common/cipher.h"

/* Return an AEAD context initialized with the given keys */
PgAeadCtx *
pg_create_aead_ctx(uint8 key[PG_AEAD_ENC_KEY_LEN], uint8 mackey[PG_AEAD_MAC_KEY_LEN])
{
	PgAeadCtx *ctx;

	ctx = (PgAeadCtx *) palloc0(sizeof(PgAeadCtx));

	/* Create and initialize a cipher context */
	ctx->cipherctx = pg_cipher_ctx_create(PG_CIPHER_AES_CBC, key, PG_AEAD_ENC_KEY_LEN);
	if (ctx->cipherctx == NULL)
		return NULL;

	/* Set encryption key and MAC key */
	memcpy(ctx->key, key, PG_AEAD_ENC_KEY_LEN);
	memcpy(ctx->mackey, mackey, PG_AEAD_MAC_KEY_LEN);

	return ctx;
}

/* Free the AEAD context */
void
pg_free_aead_ctx(PgAeadCtx *ctx)
{
	if (!ctx)
		return;

	Assert(ctx->cipherctx);

	pg_cipher_ctx_free(ctx->cipherctx);

#ifndef FRONTEND
	pfree(ctx);
#else
	pg_free(ctx);
#endif
}

/*
 * Encrypt the given data. Return true and set encrypted data to 'out' if
 * success.  Otherwise return false. The caller must allocate sufficient space
 * for cipher data calculated by using AEADSizeOfCipherText(). Please note that
 * this function modifies 'out' data even on failure case.
 */
bool
pg_aead_encrypt(PgAeadCtx *ctx, uint8 *in, int inlen, uint8 *out, int *outlen)
{
	uint8	*hmac;
	uint8	*iv;
	uint8	*enc;
	int		enclen;

	Assert(ctx && in && out);

	hmac = out;
	iv = hmac + PG_AEAD_HMAC_LEN;
	enc = iv + PG_AES_IV_SIZE;

	/* Generate IV */
	if (!pg_strong_random(iv, PG_AES_IV_SIZE))
		return false;

	if (!pg_cipher_encrypt(ctx->cipherctx, in, inlen, enc, &enclen, iv))
		return false;

	if (!pg_HMAC_SHA512(ctx->mackey, enc, enclen, hmac))
		return false;

	*outlen = AEADSizeOfCipherText(inlen);;
	Assert(*outlen == PG_AEAD_HMAC_LEN + PG_AES_IV_SIZE + enclen);

	return true;
}

/*
 * Decrypt the given Data. Return true and set plain text data to `out` if
 * success.  Otherwise return false. The caller must allocate sufficient space
 * for cipher data calculated by using AEADSizeOfPlainText(). Please note that
 * this function modifies 'out' data even on failure case.
 */
bool
pg_aead_decrypt(PgAeadCtx *ctx, uint8 *in, int inlen, uint8 *out, int *outlen)
{
	uint8	hmac[PG_AEAD_HMAC_LEN];
	uint8   *expected_hmac;
	uint8   *iv;
	uint8	*enc;
	int		enclen;

	Assert(ctx && in && out);

	expected_hmac = in;
	iv = expected_hmac + PG_AEAD_HMAC_LEN;
	enc = iv + PG_AES_IV_SIZE;
	enclen = inlen - (enc - in);

	/* Verify the correctness of HMAC */
	if (!pg_HMAC_SHA512(ctx->mackey, enc, enclen, hmac))
		return false;

	if (memcmp(hmac, expected_hmac, PG_AEAD_HMAC_LEN) != 0)
		return false;

	/* Decrypt encrypted data */
	if (!pg_cipher_decrypt(ctx->cipherctx, enc, enclen, out, outlen, iv))
		return false;

	return true;
}
