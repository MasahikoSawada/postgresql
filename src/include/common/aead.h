/*-------------------------------------------------------------------------
 *
 * aead.h
 *		Declarations for utility function for AEAD
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/common/aead.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PG_AEAD_H
#define _PG_AEAD_H

#include "common/cipher.h"

/* Encryption key and MAC key used for AEAD */
#define PG_AEAD_ENC_KEY_LEN			PG_AES256_KEY_LEN
#define PG_AEAD_MAC_KEY_LEN			PG_HMAC_SHA512_KEY_LEN
#define PG_AEAD_HMAC_LEN			PG_HMAC_SHA512_LEN

/* AEAD key consists of encryption key and mac key */
#define PG_AEAD_KEY_LEN				(PG_AEAD_ENC_KEY_LEN + PG_AEAD_MAC_KEY_LEN)

/*
 * Size of encrypted key size with padding. We use PKCS#7 padding,
 * described in RFC 5652.
 */
#define SizeOfDataWithPadding(klen) \
	((int)(klen) + (PG_AES_BLOCK_SIZE - ((int)(klen) % PG_AES_BLOCK_SIZE)))

/* Macros to compute the size of cipher text and plain text */
#define AEADSizeOfCipherText(len) \
	(PG_AEAD_MAC_KEY_LEN + PG_AES_IV_SIZE + SizeOfDataWithPadding((int)(len)))
#define AEADSizeOfPlainText(klen) \
	((int)(klen) - (PG_AEAD_MAC_KEY_LEN + PG_AES_IV_SIZE))

/* Key wrapping cipher context */
typedef struct PgAeadCtx
{
	uint8			key[PG_AEAD_ENC_KEY_LEN];
	uint8			mackey[PG_AEAD_MAC_KEY_LEN];
	PgCipherCtx		*cipherctx;
} PgAeadCtx;

extern PgAeadCtx *pg_create_aead_ctx(uint8 key[PG_AEAD_ENC_KEY_LEN],
								  uint8 mackey[PG_AEAD_MAC_KEY_LEN]);
extern void pg_free_aead_ctx(PgAeadCtx *ctx);
extern bool pg_aead_encrypt(PgAeadCtx *ctx, uint8 *in, int inlen,
							uint8 *out, int *outlen);
extern bool pg_aead_decrypt(PgAeadCtx *ctx, uint8 *in, int inlen,
							uint8 *out, int *outlen);

#endif	/* _PG_AEAD_H */
