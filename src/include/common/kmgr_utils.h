/*-------------------------------------------------------------------------
 *
 * kmgr_utils.h
 *		Declarations for utility function for key management
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/common/kmgr_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_UTILS_H
#define KMGR_UTILS_H

#include "common/cipher.h"

/* Current version number */
#define KMGR_VERSION 1

/*
 * Directory where cryptographic keys reside within PGDATA. KMGR_DIR_TMP
 * is used during cluster passphrase rotation.
 */
#define KMGR_DIR			"pg_cryptokeys"
#define KMGR_TMP_DIR		"pg_cryptokeys_tmp"

/*
 * Identifiers of internal keys.  When adding a new internal key, we
 * also need to add its key length to internalKeyLengths.
 */
#define KMGR_SQL_KEY_ID			0

#define KMGR_MAX_INTERNAL_KEYS	1

/* As of now key length supports only AES-256 key */
#define KMGR_ENCKEY_LEN				PG_AES256_KEY_LEN

/* Key management uses HMAC-256 */
#define KMGR_HMACKEY_LEN			PG_HMAC_SHA256_KEY_LEN
#define KMGR_HMAC_LEN				PG_HMAC_SHA256_LEN

/* Key length of keys wrapping other key */
#define KMGR_KEY_LEN				(KMGR_ENCKEY_LEN + KMGR_HMACKEY_LEN)

/* Allowed length of cluster passphrase */
#define KMGR_MIN_PASSPHRASE_LEN 	64
#define KMGR_MAX_PASSPHRASE_LEN		1024

/*-----------------------------------------
 * Wrapped key format:
 * 1. HMAC of ((2) + (3))
 * 2. IV
 * 3. Enc(IV + <target key>, <key>)
 *-----------------------------------------
 */
#define KMGR_WRAPPED_KEY_LEN \
	(KMGR_HMAC_LEN + AES_IV_SIZE + SizeOfKeyWithPadding(KMGR_KEY_LEN))

/* Maximum length of key the key manager can store */
#define KMGR_MAX_KEY_LEN			256
#define KMGR_MAX_WRAPPED_KEY_LEN	SizeOfWrappedKey(KMGR_MAX_KEY_LEN)

/*
 * Size of encrypted key size with padding. We use PKCS#7 padding,
 * described in RFC 5652.
 */
#define SizeOfKeyWithPadding(klen) \
	((int)(klen) + (AES_BLOCK_SIZE - ((int)(klen) % AES_BLOCK_SIZE)))

/*
 * Macro to compute the size of wrapped and unwrapped key.  The wrapped
 * key consists of HMAC of the encrypted data, IV and the encrypted data
 * that is the same length as the input.
 */
#define SizeOfWrappedKey(klen) \
	(KMGR_HMACKEY_LEN + AES_IV_SIZE + SizeOfKeyWithPadding((int)(klen)))
#define SizeOfUnwrappedKey(klen) \
	((int)(klen) - (KMGR_HMACKEY_LEN + AES_IV_SIZE))

/* CryptoKey file name is keys id */
#define CryptoKeyFilePath(path, dir, id) \
	snprintf((path), MAXPGPATH, "%s/%04X", (dir), (id))

/*
 * Cryptographic key data structure. This structure is used for
 * both on-disk (raw key) and on-memory (wrapped key).
 */
typedef struct CryptoKey
{
	int		klen;
	uint8	key[KMGR_MAX_WRAPPED_KEY_LEN];
} CryptoKey;

/* Key wrapping cipher context */
typedef struct KeyWrapCtx
{
	uint8			key[KMGR_KEY_LEN];
	uint8			hmackey[KMGR_HMACKEY_LEN];
	PgCipherCtx		*cipher;
} KeyWrapCtx;

extern KeyWrapCtx *create_keywrap_ctx(uint8 key[KMGR_KEY_LEN],
									  uint8 hmackey[KMGR_HMACKEY_LEN]);
extern void free_keywrap_ctx(KeyWrapCtx *ctx);
extern void kmgr_derive_keys(char *passphrase, Size passlen,
							 uint8 key[KMGR_KEY_LEN],
							 uint8 hmackey[KMGR_HMACKEY_LEN]);
extern bool kmgr_verify_passphrase(char *passphrase, int passlen,
								   CryptoKey *keys_in, CryptoKey *keys_out,
								   int nkey);
extern bool kmgr_wrap_key(KeyWrapCtx *ctx, CryptoKey *in, CryptoKey *out);
extern bool kmgr_unwrap_key(KeyWrapCtx *ctx, CryptoKey *in, CryptoKey *out);
extern bool kmgr_HMAC_SHA256(KeyWrapCtx *ctx, const uint8 *in, int inlen,
							 uint8 *out);
extern int	kmgr_run_cluster_passphrase_command(char *passphrase_command,
												char *buf, int size);
extern CryptoKey *kmgr_get_cryptokeys(const char *path, int *nkeys);

#endif							/* KMGR_UTILS_H */
