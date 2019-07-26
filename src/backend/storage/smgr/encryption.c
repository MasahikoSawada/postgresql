/*-------------------------------------------------------------------------
 *
 * encryption.c
 *	  This code handles encryption and decryption of data.
 *
 * Encryption is done by extension modules loaded by encryption_library GUC.
 * The extension module must register itself and provide a cryptography
 * implementation. Key setup is left to the extension module.
 *
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/encryption.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "common/fe_memutils.h"
#include "common/sha2.h"
#include "common/string.h"
#include "catalog/pg_control.h"
#include "pgstat.h"
#include "storage/bufpage.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "port.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

PGAlignedBlock encrypt_buf;

/*
 * prototype for the EVP functions that return an algorithm, e.g.
 * EVP_aes_128_cbc().
 */
typedef const EVP_CIPHER *(*ossl_EVP_cipher_func) (void);

typedef struct CipherCtx
{
	EVP_CIPHER_CTX *block_ctx;
	EVP_CIPHER_CTX *stream_ctx;
	EVP_PKEY_CTX   *derive_ctx;
} CipherCtx;

/* GUC parameter */
int database_encryption_cipher;

CipherCtx *MyCipherCtx = NULL;
int		EncryptionKeySize;

static EVP_CIPHER_CTX *create_ossl_encrypt(ossl_EVP_cipher_func func,
										   int klen);
static void createcipherContext(void);
static void setup_encryption_openssl(void);
static void evp_error(void);
static void setup_encryption(void) ;
/*
 * Encryption a buffer block on the given tablespace.
 */
void
EncryptBufferBlock(Oid spcOid, const char *tweak, const char *input,
				   char *output)
{
	char spckey[ENCRYPTION_KEY_SIZE];

	KeyringGetKey(spcOid, spckey);

	/* Always use block cipher for buffer data */
	encrypt_block(input, output, BLCKSZ, spckey, tweak, false);
}

/*
 * Decryption a buffer block on the given tablespace.
 */
void
DecryptBufferBlock(Oid spcOid, const char *tweak, const char *input,
				   char *output)
{
	char spckey[ENCRYPTION_KEY_SIZE];

	KeyringGetKey(spcOid, spckey);

	/* Always use block cipher for buffer data */
	decrypt_block(input, output, BLCKSZ, spckey, tweak, false);
}

void
DeriveEncryptionKey(const char *input, char *newkey)
{
	char salt[ENCRYPTION_KEY_SALT_LEN];
	int outlen;

	Assert(input != NULL && output != NULL);

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

    if (EVP_PKEY_CTX_set1_hkdf_salt(pctx, salt, ENCRYPTION_KEY_SALT_LEN) <= 0)
		evp_error();

    if (EVP_PKEY_CTX_set1_hkdf_key(pctx, input, EncryptionKeyLen) <= 0)
		evp_error();

    if (EVP_PKEY_derive(pctx, newkey, &outlen) <= 0)
		evp_error();

	Assert(outlen == EncryptionKeyLen);
}

/*
 * Encrypts one block of data with a specified tweak value. May only be called
 * when encryption_enabled is true.
 *
 * Input and output buffer may point to the same location.
 *
 * "size" must be a (non-zero) multiple of ENCRYPTION_BLOCK.
 *
 * "tweak" value must be TWEAK_SIZE bytes long.
 *
 * All-zero blocks are not encrypted to correctly handle relation extension,
 * and also to simplify handling of holes created by seek past EOF and
 * consequent write (see buffile.c).
 */
void
encrypt_block(const char *input, char *output, Size size,
			  const char *key, const char *iv, bool stream)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	Assert((size >= ENCRYPTION_BLOCK_SIZE &&
			size % ENCRYPTION_BLOCK_SIZE == 0) || stream);

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	if (!stream && IsAllZero(input, size))
	{
		memset(output, 0, size);
		return;
	}

	ctx = !stream ? MyCipherCtx->block_ctx : MyCipherCtx->stream_ctx;

	if (EVP_EncryptInit_ex(ctx, NULL, NULL, (unsigned char *) key,
						   (unsigned char *) tweak) != 1)
		evp_error();

	if (EVP_EncryptUpdate(ctx, (unsigned char *) output,
						  &out_size, (unsigned char *) input, size) != 1)
		evp_error();

	Assert(out_size == size);
}

/*
 * Decrypts one block of data with a specified tweak value. May only be called
 * when encryption_enabled is true.
 *
 * Input and output buffer may point to the same location.
 *
 * "size" must be a (non-zero) multiple of ENCRYPTION_BLOCK.
 *
 * "tweak" value must be ENCRYPTION_TWEAK_SIZE bytes long.
 *
 * All-zero blocks are not decrypted to correctly handle relation extension,
 * and also to simplify handling of holes created by seek past EOF and
 * consequent write (see buffile.c).
 */
void
decrypt_block(const char *input, char *output, Size size,
			  const char *key, const char *iv, bool stream)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	Assert((size >= ENCRYPTION_BLOCK_SIZE &&
			size % ENCRYPTION_BLOCK_SIZE == 0) || stream);

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	if (!stream && IsAllZero(input, size))
	{
		memset(output, 0, size);
		return;
	}

	ctx = !stream ? MyCipherCtx->block_ctx : MyCipherCtx->stream_ctx;

	if (EVP_DecryptInit_ex(ctx, NULL, NULL, (unsigned char *) key,
						   (unsigned char *) tweak) != 1)
		evp_error();

	if (EVP_DecryptUpdate(ctx, (unsigned char *) output,
						  &out_size, (unsigned char *) input, size) != 1)
		evp_error();

	Assert(out_size == size);
}

static void
createcipherContext(void)
{
	cipher_info *cipher = &cipher_info_table[database_encryption_cipher];
	CipherCtx *cctx = (CipherCtx *) palloc(sizeof(CipherCtx));

	if (MyCipherCtx == NULL)
	{
		cctx->block_ctx = create_ossl_encrypion_ctx(cipher->cipher_func_blk,
													cipher->key_len);
		cctx->stream_ctx = create_ossl_encrypion_ctx(cipher->cipher_func_strm,
													 cipher->key_len);
		cctx->derive_ctx = create_ossl_derive_ctx();
		MyCipherCtx = cctx;
		EncryptionKeySize = cipher->key_len;
	}
}

static EVP_PKEY_CTX *
create_ossl_derive_ctx(void)
{
   EVP_PKEY_CTX *pctx;

   pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_HKDF, NULL);

   if (EVP_PKEY_derive_init(pctx) <= 0)
	   return NULL;

   if (EVP_PKEY_CTX_set_hkdf_md(pctx, EVP_sha256()) <= 0)
	   return NULL;

   return pctx;
}

static EVP_CIPHER_CTX *
create_ossl_encryption_ctx(ossl_EVP_cipher_func func, int klen)
{
	EVP_CIPHER_CTX *ctx;

	/* Craete new openssl cipher context */
	ctx = EVP_CIPHER_CTX_new();
	if (ctx == NULL)
	{
		EVP_CIPHER_CTX_free(ctx);
		return NULL;
	}

	if (!EVP_EncryptInit_ex(ctx, (const EVP_CIPHER *) func(), NULL, NULL, NULL))
	{
		EVP_CIPHER_CTX_free(ctx);
		return NULL;
	}

	if (!EVP_CIPHER_CTX_set_key_length(ctx, klen))
	{
		EVP_CIPHER_CTX_free(ctx);
		return NULL;
	}

	/*
	 * No padding is needed. For a block cipher, the input block size should
	 * already be a multiple of ENCRYPTION_BLOCK. For stream cipher, we don't
	 * need padding anyway. This might save some cycles at the OpenSSL end.
	 * XXX Is it setting worth when we don't call EVP_DecryptFinal_ex()
	 * anyway?
	 */
	EVP_CIPHER_CTX_set_padding(ctx, 0);

	return ctx;
}

/*
 * Initialize encryption subsystem for use. Must be called before any
 * encryptable data is read from or written to data directory.
 */
static void
setup_encryption(void)
{
	setup_encryption_openssl();
	createcipherContext();
}

static void
setup_encryption_openssl(void)
{
	/*
	 * Setup OpenSSL.
	 *
	 * None of these functions should return a value or raise error.
	 */
	ERR_load_crypto_strings();
	OpenSSL_add_all_algorithms();
	OPENSSL_config(NULL);
}

/*
 * Error callback for openssl.
 */
static void
evp_error(void)
{
	ERR_print_errors_fp(stderr);
#ifndef FRONTEND

	/*
	 * FATAL is the appropriate level because backend can hardly fix anything
	 * if encryption / decryption has failed.
	 *
	 * XXX Do we yet need EVP_CIPHER_CTX_cleanup() here?
	 */
	elog(FATAL, "OpenSSL encountered error during encryption or decryption.");
#else
	fprintf(stderr,
			"OpenSSL encountered error during encryption or decryption.");
	exit(EXIT_FAILURE);
#endif							/* FRONTEND */
}

void
EncryptDMEK(const char *dmek, const char *kek)
{
	encrypt_data(dmek, dmek, MyCipherCtx->key_len, kek, iv, false);
}

void
DecryptDMEK(const char *dmek, const char *kek)
{
	decrypt_data(dmek, dmek, MyCipherCtx->key_len, kek, iv, false);
}


/*
 * Xlog is encrypted page at a time. Each xlog page gets a unique tweak via
 * segment and offset. Unfortunately we can't include timeline because
 * exitArchiveRecovery() can copy part of the last segment of the old timeline
 * into the first segment of the new timeline.
 *
 * TODO Consider teaching exitArchiveRecovery() to decrypt the copied pages
 * and encrypt them using a tweak that mentions the new timeline.
 *
 * The function is located here rather than some of the xlog*.c modules so
 * that front-end applications can easily use it too.
 */
void
XLogEncryptionTweak(char *tweak, XLogSegNo segment, uint32 offset)
{
	memset(tweak, 0, ENCRYPTION_TWEAK_SIZE);
	memcpy(tweak, &segment, sizeof(XLogSegNo));
	memcpy(tweak + sizeof(XLogSegNo), &offset, sizeof(offset));
}

/*
 * md files are encrypted block at a time. Tweak will alias higher numbered
 * forks for huge tables.
 */
void
BufferEncryptionTweak(char *tweak, XLogRecPtr pagelsn, BlockNumber blocknum,
					  BlockNumber blocknum, Keyid id)
{
	uint32		fork_and_block = (forknum << 24) ^ blocknum;

	memset(tweak, 0, ENCRYPTION_TWEAK_SIZE);
	memcpy(tweak, &pagelsn, sizeof(XLogRecPtr));
	memcpy(tweak + sizeof(XLogRecPtr), &blocknum, sizeof(uint32));

	/* and encrypt by key */
}
