/*-------------------------------------------------------------------------
 *
 * enc_openssl.c
 *	  This code handles encryption and decryption using OpenSSL
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/enc_openssl.c
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
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "port.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/kdf.h>

/*
 * prototype for the EVP functions that return an algorithm, e.g.
 * EVP_aes_128_cbc().
 */
typedef const EVP_CIPHER *(*ossl_EVP_cipher_func) (void);
typedef struct
{
	ossl_EVP_cipher_func cipher_func;
	int					key_len;
} cipher_info;

cipher_info cipher_info_table[] =
{
	{EVP_aes_128_ctr, 16}, /* AES-128 */
	{EVP_aes_256_ctr, 32}	/* AES-256 */
};

typedef struct CipherCtx
{
	EVP_CIPHER_CTX *enc_ctx;
	EVP_CIPHER_CTX *wrap_ctx;
	EVP_PKEY_CTX   *derive_ctx;
} CipherCtx;

/* GUC parameter */
int database_encryption_cipher;

CipherCtx *MyCipherCtx = NULL;
int		EncryptionKeySize;
PGAlignedBlock encrypt_buf;

static void encrypt_block(const char *input, char *output, Size size,
						  const char *key, const char *iv);
static void decrypt_block(const char *input, char *output, Size size,
						  const char *key, const char *iv);
static void createCipherContext(void);
static EVP_CIPHER_CTX *create_ossl_encryption_ctx(ossl_EVP_cipher_func func,
												  int klen);
static EVP_PKEY_CTX *create_ossl_derive_ctx(void);
static void setup_encryption_openssl(void);
static void setup_encryption(void) ;

/*
 *
 */
static void
encrypt_block(const char *input, char *output, Size size,
			  const char *key, const char *iv)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	/* @@@ : should be supported Page Encryption */
	if (PageIsAllZero((Page) input))
	{
		memset(output, 0, size);
		return;
	}

	ctx = MyCipherCtx->enc_ctx;

	if (EVP_EncryptInit_ex(ctx, NULL, NULL, (unsigned char *) key,
						   (unsigned char *) iv) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during encryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (EVP_EncryptUpdate(ctx, (unsigned char *) output,
						  &out_size, (unsigned char *) input, size) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered error during encryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	Assert(out_size == size);
}

/*
 *
 */
static void
decrypt_block(const char *input, char *output, Size size,
			  const char *key, const char *iv)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	/* @@@ : should be supported Page Encryption */
	if (PageIsAllZero((Page) input))
	{
		memset(output, 0, size);
		return;
	}

	ctx = MyCipherCtx->enc_ctx;

	if (EVP_DecryptInit_ex(ctx, NULL, NULL, (unsigned char *) key,
						   (unsigned char *) iv) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during decryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (EVP_DecryptUpdate(ctx, (unsigned char *) output,
						  &out_size, (unsigned char *) input, size) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered error during decryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	Assert(out_size == size);
}

static void
createCipherContext(void)
{
	cipher_info *cipher = &cipher_info_table[database_encryption_cipher];
	CipherCtx *cctx;

	if (MyCipherCtx != NULL)
		return;

	cctx = (CipherCtx *) palloc(sizeof(CipherCtx));

	/* Create encryption context */
	cctx->enc_ctx = create_ossl_encryption_ctx(cipher->cipher_func,
											   cipher->key_len);

	/* Create key wrap context */
	cctx->wrap_ctx = create_ossl_encryption_ctx(EVP_aes_256_wrap,
												cipher->key_len);

	/* Create key derivation context */
	cctx->derive_ctx = create_ossl_derive_ctx();

	/* Set my cipher context and key size */
	MyCipherCtx = cctx;
	EncryptionKeySize = cipher->key_len;
}

/* Create openssl's key derivation context */
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

/* Create openssl's encryption context */
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
	createCipherContext();
}

static void
setup_encryption_openssl(void)
{
	/*
	 * Setup OpenSSL.
	 *
	 * None of these functions should return a value or raise error.
	 */
#ifdef HAVE_OPENSSL_INIT_CRYPTO
	OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL);
#else
	/*
	 * We can initialize openssl even when openssl is 1.0 or older, but
	 * since key derivation function(KDF) has introduced at openssl 1.1.0
	 * we require 1.1.0 or higher version.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 (errmsg("openssl 1.1.0 or higher is required for transparent data encryption"))));
#endif
}

void
EncryptionGetIVForWAL(char *iv, XLogSegNo segment, uint32 offset)
{
	char *p = iv;
	uint32 pageno = offset / XLOG_BLCKSZ;

	Assert(iv != NULL);

	/* Space for counter (4 byte) */
	memset(p, 0, ENCRYPTION_WAL_COUNTER_LEN);
	p += ENCRYPTION_WAL_COUNTER_LEN;

	/* Segement number (8 byte) */
	memcpy(p, &segment, sizeof(XLogSegNo));
	p += sizeof(XLogSegNo);

	/* Page number within a WAL segment (4 byte) */
	memcpy(p, &pageno, sizeof(uint32));
}

void
EncryptionGetIVForBuffer(char *iv, XLogRecPtr pagelsn, BlockNumber blocknum)

{
	char *p = iv;

	Assert(iv != NULL);

	/* Space for counter (4 byte) */
	memset(p, 0, ENCRYPTION_BUFFER_COUNTER_LEN);
	p += ENCRYPTION_BUFFER_COUNTER_LEN;

	/* page lsn (8 byte) */
	memcpy(p, &pagelsn, sizeof(XLogRecPtr));
	p += sizeof(XLogRecPtr);

	/* block number (4 byte) */
	memcpy(p, &blocknum, sizeof(BlockNumber));
	p += sizeof(BlockNumber);
}

