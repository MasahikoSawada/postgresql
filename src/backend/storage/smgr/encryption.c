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

static bool		encryption_initialized = false;
static EVP_CIPHER_CTX *ctx_encrypt;
static EVP_CIPHER_CTX *ctx_decrypt;
static EVP_CIPHER_CTX *ctx_encrypt_stream;
static EVP_CIPHER_CTX *ctx_decrypt_stream;

static void setup_encryption_openssl(void);
static void evp_error(void);
static void setup_encryption(void) ;
static void initialize_encryption_context(EVP_CIPHER_CTX **ctx_p, bool stream);

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
			  const char *key, const char *tweak, bool stream)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	Assert((size >= ENCRYPTION_BLOCK_SIZE &&
			size % ENCRYPTION_BLOCK_SIZE == 0) || stream);

	/* Ensure encryption has setup */
	if (!encryption_initialized)
	{
		setup_encryption();
		encryption_initialized = true;
	}

	if (!stream && IsAllZero(input, size))
	{
		memset(output, 0, size);
		return;
	}

	ctx = !stream ? ctx_encrypt : ctx_encrypt_stream;

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
			  const char *key, const char *tweak, bool stream)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	Assert((size >= ENCRYPTION_BLOCK_SIZE &&
			size % ENCRYPTION_BLOCK_SIZE == 0) || stream);

	/* Ensure encryption has setup */
	if (!encryption_initialized)
	{
		setup_encryption();
		encryption_initialized = true;
	}

	if (!stream && IsAllZero(input, size))
	{
		memset(output, 0, size);
		return;
	}

	ctx = !stream ? ctx_encrypt : ctx_encrypt_stream;

	if (EVP_DecryptInit_ex(ctx, NULL, NULL, (unsigned char *) key,
						   (unsigned char *) tweak) != 1)
		evp_error();

	if (EVP_DecryptUpdate(ctx, (unsigned char *) output,
						  &out_size, (unsigned char *) input, size) != 1)
		evp_error();

	Assert(out_size == size);
}

static void
initialize_encryption_context(EVP_CIPHER_CTX **ctx_p, bool stream)
{
	EVP_CIPHER_CTX *ctx;
	const EVP_CIPHER *cipher;
	int			block_size;

	cipher = !stream ? EVP_aes_256_cbc() : EVP_aes_256_ctr();

	if ((*ctx_p = EVP_CIPHER_CTX_new()) == NULL)
		evp_error();
	ctx = *ctx_p;
	if (EVP_EncryptInit_ex(ctx, cipher, NULL, NULL, NULL) != 1)
		evp_error();

	/*
	 * No padding is needed. For a block cipher, the input block size should
	 * already be a multiple of ENCRYPTION_BLOCK. For stream cipher, we don't
	 * need padding anyway. This might save some cycles at the OpenSSL end.
	 * XXX Is it setting worth when we don't call EVP_DecryptFinal_ex()
	 * anyway?
	 */
	EVP_CIPHER_CTX_set_padding(ctx, 0);

	Assert(EVP_CIPHER_CTX_iv_length(ctx) == ENCRYPTION_TWEAK_SIZE);
	Assert(EVP_CIPHER_CTX_key_length(ctx) == ENCRYPTION_KEY_SIZE);
	block_size = EVP_CIPHER_CTX_block_size(ctx);
#ifdef USE_ASSERT_CHECKING
	if (!stream)
		Assert(block_size == ENCRYPTION_BLOCK_SIZE);
	else
		Assert(block_size == 1);
#endif

}

/*
 * Initialize encryption subsystem for use. Must be called before any
 * encryptable data is read from or written to data directory.
 */
static void
setup_encryption(void)
{
	setup_encryption_openssl();
	initialize_encryption_context(&ctx_encrypt, false);
	initialize_encryption_context(&ctx_decrypt, false);
	initialize_encryption_context(&ctx_encrypt_stream, true);
	initialize_encryption_context(&ctx_decrypt_stream, true);
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
BufferEncryptionTweak(char *tweak, RelFileNode *relnode, ForkNumber forknum,
					  BlockNumber blocknum)
{
	uint32		fork_and_block = (forknum << 24) ^ blocknum;

	memset(tweak, 0, ENCRYPTION_TWEAK_SIZE);
	memcpy(tweak, relnode, sizeof(RelFileNode));
	memcpy(tweak + sizeof(RelFileNode), &fork_and_block, 4);
}
