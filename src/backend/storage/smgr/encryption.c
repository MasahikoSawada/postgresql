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
 * NOTES
 *		This file is compiled as both front-end and backend code, so it
 *		may not use ereport, server-defined static variables, etc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "common/fe_memutils.h"
#include "common/sha2.h"
#include "common/string.h"
#include "catalog/pg_control.h"
#ifndef FRONTEND
#include "pgstat.h"
#endif							/* FRONTEND */
#include "storage/bufpage.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "port.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

char	   *encryption_buffer = NULL;
Size		encryption_buf_size = 0;

static void setup_encryption_internal(void);
static void evp_error(void);

/*
 * Encryption a buffer block on the given tablespace.
 */
void
EncryptBufferBlock(Oid spcOid, const char *tweak,
				   const char *input, char *output)
{
	const char *dataKey = NULL;

	//dataKey = KeyringGetDataKey(reln);

	encrypt_block(input, output, BLCKSZ, dataKey, tweak);
}

/*
 * Decryption a buffer block on the given tablespace.
 */
void
DecryptBufferBlock(Oid spcOid, const char *tweak,
				   const char *input, char *output)
{
	const char *dataKey = NULL;

	//dataKey = KeyringGetDataKey(reln);

	decrypt_block(input, output, BLCKSZ, dataKey, tweak);
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
			  const char *key, const char *tweak)
{
	Assert(size >= ENCRYPTION_BLOCK_SIZE && size % ENCRYPTION_BLOCK_SIZE == 0);

	/*
	 * The EVP API does not seem to expect the output buffer to be equal to
	 * the input. Ensure that we pass separate pointers.
	 */
	if (input == output)
	{
		if (size > encryption_buf_size)
			enlarge_encryption_buffer(size);

		memcpy(encryption_buffer, input, size);
		input = encryption_buffer;
	}

	if (IsAllZero(input, size))
		memset(output, 0, size);
	else
	{
		int			out_size;
		EVP_CIPHER_CTX *ctx;

		if ((ctx = EVP_CIPHER_CTX_new()) == NULL)
			evp_error();

		if (EVP_EncryptInit_ex(ctx, EVP_aes_256_xts(), NULL,
							   (unsigned char *) key,
							   (unsigned char *) tweak) != 1)
			evp_error();

		/*
		 * No padding is needed, the input block size should already be a
		 * multiple of ENCRYPTION_BLOCK_OPENSSL.
		 */
		EVP_CIPHER_CTX_set_padding(ctx, 0);

		Assert(EVP_CIPHER_CTX_block_size(ctx) == ENCRYPTION_BLOCK_OPENSSL);
		Assert(EVP_CIPHER_CTX_iv_length(ctx) == ENCRYPTION_TWEAK_SIZE);
		Assert(EVP_CIPHER_CTX_key_length(ctx) == ENCRYPTION_KEY_SIZE);

		/*
		 * Do the actual encryption. As the padding is disabled,
		 * EVP_EncryptFinal_ex() won't be needed.
		 */
		if (EVP_EncryptUpdate(ctx, (unsigned char *) output, &out_size,
							  (unsigned char *) input, size) != 1)
			evp_error();

		/*
		 * The input size is a multiple of ENCRYPTION_BLOCK_OPENSSL, so the
		 * output of AES-XTS should meet this condition.
		 */
		Assert(out_size == size);

		if (EVP_CIPHER_CTX_cleanup(ctx) != 1)
			evp_error();
	}
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
			  const char *key, const char *tweak)
{
	Assert(size >= ENCRYPTION_BLOCK_SIZE && size % ENCRYPTION_BLOCK_SIZE == 0);

	/*
	 * The EVP API does not seem to expect the output buffer to be equal to
	 * the input. Ensure that we pass separate pointers.
	 */
	if (input == output)
	{
		if (size > encryption_buf_size)
			enlarge_encryption_buffer(size);

		memcpy(encryption_buffer, input, size);
		input = encryption_buffer;
	}

	if (IsAllZero(input, size))
		memset(output, 0, size);
	else
	{
		int			out_size;
		EVP_CIPHER_CTX *ctx;

		if ((ctx = EVP_CIPHER_CTX_new()) == NULL)
			evp_error();

		if (EVP_DecryptInit_ex(ctx, EVP_aes_256_xts(), NULL,
							   (unsigned char *) key,
							   (unsigned char *) tweak) != 1)
			evp_error();

		/* The same considerations apply below as those in encrypt_block(). */
		EVP_CIPHER_CTX_set_padding(ctx, 0);
		Assert(EVP_CIPHER_CTX_block_size(ctx) == ENCRYPTION_BLOCK_OPENSSL);
		Assert(EVP_CIPHER_CTX_iv_length(ctx) == ENCRYPTION_TWEAK_SIZE);
		Assert(EVP_CIPHER_CTX_key_length(ctx) == ENCRYPTION_KEY_SIZE);

		if (EVP_DecryptUpdate(ctx, (unsigned char *) output, &out_size,
							  (unsigned char *) input, size) != 1)
			evp_error();

		Assert(out_size == size);

		if (EVP_CIPHER_CTX_cleanup(ctx) != 1)
			evp_error();
	}
}

/*
 * Report an error in an universal way so that caller does not have to care
 * whether it executes in backend or front-end.
 */
void
encryption_error(bool fatal, char *message)
{
#ifndef FRONTEND
	elog(fatal ? FATAL : INFO, "%s", message);
#else
	fprintf(stderr, "%s\n", message);
	if (fatal)
		exit(EXIT_FAILURE);
#endif
}

/*
 * Initialize encryption subsystem for use. Must be called before any
 * encryptable data is read from or written to data directory.
 */
void
setup_encryption(bool bootstrap)
{
	setup_encryption_internal();
}

static void
setup_encryption_internal(void)
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

void
enlarge_encryption_buffer(Size new_size)
{
	Assert(new_size > 0);

	/*
	 * Shrinkage is not the use case for this routine.
	 */
	if (new_size <= encryption_buf_size)
		return;

	/*
	 * Allocate a new chunk if nothing is there yet, else reallocate the
	 * existing one.
	 */
	if (encryption_buf_size == 0)
#ifndef FRONTEND
		encryption_buffer = (char *) MemoryContextAlloc(TopMemoryContext,
														new_size);
#else
		encryption_buffer = (char *) palloc(new_size);
#endif							/* FRONTEND */
	else
		encryption_buffer = (char *) repalloc(encryption_buffer, new_size);
	encryption_buf_size = new_size;
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
