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
 * Copyright (c) 2016, PostgreSQL Global Development Group
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

#ifdef USE_ENCRYPTION
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#endif

#ifdef USE_ENCRYPTION
unsigned char *encryption_key = NULL;
const char *encryption_key_prefix = "encryption_key=";
const char *encryption_pwd_prefix = "encryption_password=";
#endif

bool		data_encrypted = false;

#ifdef USE_ENCRYPTION
char	   *encryption_key_command = NULL;
char	   *encryption_buffer = NULL;
Size		encryption_buf_size = 0;

static bool initialized = false;

static void setup_encryption_internal(void);
static char *run_encryption_key_command(bool *is_key_p, size_t *len_p);
static void evp_error(void);

/*
 * Pointer to the KDF parameters.
 *
 * XXX Rename this and the write / read functions so they contain the
 * 'keysetup' string?
 */
KDFParamsData *KDFParams = NULL;

/*
 * Initialize KDFParamsData and write it to a file.
 *
 * This is very similar to WriteControlFile().
 */
#ifndef FRONTEND
extern void
write_kdf_file(void)
{
	KDFParamsPBKDF2 *params;
	int			i,
				fd;

	StaticAssertStmt(sizeof(KDFParamsData) <= KDF_PARAMS_FILE_SIZE,
					 "kdf file is too large for atomic disk writes");

	/*
	 * The initialization should not be repeated.
	 */
	Assert(KDFParams == NULL);

	KDFParams = MemoryContextAllocZero(TopMemoryContext,
									   KDF_PARAMS_FILE_SIZE);
	KDFParams->function = KDF_OPENSSL_PKCS5_PBKDF2_HMAC_SHA;
	params = &KDFParams->data.pbkdf2;
	params->niter = ENCRYPTION_KDF_NITER;
	for (i = 0; i < ENCRYPTION_KDF_SALT_LEN; i++)
		params->salt[i] = (unsigned char) random();

	/* Contents are protected with a CRC */
	INIT_CRC32C(KDFParams->crc);
	COMP_CRC32C(KDFParams->crc,
				(char *) KDFParams,
				offsetof(KDFParamsData, crc));
	FIN_CRC32C(KDFParams->crc);

	fd = BasicOpenFile(KDF_PARAMS_FILE,
					   O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create key setup file \"%s\": %m",
						KDF_PARAMS_FILE)));

	pgstat_report_wait_start(WAIT_EVENT_KDF_FILE_WRITE);
	if (write(fd, KDFParams, KDF_PARAMS_FILE_SIZE) != KDF_PARAMS_FILE_SIZE)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to key setup file: %m")));
	}
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_KDF_FILE_SYNC);
	if (pg_fsync(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync key setup file: %m")));
	pgstat_report_wait_end();

	if (close(fd))
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close key setup file: %m")));
}

/*
 * Read KDFParamsData from file and store it in local memory.
 *
 * postmaster should call the function early enough for any other process to
 * inherit valid pointer to the data.
 */
extern void
read_kdf_file(void)
{
	pg_crc32c	crc;
	int			fd;

	KDFParams = MemoryContextAllocZero(TopMemoryContext,
									   KDF_PARAMS_FILE_SIZE);

	fd = BasicOpenFile(KDF_PARAMS_FILE, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open key setup file \"%s\": %m",
						KDF_PARAMS_FILE)));
	}

	pgstat_report_wait_start(WAIT_EVENT_KDF_FILE_READ);

	if (read(fd, KDFParams, sizeof(KDFParamsData)) != sizeof(KDFParamsData))
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read from key setup file: %m")));
	}

	pgstat_report_wait_end();

	close(fd);

	/* Now check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
				(char *) KDFParams,
				offsetof(KDFParamsData, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, KDFParams->crc))
		ereport(FATAL,
				(errmsg("incorrect checksum in key setup file")));

	if (KDFParams->function != KDF_OPENSSL_PKCS5_PBKDF2_HMAC_SHA)
		ereport(FATAL,
				(errmsg("unsupported KDF function %d", KDFParams->function)));
}
#endif							/* FRONTEND */
#endif							/* USE_ENCRYPTION */

/*
 * Encrypts a fixed value into *buf to verify that encryption key is correct.
 * Caller provided buf needs to be able to hold at least ENCRYPTION_SAMPLE_SIZE
 * bytes.
 */
void
sample_encryption(char *buf)
{
#ifdef USE_ENCRYPTION
	char		tweak[TWEAK_SIZE];
	int			i;

	for (i = 0; i < TWEAK_SIZE; i++)
		tweak[i] = i;

	encrypt_block("postgresqlcrypt", buf, ENCRYPTION_SAMPLE_SIZE, tweak);
#else
	ENCRYPTION_NOT_SUPPORTED_MSG;
#endif							/* USE_ENCRYPTION */
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
encrypt_block(const char *input, char *output, Size size, const char *tweak)
{
#ifdef	USE_ENCRYPTION
	Assert(size >= ENCRYPTION_BLOCK && size % ENCRYPTION_BLOCK == 0);
	Assert(initialized);

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

		if (EVP_EncryptInit_ex(ctx, EVP_aes_256_xts(), NULL, encryption_key,
							   (unsigned char *) tweak) != 1)
			evp_error();

		/*
		 * No padding is needed, the input block size should already be a
		 * multiple of ENCRYPTION_BLOCK_OPENSSL.
		 */
		EVP_CIPHER_CTX_set_padding(ctx, 0);

		Assert(EVP_CIPHER_CTX_block_size(ctx) == ENCRYPTION_BLOCK_OPENSSL);
		Assert(EVP_CIPHER_CTX_iv_length(ctx) == TWEAK_SIZE);
		Assert(EVP_CIPHER_CTX_key_length(ctx) == ENCRYPTION_KEY_LENGTH);

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
#else
	ENCRYPTION_NOT_SUPPORTED_MSG;
#endif							/* USE_ENCRYPTION */
}

/*
 * Decrypts one block of data with a specified tweak value. May only be called
 * when encryption_enabled is true.
 *
 * Input and output buffer may point to the same location.
 *
 * "size" must be a (non-zero) multiple of ENCRYPTION_BLOCK.
 *
 * "tweak" value must be TWEAK_SIZE bytes long.
 *
 * All-zero blocks are not decrypted to correctly handle relation extension,
 * and also to simplify handling of holes created by seek past EOF and
 * consequent write (see buffile.c).
 */
void
decrypt_block(const char *input, char *output, Size size, const char *tweak)
{
#ifdef	USE_ENCRYPTION
	Assert(size >= ENCRYPTION_BLOCK && size % ENCRYPTION_BLOCK == 0);
	Assert(initialized);

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

		if (EVP_DecryptInit_ex(ctx, EVP_aes_256_xts(), NULL, encryption_key,
							   (unsigned char *) tweak) != 1)
			evp_error();

		/* The same considerations apply below as those in encrypt_block(). */
		EVP_CIPHER_CTX_set_padding(ctx, 0);
		Assert(EVP_CIPHER_CTX_block_size(ctx) == ENCRYPTION_BLOCK_OPENSSL);
		Assert(EVP_CIPHER_CTX_iv_length(ctx) == TWEAK_SIZE);
		Assert(EVP_CIPHER_CTX_key_length(ctx) == ENCRYPTION_KEY_LENGTH);

		if (EVP_DecryptUpdate(ctx, (unsigned char *) output, &out_size,
							  (unsigned char *) input, size) != 1)
			evp_error();

		Assert(out_size == size);

		if (EVP_CIPHER_CTX_cleanup(ctx) != 1)
			evp_error();
	}
#else
	ENCRYPTION_NOT_SUPPORTED_MSG;
#endif							/* USE_ENCRYPTION */
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
#ifdef USE_ENCRYPTION
	char	   *credentials;
	bool		is_key;
	size_t		len;

	credentials = run_encryption_key_command(&is_key, &len);

	/*
	 * Setup KDF if we need to derive the key from a password.
	 *
	 * Front-ends always need the key because some of them are not aware of
	 * the data directory and thus they'd need one more command line option to
	 * find the key setup file.
	 */
#ifndef FRONTEND
	if (!is_key)
	{
		if (bootstrap)
		{
			write_kdf_file();
		}
		else
			read_kdf_file();
	}
#endif							/* FRONTEND */

	setup_encryption_key(credentials, is_key, len);
	pfree(credentials);
	setup_encryption_internal();
#else
	ENCRYPTION_NOT_SUPPORTED_MSG;
#endif							/* USE_ENCRYPTION */
}

#ifdef USE_ENCRYPTION
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

	/*
	 * It makes no sense to initialize the encryption multiple times.
	 */
	Assert(!initialized);

	initialized = true;
}
#endif							/* USE_ENCRYPTION */

/*
 * If credentials is a key, just copy it to encryption_key. If it's a
 * password, derive the key from it.
 */
void
setup_encryption_key(char *credentials, bool is_key, size_t len)
{
#ifdef USE_ENCRYPTION
	Assert(credentials != NULL);
	Assert(encryption_key == NULL);

	/*
	 * Although the function should be called in the TopMemoryContext, we
	 * should be pretty sure that the key does not become garbage due to
	 * pfree(). The cluster should crash in such a case, but if it did not
	 * happen immediately, some data could be encrypted using an invalid key
	 * and therefore become lost.
	 */
#ifndef FRONTEND
	encryption_key = (unsigned char *) MemoryContextAllocZero(TopMemoryContext,
															  ENCRYPTION_KEY_LENGTH);
#else
	encryption_key = (unsigned char *) palloc(ENCRYPTION_KEY_LENGTH);
#endif							/* FRONTEND */
	if (is_key)
	{
		Assert(len = ENCRYPTION_KEY_LENGTH);
		memcpy(encryption_key, credentials, len);
	}
	else
	{
		KDFParamsPBKDF2 *params;
		int			rc;

		/*
		 * The file contains password so we need the KDF parameters to turn it
		 * to key.
		 */
		if (KDFParams == NULL)
		{
#ifndef FRONTEND
			ereport(FATAL,
					(errmsg("this instance does not accept encryption password"),
					 errdetail("Encryption key was probably used to initialize the instance.")));
#else
			encryption_error(true,
							 "this instance does not accept encryption password.\n"
							 "Encryption key was probably used to initialize the instance.\n");
#endif							/* FRONTEND */
		}

		/*
		 * Turn the password into the encryption key.
		 */
		params = &KDFParams->data.pbkdf2;
		rc = PKCS5_PBKDF2_HMAC(credentials,
							   len,
							   params->salt,
							   ENCRYPTION_KDF_SALT_LEN,
							   params->niter,
							   EVP_sha(),
							   ENCRYPTION_KEY_LENGTH,
							   encryption_key);

		if (rc != 1)
		{
#ifndef FRONTEND
			ereport(FATAL,
					(errmsg("failed to derive key from password")));
#else
			encryption_error(true, "failed to derive key from password");
#endif							/* FRONTEND */
		}
	}

#else							/* USE_ENCRYPTION */
	ENCRYPTION_NOT_SUPPORTED_MSG;
#endif							/* USE_ENCRYPTION */
}

#ifdef USE_ENCRYPTION
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
 * Run the command stored in encryption_key_command and return the key or
 * password.
 *
 * *is_key_p receives true if the command returns key, false if it's
 * password. *len_p receives length of the data.
 */
static char *
run_encryption_key_command(bool *is_key_p, size_t *len_p)
{
	FILE	   *fp;
	char	   *buf,
			   *result;
	bool		is_key = false;
	size_t		key_pref_len,
				pwd_pref_len,
				read_len,
				bytes_read;
	size_t		buf_size,
				result_size;
	int			i;

	if (encryption_key_command == NULL ||
		!strlen(encryption_key_command))
	{
		/*
		 * encryption_key_command should have been set by initdb. It's weird
		 * if it was not, but there's no better recommendation we can give the
		 * user.
		 */
#ifndef FRONTEND
		ereport(FATAL,
				(errmsg("encryption key not provided"),
				 errdetail("The database cluster was initialized with encryption"
						   " but the server was started without an encryption key."),
				 errhint("Set the encryption_key_command configuration variable.")));
#else							/* FRONTEND */
		encryption_error(true,
						 "The database cluster was initialized with encryption"
						 " but the server was started without an encryption key. "
						 "Set the encryption_key_command configuration variable.\n");
#endif							/* FRONTEND */
	}

	encryption_error(false,
					 psprintf("Executing \"%s\" to set up encryption key",
							  encryption_key_command));

	fp = popen(encryption_key_command, "r");

	if (fp == NULL)
		encryption_error(true,
						 psprintf("Failed to execute encryption_key_command \"%s\"",
								  encryption_key_command));

	/*
	 * Check which prefix the file starts with.
	 *
	 * The prefixes probably won't change after the release but they might
	 * change during development. The reading logic should be generic so that
	 * change of prefix length requires no additional coding.
	 */
	key_pref_len = strlen(encryption_key_prefix);
	pwd_pref_len = strlen(encryption_pwd_prefix);

	/*
	 * The buffer must accommodate either prefix.
	 */
	buf_size = Max(key_pref_len, pwd_pref_len);
	buf = (char *) palloc(buf_size);

	/*
	 * Read as few bytes as necessary so that we don't have to move back in
	 * the buffer if the first comparison does not match.
	 */
	read_len = Min(key_pref_len, pwd_pref_len);

	if (fread(buf, 1, read_len, fp) != read_len)
		encryption_error(true, "Not enough data received from encryption_key_command");

	if (read_len == key_pref_len &&
		strncmp(buf, encryption_key_prefix, key_pref_len) == 0)
		is_key = true;
	else if (read_len == pwd_pref_len &&
			 strncmp(buf, encryption_pwd_prefix, pwd_pref_len) == 0)
		is_key = false;
	else if (buf_size > read_len)
	{
		size_t		len_diff;

		/*
		 * Read enough data so that one of the prefixes must match.
		 */
		len_diff = buf_size - read_len;
		if (fread(buf + read_len, 1, len_diff, fp) != len_diff)
			encryption_error(true,
							 "Not enough data received from encryption_key_command2");
		read_len += len_diff;

		/*
		 * Try to match the prefixes again.
		 */
		if (read_len == key_pref_len &&
			strncmp(buf, encryption_key_prefix, key_pref_len) == 0)
			is_key = true;
		else if (read_len == pwd_pref_len &&
				 strncmp(buf, encryption_pwd_prefix, pwd_pref_len) == 0)
			is_key = false;
		else
			encryption_error(true,
							 "Unknown data received from encryption_key_command");
	}

	*is_key_p = is_key;

	/*
	 * Read the actual credentials.
	 */
	read_len = is_key ? ENCRYPTION_KEY_LENGTH * 2 : ENCRYPTION_PWD_MAX_LENGTH;

	/*
	 * One extra character can be read and this should be nothing but line
	 * delimiter, see below.
	 */
	read_len++;

	if (read_len > buf_size)
	{
		buf = (char *) repalloc(buf, read_len);
		buf_size = read_len;
	}

	bytes_read = fread(buf, 1, read_len, fp);

	if ((is_key && bytes_read < read_len) ||
		(!is_key && bytes_read < (ENCRYPTION_PWD_MIN_LENGTH + 1)))
	{
		if (feof(fp))
			encryption_error(true,
							 "Not enough data provided by encryption_key_command");
		else
			encryption_error(true,
							 psprintf("encryption_key_command returned error code %d",
									  ferror(fp)));
	}
	Assert(bytes_read > 0);

	if (buf[bytes_read - 1] != '\n')
		encryption_error(true, "Encryption key is too long.");

	/*
	 * The credentials must not contain line delimiter.
	 *
	 * If that was allowed, the line delimiter would have to terminate
	 * password even if more characters followed, however those extra
	 * characters would be useless. Since distinguishing of the special (still
	 * legal) case "password\n<EOF>" would make the checks less simple and
	 * since the delimiter itself is useless in the case of a key (because the
	 * key has constant length), let's prohibit the line delimiters anywhere
	 * except for the last position.
	 */
	for (i = 0; i < bytes_read; i++)
		if (buf[i] == '\n' && i < (bytes_read - 1))
			encryption_error(true,
							 "Neither password nor key may contain line break.");

	/*
	 * For a key the result size is different from the amount of data read.
	 */
	result_size = is_key ? ENCRYPTION_KEY_LENGTH : ENCRYPTION_PWD_MAX_LENGTH;

	result = (char *) palloc(result_size);

	if (is_key)
	{
		int			i;

		for (i = 0; i < ENCRYPTION_KEY_LENGTH; i++)
		{
			if (sscanf(buf + 2 * i, "%2hhx", result + i) == 0)
				encryption_error(true,
								 psprintf("Invalid character in encryption key at position %d",
										  2 * i));
		}
		*len_p = ENCRYPTION_KEY_LENGTH;
	}
	else
	{
		/*
		 * Ignore the line delimiter.
		 */
		*len_p = bytes_read - 1;
		memcpy(result, buf, *len_p);

	}

	/*
	 * No extra data is allowed.
	 */
	if (fread(buf, 1, 1, fp) > 0)
		encryption_error(true,
						 "Credentials are followed by useless data");


	pfree(buf);
	pclose(fp);

	return result;
}
#endif							/* USE_ENCRYPTION */

/*
 * Error callback for openssl.
 */
#ifdef USE_ENCRYPTION
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
#endif							/* USE_ENCRYPTION */

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
#ifdef USE_ENCRYPTION
	memset(tweak, 0, TWEAK_SIZE);
	memcpy(tweak, &segment, sizeof(XLogSegNo));
	memcpy(tweak + sizeof(XLogSegNo), &offset, sizeof(offset));
#else
	ENCRYPTION_NOT_SUPPORTED_MSG;
#endif							/* USE_ENCRYPTION */
}
