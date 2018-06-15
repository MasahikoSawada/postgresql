/*-------------------------------------------------------------------------
 *
 * encryption.c
 *  This code handles encryption and decryption of data.
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *  src/backend/storage/encryption/encryption.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "storage/encryption.h"
#include "utils/memutils.h"

static void encrypt_block(const char *input, char *output, Size size, char *key);
static void decrypt_block(const char *input, char *output, Size size, char *key);
/*
 * Encrypt one block data.
 */
char *
EncryptOneBuffer(char *raw_buf, char *key)
{
	static char *bufCopy = NULL;

	if (bufCopy == NULL)
		bufCopy = MemoryContextAlloc(TopMemoryContext, BLCKSZ);

	encrypt_block(raw_buf, bufCopy, BLCKSZ, key);
//	fprintf(stderr, "%s\n", bufCopy);
	return bufCopy;
}

/*
 * Decrypt one block data.
 */
char *
DecryptOneBuffer(char *encrypted_buf, char *key)
{
	static char *bufCopy = NULL;

	if (bufCopy == NULL)
		bufCopy = MemoryContextAlloc(TopMemoryContext, BLCKSZ);

	decrypt_block(encrypted_buf, encrypted_buf, BLCKSZ, key);
	return encrypted_buf;
//	decrypt_block(encrypted_buf, bufCopy, BLCKSZ, key);
//	return bufCopy;
}
/*
 * Encrypt data.
 */
static void
encrypt_block(const char *input, char *output, Size size, char *key)
{
	int i;

	for (i = 0; i < size; i++)
		output[i] = input[i] + 1;
}

/*
 * Decrypts data.
 */
static void
decrypt_block(const char *input, char *output, Size size, char *key)
{
	int i;

	for (i = 0; i < size; i++)
		output[i] = input[i] - 1;
}
