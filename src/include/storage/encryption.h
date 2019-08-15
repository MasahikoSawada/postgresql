/*-------------------------------------------------------------------------
 *
 * encryption.h
 *	  Cluster encryption functions.
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/encryption.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENCRYPTION_H
#define ENCRYPTION_H

#include "access/xlogdefs.h"
#include "storage/bufpage.h"
#include "storage/enc_cipher.h"
#include "storage/enc_common.h"

#define DataEncryptionEnabled() \
	(data_encryption_cipher > TDE_ENCRYPTION_OFF)

/*
 * The encrypted data is a series of blocks of size ENCRYPTION_BLOCK.
 * Initialization vector(IV) is the same size of cipher block.
 */
#define ENC_BLOCK_SIZE 16
#define ENC_IV_SIZE		(ENC_BLOCK_SIZE)

/*
 * Maximum encryption key size is used by AES-256.
 */
#define ENC_MAX_ENCRYPTION_KEY_SIZE	32

/* bufenc.c */
extern void DecryptBufferBlock(BlockNumber blocknum, Page page);
extern void EncryptBufferBlock(BlockNumber blocknum, Page page);

#endif							/* ENCRYPTION_H */
