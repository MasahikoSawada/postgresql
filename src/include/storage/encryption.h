/*-------------------------------------------------------------------------
 *
 * encryption.huffer
 *	  Full database encryption support
 *
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
#include "port/pg_crc32c.h"
#include "storage/smgr.h"

/*
 * The encrypted data is a series of blocks of size
 * ENCRYPTION_BLOCK. Currently we use the EVP_aes_256_xts implementation. Make
 * sure the following constants match if adopting another algorithm.
 */
#define ENCRYPTION_BLOCK_SIZE 16
#define ENCRYPTION_IV_SIZE		ENCRYPTION_BLOCK_SIZE

/*
 * The openssl EVP API refers to a block in terms of padding of the output
 * chunk. That's the purpose of this constant. However the openssl
 * implementation of AES XTS still uses the 16-byte block internally, as
 * defined by ENCRYPTION_BLOCK.
 */
#define ENCRYPTION_BLOCK_OPENSSL 1

/*
 * If one XLOG record ended and the following one started in the same block,
 * we'd have to either encrypt and decrypt both records together, or encrypt
 * (after having zeroed the part of the block occupied by the other record)
 * and decrypt them separate. Neither approach is compatible with streaming
 * replication. In the first case we can't ask standby not to decrypt the
 * first record until the second has been streamed. The second approach would
 * imply streaming of two different versions of the same block two times.
 *
 * We avoid this problem by aligning XLOG records to the encryption block
 * size. This way no adjacent XLOG records should appear in the same block.
 *
 * For similar reasons, the alignment to ENCRYPTION_BLOCK also has to be
 * applied when storing changes to disk in reorderbuffer.c. Another module
 * that takes the block into account is buffile.c.
 *
 * TODO If the configuration allows walsender to decrypt the XLOG stream
 * before sending it, adjust this expression so that the additional padding is
 * not added to XLOG records. (Since the XLOG alignment cannot change without
 * initdb, the same would apply to the configuration variable that makes
 * walsender perform the decryption. Does such a variable make sense?)
 */
#define DO_ENCRYPTION_BLOCK_ALIGN	data_encrypted

/*
 * Use TYPEALIGN64 since besides record size we also need to align XLogRecPtr.
 */
#define ENCRYPTION_BLOCK_ALIGN(LEN)		TYPEALIGN64(ENCRYPTION_BLOCK, (LEN))

/*
 * Universal computation of XLOG record alignment.
 */
#define XLOG_REC_ALIGN(LEN) MAXALIGN(LEN)

/*
 * Maximum encryption key size is used by AES-256.
 */
#define MAX_ENCRYPTION_KEY_SIZE	32
#define ENCRYPTION_KEY_SALT_LEN	16

#define ENCRYPTION_WAL_COUNTER_LEN 32
#define ENCRYPTION_BUFFER_COUNTER_LEN 32

enum database_encryption_cipher_kind
{
	ENCRYPTION_AES_128 = 0,
	ENCRYPTION_AES_256
};

/* GUC parameter */
extern int database_encryption_cipher;

extern int EncryptionKeyLen;
extern PGAlignedBlock encrypt_buf;

extern void EncryptionGetIVForWAL(char *iv, XLogSegNo segment, uint32 offset);
extern void EncryptionGetIVForBuffer(char *iv, XLogRecPtr pagelsn,
										 BlockNumber blocknum);

#endif							/* ENCRYPTION_H */
