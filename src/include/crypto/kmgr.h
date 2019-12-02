/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  Key management module for transparent data encryption
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "storage/relfilenode.h"
#include "storage/bufpage.h"

/* Size of HMAC key is the same as the length of hash, we use SHA-256 */
#define TDE_HMAC_KEY_SIZE		32

/* SHA-256 results 256 bits HMAC */
#define TDE_HMAC_SIZE			32

/* Size of key encryption key (KEK), which is always AES-256 key */
#define TDE_KEK_SIZE			32

/*
 * Max size of data encryption key. We support AES-128 and AES-256, the
 * maximum key size is 32.
 */
#define TDE_MAX_DEK_SIZE			32

/* Key wrapping appends the initial 8 bytes value */
#define TDE_DEK_WRAP_VALUE_SIZE		8

/* Wrapped key size is n+1 value */
#define TDE_MAX_WRAPPED_DEK_SIZE	(TDE_MAX_DEK_SIZE + TDE_DEK_WRAP_VALUE_SIZE)

#define TDE_MAX_PASSPHRASE_LEN		1024

typedef unsigned char keydata_t;

/*
 * Struct for keys that needs to be verified using its HMAC.
 */
typedef struct WrappedEncKeyWithHmac
{
	keydata_t key[TDE_MAX_WRAPPED_DEK_SIZE];
	keydata_t hmac[TDE_HMAC_SIZE];
} WrappedEncKeyWithHmac;

/* Struct for bootstrap information passing to the bootstrap routine */
typedef struct KmgrBootstrapInfo
{
	WrappedEncKeyWithHmac relEncKey;
	WrappedEncKeyWithHmac walEncKey;
} KmgrBootstrapInfo;

/* GUC variable */
extern char *cluster_passphrase_command;

extern KmgrBootstrapInfo *BootStrapKmgr(int bootstrap_data_encryption_cipher);

void kmgr_init(void);
extern const char *KmgrGetRelationEncryptionKey(void);
extern const char *KmgrGetWALEncryptionKey(void);


extern int kms_run_passphrase_command(char *buf);
extern void kms_get_kek_and_hmackey(char *passphrase, Size passlen, keydata_t kek[TDE_KEK_SIZE], keydata_t hmackey[TDE_HMAC_KEY_SIZE]);
extern bool kms_verify_passphrase(char *passphrase, int passlen, WrappedEncKeyWithHmac *rdek);

extern void kms_wrap_key(const unsigned char *key, int key_size, unsigned char *in, int in_size, unsigned char *out, int *out_size);
extern void kms_unwrap_key(const unsigned char *key, int key_size, unsigned char *in, int in_size, unsigned char *out, int *out_size);
extern void kms_compute_hmac(const unsigned char *hmac_key, int key_size, unsigned char *data, int data_size, unsigned char *hmac);

#endif /* KMGR_H */
