/*-------------------------------------------------------------------------
 *
 * kmgr_utils.h
 *		Declarations for utility function for cryptographic key management
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/kmgr_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_UTILS_H
#define KMGR_UTILS_H

#include "common/cipher.h"

/* Key encryption key is always AES-256 key */
#define KMGR_KEK_SIZE		AES256_KEY_SIZE

/* Data encryption key supports AES-128 and AES-256 */
#define KMGR_MAX_DEK_SIZE	AES256_KEY_SIZE

/* Value of data_encryption_cipher */
enum
{
	KMGR_ENCRYPTION_OFF = 0,
	KMGR_ENCRYPTION_AES128,
	KMGR_ENCRYPTION_AES256
};

/*
 * Struct for keys that needs to be verified using its HMAC.
 */
typedef struct WrappedEncKeyWithHmac
{
	uint8 key[AES256_MAX_WRAPPED_KEY_SIZE];
	uint8 hmac[AES256_HMAC_SIZE];
} WrappedEncKeyWithHmac;

extern void kmgr_derive_keys(char *passphrase, Size passlen,
							 uint8 kek[KMGR_KEK_SIZE],
							 uint8 hmackey[AES256_HMAC_KEY_SIZE]);
extern bool kmgr_verify_passphrase(char *passphrase, int passlen,
								   WrappedEncKeyWithHmac **keys,
								   int nkeys, int keysize);
extern bool kmgr_wrap_key(uint8 *key, const uint8 *in, int insize,
						  uint8 *out);
extern bool kmgr_unwrap_key(uint8 *key, const uint8 *in, int insize,
							uint8 *out);
extern bool kmgr_compute_HMAC(uint8 *key, const uint8 *data, int size,
							  uint8 *result);
extern int kmgr_cipher_value(const char *name);
extern char * kmgr_cipher_string(int value);

#endif /* KMGR_UTILS_H */
