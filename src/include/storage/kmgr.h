/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  Full database encryption support
 *
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

/* Key encryption key */
#define TDE_KEK_SIZE				32

/* HMAC key size is the same as the length of hash, we use SHA-256 */
#define TDE_KEK_HMAC_KEY_SIZE		32

/* Using SHA-256 results 256 bits HMAC */
#define TDE_KEK_HMAC_SIZE			32

#define TDE_KEK_DERIVED_KEY_SIZE	(TDE_KEK_SIZE + TDE_KEK_HMAC_KEY_SIZE)

/* NIST recommends that minimum iteratin count is 1000 */
#define TDE_KEK_DEVIRATION_ITER_COUNT	10000

/* NIST recommended that salt size is at least 16 */
#define TDE_KEK_DEVIRATION_SALT_SIZE	64

/* Definition of key wrap */
#define TDE_KEY_WRAP_VALUE_SIZE		8

/* Master data encryption key */
#define TDE_MDEK_SIZE				32
#define TDE_MDEK_WRAPPED_SIZE		(TDE_MDEK_SIZE + TDE_KEY_WRAP_VALUE_SIZE)

#define TDE_MAX_PASSPHRASE_LEN		1024


/* GUC variable */
extern char *data_encryption_key_passphrase_command;

extern void BootStrapKmgr(void);
extern Size KmgrShmemSize(void);
extern void KmgrShmemInit(void);
extern void InitializeKmgr(void);
extern void KeyringSetup(void);
extern void SetupKmgr(void);
extern void GetRelEncKey(RelFileNode rnode, char *key, bool create);
extern void DropRelEncKey(RelFileNode rnode);


extern void dp (const char *m, unsigned char *d, int l);

#endif /* KMGR_H */
