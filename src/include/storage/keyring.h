/*-------------------------------------------------------------------------
 *
 * keyring.h
 *	  Keyring support
 *
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/keyring.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KEYRING_H
#define KEYRING_H

#include "postgres.h"

#include "storage/encryption.h"

#define KEYRING_MASTER_KEY_ID_SIZE NAMEDATALEN

typedef struct TblspKeyData
{
	Oid		spcOid;
	char	masterKeyID[KEYRING_MASTER_KEY_ID_SIZE];
	char	plainKey[ENCRYPTION_KEY_SIZE];
} TblspKeyData;

void KeyringSetup(void);

/* ------------------------------------
 * exporting function for testing.
 * ------------------------------------
 */
extern TblspKeyData *test_generateTblspKey(Oid oid);
extern void test_dumpLocalTblspKeyring(void);
extern void test_updateTblspKeyringFile(void);

#endif /* KEYRING_H */
