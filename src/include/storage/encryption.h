/*-------------------------------------------------------------------------
 *
 * encryption.h
 *  Full database encryption support
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/encryption.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENCRYPTION_H
#define ENCRYPTION_H

#define MAX_ENCRYPTION_KEY_LEN 256

typedef struct MasterKey
{
	/* The master key identifier */
	NameData	name;
	Oid			dbid;
	bool		is_dirty;
	char		masterkey[MAX_ENCRYPTION_KEY_LEN];
} MasterKey;

typedef struct MasterKeyCtlData
{
	int			num_used;
	MasterKey	masterkeys[FLEXIBLE_ARRAY_MEMBER];
} MasterKeyCtlData;

MasterKeyCtlData *MasterKeyCtl;

extern int max_master_keys;

Size MasterKeyShmemSize(void);
void MasterKeyShmemInit(void);
void CheckPointMasterKeys(void);

char *EncryptOneBuffer(char *raw_buf, char *key);
char *DecryptOneBuffer(char *encrypted_buf, char *key);

#endif	/* ENCRYPTION_H */
