/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  Key management
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "postgres.h"

#include "storage/encryption.h"

#define TransparentEncryptionEnabled() \
	(kmgr_plugin_library != NULL && kmgr_plugin_library[0] != '\0')

#define MAX_MASTER_KEY_ID_LEN NAMEDATALEN

/*
 *
 * masterKeySeqno is the sequence number starting from 0 and get incremented
 * every time key rotation.
 */
typedef uint32 MasterKeySeqNo;
extern char *kmgr_plugin_library;

/* keyring.c */
extern void KeyringSetup(void);
extern char *KeyringCreateKey(Oid tablespaceoid);
extern char *KeyringGetKey(Oid spcOid);
extern void KeyringDropKey(Oid tablespaceoid);
extern bool KeyringKeyExists(Oid spcOid);
extern void reencryptKeyring(char *key);

/* masterkey.c */
extern void processKmgrPlugin(void);
extern void InitializeMasterKey(void);
extern Size MasterKeyCtlShmemSize(void);
extern void MasterKeyCtlShmemInit(void);
extern void SetMasterKeySeqNo(MasterKeySeqNo seqno);
extern MasterKeySeqNo GetMasterKeySeqNo(void);
extern char *GetMasterKey(char *id);
extern void GetCurrentMasterKeyId(char *keyid);
extern void RotateMasterKey(char *keyid_new);

#endif /* KMGR_H */
