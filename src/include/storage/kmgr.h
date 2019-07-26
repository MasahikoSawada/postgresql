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
#include "storage/bufpage.h"

#define KMGR_KEK_INTERATION_COUNT	1000000
#define KMGR_WDEK_ID				InvalidOid

/*
 * masterKeySeqno is the sequence number starting from FIRST_MASTER_KEY_SEQNO
 *  and get incremented every time key rotation.
 */
typedef uint32 MasterKeySeqNo;

/* GUC variable */
extern char *database_encryption_key_passphrase_command;

/* kmgr.c */
extern Size KmgrShmemSize(void);
extern void KmgrShmemInit(void);
extern void processKmgrPlugin(void);
extern void InitializeKmgr(void);
extern void KeyringSetup(void);
extern char *KeyringCreateKey(Oid tablespaceoid);
extern void KeyringGetKey(Oid spcOid, char *key);
extern void KeyringDropKey(Oid tablespaceoid);
extern bool KeyringKeyExists(Oid spcOid);
extern void KeyringAddKey(Oid spcOid, char *encrypted_key,
						  const char *masterkeyid);

/* tempkey.c */
extern char * GetBackendKey(void);

#endif /* KMGR_H */
