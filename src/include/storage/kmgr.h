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

#define TransparentEncryptionEnabled() \
	(kmgr_plugin_library != NULL && kmgr_plugin_library[0] != '\0')

/*
 * The master key format is "pg_master_key-<database systemid>-<seqno>".
 * The maximum length of database system identifer is
 * 20 (=18446744073709551615) as it is an uint64 value and the maximum
 * string length of seqno is 10 (=4294967295).
 */
#define MASTER_KEY_ID_FORMAT "pg_master_key-%.7s-%04u"
#define MASTER_KEY_ID_FORMAT_SCAN "pg_master_key-%u-%u"
#define MASTER_KEY_ID_LEN (16 + 7 + 4 + 1)

/* The initial master key sequence number */
#define FIRST_MASTER_KEY_SEQNO	0

/*
 * masterKeySeqno is the sequence number starting from FIRST_MASTER_KEY_SEQNO
 *  and get incremented every time key rotation.
 */
typedef uint32 MasterKeySeqNo;

/* GUC variable */
extern char *kmgr_plugin_library;

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
