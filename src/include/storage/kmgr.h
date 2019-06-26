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
#define MASTER_KEY_ID_FORMAT "pg_master_key-%07lu-%04u"
#define MASTER_KEY_ID_FORMAT_SCAN "pg_master_key-%lu-%s"

#define MASTER_KEY_ID_LEN (15 + 7 + 4)

#define DEBUG_TDE 1

/*
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
extern void KeyringAddKey(Oid spcOid, char *encrypted_key,
						  const char *masterkeyid);

/* kmgr.c */
extern void processKmgrPlugin(void);
extern void InitializeKmgr(void);

/* tempkey.c */
extern char * GetBackendKey(void);

#ifdef DEBUG_TDE
extern char* ddp(const char *p);
extern char* dt(const char *p);
extern char* dp(const char *p);
extern char* dk(const char *p);
#endif

#endif /* KMGR_H */
