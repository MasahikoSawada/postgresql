/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  PostgreSQL symmetric key manager
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/utils/kmgr.h
 *-------------------------------------------------------------------------
 */

#ifndef KMGR_H
#define KMGR_H

#define MAX_KEY_LEN 128
#define MAX_KEY_ID_LEN NAMEDATALEN

typedef uint32 KeyGeneration;

/* GUC parameter */
extern char *key_management_provider;

/* For TDE */
extern KeyGeneration TDEGetCurrentKeyGeneration(void);

/* Interface */
extern void KeyMgrInit(void);
extern void AtEOXact_KMgr(bool is_commit);
extern bool GenerateKey(char *keyid, char *keytype);
extern bool GetKey(char *keyid, KeyGeneration generateion, char **key, int *keylen);
extern KeyGeneration RotateKey(char *keyid);
extern bool RemoveKey(char *keyid, KeyGeneration generation);

#endif	/* KMGR_H */
