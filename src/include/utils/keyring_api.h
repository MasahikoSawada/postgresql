/*-------------------------------------------------------------------------
 *
 * keyring_api.h
 *	  API for key ring plugins
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/utils/keyring_api.h
 *-------------------------------------------------------------------------
 */

#ifndef KEYRING_API_H
#define KEYRING_API_H

struct KeyringPluginCallbacks;

typedef void (*KeyringPluginInit) (struct KeyringPluginCallbacks *cb);
typedef void (*StartupKey_function) (void);
typedef bool (*GetKey_function) (char *keyid, Oid userid,
								 char **key, int *keylen);
typedef bool (*GenerateKey_function) (char *keyid, Oid userid);
typedef bool (*RemoveKey_function) (char *keyid, Oid userid);

/*
 * Key management plugin callbacks
 */
typedef struct KeyringPluginCallbacks
{
	StartupKey_function startup_cb;
	GetKey_function getkey_cb;
	GenerateKey_function generatekey_cb;
	RemoveKey_function removekey_cb;
} KeyringPluginCallbacks;

#endif /* KEYRING_API_H */
