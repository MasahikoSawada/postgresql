/*-------------------------------------------------------------------------
 *
 * kmgrapi.h
 *	  API for key management
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/utils/kmgrapi.h
 *-------------------------------------------------------------------------
 */

#ifndef KMGRAPI_H
#define KMGRAPI_H

struct KeyMgrPluginCallbacks;

typedef void (*KeyMgrPluginInit) (struct KeyMgrPluginCallbacks *cb);
typedef void (*StartupKey_function) (void);
typedef bool (*GetKey_function) (char *keyid, Oid userid,
								 char **key, int *keylen);
typedef bool (*GenerateKey_function) (char *keyid, Oid userid);
typedef bool (*RemoveKey_function) (char *keyid, Oid userid);

/*
 * Key management plugin callbacks
 */
typedef struct KeyMgrPluginCallbacks
{
	StartupKey_function startup_cb;
	GetKey_function getkey_cb;
	GenerateKey_function generatekey_cb;
	RemoveKey_function removekey_cb;
} KeyMgrPluginCallbacks;

#endif /* KMGRAPI_H */
