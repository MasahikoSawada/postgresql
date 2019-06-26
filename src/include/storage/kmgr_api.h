/*-------------------------------------------------------------------------
 *
 * kmgr_api.h
 *    APIs for kmgr plugin
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/include/storage/kmgr_api.h
 *-------------------------------------------------------------------------
 */

#ifndef KMGR_API_H
#define KMGR_API_H

struct KmgrPluginCallbacks;

typedef void (*KmgrPluginInit) (struct KmgrPluginCallbacks *cb);
typedef void (*Startup_function) (void);
typedef char *(*GetKey_function) (const char *keyid);
typedef void (*GenerateKey_function) (const char *keyid);
typedef bool (*IsExistKey_function) (const char *keyid);
typedef void (*RemoveKey_function) (const char *keyid);

/*
 * Key management plugin callbacks
 */
typedef struct KmgrPluginCallbacks
{
    Startup_function startup_cb;
    GetKey_function getkey_cb;
    GenerateKey_function generatekey_cb;
	IsExistKey_function isexistkey_cb;
    RemoveKey_function removekey_cb;
} KmgrPluginCallbacks;

#endif /* KMGR_API_H */
