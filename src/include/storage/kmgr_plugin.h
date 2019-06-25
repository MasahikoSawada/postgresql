/*-------------------------------------------------------------------------
 *
 * kmgr_plugin.h
 *    Key management plugin manager
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/include/storage/kmgr_plugin.h
 *-------------------------------------------------------------------------
 */

#ifndef KMGR_PLUGIN_H
#define KMGR_PLUGIN_H

#include "storage/kmgr_api.h"

extern void startupKmgrPlugin(const char *library_name);
extern void KmgrPluginGetKey(const char *id, char **key);
extern void KmgrPluginGenerateKey(const char *id);
extern void KmgrPluginRemoveKey(const char *id);
extern bool KmgrPluginIsExist(const char *id);
extern void KmgrPluginStartup(void);

#endif /* KMGR_PLUGIN_H */
