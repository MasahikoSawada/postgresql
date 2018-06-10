/*-------------------------------------------------------------------------
 *
 * fdwxact_launcher.h
 *	  PostgreSQL foreign transaction launcher definitions
 *
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact_launcher.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FDWXACT_LAUNCHER_H
#define FDWXACT_LAUNCHER_H

#include "access/fdwxact.h"

extern void FdwXactLauncherRegister(void);
extern void FdwXactLauncherMain(Datum main_arg);
extern void FdwXactLauncherWakeupToRequest(void);
extern void FdwXactLauncherWakeupToRetry(void);

extern Size FdwXactRslvShmemSize(void);
extern void FdwXactRslvShmemInit(void);

extern bool IsFdwXactLauncher(void);

extern void fdwxact_maybe_launch_resolver(bool ignore_error);


#endif	/* FDWXACT_LAUNCHER_H */
