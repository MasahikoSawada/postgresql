/*-------------------------------------------------------------------------
 *
 * xloglevelworker.h
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/access/xloglevelworker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XLOGLEVELWORKER_H
#define XLOGLEVELWORKER_H

extern void WalLevelCtlWorkerLaunchIfNecessary(void);
extern void WalLevelCtlWorkerMain(Datum main_arg);

#endif
