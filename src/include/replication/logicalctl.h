/*-------------------------------------------------------------------------
 *
 * logicalctl.h
 *		Definitions for logical decoding status control facility.
 *
 * Portions Copyright (c) 2013-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/replication/logicalctl.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XLOGLEVEL_H
#define XLOGLEVEL_H

#include "access/xlog.h"
#include "port/atomics.h"
#include "storage/condition_variable.h"

typedef struct LogicalDecodingCtlData
{
	/* True while the logical decoding status is being changed */
	bool		transition_in_progress;

	/* Condition variable signaled when a transition completes */
	ConditionVariable transition_cv;

	/*
	 * xlog_logical_info is the authoritative value used by the all process to
	 * determine whether to write additional information required by logical
	 * decoding to WAL. Since this information could be checked frequently,
	 * each process caches this value in XLogLogicalInfo for better
	 * performance.
	 *
	 * logical_decoding_enabled is true if we allow creating logical slots and
	 * the logical decoding is enabled.
	 *
	 * Both fields are initialized at server startup time. On standbys, these
	 * values are synchronized to the primary's values when replaying the
	 * XLOG_LOGICAL_DECODING_STATUS_CHANGE record.
	 */
	pg_atomic_flag xlog_logical_info;
	pg_atomic_flag logical_decoding_enabled;
}			LogicalDecodingCtlData;
extern LogicalDecodingCtlData * LogicalDecodingCtl;

extern Size LogicalDecodingCtlShmemSize(void);
extern void LogicalDecodingCtlShmemInit(void);
extern void StartupLogicalDecodingStatus(bool status_in_control_file);
extern void InitializeProcessXLogLogicalInfo(void);
extern bool ProcessBarrierUpdateXLogLogicalInfo(void);
extern bool IsLogicalDecodingEnabled(void);
extern bool IsXLogLogicalInfoEnabled(void);
extern void EnsureLogicalDecodingEnabled(void);
extern void DisableLogicalDecodingIfNecessary(void);
extern void UpdateLogicalDecodingStatus(bool new_status);
extern void UpdateLogicalDecodingStatusEndOfRecovery(void);

#endif
