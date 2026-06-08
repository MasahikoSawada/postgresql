/*-------------------------------------------------------------------------
 *
 * logicalworker.h
 *	  Exports for logical replication workers.
 *
 * Portions Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalworker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALWORKER_H
#define LOGICALWORKER_H

#include <signal.h>

extern PGDLLIMPORT volatile sig_atomic_t ParallelApplyMessagePending;

/* avoid include logicalproto.h */
struct LogicalRepMessageData;

/*
 * Hook for extensions to handle logical decoding messages (see
 * pg_logical_emit_message()) received from the publisher. Called only when
 * the subscription has the "message" option enabled.
 *
 * The handler runs in the apply worker, inside a transaction, with an active
 * snapshot pushed. For a transactional message the handler's work is part of
 * the remote transaction and commits with it; a non-transactional message is
 * committed as an independent unit. The same message may be delivered more
 * than once, so the handler must be idempotent.
 *
 * The payload is an arbitrary string of bytes. The WAL record makes no
 * distinction between the text and the bytea form of
 * pg_logical_emit_message(), and the bytes are transmitted without character
 * set conversion, so the payload may contain embedded nulls and bytes that
 * are not valid in the subscriber's encoding. Its length is given by
 * msg->message_size, not by strlen(); the buffer is null-terminated only for
 * convenience, and that null is not part of the payload.
 *
 * Note that the apply worker runs with an empty search_path. An ERROR thrown
 * by the handler restarts the apply worker, or disables the subscription if
 * disable_on_error is set. Because the message is delivered again after the
 * restart, a handler cannot reject a message by throwing an error; that only
 * loops. A message the handler does not want must simply be ignored.
 */
typedef void (*LogicalRepMessageHandle_hook_type) (const struct LogicalRepMessageData *msg);
extern PGDLLIMPORT LogicalRepMessageHandle_hook_type LogicalRepMessageHandle_hook;

extern void ApplyWorkerMain(Datum main_arg);
extern void ParallelApplyWorkerMain(Datum main_arg);
extern void TableSyncWorkerMain(Datum main_arg);
extern void SequenceSyncWorkerMain(Datum main_arg);

extern bool IsLogicalWorker(void);
extern bool IsLogicalParallelApplyWorker(void);

extern void HandleParallelApplyMessageInterrupt(void);
extern void ProcessParallelApplyMessages(void);

extern void LogicalRepWorkersWakeupAtCommit(Oid subid);

extern void AtEOXact_LogicalRepWorkers(bool isCommit);

#endif							/* LOGICALWORKER_H */
