/*-------------------------------------------------------------------------
 *
 * logicalworker.h
 *	  Exports for logical replication workers.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalworker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALWORKER_H
#define LOGICALWORKER_H

typedef struct LogicalRepWorker
{
	/* Pointer to proc array. NULL if not running. */
	PGPROC *proc;

	/* Database id to connect to. */
	Oid		dbid;

	/* Subscription id for the worker. */
	Oid		subid;

	/* Used for initial table synchronization. */
	Oid		relid;

	/* Stats. */
	XLogRecPtr	last_lsn;
	TimestampTz	last_send_time;
	TimestampTz	last_recv_time;
	XLogRecPtr	reply_lsn;
	TimestampTz	reply_time;
} LogicalRepWorker;

extern int max_logical_replication_workers;

extern void ApplyLauncherMain(Datum main_arg);
extern void ApplyWorkerMain(Datum main_arg);

extern Size ApplyLauncherShmemSize(void);
extern void ApplyLauncherShmemInit(void);
extern void ApplyLauncherWakeup(void);
extern void ApplyLauncherWakeupOnCommit(void);

extern void ApplyLauncherWakeupOnCommit(void);
extern void ApplyLauncherWakeup(void);

extern void logicalrep_worker_attach(int slot);
extern LogicalRepWorker *logicalrep_worker_find(Oid subid, Oid relid);
extern int logicalrep_worker_count(Oid subid);
extern void logicalrep_worker_launch(Oid dbid, Oid subid, Oid relid);
extern void logicalrep_worker_stop(LogicalRepWorker *worker);

extern Datum pg_stat_get_subscription(PG_FUNCTION_ARGS);

#endif   /* LOGICALWORKER_H */
