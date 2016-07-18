/*-------------------------------------------------------------------------
 *
 * worker_internal.h
 *	  Internal headers shared by logical replication workers.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * src/include/replication/worker_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WORKER_INTERNAL_H
#define WORKER_INTERNAL_H

/* filled by libpqreceiver when loaded */
extern struct WalReceiverConnAPI	   *wrcapi;
extern struct WalReceiverConnHandle    *wrchandle;

/* Worker and subscription objects. */
extern Subscription		   *MySubscription;
extern LogicalRepWorker	   *MyLogicalRepWorker;

extern bool	in_remote_transaction;
extern bool	got_SIGTERM;

extern void logicalrep_worker_sigterm(SIGNAL_ARGS);
extern void LogicalRepApplyLoop(XLogRecPtr last_received);
extern char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos);
void process_syncing_tables(char *slotname, XLogRecPtr end_lsn);
void invalidate_syncing_table_states(Datum arg, int cacheid,
									 uint32 hashvalue);

#endif   /* WORKER_INTERNAL_H */
