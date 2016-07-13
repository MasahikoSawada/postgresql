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

extern bool	got_SIGTERM;

extern void logicalrep_worker_sigterm(SIGNAL_ARGS);

#endif   /* WORKER_INTERNAL_H */
