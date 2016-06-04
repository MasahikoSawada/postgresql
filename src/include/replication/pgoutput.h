/*-------------------------------------------------------------------------
 *
 * pgoutput.h
 *		Logical Replication output plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pgoutput.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGOUTPUT_H
#define PGOUTPUT_H


typedef struct PGOutputData
{
	MemoryContext	context;			/* pricate memory context for transient
										 * allocations */

	/* client info */
	uint32			protocol_version;
	const char	   *client_encoding;
	uint32			client_pg_version;

	List		   *publication_names;
	List		   *publications;
} PGOutputData;

extern void pgoutput_process_parameters(List *options, PGOutputData *data);

#endif /* PGOUTPUT_H */
