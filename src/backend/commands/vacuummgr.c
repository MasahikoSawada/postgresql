/*-------------------------------------------------------------------------
 *
 * vacuummgr.c
 *	  Vacuum manager
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuummgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "catalog/storage.h"
#include "commands/dbcommands.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"

static PgStat_StatTabEntry *get_pgstat_tabentry(Relation rel);

static PgStat_StatTabEntry *
get_pgstat_tabentry(Relation rel)
{
	bool isshared = rel->rd_rel->relisshared;
	Oid	dbid = isshared ? InvalidOid : MyDatabaseId;
	PgStat_StatDBEntry	*dbentry;
	PgStat_StatTabEntry *tabentry = NULL;

	dbentry = pgstat_fetch_stat_dbentry(dbid);

	if (PointerIsValid(dbentry))
		tabentry = hash_search(dbentry->tables, &RelationGetRelid(rel),
							   HASH_FIND, NULL);
	return tabentry;
}

VacuumWorkItem *
VacuumMgrGetWorkItem(Relation onerel, int options,
					 TransactionId xidFullScanLimit,
					 MultiXactId mxactFullScanLimit)
{
	VacuumWorkItem *workitem = palloc(sizeof(VacuumWorkItem));
	PgStat_StatTabEntry *tabentry = NULL;
	bool				aggressive;

	/* Initialize */
	workitem->wi_rel = onerel;
	workitem->wi_options = options;
	workitem->wi_parallel_workers = 0;

	/* Get stats of relation */
	tabentry = get_pgstat_tabentry(onerel);

	/*
	 * We request an aggressive scan if the table's frozen Xid is now older
	 * than or equal to the requested Xid full-table scan limit; or if the
	 * table's minimum MultiXactId is older than or equal to the requested
	 * mxid full-table scan limit; or if DISABLE_PAGE_SKIPPING was specified.
	 */
	aggressive = TransactionIdPrecedesOrEquals(onerel->rd_rel->relfrozenxid,
											   xidFullScanLimit);
	aggressive |= MultiXactIdPrecedesOrEquals(onerel->rd_rel->relminmxid,
											  mxactFullScanLimit);
	if (options & VACOPT_DISABLE_PAGE_SKIPPING)
		aggressive = true;

	workitem->wi_aggressive = aggressive;

	/* Set start and end block */
	workitem->wi_startblk = 0;
	workitem->wi_endblk = RelationGetNumberOfBlocks(onerel);

	return workitem;
}
