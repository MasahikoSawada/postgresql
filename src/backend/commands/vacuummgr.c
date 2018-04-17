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

static void get_pgstat_entries(Relation rel, PgStat_StatTabEntry **tabentry,
								PgStat_StatGarbageEntry **gentry);

static void GarbageMapCount_common(Relation rel, BlockNumber blkno,
								   int count, bool is_insert);

static void
get_pgstat_entries(Relation rel, PgStat_StatTabEntry **tabentry,
					PgStat_StatGarbageEntry **gentry)
{
	bool isshared = rel->rd_rel->relisshared;
	Oid	dbid = isshared ? InvalidOid : MyDatabaseId;
	PgStat_StatDBEntry	*dbentry;

	dbentry = pgstat_fetch_stat_dbentry(dbid);

	if (PointerIsValid(dbentry))
	{
		*tabentry = hash_search(dbentry->tables, &RelationGetRelid(rel),
								HASH_FIND, NULL);
		*gentry = hash_search(dbentry->garbages, &RelationGetRelid(rel),
							  HASH_FIND, NULL);
	}
}

VacuumWorkItem *
VacuumMgrGetWorkItem(Relation onerel, int options,
					 TransactionId xidFullScanLimit,
					 MultiXactId mxactFullScanLimit)
{
	VacuumWorkItem *workitem = palloc(sizeof(VacuumWorkItem));
	PgStat_StatTabEntry *tabentry = NULL;
	PgStat_StatGarbageEntry *gentry = NULL;
	bool				aggressive;

	/* Initialize */
	workitem->wi_rel = onerel;
	workitem->wi_options = options;
	workitem->wi_parallel_workers = 0;

	/* Get stats of relation */
	if (IsUnderPostmaster)
		get_pgstat_entries(onerel, &tabentry, &gentry);
//		tabentry = get_pgstat_tabentry(onerel);

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

static void
GarbageMapCount_common(Relation rel, BlockNumber blkno, int count,
					   bool is_insert)
{
	int slotno;
	int bankno;
	GarbageMap	*gmap;

	Assert(rel->pgstat_info);

	/* Quick return, if garbage map is not enabled */
	if (!rel->pgstat_info->gmap)
		return;

	/* Get bank and slot number for GarbageMap */
	slotno = GarbageMapGetSlotNo(blkno,
								 rel->pgstat_info->gmap->pages_per_range);
	bankno = GarbageMapGetBankNo(
		GarbageMapGetRangeNo(blkno,
							 rel->pgstat_info->gmap->pages_per_range));
	if (is_insert)
		gmap = rel->pgstat_info->trans->tran_gmap.insmap;
	else
		gmap = rel->pgstat_info->trans->tran_gmap.delmap;

	/* Initialize slots we're interested in */
	if (!gmap->gmbank[bankno])
	{
		gmap->gmbank[bankno] = (GarbageMapBank)
			MemoryContextAllocZero(TopMemoryContext,
								   sizeof(GarbageMapSlot));

		/* Update min/max bank number */
		if (bankno > gmap->max_bank)
			gmap->max_bank = bankno;
		if (bankno < gmap->min_bank)
			gmap->min_bank = bankno;
	}

	/* Count tuples */
	gmap->gmbank[bankno][slotno] += count;
	gmap->n_tuples += abs(count);

//	elog(WARNING, "count blkno %u, count %d, [%d][%d] = %d",
//		 blkno, count, bankno, slotno, gmap->gmbank[bankno][slotno]);

	/* Keep sane value */
//	if (gmap->n_tuples < 0)
//		gmap->n_tuples = 0;
}

/*
 * Count up garbagemap by insert
 */
void
GarbageMapCountInsert(Relation rel, BlockNumber blkno, int count)
{
	GarbageMapCount_common(rel, blkno, count, true);
}

void
GarbageMapCountDelete(Relation rel, BlockNumber blkno)
{
	GarbageMapCount_common(rel, blkno, 1, false);
}

void
GarbageMapCountUpdate(Relation rel, BlockNumber old_blkno,
					  BlockNumber new_blkno)
{
	if (BlockNumberIsValid(new_blkno))
		GarbageMapCount_common(rel, new_blkno, 1, true);
	if (BlockNumberIsValid(old_blkno))
		GarbageMapCount_common(rel, old_blkno, 1, false);
}

/* -- Calculation logic of temprature --
	rand = random() % 10000;
	prob = (long) (powf(0.5, curval) * 10000);
	if (rand < prob)
	{
		(gmap->gmbank[bankno][slotno])++;
		elog(NOTICE, "blk %d : map[%d][%d] = %d ... rand %ld, prob %ld",
			 blkno, bankno, slotno,
			 gmap->gmbank[bankno][slotno],
			 rand, prob);
	}
*/

void
GarbageMapCountVacuum(Relation rel, BlockNumber blkno,
					  int count)
{
	GarbageMapCount_common(rel, blkno, (-1) * count, false);
//	GarbageMapCount_common(rel, blkno, (-1) * count, false);
}

GarbageMap *
GarbageMapInitMap(int pages_per_range)
{
	GarbageMap		*gmap;
	int nbanks;

	nbanks = MaxBlockNumber / pages_per_range / GM_SLOTS_PER_BANK;

	/* Initialize garbage map */
	gmap = MemoryContextAllocZero(TopMemoryContext,
								  sizeof(GarbageMap));
	gmap->gmbank = MemoryContextAllocZero(TopMemoryContext,
										  sizeof(GarbageMapBank) * nbanks);
	gmap->max_bank = 0;
	gmap->min_bank = INT_MAX;
	gmap->pages_per_range = pages_per_range;
	gmap->n_tuples = 0;

	return gmap;
}

void
AtEOXact_GarbageMap(GarbageMap *gmap, GarbageMapTran tran_gmap,
					bool isCommit)
{
	GarbageMap *target_map;
	int bankno, slotno;
	StringInfoData buf;

	/* Return if not enabled */
	if (!gmap)
		return;

	/*
	 * In commit case, we're interested in only deleted tuples.
	 */
	if (isCommit)
		target_map = tran_gmap.delmap;
	else
		target_map = tran_gmap.insmap;

	Assert(target_map);

	/* There is no garbage tuples, return */
	if (target_map->n_tuples == 0)
		return;

	for (bankno = target_map->min_bank;
		 bankno <= target_map->max_bank;
		 bankno++)
	{
		for (slotno = 0; slotno < GM_SLOTS_PER_BANK; slotno++)
		{
			if (target_map->gmbank[bankno][slotno] == 0)
				continue;

			if (!gmap->gmbank[bankno])
			{
				gmap->gmbank[bankno] = (GarbageMapBank)
					MemoryContextAllocZero(TopMemoryContext,
										   sizeof(GarbageMapSlot));
			}

			gmap->gmbank[bankno][slotno] +=
				target_map->gmbank[bankno][slotno];
		}
	}

	/* Update table garbage map stats */
	if (target_map->min_bank < gmap->min_bank)
		gmap->min_bank = target_map->min_bank;
	if (target_map->max_bank > gmap->max_bank)
		gmap->max_bank = target_map->max_bank;
	gmap->n_tuples += target_map->n_tuples;

	/* DEBUG output */
	initStringInfo(&buf);
	for (bankno = gmap->min_bank; bankno <= gmap->max_bank; bankno++)
	{
		int prev = gmap->gmbank[bankno][0];
		int nconts = 0;
		bool is_first = true;
		for (slotno = 1; slotno < GM_SLOTS_PER_BANK; slotno++)
		{
			int cur = gmap->gmbank[bankno][slotno];

			if (cur != prev)
			{
				if (nconts > 0)
					appendStringInfo(&buf, "%s%d:%d",
									 (is_first) ? "" : ",",
									 prev,
									 nconts);
				else
					appendStringInfo(&buf, "%s%d",
									 (is_first) ? "" : ",",
									 prev);
				nconts = 0;
				is_first = false;
				prev = cur;
				continue;
			}

			nconts++;
			prev = cur;
		}

		if (nconts > 0)
			appendStringInfo(&buf, "%s%d:%d",
							 (is_first) ? "" : ",",
							 prev,
							 nconts);
		else
			appendStringInfo(&buf, "%s%d",
							 (is_first) ? "" : ",",
							 prev);

		elog(NOTICE, "bank[%d] \"%s\" : ntup %d", bankno, buf.data, gmap->n_tuples);
		resetStringInfo(&buf);
	}
}

GarbageMapScanState *
garbagemap_beginscan(GarbageMap *gmap)
{
	GarbageMapScanState *state;

	state = (GarbageMapScanState *)palloc(sizeof(GarbageMapScanState));
	state->gmap = gmap;
	state->n_ents = 0;
	state->cur_slot = 0;
	state->cur_bank = gmap->min_bank;
	state->max_bank = gmap->max_bank;

	return state;
}

void
garbagemap_endscan(GarbageMapScanState *state)
{
	pfree(state);
}

GarbageMapEntry *
garbagemap_getnext(GarbageMapScanState *state)
{
	GarbageMapEntry *ent = NULL;
	int slotno;
	int bankno;

	/* Reached to end of garbagemap */
	if (state->cur_bank > state->max_bank)
		return NULL;

	for (bankno = state->cur_bank; bankno <= state->max_bank; bankno++)
	{
		for (slotno = state->cur_slot; slotno < GM_SLOTS_PER_BANK; slotno++)
		{
			/* Found! */
			if (state->gmap->gmbank[bankno][slotno] != 0)
			{
				ent = (GarbageMapEntry *) palloc(sizeof(GarbageMapEntry));
				ent->bankno = bankno;
				ent->slotno = slotno;
				ent->val = state->gmap->gmbank[bankno][slotno];
				state->n_ents++;
				break;
			}
		}

		/* Break if we already found the entry */
		if (ent)
			break;
	}

	/* Advance the scan state to the next value */
	state->cur_slot = ++slotno;
	state->cur_bank = bankno;

	return ent;
}

/*
 * Construct garbagemap using by entries.
 */
void
GarbageMapConstructMapByEnt(GarbageMap *gmap, GarbageMapEntry *entries,
							int n_entries)
{
	int i;

	/* Register each garbage map entries */
	for (i = 0; i < n_entries; i++)
	{
		GarbageMapEntry *ent = &(entries[i]);

		/* Alloc slots of the bank */
		if (!gmap->gmbank[ent->bankno])
		{
			gmap->gmbank[ent->bankno] = (GarbageMapBank)
				MemoryContextAllocZero(TopMemoryContext,
									   sizeof(GarbageMapSlot));
		}


		/* Update min/max bank number */
		if (ent->bankno > gmap->max_bank)
			gmap->max_bank = ent->bankno;
		if (ent->bankno < gmap->min_bank)
			gmap->min_bank = ent->bankno;

		/* XXXX : maybe use temprature? */
		gmap->gmbank[ent->bankno][ent->slotno] += ent->val;
		gmap->n_tuples += ent->val;

		elog(WARNING, "   Restored an ent : bank[%d] val = %d, ret = %d, ntup %d, min %d, max %d",
			 ent->bankno, ent->val,
			 gmap->gmbank[ent->bankno][ent->slotno],
			 gmap->n_tuples,
			 gmap->min_bank, gmap->max_bank);
	}
}

GarbageMap *
GarbageMapCopy(GarbageMap *orig)
{
	GarbageMap *copied;
	int i;

	copied = GarbageMapInitMap(orig->pages_per_range);
	for (i = orig->min_bank; i <= orig->max_bank; i++)
	{
		copied->gmbank[i] = (GarbageMapBank)
			MemoryContextAllocZero(TopMemoryContext,
								   sizeof(GarbageMapSlot));

		memcpy(copied->gmbank[i],
			   orig->gmbank[i],
			   sizeof(GarbageMapBankInfo));
	}

	return copied;
}

/*
 * Reset garbagemap struct
 */
void
GarbageMapReset(GarbageMap *gmap)
{
	int bankno;

	for (bankno = gmap->min_bank; bankno <= gmap->max_bank; bankno++)
	{
		pfree(gmap->gmbank[bankno]);
		gmap->gmbank[bankno] = NULL;
	}

	gmap->min_bank = INT_MAX;
	gmap->max_bank = 0;
	gmap->n_tuples = 0;
}

/*
 * Constrcut garbage map by bankinfos.
 *
 * Note caller must reset 'gmap' beforehand.
 */
void
GarbageMapConstructMapByBankinfo(GarbageMap *gmap,
								 GarbageMapBankInfo *banks,
								 int nbanks)
{
	int i;

	/* Register each garbage map entries */
	for (i = 0; i < nbanks; i++)
	{
		GarbageMapBankInfo *b = &(banks[i]);

		Assert(gmap->gmbank[b->bankno] == NULL);

		gmap->gmbank[b->bankno] = (GarbageMapBank)
			MemoryContextAllocZero(TopMemoryContext,
								   sizeof(GarbageMapSlot));

		/* Update min/max bank number */
		if (b->bankno > gmap->max_bank)
			gmap->max_bank = b->bankno;
		if (b->bankno < gmap->min_bank)
			gmap->min_bank = b->bankno;

		memcpy(gmap->gmbank[b->bankno],
			   b->bank,
			   sizeof(GarbageMapBankInfo));
	}
}

/*
 * Serialize contents of gmap for dump
 */
GarbageMapSerializedData *
GarbageMapSerialize(GarbageMap *gmap, Oid relid, int *size)
{
	GarbageMapSerializedData *serialized_gmap;
	int n_serialized = 0;
	int	nbanks;
	int i;

	nbanks = gmap->max_bank - gmap->min_bank + 1;
	*size =	sizeof(GarbageMapSerializedHeaderData) +
		sizeof(GarbageMapBankInfo) * nbanks;

	serialized_gmap = (GarbageMapSerializedData *) palloc(*size);

	/* Set header info */
	serialized_gmap->gm_header.relid = relid;
	serialized_gmap->gm_header.pages_per_range = gmap->pages_per_range;
	serialized_gmap->gm_header.nbanks = nbanks;
	serialized_gmap->gm_header.n_tuples = gmap->n_tuples;

	/* serialize each bank */
	for (i = gmap->min_bank; i <= gmap->max_bank; i++)
	{
		serialized_gmap->bankinfo[n_serialized].bankno = i;
		memcpy(&(serialized_gmap->bankinfo[n_serialized].bank),
			   gmap->gmbank[i], sizeof(GarbageMapSlot));
		n_serialized++;
	}

	return serialized_gmap;
}
