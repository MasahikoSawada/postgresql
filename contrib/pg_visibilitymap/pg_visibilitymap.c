/*-------------------------------------------------------------------------
 *
 * pg_visibilitymap.c
 *	  display contents of a visibility map and page level bits
 *
 *	  contrib/pg_visibilitymap/pg_visibilitymap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/visibilitymap.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_visibilitymap);
PG_FUNCTION_INFO_V1(pg_page_flags);

Datum
pg_visibilitymap(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		blkno = PG_GETARG_INT64(1);
	int32		mapbits;
	Relation	rel;
	Buffer		vmbuffer = InvalidBuffer;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_visibilitymap functions"))));

	rel = relation_open(relid, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table or materialized view",
						RelationGetRelationName(rel))));

	if (blkno < 0 || blkno > MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid block number")));

	mapbits = (int32) visibilitymap_get_status(rel, blkno, &vmbuffer);
	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	relation_close(rel, AccessShareLock);
	PG_RETURN_INT32(mapbits);
}

Datum
pg_page_flags(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		blkno = PG_GETARG_INT64(1);
	int32		pagebits;
	Relation	rel;
	Buffer		buffer;
	Page		page;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_visibilitymap functions"))));

	rel = relation_open(relid, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table or materialized view",
						RelationGetRelationName(rel))));

	if (blkno < 0 || blkno > MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid block number")));

	buffer = ReadBuffer(rel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buffer);
	pagebits = (int32) (((PageHeader) (page))->pd_flags);

	UnlockReleaseBuffer(buffer);
	relation_close(rel, AccessShareLock);
	PG_RETURN_INT32(pagebits);
}
