/*-------------------------------------------------------------------------
 *
 * pg_encryption_key.c
 *	  routines to support manipulation of the pg_encryption_key relation
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_encryption_key.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_encryption_key.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

void
StoreCatalogRelationEncryptionKey(Oid relationId)
{
	Datum	values[Natts_pg_encryption_key];
	bool	nulls[Natts_pg_encryption_key];
	HeapTuple	tuple;
	Relation	tabkeyRel;

	tabkeyRel = heap_open(EncryptionKeyRelationId, RowExclusiveLock);

	values[Anum_pg_encryption_key_relid - 1] =
		ObjectIdGetDatum(relationId);
	values[Anum_pg_encryption_key_relkey - 1] =
		CStringGetDatum("secret key");

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(RelationGetDescr(tabkeyRel), values, nulls);

	CatalogTupleInsert(tabkeyRel, tuple);

	heap_freetuple(tuple);

	heap_close(tabkeyRel, RowExclusiveLock);
}
