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
#include "common/md5.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"

static char *generate_encryption_key(void);

/*
 * Insert data encryption key for given relation into system catalog.
 * Generate random encryption key and then encrypts it with a database
 * key. Note that stored encryption key must be encrypted with a
 * database key for security.
 */
void
StoreCatalogRelationEncryptionKey(Oid relationId)
{
	Datum	values[Natts_pg_encryption_key];
	bool	nulls[Natts_pg_encryption_key];
	HeapTuple	tuple;
	Relation	enckeyRel;
	char	*encKey = generate_encryption_key();

	enckeyRel = heap_open(EncryptionKeyRelationId, RowExclusiveLock);

	values[Anum_pg_encryption_key_relid - 1] = ObjectIdGetDatum(relationId);
	values[Anum_pg_encryption_key_relkey - 1] =
		PointerGetDatum(cstring_to_text(encKey));

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(RelationGetDescr(enckeyRel), values, nulls);

	CatalogTupleInsert(enckeyRel, tuple);

	heap_freetuple(tuple);

	heap_close(enckeyRel, RowExclusiveLock);
}

/*
 * Drop encryption key by OID.
 * Encryption key OID is the same as oid of the corresponding relation
 */
void
DropEncryptionKeyById(Oid keyid)
{
	Relation rel;
	HeapTuple tuple;

	rel = heap_open(EncryptionKeyRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(ENCRYPTIONKEYOID, ObjectIdGetDatum(keyid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for encryption key for relation %u", keyid);

	CatalogTupleDelete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);
	heap_close(rel, RowExclusiveLock);
}

/*
 * GetEncryptionKey
 *
 * Search data encryption key by relation id and returns encryption key
 * string.
 */
char *
GetEncryptionKey(Oid relid)
{
	Relation rel;
	HeapTuple tuple;
	Form_pg_encryption_key	enckeyForm;
	char	*encKey = NULL;

	rel = heap_open(EncryptionKeyRelationId, AccessShareLock);

	tuple = SearchSysCache1(ENCRYPTIONKEYOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for encryption key for relation %u", relid);

	enckeyForm = (Form_pg_encryption_key) GETSTRUCT(tuple);

	Assert(OidIsValid(enckeyForm));

	encKey = text_to_cstring(&enckeyForm->relkey);

	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);

	return encKey;
}

/*
 * Generate relation encryption key.
 *
 * FIXME: Everything!!
 */
static char *
generate_encryption_key(void)
{
	TimestampTz now = GetCurrentTimestamp();
	char		*key = palloc(sizeof(char) * MAX_ENCRYPTION_KEY_LEN);

	if (pg_md5_hash((void *) &now, sizeof(TimestampTz), key) == false)
		return NULL;

	return key;
}
