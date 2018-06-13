/*-------------------------------------------------------------------------
 *
 * pg_encryption_key.h
 *	  definition of the per relation encryption key
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_encryption_key.h
 *
 * NOTES
 *	  All relation encryption keys must be encrypted before storing.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ENCRYPTION_KEY_H
#define PG_ENCRYPTION_KEY_H

#include "catalog/genbki.h"
#include "catalog/pg_encryption_key_d.h"

/* ----------------
 *		pg_encryption_key definition.  cpp turns this into
 *		typedef struct FormData_pg_encryption_key
 * ----------------
 */
CATALOG(pg_encryption_key,3423,EncryptionKeyRelationId) BKI_WITHOUT_OIDS
{
	Oid			relid;
	text		relkey;
} FormData_pg_encryption_key;

/* ----------------
 *		Form_pg_encryption_key corresponds to a pointer to a tuple with
 *		the format of pg_encryption_key relation.
 * ----------------
 */
typedef FormData_pg_encryption_key *Form_pg_encryption_key;

void StoreCatalogRelationEncryptionKey(Oid relationId);

#endif							/* PG_ENCRYPTION_KEY_H */
