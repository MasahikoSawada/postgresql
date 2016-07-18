/*-------------------------------------------------------------------------
 *
 * proto.c
 * 		logical replication protocol functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/logical/proto.c
 *
 * TODO
 *		unaligned access
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/heapam.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "catalog/catversion.h"
#include "catalog/index.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "executor/spi.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "nodes/makefuncs.h"

#include "replication/logicalproto.h"
#include "replication/reorderbuffer.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#define IS_REPLICA_IDENTITY	1

static void logicalrep_write_attrs(StringInfo out, Relation rel);
static void logicalrep_write_tuple(StringInfo out, Relation rel,
								   HeapTuple tuple);

static void logicalrep_read_attrs(StringInfo in, char ***attrnames,
								  int *nattrnames);
static void logicalrep_read_tuple(StringInfo in, LogicalRepTupleData *tuple);


/*
 * Given a List of strings, return it as single comma separated
 * string, quoting identifiers as needed.
 *
 * This is essentially the reverse of SplitIdentifierString.
 *
 * The caller should free the result.
 */
static char *
stringlist_to_identifierstr(List *strings)
{
	ListCell *lc;
	StringInfoData res;
	bool first = true;

	initStringInfo(&res);

	foreach (lc, strings)
	{
		if (first)
			first = false;
		else
			appendStringInfoChar(&res, ',');

		appendStringInfoString(&res, quote_identifier(strVal(lfirst(lc))));
	}

	return res.data;
}

/*
 * Build string of options for logical replication plugin.
 */
char *
logicalrep_build_options(List *publications)
{
	StringInfoData	options;
	char		   *publicationstr;

	initStringInfo(&options);
	appendStringInfo(&options, "proto_version '%u'", LOGICALREP_PROTO_VERSION_NUM);
	appendStringInfo(&options, ", encoding %s",
					 quote_literal_cstr(GetDatabaseEncodingName()));
	appendStringInfo(&options, ", pg_version '%u'", PG_VERSION_NUM);
	publicationstr = stringlist_to_identifierstr(publications);
	appendStringInfo(&options, ", publication_names %s",
					 quote_literal_cstr(publicationstr));
	pfree(publicationstr);

	return options.data;
}

/*`
 * Write BEGIN to the output stream.
 */
void
logicalrep_write_begin(StringInfo out, ReorderBufferTXN *txn)
{
	pq_sendbyte(out, 'B');		/* BEGIN */

	/* fixed fields */
	pq_sendint64(out, txn->final_lsn);
	pq_sendint64(out, txn->commit_time);
	pq_sendint(out, txn->xid, 4);
}

/*
 * Read transaction BEGIN from the stream.
 */
void
logicalrep_read_begin(StringInfo in, XLogRecPtr *remote_lsn,
					  TimestampTz *committime, TransactionId *remote_xid)
{
	/* read fields */
	*remote_lsn = pq_getmsgint64(in);
	Assert(*remote_lsn != InvalidXLogRecPtr);
	*committime = pq_getmsgint64(in);
	*remote_xid = pq_getmsgint(in, 4);
}


/*
 * Write COMMIT to the output stream.
 */
void
logicalrep_write_commit(StringInfo out, ReorderBufferTXN *txn,
						XLogRecPtr commit_lsn)
{
	uint8 flags = 0;

	pq_sendbyte(out, 'C');		/* sending COMMIT */

	/* send the flags field (unused for now) */
	pq_sendbyte(out, flags);

	/* send fields */
	pq_sendint64(out, commit_lsn);
	pq_sendint64(out, txn->end_lsn);
	pq_sendint64(out, txn->commit_time);
}

/*
 * Read transaction COMMIT from the stream.
 */
void
logicalrep_read_commit(StringInfo in, XLogRecPtr *commit_lsn,
					   XLogRecPtr *end_lsn, TimestampTz *committime)
{
	/* read flags (unused for now) */
	uint8   flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	/* read fields */
	*commit_lsn = pq_getmsgint64(in);
	*end_lsn = pq_getmsgint64(in);
	*committime = pq_getmsgint64(in);
}

/*
 * Write ORIGIN to the output stream.
 */
void
logicalrep_write_origin(StringInfo out, const char *origin,
						XLogRecPtr origin_lsn)
{
	uint8	len;

	Assert(strlen(origin) < 255);

	pq_sendbyte(out, 'O');		/* ORIGIN */

	/* fixed fields */
	pq_sendint64(out, origin_lsn);

	/* origin */
	len = strlen(origin) + 1;
	pq_sendbyte(out, len);
	pq_sendbytes(out, origin, len);
}


/*
 * Read ORIGIN from the output stream.
 */
char *
logicalrep_read_origin(StringInfo in, XLogRecPtr *origin_lsn)
{
	uint8	len;

	/* fixed fields */
	*origin_lsn = pq_getmsgint64(in);

	/* origin */
	len = pq_getmsgbyte(in);

	return pnstrdup(pq_getmsgbytes(in, len), len);
}


/*
 * Write INSERT to the output stream.
 */
void
logicalrep_write_insert(StringInfo out,	Relation rel, HeapTuple newtuple)
{
	pq_sendbyte(out, 'I');		/* action INSERT */

	/* use Oid as relation identifier */
	pq_sendint(out, RelationGetRelid(rel), 4);

	pq_sendbyte(out, 'N');		/* new tuple follows */
	logicalrep_write_tuple(out, rel, newtuple);
}

/*
 * Read INSERT from stream.
 *
 * Fills the new tuple.
 */
LogicalRepRelId
logicalrep_read_insert(StringInfo in, LogicalRepTupleData *newtup)
{
	char		action;
	LogicalRepRelId		relid;

	/* read the relation id */
	relid = pq_getmsgint(in, 4);

	action = pq_getmsgbyte(in);
	if (action != 'N')
		elog(ERROR, "expected new tuple but got %d",
			 action);

	logicalrep_read_tuple(in, newtup);

	return relid;
}

/*
 * Write UPDATE to the output stream.
 */
void
logicalrep_write_update(StringInfo out, Relation rel, HeapTuple oldtuple,
					   HeapTuple newtuple)
{
	pq_sendbyte(out, 'U');		/* action UPDATE */

	/* use Oid as relation identifier */
	pq_sendint(out, RelationGetRelid(rel), 4);

	if (oldtuple != NULL)
	{
		pq_sendbyte(out, 'O');	/* old tuple follows */
		logicalrep_write_tuple(out, rel, oldtuple);
	}

	pq_sendbyte(out, 'N');		/* new tuple follows */
	logicalrep_write_tuple(out, rel, newtuple);
}

/*
 * Read UPDATE from stream.
 */
LogicalRepRelId
logicalrep_read_update(StringInfo in, bool *hasoldtup,
					   LogicalRepTupleData *oldtup,
					   LogicalRepTupleData *newtup)
{
	char		action;
	LogicalRepRelId		relid;

	/* read the relation id */
	relid = pq_getmsgint(in, 4);

	/* read and verify action */
	action = pq_getmsgbyte(in);
	if (action != 'O' && action != 'N')
		elog(ERROR, "expected action 'N' or 'O', got %c",
			 action);

	/* check for old tuple */
	if (action == 'O')
	{
		logicalrep_read_tuple(in, oldtup);
		*hasoldtup = true;
		action = pq_getmsgbyte(in);
	}
	else
		*hasoldtup = false;

	/* check for new  tuple */
	if (action != 'N')
		elog(ERROR, "expected action 'N', got %c",
			 action);

	logicalrep_read_tuple(in, newtup);

	return relid;
}

/*
 * Write DELETE to the output stream.
 */
void
logicalrep_write_delete(StringInfo out, Relation rel, HeapTuple oldtuple)
{
	pq_sendbyte(out, 'D');		/* action DELETE */

	/* use Oid as relation identifier */
	pq_sendint(out, RelationGetRelid(rel), 4);

	pq_sendbyte(out, 'O');	/* old tuple follows */
	logicalrep_write_tuple(out, rel, oldtuple);
}

/*
 * Read DELETE from stream.
 *
 * Fills the old tuple.
 */
LogicalRepRelId
logicalrep_read_delete(StringInfo in, LogicalRepTupleData *oldtup)
{
	char		action;
	LogicalRepRelId		relid;

	/* read the relation id */
	relid = pq_getmsgint(in, 4);

	/* read and verify action */
	action = pq_getmsgbyte(in);
	if (action != 'O')
		elog(ERROR, "expected action 'O', got %c", action);

	logicalrep_read_tuple(in, oldtup);

	return relid;
}

/*
 * Write qualified relation name to the output stream.
 */
void
logicalrep_write_rel_name(StringInfo out, char *nspname, char *relname)
{
	uint8		nspnamelen;
	uint8		relnamelen;

	nspnamelen = strlen(nspname) + 1;
	relnamelen = strlen(relname) + 1;

	pq_sendbyte(out, nspnamelen);		/* schema name length */
	pq_sendbytes(out, nspname, nspnamelen);

	pq_sendbyte(out, relnamelen);		/* table name length */
	pq_sendbytes(out, relname, relnamelen);
}

/*
 * Read qualified relation name from the stream.
 */
void
logicalrep_read_rel_name(StringInfo in, char **nspname, char **relname)
{
	int			len;

	len = pq_getmsgbyte(in);
	*nspname = (char *) pq_getmsgbytes(in, len);

	len = pq_getmsgbyte(in);
	*relname = (char *) pq_getmsgbytes(in, len);
}


/*
 * Write relation description to the output stream.
 */
void
logicalrep_write_rel(StringInfo out, Relation rel)
{
	char	   *nspname;
	char	   *relname;

	pq_sendbyte(out, 'R');		/* sending RELATION */

	/* use Oid as relation identifier */
	pq_sendint(out, RelationGetRelid(rel), 4);

	/* send the relation name */
	nspname = get_namespace_name(RelationGetNamespace(rel));
	if (nspname == NULL)
		elog(ERROR, "cache lookup failed for namespace %u",
			 rel->rd_rel->relnamespace);
	relname = RelationGetRelationName(rel);

	logicalrep_write_rel_name(out, nspname, relname);

	/* send the attribute info */
	logicalrep_write_attrs(out, rel);
}

/*
 * Read the relation info from stream and return as LogicalRepRelation.
 */
LogicalRepRelation *
logicalrep_read_rel(StringInfo in)
{
	LogicalRepRelation	*rel = palloc(sizeof(LogicalRepRelation));

	rel->remoteid = pq_getmsgint(in, 4);

	/* Read relation name from stream */
	logicalrep_read_rel_name(in, &rel->nspname, &rel->relname);

	/* Get attribute description */
	logicalrep_read_attrs(in, &rel->attnames, &rel->natts);

	return rel;
}

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void
logicalrep_write_tuple(StringInfo out, Relation rel, HeapTuple tuple)
{
	TupleDesc	desc;
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	int			i;
	uint16		nliveatts = 0;

	desc = RelationGetDescr(rel);

	pq_sendbyte(out, 'T');			/* sending TUPLE */

	for (i = 0; i < desc->natts; i++)
	{
		if (desc->attrs[i]->attisdropped)
			continue;
		nliveatts++;
	}
	pq_sendint(out, nliveatts, 2);

	/* try to allocate enough memory from the get go */
	enlargeStringInfo(out, tuple->t_len +
					  nliveatts * (1 + 4));

	heap_deform_tuple(tuple, desc, values, isnull);

	/* Write the values */
	for (i = 0; i < desc->natts; i++)
	{
		HeapTuple	typtup;
		Form_pg_type typclass;
		Form_pg_attribute att = desc->attrs[i];
		char   	   *outputstr;
		int			len;

		/* skip dropped columns */
		if (att->attisdropped)
			continue;

		if (isnull[i])
		{
			pq_sendbyte(out, 'n');	/* null column */
			continue;
		}
		else if (att->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(values[i]))
		{
			pq_sendbyte(out, 'u');	/* unchanged toast column */
			continue;
		}

		typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(att->atttypid));
		if (!HeapTupleIsValid(typtup))
			elog(ERROR, "cache lookup failed for type %u", att->atttypid);
		typclass = (Form_pg_type) GETSTRUCT(typtup);

		pq_sendbyte(out, 't');	/* 'text' data follows */

		outputstr =	OidOutputFunctionCall(typclass->typoutput, values[i]);
		len = strlen(outputstr) + 1;	/* null terminated */
		pq_sendint(out, len, 4);		/* length */
		appendBinaryStringInfo(out, outputstr, len); /* data */

		pfree(outputstr);

		ReleaseSysCache(typtup);
	}
}

/*
 * Read tuple in remote format from stream.
 *
 * The returned tuple points into the input stringinfo.
 */
static void
logicalrep_read_tuple(StringInfo in, LogicalRepTupleData *tuple)
{
	int			i;
	int			natts;
	char		action;

	/* Check that the action is what we expect. */
	action = pq_getmsgbyte(in);
	if (action != 'T')
		elog(ERROR, "expected TUPLE, got %c", action);

	/* Get of attributes. */
	natts = pq_getmsgint(in, 2);

	memset(tuple->changed, 0, sizeof(tuple->changed));

	/* Read the data */
	for (i = 0; i < natts; i++)
	{
		char		kind;
		int			len;

		kind = pq_getmsgbyte(in);

		switch (kind)
		{
			case 'n': /* null */
				tuple->values[i] = NULL;
				tuple->changed[i] = true;
				break;
			case 'u': /* unchanged column */
				tuple->values[i] = (char *) 0xdeadbeef; /* make bad usage more obvious */
				break;
			case 't': /* text formatted value */
				{
					tuple->changed[i] = true;

					len = pq_getmsgint(in, 4); /* read length */

					/* and data */
					tuple->values[i] = (char *) pq_getmsgbytes(in, len);
				}
				break;
			default:
				elog(ERROR, "unknown data representation type '%c'", kind);
		}
	}
}


/*
 * Write relation attributes to the outputstream.
 */
static void
logicalrep_write_attrs(StringInfo out, Relation rel)
{
	TupleDesc	desc;
	int			i;
	uint16		nliveatts = 0;
	Bitmapset  *idattrs;

	desc = RelationGetDescr(rel);

	pq_sendbyte(out, 'A');			/* sending ATTRS */

	/* send number of live attributes */
	for (i = 0; i < desc->natts; i++)
	{
		if (desc->attrs[i]->attisdropped)
			continue;
		nliveatts++;
	}
	pq_sendint(out, nliveatts, 2);

	/* fetch bitmap of REPLICATION IDENTITY attributes */
	idattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);

	/* send the attributes */
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = desc->attrs[i];
		uint8			flags = 0;
		uint8			len;
		const char	   *attname;

		if (att->attisdropped)
			continue;

		if (bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber,
						  idattrs))
			flags |= IS_REPLICA_IDENTITY;

		pq_sendbyte(out, 'C');		/* column definition follows */
		pq_sendbyte(out, flags);

		attname = NameStr(att->attname);
		len = strlen(attname) + 1;
		pq_sendbyte(out, len);
		pq_sendbytes(out, attname, len); /* data */
	}

	bms_free(idattrs);
}


/*
 * Read relation attribute names from the outputstream.
 */
static void
logicalrep_read_attrs(StringInfo in, char ***attrnames, int *nattrnames)
{
	int			i;
	uint16		nattrs;
	char	  **attrs;
	char		blocktype;

	blocktype = pq_getmsgbyte(in);
	if (blocktype != 'A')
		elog(ERROR, "expected ATTRS, got %c", blocktype);

	nattrs = pq_getmsgint(in, 2);
	attrs = palloc(nattrs * sizeof(char *));

	/* read the attributes */
	for (i = 0; i < nattrs; i++)
	{
		uint8			len;

		blocktype = pq_getmsgbyte(in);		/* column definition follows */
		if (blocktype != 'C')
			elog(ERROR, "expected COLUMN, got %c", blocktype);

		/* We ignore flags atm. */
		(void) pq_getmsgbyte(in);

		/* attribute name */
		len = pq_getmsgbyte(in);
		/* the string is NULL terminated */
		attrs[i] = (char *) pq_getmsgbytes(in, len);
	}

	*attrnames = attrs;
	*nattrnames = nattrs;
}
