/*-------------------------------------------------------------------------
 *
 * pg_publication.h
 *	  definition of the relation sets relation (pg_publication)
  *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_publication.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_H
#define PG_PUBLICATION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_publication definition.  cpp turns this into
 *		typedef struct FormData_pg_publication
 *
 * ----------------
 */
#define PublicationRelationId			6104

CATALOG(pg_publication,6104)
{
	NameData	pubname;			/* name of the publication */

	/*
	 * indicates that this is special publication which should encompass
	 * all tables in the database (except for the unlogged and temp ones)
	 */
	bool		puballtables;

	/* true if inserts are replicated */
	bool		pubreplins;

	/* true if updates are replicated */
	bool		pubreplupd;

	/* true if deletes are replicated */
	bool		pubrepldel;

} FormData_pg_publication;

/* ----------------
 *		Form_pg_publication corresponds to a pointer to a tuple with
 *		the format of pg_publication relation.
 * ----------------
 */
typedef FormData_pg_publication *Form_pg_publication;

/* ----------------
 *		compiler constants for pg_publication
 * ----------------
 */

#define Natts_pg_publication				5
#define Anum_pg_publication_pubname			1
#define Anum_pg_publication_puballtables	2
#define Anum_pg_publication_pubreplins		3
#define Anum_pg_publication_pubreplupd		4
#define Anum_pg_publication_pubrepldel		5

typedef struct Publication
{
	Oid		oid;
	char   *name;
	bool	alltables;
	bool	replicate_insert;
	bool	replicate_update;
	bool	replicate_delete;
} Publication;

extern Publication *GetPublication(Oid pubid);
extern Publication *GetPublicationByName(const char *pubname, bool missing_ok);
extern List *GetRelationPublications(Oid relid);
extern List *GetAllTablesPublications(void);
extern List *GetPublicationRelations(Oid pubid);

extern Oid publication_add_relation(Oid pubid, Relation targetrel,
							 bool if_not_exists);

extern Oid get_publication_oid(const char *pubname, bool missing_ok);
extern char *get_publication_name(Oid pubid);

#endif   /* PG_PUBLICATION_H */
