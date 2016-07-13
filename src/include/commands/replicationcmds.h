/*-------------------------------------------------------------------------
 *
 * replicationcmds.h
 *	  prototypes for publicationcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/replicationcmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef REPLICATIONCMDS_H
#define REPLICATIONCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress CreatePublication(CreatePublicationStmt *stmt);
extern void AlterPublication(AlterPublicationStmt *stmt);
extern void DropPublicationById(Oid pubid);
extern void RemovePublicationRelById(Oid prid);

#endif   /* REPLICATIONCMDS_H */
