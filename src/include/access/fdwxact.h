/*
 * fdwxact.h
 *
 * PostgreSQL global transaction manager
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact.h
 */
#ifndef FDWXACT_H
#define FDWXACT_H

#include "foreign/foreign.h"

/* Flag passed to FDW transaction management APIs */
#define FDWXACT_FLAG_ONEPHASE		0x01	/* transaction can commit/rollback
											 * without preparation */

/* State data for foreign transaction resolution, passed to FDW callbacks */
typedef struct FdwXactRslvState
{
	/* Foreign transaction information */
	ForeignServer *server;
	UserMapping *usermapping;

	int			flags;			/* OR of FDWXACT_FLAG_xx flags */
} FdwXactRslvState;

/* Function declarations */
extern void AtEOXact_FdwXact(bool is_commit);
extern void PrePrepare_FdwXact(void);

#endif /* FDWXACT_H */
