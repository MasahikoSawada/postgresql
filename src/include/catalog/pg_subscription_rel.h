/* -------------------------------------------------------------------------
 *
 * pg_subscription_rel.h
 *		Local info about tables that come from the provider of a
 *		subscription (pg_subscription_rel).
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_REL_H
#define PG_SUBSCRIPTION_REL_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_subscription_rel definition. cpp turns this into
 *		typedef struct FormData_pg_subscription_rel
 * ----------------
 */
#define SubscriptionRelRelationId			6102
#define SubscriptionRelRelation_Rowtype_Id	6103

/* Workaround for genbki not knowing about XLogRecPtr */
#define pg_lsn XLogRecPtr

CATALOG(pg_subscription_rel,6102) BKI_ROWTYPE_OID(6103)
{
	Oid			subid;			/* Oid of subscription */
	Oid			relid;			/* Oid of relation */
	char		substate;		/* state of the relation in subscription */
	pg_lsn		sublsn;			/* remote lsn of the state change
								 * used for synchronization coordination */
} FormData_pg_subscription_rel;

typedef FormData_pg_subscription_rel *Form_pg_subscription_rel;

/* ----------------
 *		compiler constants for pg_subscription_rel
 * ----------------
 */
#define Natts_pg_subscription_rel			4
#define Anum_pg_subscription_rel_subid		1
#define Anum_pg_subscription_rel_subrelid	2
#define Anum_pg_subscription_rel_substate	3
#define Anum_pg_subscription_rel_sublsn		4

/* ----------------
 *		substate constants
 * ----------------
 */
#define		  SUBREL_STATE_UNKNOWN			'\0'	/* unknown state (sublsn NULL) */
#define		  SUBREL_STATE_INIT				'i'		/* initializing (sublsn NULL) */
#define		  SUBREL_STATE_DATA				'd'		/* data copy (sublsn NULL) */
#define		  SUBREL_STATE_SYNCWAIT			'w'		/* waiting for sync (sublsn set) */
#define		  SUBREL_STATE_CATCHUP			'c'		/* catchup (sublsn set) */
#define		  SUBREL_STATE_SYNCDONE			's'		/* synced (sublsn set) */
#define		  SUBREL_STATE_READY			'r'		/* ready (sublsn NULL) */

#endif   /* PG_SUBSCRIPTION_REL_H */
