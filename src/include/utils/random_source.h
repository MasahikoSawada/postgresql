/*-------------------------------------------------------------------------
 *
 * random_source.h
 *
 * Copyright (c) 2007-2025, PostgreSQL Global Development Group
 *
 * src/include/utils/random_source.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef RANDOM_SOURCE_H
#define RANDOM_SOURCE_H

typedef enum RandomSourceType
{
	RANDOM_SOURCE_TYPE_SYSTEM = 0,
#ifdef USE_OPENSSL
	RANDOM_SOURCE_TYPE_OSSL,
#endif
}			RandomSourceType;

#ifdef USE_OPENSSL
#define DEFAULT_RANDOM_SOURCE_TYPE RANDOM_SOURCE_TYPE_OSSL
#else
#define DEFAULT_RANDOM_SOURCE_TYPE RANDOM_SOURCE_TYPE_SYSTEM
#endif

/* GUC */
extern PGDLLIMPORT int random_source_type;

#endif							/* RANDOM_SOURCE_H */
