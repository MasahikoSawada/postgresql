/*-------------------------------------------------------------------------
 *
 * random_source.c
 *
 * Dynamically setup random generation function based on random_source_type
 * GUC parameter.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/random_source.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "port.h"
#include "utils/random_source.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"

const struct config_enum_entry random_source_type_options[] = {
	{"system", RANDOM_SOURCE_TYPE_SYSTEM, false},
#ifdef USE_OPENSSL
	{"openssl", RANDOM_SOURCE_TYPE_OSSL, false},
#endif
	{NULL, 0, false}
};

static const struct
{
	pg_strong_random_init_fn initfn;
	pg_strong_random_fn genfn;
}			random_source_type_table[] = {

	[RANDOM_SOURCE_TYPE_SYSTEM] = {
		.initfn = pg_strong_random_init_system,
		.genfn = pg_strong_random_system,
	},
#ifdef USE_OPENSSL
	[RANDOM_SOURCE_TYPE_OSSL] = {
		.initfn = pg_strong_random_init_openssl,
		.genfn = pg_strong_random_openssl,
	},
#endif
};

int			random_source_type = DEFAULT_RANDOM_SOURCE_TYPE;

/* Assign hook for random_source_type */
void
assign_random_source_type(int newval, void *extra)
{
	pg_strong_random_init_impl = random_source_type_table[newval].initfn;
	pg_strong_random_impl = random_source_type_table[newval].genfn;
}
