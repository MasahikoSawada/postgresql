/*-------------------------------------------------------------------------
 *
 * kmgrfunc.c
 *	  PostgreSQL key manager SQL interface
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/kmgr/kmgrfunc.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "utils/builtins.h"
#include "utils/kmgr.h"

Datum
pg_get_key(PG_FUNCTION_ARGS)
{
	text	*keyid = PG_GETARG_TEXT_P(0);
	bool ret;
	char *key;
	int keylen;

	KeyMgrInit();
	ret = GetKey(text_to_cstring(keyid),
				 TDEGetCurrentKeyGeneration(),
				 &key, &keylen);

	PG_RETURN_TEXT_P(cstring_to_text(key));
}

Datum
pg_generate_key(PG_FUNCTION_ARGS)
{
	text	*keyid = PG_GETARG_TEXT_P(0);
	text	*keytype = PG_GETARG_TEXT_P(1);
	bool ret;

	KeyMgrInit();
	ret = GenerateKey(text_to_cstring(keyid),
					  text_to_cstring(keytype));

	PG_RETURN_BOOL(ret);
}

Datum
pg_remove_key(PG_FUNCTION_ARGS)
{
	text	*keyid = PG_GETARG_TEXT_P(0);
	bool ret;

	KeyMgrInit();
	ret = RemoveKey(text_to_cstring(keyid),
					TDEGetCurrentKeyGeneration());

	PG_RETURN_BOOL(ret);
}

Datum
pg_rotate_key(PG_FUNCTION_ARGS)
{
	text	*keyid = PG_GETARG_TEXT_P(0);
	KeyGeneration newgen;

	KeyMgrInit();
	newgen = RotateKey(text_to_cstring(keyid));

	PG_RETURN_INT64(newgen);
}
