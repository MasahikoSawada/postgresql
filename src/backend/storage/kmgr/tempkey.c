/*-------------------------------------------------------------------------
 *
 * tempkey.c
 *	 Provide backend-local temporary key
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/kmgr/tempkey.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/kmgr.h"

static bool tempkey_initialized = false;
static char tempkey[ENCRYPTION_KEY_SIZE];

char *
GetBackendKey(void)
{
	if (!tempkey_initialized)
	{
		char keybuf[ENCRYPTION_KEY_SIZE];
		int ret;

		ret = pg_strong_random(keybuf, ENCRYPTION_KEY_SIZE);
		if (!ret)
			ereport(ERROR,
					(errmsg("failed to generate temporary key")));

		memcpy(tempkey, keybuf, ENCRYPTION_KEY_SIZE);
		tempkey_initialized = true;
	}

	return tempkey;
}
