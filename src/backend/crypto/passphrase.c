/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Encryption key management module.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/kms/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/err.h>
#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xlog.h"
#include "common/sha2.h"
#include "crypto/kmgr.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define KMGR_PROMPT_MSG "Enter database encryption pass phrase:"

#define EncryptionKeySize 256
#define KEY_TDE_MAX_DEK_SIZE 256

/*
 * Run cluster_passphrase_command
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf
 * The return value is the length of the actual result.
 */
int
kms_run_passphrase_command(char *buf)
{
	StringInfoData 	command;
	char	   	*p;
	FILE	   	*fh;
	int		pclose_rc;
	size_t		len = 0;

	if (cluster_passphrase_command == NULL || strlen(cluster_passphrase_command) == 0)
		return 0;

	initStringInfo(&command);

	for (p = cluster_passphrase_command; *p; p++)
	{
		if (p[0] == '%')
		{
			switch (p[1])
			{
				case 'p':
					appendStringInfoString(&command, KMGR_PROMPT_MSG);
					p++;
					break;
				case '%':
					appendStringInfoChar(&command, '%');
					p++;
					break;
				default:
					appendStringInfoChar(&command, p[0]);
			}
		}
		else
			appendStringInfoChar(&command, p[0]);
	}
	
	fh = OpenPipeStream(command.data, "r");
	if (fh == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command.data)));

	if (!fgets(buf, TDE_MAX_PASSPHRASE_LEN, fh))
	{
		if (ferror(fh))
		{
			pfree(command.data);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from command \"%s\": %m",
							command.data)));
		}
	}
	pclose_rc = ClosePipeStream(fh);
	if (pclose_rc == -1)
	{
		pfree(command.data);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
	}
	else if (pclose_rc != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("command \"%s\" failed",
						command.data),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
	}

	/* strip trailing newline */
	len = strlen(buf);
	if (len > 0 && buf[len - 1] == '\n')
		buf[--len] = '\0';

	pfree(command.data);

	return len;
}

/*
 * Hash the given passphrase and extract it into KEK and HMAC
 * key.
 */
void
kms_get_kek_and_hmackey(char *passphrase, Size passlen,
					keydata_t kek_out[TDE_KEK_SIZE],
					keydata_t hmackey_out[TDE_HMAC_KEY_SIZE])
{
	keydata_t enckey_and_hmackey[PG_SHA512_DIGEST_LENGTH];
	pg_sha512_ctx ctx;

	pg_sha512_init(&ctx);
	pg_sha512_update(&ctx, (const uint8 *) passphrase, passlen);
	pg_sha512_final(&ctx, enckey_and_hmackey);

	/*
	 * SHA-512 results 64 bytes. We extract it into two keys for
	 * each 32 bytes: one for key encryption and another one for
	 * HMAC.
	 */
	memcpy(kek_out, enckey_and_hmackey, TDE_KEK_SIZE);
	memcpy(hmackey_out, enckey_and_hmackey + TDE_KEK_SIZE, TDE_HMAC_KEY_SIZE);
}

/*
 * Verify the correctness of the given passphrase. We compute HMACs of the
 * wrapped key RDEK using the HMAC key retrived from the user
 * provided passphrase. And then we compare it with the HMAC stored alongside
 * the controlfile. Return true if both HMACs are matched, meaning the given
 * passphrase is correct. Otherwise return false.
 */
bool
kms_verify_passphrase(char *passphrase, int passlen, WrappedEncKeyWithHmac *rdek)
{
	keydata_t 	user_kek[TDE_KEK_SIZE];
	keydata_t 	user_hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t 	result_hmac[TDE_HMAC_SIZE];
	int		wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;

	elog(WARNING, "%s:%d", passphrase, passlen);
	kms_get_kek_and_hmackey(passphrase, passlen, user_kek, user_hmackey);

	/* Verify both HMACs of RDEK and WDEK */
	kms_compute_hmac(user_hmackey, TDE_HMAC_KEY_SIZE,
					rdek->key, wrapped_keysize,
					result_hmac);

	if (memcmp(result_hmac, rdek->hmac, TDE_HMAC_SIZE) != 0)
		return false;

	return true;
}

