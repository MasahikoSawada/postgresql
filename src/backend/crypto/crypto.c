/*-------------------------------------------------------------------------
 *
 * crypto.c
 *	 Encryption and decryption functions using internal SQL key
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/crypto/crypto.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"

#include "common/sha2.h"
#include "common/kmgr_utils.h"
#include "crypto/kmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "storage/ipc.h"

/* AEAD context initialized with the SQL key to encrypt and decrypt data */
static PgAeadCtx *AeadCtx = NULL;
static bool	crypto_initialized = false;

static void initCryptoCtx(void);
static void ShutdownCrypto(int code, Datum arg);
static bool crypto_internal(uint8 *in, int inlen, uint8 *out, int *outlen,
							bool for_enc);

/* Initialize kmgr context in backend process */
static void
initCryptoCtx(void)
{
	const CryptoKey	*sqlkey;
	uint8		*sql_enckey;
	uint8		*sql_hmackey;
	MemoryContext oldctx;

	Assert(AeadCtx == NULL);

	on_shmem_exit(ShutdownCrypto, 0);

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Prepare AEAD context with SQL internal key for pg_encrypt and pg_decrypt
	 * SQL functions.
	 */
	sqlkey = KmgrGetKey(KMGR_SQL_KEY_ID);
	sql_enckey = (uint8 *) sqlkey->key;
	sql_hmackey = (uint8 *) ((char *) sqlkey->key + PG_AEAD_ENC_KEY_LEN);
	AeadCtx = pg_create_aead_ctx(sql_enckey, sql_hmackey);
	if (!AeadCtx)
		elog(ERROR, "could not initialize encryption contect");

	MemoryContextSwitchTo(oldctx);
	crypto_initialized = true;
}

/* Callback function to cleanup AEAD context */
static void
ShutdownCrypto(int code, Datum arg)
{
	if (AeadCtx)
		pg_free_aead_ctx(AeadCtx);
}

/* Internal function to encrypt or decrypt the given  */
static bool
crypto_internal(uint8 *in, int inlen, uint8 *out, int *outlen, bool for_enc)
{
	if (!crypto_initialized)
		initCryptoCtx();

	Assert(AeadCtx != NULL);

	if (for_enc)
		return pg_aead_encrypt(AeadCtx, in, inlen, out, outlen);
	else
		return pg_aead_decrypt(AeadCtx, in, inlen, out, outlen);
}

/*
 * SQL function to encrypt the given data by the internal SQL key.
 */
Datum
pg_encrypt(PG_FUNCTION_ARGS)
{
	text	   *data = PG_GETARG_TEXT_PP(0);
	bytea	   *res;
	int			datalen;
	int			reslen;
	int			len;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not encrypt data because key management is not supported")));

	datalen = VARSIZE_ANY_EXHDR(data);
	reslen = VARHDRSZ + AEADSizeOfCipherText(datalen);
	res = palloc(reslen);

	if (!crypto_internal((uint8 *) VARDATA_ANY(data), datalen,
							(uint8 *) VARDATA(res), &len, true))
		elog(ERROR, "could not encrypt data");

	SET_VARSIZE(res, reslen);

	PG_RETURN_BYTEA_P(res);
}

/*
 * SQL function to decrypt the given data by the internal SQL key.
 */
Datum
pg_decrypt(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_PP(0);
	text	   *res;
	int			datalen;
	int			buflen;
	int			len;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not decrypt data because key management is not supported")));

	datalen = VARSIZE_ANY_EXHDR(data);

	/* Check if the input length is more than minimum length of cipher text */
	if (datalen < AEADSizeOfCipherText(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid input data")));

	buflen = VARHDRSZ + AEADSizeOfPlainText(datalen);
	res = palloc(buflen);

	if (!crypto_internal((uint8 *) VARDATA_ANY(data), datalen,
							(uint8 *) VARDATA(res), &len, false))
		elog(ERROR, "could not decrypt the given secret");

	/*
	 * The size of plaintext data can be smaller than the size estimated before
	 * decryption since the padding is removed during decryption.
	 */
	SET_VARSIZE(res, VARHDRSZ + len);

	PG_RETURN_TEXT_P(res);
}
