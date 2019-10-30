/*-------------------------------------------------------------------------
 *
 * walenc.c
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/walenc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "storage/encryption.h"
#include "storage/kmgr.h"

static char wal_encryption_iv[ENC_IV_SIZE];
static char wal_encryption_buf[XLOG_BLCKSZ];
static char encryption_key_cache[TDE_MAX_DEK_SIZE];
static bool key_cached = false;

static void
set_wal_encryption_iv(XLogSegNo segment, uint32 offset)
{
	char *p = wal_encryption_iv;
	uint32 pageno = offset / XLOG_BLCKSZ;

	/* Space for counter (4 byte) */
	memset(p, 0, ENC_WAL_AES_COUNTER_SIZE);
	p += ENC_WAL_AES_COUNTER_SIZE;

	/* Segment number (8 byte) */
	memcpy(p, &segment, sizeof(XLogSegNo));
	p += sizeof(XLogSegNo);

	/* Page number within a WAL segment (4 byte) */
	memcpy(p, &pageno, sizeof(uint32));
}

/*
 * Copy the contents of WAL page and encrypt it. Returns the copied and
 * encrypted WAL page.
 */
char *
EncryptXLog(char *page, Size nbytes, XLogSegNo segno, uint32 offset)
{
	Assert(nbytes <= XLOG_BLCKSZ);

	if (!key_cached)
	{
		KmgrGetWALEncryptionKey(encryption_key_cache);
		key_cached = true;
	}

	set_wal_encryption_iv(segno, offset);

	/* Copy to work buffer */
	memcpy(wal_encryption_buf, page, XLOG_BLCKSZ);

	pg_encrypt_data(wal_encryption_buf + XLogEncryptionOffset,
					wal_encryption_buf + XLogEncryptionOffset,
					nbytes - XLogEncryptionOffset,
					encryption_key_cache,
					wal_encryption_iv);

	return wal_encryption_buf;
}

/*
 * Decrypt a WAL page and return. Unlike EncryptXLog, this function encrypt
 * the given buffer directly.
 */
void
DecryptXLog(char *page, Size nbytes, XLogSegNo segno, uint32 offset)
{
	Assert(nbytes <= XLOG_BLCKSZ);

	if (!key_cached)
	{
		KmgrGetWALEncryptionKey(encryption_key_cache);
		key_cached = true;
	}

	set_wal_encryption_iv(segno, offset);

	pg_decrypt_data(page + XLogEncryptionOffset,
					page + XLogEncryptionOffset,
					nbytes - XLogEncryptionOffset,
					encryption_key_cache,
					wal_encryption_iv);
}

