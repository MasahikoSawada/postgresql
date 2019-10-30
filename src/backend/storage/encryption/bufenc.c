/*-------------------------------------------------------------------------
 *
 * bufenc.c
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/bufenc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/bufpage.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"

static char buf_encryption_iv[ENC_IV_SIZE];
static char encryption_key_cache[TDE_MAX_DEK_SIZE];
static bool key_cached = false;

static void set_buffer_encryption_iv(Page page, BlockNumber blocknum);

void
EncryptBufferBlock(BlockNumber blocknum, Page page)
{
	if (!key_cached)
	{
		KmgrGetRelationEncryptionKey(encryption_key_cache);
		key_cached = true;
	}

	set_buffer_encryption_iv(page, blocknum);
	pg_encrypt_data(page + PageEncryptOffset,
					page + PageEncryptOffset,
					SizeOfPageEncryption,
					encryption_key_cache,
					buf_encryption_iv);
}

void
DecryptBufferBlock(BlockNumber blocknum, Page page)
{
	if (!key_cached)
	{
		KmgrGetRelationEncryptionKey(encryption_key_cache);
		key_cached = true;
	}

	set_buffer_encryption_iv(page, blocknum);
	pg_decrypt_data(page + PageEncryptOffset,
					page + PageEncryptOffset,
					SizeOfPageEncryption,
					encryption_key_cache,
					buf_encryption_iv);
}

/*
 * Nonce for buffer encryption consists of page lsn, block number
 * and counter. The counter is a counter value for CTR cipher mode.
 */
static void
set_buffer_encryption_iv(Page page, BlockNumber blocknum)
{
	char *p = buf_encryption_iv;

	MemSet(buf_encryption_iv, 0, ENC_IV_SIZE);

	/* page lsn (8 byte) */
	memcpy(p, &((PageHeader) page)->pd_lsn, sizeof(PageXLogRecPtr));
	p += sizeof(PageXLogRecPtr);

	/* block number (4 byte) */
	memcpy(p, &blocknum, sizeof(BlockNumber));
	p += sizeof(BlockNumber);

	/* Space for counter (4 byte) */
	memset(p, 0, ENC_BUFFER_AES_COUNTER_SIZE);
	p += ENC_BUFFER_AES_COUNTER_SIZE;

}

