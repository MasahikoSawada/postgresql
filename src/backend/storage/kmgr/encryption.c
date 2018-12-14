#include "postgres.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#include "storage/encryption.h"
#include "storage/kmgr.h"
#include "utils/memutils.h"

static char	*encryption_buf = NULL;
static int	encryption_bufsize = 0;
static char	*decryption_buf = NULL;
static int	decryption_bufsize = 0;

/*
 * Encryption data using XTS mode.
 */
void
encrypt_data(const char *in, char *out, Size size, const char *key,
			 const char *tweak)
{
	EVP_CIPHER_CTX *ctx;
	int out_size;

	Assert(size % 16 == 0);

	if (in == out)
	{
		if (encryption_bufsize == 0)
		{
			encryption_buf = MemoryContextAlloc(TopMemoryContext,
												size);
			encryption_bufsize = size;
		}
		if (encryption_bufsize < size)
		{
			encryption_buf = repalloc(encryption_buf, size);
			encryption_bufsize = size;
		}

		memcpy(encryption_buf, in, size);
		in = encryption_buf;
	}

	if ((ctx = EVP_CIPHER_CTX_new()) == NULL)
		ereport(ERROR, (errmsg("could not initlaize cipher ctx")));

	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_xts(), NULL,
						   (unsigned char *) key,
						   (unsigned char *) tweak) != 1)
		ereport(ERROR, (errmsg("could not initlaize encryption")));

	EVP_CIPHER_CTX_set_padding(ctx, 0);

	if (EVP_EncryptUpdate(ctx, (unsigned char *) out, &out_size,
						  (unsigned char *) in, size) != 1)
	{
//		unsigned long e = ERR_get_error();
//		char errbuf[8192];
//		char *res = ERR_error_string(e, errbuf);
		ERR_print_errors_fp(stderr);
		ereport(ERROR, (errmsg("could not encrypt data")));
	}

	Assert(size == out_size);

#ifdef DEBUG_TDE
	{
		char *out_text = palloc(out_size + 1);
		char *in_text = palloc(size + 1);
		char *key_text = palloc(64 + 1);
		memcpy(out_text, out, out_size);
		memcpy(in_text, in, size);
		memcpy(key_text, key, 64);
		key_text[64] = '\0';
		out_text[out_size] = '\0';
		in_text[size] = '\0';
		fprintf(stderr, "  == encrypt key : key \"%s\", tweak : \"%s\", in : \"%s\", out(%d) : \"%s\"\n",
			 key_text, tweak, in_text, out_size, out_text);
	}
#endif

	if (EVP_CIPHER_CTX_cleanup(ctx) != 1)
		ereport(ERROR, (errmsg("could not cleanup cipher ctx")));
}

void
decrypt_data(const char *in, char *out, Size size, const char *key,
			 const char *tweak)
{
	EVP_CIPHER_CTX *ctx;
	int out_size;

	Assert(size % 16 == 0);

	if (in == out)
	{
		if (decryption_bufsize == 0)
		{
			decryption_buf = MemoryContextAlloc(TopMemoryContext,
												size);
			decryption_bufsize = size;
		}
		if (decryption_bufsize < size)
		{
			decryption_buf = repalloc(decryption_buf, size);
			decryption_bufsize = size;
		}

		MemSet(decryption_buf, 0, decryption_bufsize);
		memcpy(decryption_buf, in, size);
		in = decryption_buf;
	}

	if ((ctx = EVP_CIPHER_CTX_new()) == NULL)
		ereport(ERROR, (errmsg("could not initlaize cipher ctx")));

	if (EVP_DecryptInit_ex(ctx, EVP_aes_256_xts(), NULL,
						   (unsigned char *) key,
						   (unsigned char *) tweak) != 1)
		ereport(ERROR, (errmsg("could not initlaize decryption")));

	EVP_CIPHER_CTX_set_padding(ctx, 0);

	if (EVP_DecryptUpdate(ctx, (unsigned char *) out, &out_size,
						  (unsigned char *) in, size) != 1)
		ereport(ERROR, (errmsg("could not decrypt data")));

	Assert(size == out_size);

#ifdef DEBUG_TDE
	{
		char *out_text = palloc(out_size + 1);
		char *in_text = palloc(size + 1);
		char *key_text = palloc(64 + 1);
		memcpy(out_text, out, out_size);
		memcpy(in_text, in, size);
		memcpy(key_text, key, 64);
		out_text[out_size] = '\0';
		in_text[size] = '\0';
		key_text[64] = '\0';
		fprintf(stderr, "  == decrypt key : \"%s\", tweak : \"%s\", in : \"%s\", out(%d) : \"%s\"\n",
			 key_text, tweak, in_text, out_size, out_text);
//		elog(NOTICE, "  == decrypt key : \"%s\", tweak : \"%s\"",
//			 key, tweak);
//		elog(NOTICE, "Decryption key : \"%s\", tweak : \"%s\", in : \"%s\", out(%d) : \"%s\"",
//			 key, tweak, in_text, out_size, out_text);
	}
#endif

	if (EVP_CIPHER_CTX_cleanup(ctx) != 1)
		ereport(ERROR, (errmsg("could not cleanup cipher ctx")));
}

void
BufferEncryptionTweak(char *tweak, RelFileNode *rnode, ForkNumber forknum,
					  BlockNumber blocknum)
{
	uint32 fork_and_block = (forknum << 24) ^ blocknum;
	memcpy(tweak, rnode, sizeof(RelFileNode));
	memcpy(tweak + sizeof(RelFileNode), &fork_and_block, sizeof(uint32));
#ifdef DEBUG_TDE
	elog(NOTICE, "make tweak \"%s\"", tweak);
#endif
}

void
TablespaceKeyEncryptionTweak(char *tweak, Oid tablespaceoid)
{
	memcpy(tweak, &tablespaceoid, sizeof(Oid));
}
