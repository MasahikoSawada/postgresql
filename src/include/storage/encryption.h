#ifndef ENCRPTION_H
#define ENCRPTION_H

#include "storage/smgr.h"
#include "storage/relfilenode.h"
#include "storage/block.h"

#define TWEAK_SIZE 16

extern void encrypt_data(const char *in, char *out, Size size, const char *key,
				  const char *tweak);
extern void decrypt_data(const char *in, char *out, Size size, const char *key,
				  const char *tweak);
extern void BufferEncryptionTweak(char *tweak, RelFileNode *rnode, ForkNumber forknum,
						   BlockNumber blockNum);
extern void TablespaceKeyEncryptionTweak(char *tweak, Oid tablespaceoid);

#endif /* ENCRPTIO_H */
