/*-------------------------------------------------------------------------
 *
 * encryption.h
 *  Full database encryption support
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/encryption.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENCRYPTION_H
#define ENCRYPTION_H

#define MAX_ENCRYPTION_KEY_LEN 256

char *EncryptOneBuffer(char *raw_buf, char *key);
char *DecryptOneBuffer(char *encrypted_buf, char *key);

#endif	/* ENCRYPTION_H */
