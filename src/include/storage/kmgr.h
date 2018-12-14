#ifndef KMGR_H
#define KMGR_H

#include "port/atomics.h"
#include "storage/smgr.h"
#include "storage/s_lock.h"
#include "storage/kmgr_xlog.h"

//#define DEBUG_TDE 1

#define InvalidKeyGeneration (KeyGeneration) 0
#define FirstKeyGeneration	(KeyGeneration) 1
#define MASTER_ENCRYPTION_KEY_LEN 64

typedef uint32 KeyGeneration;

typedef struct MasterKeyCtlData
{
	char key[MASTER_ENCRYPTION_KEY_LEN];
	pg_atomic_uint32 cur_generation;
	slock_t	mutex;
} MasterKeyCtlData;

typedef struct xl_enckey_rotate
{
	Oid oid;
} xl_enckey_rotate;

extern MasterKeyCtlData *MasterKeyCtl;

extern KeyGeneration CurrentKeyGeneration;
extern char *MasterKeyGetCommand;

extern char *GetEncryptionMasterKey(void);

extern void KmgrShmemInit(void);
extern Size KmgrShmemSize(void);
extern bool TablespaceHasEncryptionKey(Oid tablespaceoid);
extern char *GetTablespaceKey(Oid tblspoid);
extern void StartupKmgr(KeyGeneration generation);
extern void SetKeyGeneration(KeyGeneration generation);
extern void CreateTablespaceKey(Oid tablespaceoid);
extern void DropTablespaceKey(Oid tablespaceoid);
extern void CheckPointKmgr(void);

static inline KeyGeneration
GetCurrentKeyGeneration(void)
{
	return pg_atomic_read_u32(&(MasterKeyCtl->cur_generation));
}

#endif /* KMGR_H */
