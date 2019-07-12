#include "postgres.h"

#include "utils/guc.h"

extern char *pageRecoveryTargetRelFiles_str;

extern void PageRecoverMain(void);
extern bool check_page_recovery_target_relfiles(const char **newval, void **extra,
												GucSource source);
