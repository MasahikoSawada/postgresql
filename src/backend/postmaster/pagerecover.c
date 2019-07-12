#include "postgres.h"

#include "nodes/pg_list.h"
#include "postmaster/pagerecover.h"
#include "storage/procsignal.h"
#include "utils/memutils.h"
#include "utils/varlena.h"

char *pageRecoveryTargetRelFiles_str = NULL;
static List *pageRecoveryTargetRelFiles = NIL;

void
PageRecoverMain(void)
{
	pg_usleep(20000000);
	return;
}

bool
check_page_recovery_target_relfiles(const char **newval, void **extra,
									GucSource source)
{
	char	*rawstring;
	List	*elemlist;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* parse string into list of identifiers */
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	pfree(rawstring);
	list_free(elemlist);

	return true;
}
