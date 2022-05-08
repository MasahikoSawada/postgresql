#ifndef DDL_DEPARSE_H
#define DDL_DEPARSE_H

#include "tcop/deparse_utility.h"

extern char *deparse_utility_command(CollectedCommand *cmd);
extern char *ddl_deparse_json_to_string(char *jsonb);

#endif		/* DDL_DEPARSE_H */
