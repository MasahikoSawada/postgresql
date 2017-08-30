%{
/*-------------------------------------------------------------------------
 *
 * syncrep_gram.y				- Parser for synchronous_standby_names
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep_gram.y
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/syncrep.h"

/* Result of parsing is returned in one of these two variables */
SyncRepConfigData *syncrep_parse_result;
char	   *syncrep_parse_error_msg;
int			nest_level;

static SyncRepConfigData *create_syncrep_config_group(const char *num_sync,
					List *members, uint8 syncrep_method);
static SyncRepConfigData *create_syncrep_config_node(const char *standby_name);
static void dumpRepNode(SyncRepConfigData *config, int nested);

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

%}

%expect 0
%name-prefix="syncrep_yy"

%union
{
	char	   *str;
	List	   *list;
	SyncRepConfigData *config;
}

%token <str> NAME NUM JUNK ANY FIRST

%type <config>	result standby_name standby_list_with_method
				syncrep_config_elem standby_config
%type <list>	standby_list syncrep_config_list

%start result

%%
result:
		standby_config				{ syncrep_parse_result = $1; dumpRepNode($1, 0);}
;

standby_config:
		standby_list { $$ = create_syncrep_config_group("1", $1, SYNC_REP_PRIORITY); }
		| standby_list_with_method { $$ = $1;}
;

syncrep_config_list:
syncrep_config_elem { $$ = list_make1($1); }
| syncrep_config_list ',' syncrep_config_elem { $$ = lappend($1, $3); }
;

syncrep_config_elem:
standby_list_with_method { $$ = $1; }
| standby_name { $$ = $1; }
;

standby_list_with_method:
NUM '(' syncrep_config_list ')'				{ $$ = create_syncrep_config_group($1, $3, SYNC_REP_PRIORITY); }
| FIRST NUM '(' syncrep_config_list ')'		{ $$ = create_syncrep_config_group($2, $4, SYNC_REP_PRIORITY); }
| ANY NUM '(' syncrep_config_list ')'		{ $$ = create_syncrep_config_group($2, $4, SYNC_REP_QUORUM); }
;

standby_list:
standby_name						{ $$ = list_make1($1);}
| standby_list ',' standby_name		{ $$ = lappend($1, $3);}
;
standby_name:
NAME						{ $$ = create_syncrep_config_node($1);}
	;
%%

static void
dumpRepNode(SyncRepConfigData *config, int nested)
{
	int i;
	char *indent = palloc(sizeof(char) * (nested  * 3) + 1);

	for (i = 0; i < (nested * 3); i++)
		indent[i] = ' ';
	indent[strlen(indent)] = '\0';


	if (config->type == SYNC_REP_CONFIG_GROUP)
	{
		char *ptr;
		int j;

		fprintf(stderr, "%sGROUP(%s) : size = %3d, num_sync = %1d, nmembers = %2d\n",
				indent, config->syncrep_method == SYNC_REP_QUORUM ? "quorum" : "priori",
				config->config_size, config->num_sync, config->nmembers);

		ptr = config->member_names;
		for (j = 0; j < config->nmembers; j++)
		{
			SyncRepConfigData *c = (SyncRepConfigData *) ptr;

			if (c->type == SYNC_REP_CONFIG_GROUP)
				dumpRepNode(c, nested + 1);
			else
				fprintf(stderr, "%s-> NODE %s : size = %3d\n",
						indent, c->member_names, c->config_size);

			ptr += c->config_size;
		}
	}
	else
		fprintf(stderr, "%s-> NODE %s : size = %3d\n",
				indent, config->member_names, config->config_size);

}

static SyncRepConfigData *
create_syncrep_config_node(const char *standby_name)
{
	SyncRepConfigData *config;
	int size;

	size = offsetof(SyncRepConfigData, member_names);
	size += strlen(standby_name) + 1;

	config = (SyncRepConfigData *) palloc(size);

	config->type = SYNC_REP_CONFIG_NODE;
	config->config_size = size;
	config->num_sync = 0;
	config->syncrep_method = SYNC_REP_INVALID;
	config->nmembers = 1;
	config->has_group = false;
	strcpy(config->member_names, standby_name);

	return config;
}

static SyncRepConfigData *
create_syncrep_config_group(const char *num_sync, List *members, uint8 syncrep_method)
{
	SyncRepConfigData *config;
	int			size;
	ListCell   *lc;
	char	   *ptr;

	/* Compute space needed for flat representation */
	size = offsetof(SyncRepConfigData, member_names);
	foreach(lc, members)
	{
		SyncRepConfigData *c = (SyncRepConfigData *) lfirst(lc);

		/* group node cannot have group node that has group node */
		if (c->has_group)
			ereport(ERROR,
				(errmsg("more than 1 nest level is not supported\n")));

		size += c->config_size;
	}

	/* And transform the data into flat representation */
	config = (SyncRepConfigData *) palloc(size);

	config->type = SYNC_REP_CONFIG_GROUP;
	config->config_size = size;
	config->num_sync = atoi(num_sync);
	config->syncrep_method = syncrep_method;
	config->nmembers = list_length(members);
	config->has_group = false;
	ptr = config->member_names;
	foreach(lc, members)
	{
		SyncRepConfigData *c = (SyncRepConfigData *) lfirst(lc);

		if (c->type == SYNC_REP_CONFIG_GROUP)
			config->has_group = true;

		memcpy(ptr, c, c->config_size);
		ptr += c->config_size;
	}

	return config;
}

#include "syncrep_scanner.c"
