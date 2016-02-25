%{
/*-------------------------------------------------------------------------
 *
 * syncgroup_gram.y				- Parser for synchronous replication group
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncgroup_gram.y
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "replication/syncrep.h"

static SyncGroupNode *create_name_node(char *name);
static SyncGroupNode *add_node(SyncGroupNode *node_list, SyncGroupNode *node);
static SyncGroupNode *create_group_node(char *wait_num, SyncGroupNode *node_list);

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
%name-prefix="syncgroup_yy"

%union
{
	char	   *str;
	SyncGroupNode  *expr;
}

%token <str> NAME NUM
%token <str> AST

%type <expr> result sync_list sync_list_ast sync_element sync_element_ast
			 sync_node_group sync_group_old sync_group

%start result

%%
result:
		sync_node_group						{ SyncRepStandbys = $1; }
;
sync_node_group:
		sync_group_old						{ $$ = $1; }
		| sync_group						{ $$ = $1; }
;
sync_group_old:
		sync_list							{ $$ = create_group_node("1", $1); }
		| sync_list_ast						{ $$ = create_group_node("1", $1); }
;
sync_group:
		NUM '[' sync_list ']'	 			{ $$ = create_group_node($1, $3); }
		| NUM '[' sync_list_ast ']'			{ $$ = create_group_node($1, $3); }
;
sync_list:
		sync_element 						{ $$ = $1;}
		| sync_list ',' sync_element		{ $$ = add_node($1, $3);}
;
sync_list_ast:
		sync_element_ast					{ $$ = $1;}
		| sync_list ',' sync_element_ast	{ $$ = add_node($1, $3);}
;
sync_element:
		NAME	 							{ $$ = create_name_node($1); }
		| NUM								{ $$ = create_name_node($1); }
;
sync_element_ast:
		AST									{ $$ = create_name_node($1); }
;
%%

static SyncGroupNode *
create_name_node(char *name)
{
	SyncGroupNode *name_node = (SyncGroupNode *)malloc(sizeof(SyncGroupNode));

	/* Common information */
	name_node->type = SYNC_REP_GROUP_NAME;
	name_node->name = strdup(name);
	name_node->next = NULL;

	/* For GROUP node */
	name_node->sync_method = 0;
	name_node->wait_num = 0;
	name_node->members = NULL;
	name_node->SyncRepGetSyncedLsnsFn = NULL;
	name_node->SyncRepGetSyncStandbysFn = NULL;

	return name_node;
}

static SyncGroupNode *
create_group_node(char *wait_num, SyncGroupNode *node_list)
{
	SyncGroupNode *group_node = (SyncGroupNode *)malloc(sizeof(SyncGroupNode));

	/* For NAME node */
	group_node->type = SYNC_REP_GROUP_GROUP | SYNC_REP_GROUP_MAIN;
	group_node->name = "main";
	group_node->next = NULL;

	/* For GROUP node */
	group_node->sync_method = SYNC_REP_METHOD_PRIORITY;
	group_node->wait_num = atoi(wait_num);
	group_node->members = node_list;
	group_node->SyncRepGetSyncedLsnsFn = SyncRepGetSyncedLsnsUsingPriority;
	group_node->SyncRepGetSyncStandbysFn = SyncRepGetSyncStandbysUsingPriority;

	return group_node;
}

static SyncGroupNode *
add_node(SyncGroupNode *node_list, SyncGroupNode *node)
{
	SyncGroupNode *tmp = node_list;

	/* Add node to tailing of node_list */
	while(tmp->next != NULL) tmp = tmp->next;

	tmp->next = node;
	return node_list;
}

#include "syncgroup_scanner.c"
