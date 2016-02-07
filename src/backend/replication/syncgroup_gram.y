%{
#include "postgres.h"

#include "replication/syncrep.h"

static SyncGroupNode *create_name_node(char *name);
static SyncGroupNode *add_node(SyncGroupNode *node1, SyncGroupNode *node2);
static SyncGroupNode *create_group_node(int wait_num, SyncGroupNode *list);

#define YYMALLOC palloc
#define YYFREE   pfree

%}

%expect 0
%name-prefix="syncgroup_yy"

%union
{
	int32		val;
	char	   *str;
	SyncGroupNode  *expr;
}

%token <str> NAME
%token <val> INT

%type <expr> result sync_list sync_element sync_node_group

%start result

%%

result:				{SyncRepStandbyGroup = NULL;}
	| sync_list									{SyncRepStandbyGroup = $1; }
	;

sync_list:
	sync_element 					{ $$ = $1;}
	| sync_list ',' sync_element	{ $$ = add_node($3, $1);}
;

sync_element:
	NAME 							{ $$ = create_name_node($1);}
	| sync_node_group 				{ $$ = $1}
;

sync_node_group:
	INT '[' sync_list ']' 			{ $$ = create_group_node($1, $3);}
;

%%

static SyncGroupNode *
create_name_node(char *name)
{
	SyncGroupNode *name_node = malloc(sizeof(SyncGroupNode));

	name_node->type = SYNCGROUP_NAME;

	/* For NAME */
	name_node->next = NULL;
	name_node->name = strdup(name);

	/* For GROUP */
	name_node->wait_num = -1;
	name_node->member = NULL;
	name_node->SyncRepGetSyncedLsnsFn = SyncRepGetSyncedLsns;
	elog(WARNING, "create node : %s", name);
	return name_node;
}

static SyncGroupNode *
create_group_node(int wait_num, SyncGroupNode *list)
{
	SyncGroupNode *group_node = malloc(sizeof(SyncGroupNode));

	group_node->type = SYNCGROUP_GROUP;

	/* For NAME */
	group_node->next = NULL;
	group_node->name = "main";

	/* For GROUP */
	group_node->wait_num = wait_num;
	group_node->member = list;

	elog(WARNING, "add group : wait_num = %d, (group)->(%s)", group_node->wait_num, group_node->name);
	return group_node;
}

static SyncGroupNode *
add_node(SyncGroupNode *node1, SyncGroupNode *node2)
{
	node1->next = node2;
	return node1;
}

#include "syncgroup_scanner.c"
