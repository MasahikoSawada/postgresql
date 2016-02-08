%{
#include "postgres.h"

#include "replication/syncrep.h"

static SyncGroupNode *create_name_node(char *name);
static SyncGroupNode *add_node(SyncGroupNode *node_list, SyncGroupNode *node);
static SyncGroupNode *create_group_node(int wait_num, SyncGroupNode *node_list);

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

result:
	sync_node_group						{ SyncRepStandbyGroup = $1; }
;

sync_list:
sync_element 					{ $$ = $1;}
| sync_list ',' sync_element	{ $$ = add_node($1, $3);}
;

sync_node_group:
	INT '[' sync_list ']' 			{ $$ = create_group_node($1, $3);}
;

sync_element:
NAME 							{ $$ = create_name_node($1);}
;


%%

static SyncGroupNode *
create_name_node(char *name)
{
	SyncGroupNode *name_node = (SyncGroupNode *)malloc(sizeof(SyncGroupNode));

	name_node->type = SYNC_REP_GROUP_NAME;

	/* For NAME */
	name_node->name = strdup(name);

	/* For GROUP */
	name_node->sync_method = 0;
	name_node->wait_num = 0;
	name_node->member = NULL;
	name_node->next = NULL;
	name_node->SyncRepGetSyncedLsnsFn = NULL;
	name_node->SyncRepGetSyncStandbysFn = NULL;
	elog(WARNING, "create node : %s", name);
	return name_node;
}

static SyncGroupNode *
create_group_node(int wait_num, SyncGroupNode *node_list)
{
	SyncGroupNode *group_node = (SyncGroupNode *) malloc(sizeof(SyncGroupNode));

	group_node->type = SYNC_REP_GROUP_GROUP | SYNC_REP_GROUP_MAIN;

	/* For NAME */
	group_node->name = "main";

	/* For GROUP */
	group_node->sync_method = SYNC_REP_METHOD_PRIORITY;
	group_node->wait_num = wait_num;
	group_node->member = node_list;
	group_node->next = NULL;
	group_node->SyncRepGetSyncedLsnsFn = SyncRepGetSyncedLsns;
	group_node->SyncRepGetSyncStandbysFn = SyncRepGetSyncStandbys;

	elog(WARNING, "create group : wait_num = %d", group_node->wait_num);
	return group_node;
}

static SyncGroupNode *
add_node(SyncGroupNode *node_list, SyncGroupNode *node)
{
	SyncGroupNode *tmp = node_list;

	while(tmp->next != NULL) tmp = tmp->next;

	tmp->next = node;
	return node_list;;
}

#include "syncgroup_scanner.c"
