%{
#include "postgres.h"

#include "replication/syncrep.h"

typedef struct SyncGroupArray
{
	SyncGroupNode array[SYNC_REP_MAX_STANDBY_NODE];
	int	size;
} SyncGroupArray;


static List *list_make(SyncGroupNode *node);
static SyncGroupNode *create_name_node(char *name);
//static SyncGroupArray *add_node(List *node_list, SyncGroupNode *node);
static SyncGroupNode *create_group_node(int wait_num, List *node_list);

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
sync_element 					{ $$ = list_make((void *)$1);}
| sync_list ',' sync_element	{ $$ = lappend($1, (void *)$3);}
;

sync_node_group:
	INT '[' sync_list ']' 			{ $$ = create_group_node($1, $3);}
;

sync_element:
	NAME 							{ $$ = create_name_node($1);}
;


%%

static List *
list_make(SyncGroupNode *node)
{
	List 		*new_list;
	ListCell	*new_head;

	new_head = (ListCell *) malloc(sizeof(*new_head));
	new_head->next = NULL;

	new_list = (List *) malloc(sizeof(*new_list));
	new_list->type = T_List;
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	lfirst(new_list->head) = (void *)node;

	return new_list;
}

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
	name_node->SyncRepGetSyncedLsnsFn = NULL;
	name_node->SyncRepGetSyncStandbysFn = NULL;
	elog(WARNING, "create node : %s", name);
	return name_node;
}

static SyncGroupNode *
create_group_node(int wait_num, List *node_list)
{
	SyncGroupNode *group_node = (SyncGroupNode *) malloc(sizeof(SyncGroupNode));

	group_node->type = SYNC_REP_GROUP_GROUP | SYNC_REP_GROUP_MAIN;

	/* For NAME */
	group_node->name = "main";

	/* For GROUP */
	group_node->sync_method = SYNC_REP_METHOD_PRIORITY;
	group_node->wait_num = wait_num;
	group_node->member = node_list;
	group_node->SyncRepGetSyncedLsnsFn = SyncRepGetSyncedLsns;
	group_node->SyncRepGetSyncStandbysFn = SyncRepGetSyncStandbys;

	elog(WARNING, "create group : wait_num = %d", group_node->wait_num);
	return group_node;
}
/*
static SyncGroupArray *
add_node(SyncGroupArray *array, SyncGroupNode *node)
{
	array->array[array->size] = *node;
	array->size++;

	return array;
}
*/

#include "syncgroup_scanner.c"
