%{
#include "postgres.h"
#include "replication/syncrep.h"

static GroupNode *create_node(char *name);
static GroupNode *add_node(GroupNode *grp1, GroupNode *grp2);
static GroupNode *add_new_node(char *name, GroupNode *grp2);
static GroupNode *create_group(int count, GroupNode *grp1);

#define YYMALLOC palloc
#define YYFREE   pfree

%}

%expect 0
%name-prefix="repl_guc_yy"

%union
{
	int32		val;
	char	   *str;
	GroupNode  *expr;
}

%token <str> NAME
%token <val> INT

%type <expr> result sync_list sync_element sync_node_group

%start result

%%

result: {SyncRepStandbyNames = NULL;}
| sync_list									{SyncRepStandbyNames = $1; }
	;

sync_list:
sync_element { $$ = $1;}
| sync_list ',' NAME { $$ = add_new_node($3, $1);}
| sync_list ',' sync_node_group { $$ = add_node($1, $3);}
;

sync_element:
NAME { $$ = create_node($1);}
| sync_node_group { $$ = $1}
;

sync_node_group:
INT '(' sync_list ')' { $$ = create_group($1, $3);}
;

%%

static GroupNode *
create_node(char *name)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_NAME;

	/* For NAME */
	expr->next = NULL;
	expr->name = name;

	/* For GROUP */
	expr->quorum = -1;
	expr->group = NULL;
	elog(WARNING, "create node : %s", name);
	return expr;
}

static GroupNode *
create_group(int count, GroupNode *grp1)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_GROUP;

	/* For NAME */
	expr->next = NULL;
	expr->name = NULL;

	/* For GROUP */
	expr->quorum = count;
	expr->group = grp1;

	elog(WARNING, "add group : quorum = %d, (group)->(%s)", expr->quorum, grp1->name);
	return expr;
}

static GroupNode *
add_node(GroupNode *grp1, GroupNode *grp2)
{
	grp1->next = grp2;
	elog(WARNING, "add node : (%s)->(%s:%d)",
		 grp1->name,
		 grp2->gtype == GNODE_NAME ? grp2->name : "GGGROUPPP",
		 grp2->quorum);
	return grp1;
}


static GroupNode *
add_new_node(char *name, GroupNode *grp2)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_NAME;

	expr->next = NULL;
	expr->name = name;

	expr->quorum = -1;
	expr->group = NULL;

	expr->next = grp2;

	elog(WARNING, "add node : (%s)->(%s:%d)",
		 expr->name,
		 grp2->gtype == GNODE_NAME ? grp2->name : "GGGROUPPP",
		 grp2->quorum);

	return expr;
}

#include "guc_scanner.c"
