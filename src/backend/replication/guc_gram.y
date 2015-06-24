%{
#include "postgres.h"
#include "replication/syncrep.h"

static GroupNode *create_node(char *name);
static GroupNode *add_node(char *name, GroupNode *grp2);
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

%type <expr> result sync_expr list
 
%start result

%%

result: {SyncRepStandbyNames = NULL;}
| sync_expr									{SyncRepStandbyNames = $1; }
	;

sync_expr:
NAME { $$ = create_node($1);}
| INT '(' list ')' { $$ = create_group($1, $3);}
;

list:
	NAME { $$ = create_node($1);}
	| NAME ',' list { $$ = add_node($1, $3);}
	| INT '(' list ')' { $$ = create_group($1, $3);}
;
%%

static GroupNode *
create_node(char *name)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_NAME;

	/* For NAME */
	expr->walsnd = NULL;
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
	expr->walsnd = NULL;
	expr->next = NULL;
	
	/* For GROUP */
	expr->quorum = count;
	expr->group = grp1;

	elog(WARNING, "add group : quorum = %d, (group)->(%s)", expr->quorum, grp1->name);
	return expr;
}

static GroupNode *
add_node(char *name, GroupNode *grp2)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_NAME;

	/* For NAME */
	expr->walsnd = NULL;
	expr->next = NULL;
	expr->name = name;
	
	/* For GROUP */
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
