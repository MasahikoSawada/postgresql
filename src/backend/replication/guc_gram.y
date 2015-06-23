%{
#include "postgres.h"
#include "replication/syncrep.h"

static GroupNode *create_node(char *name);
static GroupNode *add_node(char *name, GroupNode *grp2);
static GroupNode *add_group(int count, GroupNode *grp1, GroupNode *grp2);
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

%type <expr> result input values
 
%start result

%%

result:										{SyncRepStandbyNames = NULL;}
	| input									{SyncRepStandbyNames = $1; }
	;

input:	NAME								{$$ = create_node($1);}
		| INT '(' values ')'				{$$ = create_group($1, $3);}
	;

values:	NAME								{$$ = create_node($1);}
		| NAME ',' values					{$$ = add_node($1, $3);}
		| INT '(' values ')'				{$$ = create_group($1, $3);}
		| INT '(' values ')' ',' values		{$$ = add_group($1, $3, $6);}
	;
%%

static GroupNode *
create_node(char *name)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_NAME;
	expr->gcount = 1;
	expr->u.node.name = name;
	return expr;
}

static GroupNode *
create_group(int count, GroupNode *grp1)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_GROUP;
    expr->gcount = count;
	expr->u.groups.lgroup = grp1;
	expr->u.groups.rgroup = NULL;
	return expr;
}

static GroupNode *
add_node(char *name, GroupNode *grp2)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_GROUP;
    expr->gcount = -1;
	expr->u.groups.lgroup = create_node(name);
	expr->u.groups.rgroup = grp2;
	return expr;
}

static GroupNode *
add_group(int count, GroupNode *grp1, GroupNode *grp2)
{
	GroupNode *expr = malloc(sizeof(GroupNode));

	expr->gtype = GNODE_GROUP;
    expr->gcount = -1;
	expr->u.groups.lgroup = create_group(count, grp1);
	expr->u.groups.rgroup = grp2;
	return expr;
}

#include "guc_scanner.c"
