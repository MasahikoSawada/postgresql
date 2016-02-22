%{
#include "postgres.h"

/* No reason to constrain amount of data slurped */
#define YY_READ_BUF_SIZE 16777216

#define BUFSIZE 8192

/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;

/* Functions for handling double quoted string */
static void init_xd_string(void);
static void addlit_xd_string(char *ytext, int yleng);
static void addlitchar_xd_string(unsigned char ychar);

char  *scanbuf;
char *xd_string;
int	xd_size; /* actual size of xd_string */
int	xd_len; /* string length of xd_string  */
%}
%option 8bit
%option never-interactive
%option nounput
%option noinput
%option noyywrap
%option warn
%option prefix="syncgroup_yy"

/*
 * <xd> delimited identifiers (double-quoted identifiers)
 */
%x xd

space		[ \t\n\r\f]
non_newline	[^\n\r]
whitespace	({space}+)
self		[\[\]\,]
asterisk	\*

/*
 * Basically all ascii characteres except for {self} and {whitespace} are allowed
 * to be used for node name. These special charater could be used by double-quoted.
 */
node_name	[a-zA-Z0-9\!\#\$\%\&\'\(\)\-\=\^\~\|\\\{\}\@\`\;\:\+\/\.\_\*]
dquoted_name	[a-zA-Z0-9\!\#\$\%\&\'\(\)\-\=\^\~\|\\\{\}\@\`\;\:\+\/\.\_\*\,\[\]\ ]

/* Double-quoted string */
dquote		\"
xdstart		{dquote}
xddouble	{dquote}{dquote}
xdstop		{dquote}
xdinside	{dquoted_name}+

%%
{whitespace}	{ /* ignore */ }

{xdstart} {
				init_xd_string();
				BEGIN(xd);
		}
<xd>{xddouble} {
				addlitchar_xd_string('\"');
		}
<xd>{xdinside} {
				addlit_xd_string(yytext, yyleng);
		}
<xd>{xdstop} {
				xd_string[xd_len] = '\0';
				yylval.str = xd_string;
				BEGIN(INITIAL);
				return NAME;
		}
[1-9][0-9]* {
				yylval.val = atoi(yytext);
				return INT;
		}
{node_name}{node_name}+ {
				yylval.str = strdup(yytext);
				return NAME;
			}
{asterisk} {
				yylval.str = strdup(yytext);
				return AST;
			}
{self} {
				return yytext[0];
		}
. {
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("syntax error: unexpected character \"%s\"", yytext)));
	}
%%

void
yyerror(const char *message)
{
	ereport(ERROR,
		(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("%s at or near \"%s\" in \"%s\"", message,
			       yytext, scanbuf)));
}

void
syncgroup_scanner_init(const char *str)
{
	Size		slen = strlen(str);

	/*
	 * Might be left over after ereport()
	 */
	if (YY_CURRENT_BUFFER)
		yy_delete_buffer(YY_CURRENT_BUFFER);

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	scanbuf = (char *) palloc(slen + 2);
	memcpy(scanbuf, str, slen);
	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	scanbufhandle = yy_scan_buffer(scanbuf, slen + 2);
}

void
syncgroup_scanner_finish(void)
{
	yy_delete_buffer(scanbufhandle);
	scanbufhandle = NULL;
}

static void
init_xd_string()
{
	xd_string = palloc(sizeof(char) * BUFSIZE);
	xd_size = BUFSIZE;
	xd_len = 0;
}

static void
addlit_xd_string(char *ytext, int yleng)
{
	/* enlarge buffer if needed */
	if ((xd_len + yleng) > xd_size)
		xd_string = repalloc(xd_string, xd_size + BUFSIZE);

	memcpy(xd_string + xd_len, ytext, yleng);
	xd_len += yleng;
}

static void
addlitchar_xd_string(unsigned char ychar)
{
	/* enlarge buffer if needed */
	if ((xd_len + 1) > xd_size)
		xd_string = repalloc(xd_string, xd_size + BUFSIZE);

	xd_string[xd_len] = ychar;
	xd_len += 1;
}