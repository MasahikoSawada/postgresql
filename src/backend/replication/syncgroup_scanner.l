%{
#include "postgres.h"

/* No reason to constrain amount of data slurped */
#define YY_READ_BUF_SIZE 16777216

/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;

%}
%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="syncgroup_yy"

/*
 * <xd> delimited identifiers (double-quoted identifiers)
 */
%x xd

space		[ \t\n\r\f]
separater	,
non_newline	[^\n\r]
whitespace	({space}+)
self		[\[\]]

ascii		[\x20-\x70]

/* Double quote */
dquote		\"
xdstart		{dquote}
xdstop		{dquote}
xddouble	{dquote}{dquote}
xdinside	[^\"]+

%%

{whitespace}	{ /* ignore */ }

[1-9][0-9]*  {
				yylval.val = atoi(yytext);
			       	return INT;
		}
\*	{
		yylval.str = strdup(yytext);
		return AST;
}
{separater}	{
				return yytext[0];
		}

{self}	{
				return yytext[0];
		}
{xdstart} {
		elog(NOTICE, "xdstart, return yytext[0] = %c", yytext[0]);
				BEGIN(xd);
		yymore();
		}
<xd>{xddouble} {
		elog(NOTICE, "xddouble, yytext = %s", yytext);
		yymore();
		}
<xd>{xdstop} {
		elog(NOTICE, "xdstop, yytext = %s", yytext);
				BEGIN(INITIAL);
				yylval.str = strdup(yytext);
				return IDENT;
		}
<xd>{xdinside} {
		elog(NOTICE, "    xdinside, yytext = %s", yytext);
		yymore();
		}
.	{
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
			errmsg_internal("%s", message)));
}

void
syncgroup_scanner_init(const char *str)
{
	Size		slen = strlen(str);
	char		*scanbuf;

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
