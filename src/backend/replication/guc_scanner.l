%{
#include "postgres.h"

/* No reason to constrain amount of data slurped */
#define YY_READ_BUF_SIZE 16777216

/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;
static char *scanbuf;

%}
%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="repl_guc_yy"

%%

[a-zA-Z][a-zA-Z0-9]*    {
				yylval.str = strdup(yytext);
				return NAME;
			}

[1-9][0-9]*  {yylval.val = atoi(yytext);   return INT;}

[,()\n]        {
			return yytext[0];
		}
%%

void
yyerror(const char *message)
{
    ereport(LOG,
	    (errcode(ERRCODE_SYNTAX_ERROR),
		    errmsg_internal("%s", message)));
}

void
repl_guc_scanner_init(const char *str)
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

	BEGIN(INITIAL);
}

void
repl_guc_scanner_finish(void)
{
	yy_delete_buffer(scanbufhandle);
	pfree(scanbuf);
}
