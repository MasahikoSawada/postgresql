%{
/*-------------------------------------------------------------------------
 *
 * syncrep_scanner.l
 *	  a lexical scanner for synchronous_standby_names
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep_scanner.l
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "lib/stringinfo.h"

/*
 * flex emits a yy_fatal_error() function that it calls in response to
 * critical errors like malloc failure, file I/O errors, and detection of
 * internal inconsistency.  That function prints a message and calls exit().
 * Mutate it to instead call our handler, which jumps out of the parser.
 */
#undef fprintf
#define fprintf(file, fmt, msg) syncrep_flex_fatal(msg)

/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;

static StringInfoData xdbuf;

static const char *syncrep_flex_fatal_errmsg;
static sigjmp_buf *syncrep_flex_fatal_jmp;

static void	syncrep_scanner_init(const char *str);
static void	syncrep_scanner_finish(void);
static int	syncrep_flex_fatal(const char *msg);
%}

%option 8bit
%option never-interactive
%option nounput
%option noinput
%option noyywrap
%option warn
%option prefix="syncrep_yy"

/*
 * <xd> delimited identifiers (double-quoted identifiers)
 */
%x xd

space		[ \t\n\r\f\v]

undquoted_start	[^ ,\(\)\"]
undquoted_cont		[^,\(\) \t\n\r\f\v]
undquoted_name    {undquoted_start}{undquoted_cont}*
dquoted_name		[^\"]+

/* Double-quoted string */
dquote		\"
xdstart		{dquote}
xddouble		{dquote}{dquote}
xdstop		{dquote}
xdinside		{dquoted_name}

%%
{space}+		{ /* ignore */ }
{xdstart}	{
				initStringInfo(&xdbuf);
				BEGIN(xd);
		}
<xd>{xddouble} {
				appendStringInfoChar(&xdbuf, '\"');
		}
<xd>{xdinside} {
				appendStringInfoString(&xdbuf, yytext);
		}
<xd>{xdstop} {
				yylval.str = pstrdup(xdbuf.data);
				pfree(xdbuf.data);
				BEGIN(INITIAL);
				return NAME;
		}
","			{ return ','; }
"("			{ return '('; }
")"			{ return ')'; }
[1-9][0-9]*	{
				yylval.str = pstrdup(yytext);
				return NUM;
		}
{undquoted_name} {
				yylval.str = pstrdup(yytext);
				return NAME;
		}
%%

void
yyerror(const char *message)
{
	ereport(IsUnderPostmaster ? DEBUG2 : LOG,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("%s at or near \"%s\"", message, yytext)));
}

void
syncrep_scanner_init(const char *str)
{
	Size		slen = strlen(str);
	char	   *scanbuf;

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
syncrep_scanner_finish(void)
{
	yy_delete_buffer(scanbufhandle);
	scanbufhandle = NULL;
}

/*
 * Flex fatal errors bring us here.  Stash the error message and jump back to
 * syncrep_scanstr().  Assume all msg arguments point to string constants; this
 * holds for flex 2.5.31 (earliest we support) and flex 2.5.35 (latest as of
 * this writing).  Otherwise, we would need to copy the message.
 *
 * We return "int" since this takes the place of calls to fprintf().
*/
static int
syncrep_flex_fatal(const char *msg)
{
	syncrep_flex_fatal_errmsg = msg;
	siglongjmp(*syncrep_flex_fatal_jmp, 1);
	return 0;					/* keep compiler quiet */
}

bool
syncrep_scanstr(const char *str)
{
	int	parse_res;
	bool	ret = true;
	sigjmp_buf	flex_fatal_jmp;

	if (sigsetjmp(flex_fatal_jmp, 1) == 0)
		syncrep_flex_fatal_jmp = &flex_fatal_jmp;
	else
	{
		/*
		 * Regain control after a fatal, internal flex error.  It may have
		 * corrupted parser state.  Consequently, abandon the file, but trust
		 * that the state remains sane enough for yy_delete_buffer().
		 */
		elog(LOG, "%s", syncrep_flex_fatal_errmsg);
		ret = false;
		goto cleanup;
	}

	syncrep_scanner_init(str);
	parse_res = syncrep_yyparse();

	if (parse_res != 0)
		ret = false;
cleanup:
	syncrep_scanner_finish();
	return ret;
}