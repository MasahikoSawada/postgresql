%{
#include "postgres.h"

#include "miscadmin.h"
#include "lib/stringinfo.h"

/* No reason to constrain amount of data slurped */
#define YY_READ_BUF_SIZE 16777216

/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;

/* Functions for handling double quoted string */
static void init_xd_string(void);
static void addlit_xd_string(char *ytext, int yleng);
static void addlitchar_xd_string(unsigned char ychar);

static int   xd_size;
static int   xd_len;
static char *xd_string;
static char *scanbuf;
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
whitespace	({space}+)
self		[\[\]\,]

/*
 * Basically all ascii characteres except for special characters(' ', '\"', ',', '[', ']') are allowed
 * to be used for node name. These special charater could be used in double-quoted representation.
 * Note that double-quote character can be used except at leading string.
 */
node_name_start	[^\ \",\[\]]
node_name_chars	[^\ ,\[\]]
dquoted_name	[^\"]+

/* Double-quoted string */
dquote		\"
xdstart		{dquote}
xddouble	{dquote}{dquote}
xdstop		{dquote}
xdinside	{dquoted_name}

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
				yylval.str = strdup(xd_string);
				BEGIN(INITIAL);
				return NAME;
		}
{self} {
				BEGIN(INITIAL);
				return yytext[0];
}
[0-9]+ {
				yylval.str = yytext;
				return NUM;
		}
{node_name_start}{node_name_chars}* {
				yylval.str = yytext;
				return NAME;
}
%%

void
yyerror(const char *message)
{
	int	elevel = IsUnderPostmaster ? DEBUG2 : LOG;
	ereport(elevel,
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
	xd_len = 0;
	xd_size = NAMEDATALEN;
	xd_string = (char *)malloc(sizeof(char) * NAMEDATALEN);
}

static void
addlit_xd_string(char *ytext, int yleng)
{
	/* Enlarge buffer if needed */
	if ((xd_len + yleng) > NAMEDATALEN)
	{
		xd_string = realloc(xd_string, xd_size + NAMEDATALEN);
		xd_size += NAMEDATALEN;
	}

	memcpy(xd_string + xd_len, ytext, yleng);
	xd_len += yleng;
}

static void
addlitchar_xd_string(unsigned char ychar)
{
	if ((xd_len + 1) > NAMEDATALEN)
	{
		xd_string = realloc(xd_string, xd_size + NAMEDATALEN);
		xd_size += NAMEDATALEN;
	}

	xd_string[xd_len] = ychar;
	xd_len += 1;
}
