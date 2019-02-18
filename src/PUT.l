%{
	#include<string.h>
	#include"lex.PUT_.h"
	#include"QL.tab.h"
	#include"defs.h"
%}

%option reentrant
%option bison-bridge
%option prefix="PUT_"
%option header-file="lex.PUT_.h"
%option noyywrap
%option warn
%array 

DIGIT 		[0-9]
ID		[_a-zA-Z][_a-zA-Z0-9\.]*

%%

null			{LOG(debug,"Symbol for NULL.\n"); return null;}
heapfile		{LOG(debug,"Symbol for HEAPFILE.\n"); return _HEAPFILE_;}
data			{LOG(debug,"Symbol for DATA.\n"); return _DATA_;}
object			{LOG(debug,"Symbol for _OBJECT_.\n"); return _OBJECT_;}
key			{LOG(debug,"Symbol for _KEY_.\n"); return _KEY_;}

:			{LOG(debug,"Colon.\n"); return ':';}
\"			{LOG(debug,"Double quote.\n"); return '"';}
\{			{LOG(debug,"Opening brace.\n"); return '{';}
\}			{LOG(debug,"Closing brace.\n"); return '}';}
\[			{LOG(debug,"Opening bracket.\n"); return '[';}
]			{LOG(debug,"Closing bracket.\n"); return ']';}
,			{LOG(debug,"Symbol sequence delimiter.\n"); return ',';}
\-?{DIGIT}+		{
				LOG(debug,"An integer: %s\n", yytext); 
				yylval->dval= atof (yytext);
				return REAL;
			}
\-?{DIGIT}*"."{DIGIT}*	{
				LOG(debug,"A float: %s\n", yytext); 
				yylval->dval= atof (yytext);
				return REAL;
			}
{ID}			{
				LOG(debug,"An identifier: %s\n", yytext); 
				yylval->str = strdup (yytext);
				return ID; /* heapfile name */
			}

[:blank:]+		/* eat it up */
\n+
.			{LOG(debug,"Unrecognized character: %s\n", yytext);}
%%

/*
int main (int  argc, char* argv[])
{
	++argv, --argc;
	yyin = argc > 0 ? fopen (argv[0],"r") : stdin;
	PUT_lex();
}
*/

