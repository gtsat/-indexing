%{
	#include<string.h>
	#include"PUT.tab.h"
	#include"defs.h"
%}

%option prefix="PUT_"
%option noyywrap
%option stack
%array 

DIGIT 		[0-9]
ID		[_a-zA-Z][_a-zA-Z0-9\.]*

%%

heapfile		{LOG(debug,"Symbol for HEAPFILE"); return _HEAPFILE_;}
data			{LOG(debug,"Symbol for DATA."); return _DATA_;}
object			{LOG(debug,"Symbol for _OBJECT_."); return _OBJECT_;}
key			{LOG(debug,"Symbol for _KEY_."); return _KEY_;}

\[			{LOG(debug,"Opening bracket.\n"); return '[';}
]			{LOG(debug,"Closing bracket.\n"); return ']';}
,			{LOG(debug,"Symbol sequence delimiter.\n"); return ',';}
\-?{DIGIT}+		{
				LOG(debug,"An integer: %s\n", yytext); 
				PUT_lval.dval= atof (yytext);
				return REAL;
			}
\-?{DIGIT}*"."{DIGIT}*	{
				LOG(debug,"A float: %s\n", yytext); 
				PUT_lval.dval= atof (yytext);
				return REAL;
			}
{ID}			{
				LOG(debug,"An identifier: %s\n", yytext); 
				PUT_lval.str = strdup (yytext);
				return ID; /* heapfile name */
			}

[:blank:]+		/* eat it up */
.			{LOG(debug,"Unrecognized character: %s\n", yytext);}

%%

/*
int main (int  argc, char* argv[])
{
	++argv, --argc;
	yyin = argc > 0 ? fopen (argv[0],"r") : stdin;
	yylex();
}
*/

