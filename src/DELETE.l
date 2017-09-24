%{
	#include<string.h>
	#include"DELETE.tab.h"
	#include"defs.h"
%}

%option prefix="DELETE_"
%option noyywrap
%option stack
%array 

DIGIT 		[0-9]
ID		[_a-zA-Z][_a-zA-Z0-9\.]*

%%

heapfile		{LOG(debug,"Symbol for HEAPFILE"); return _HEAPFILE_;}
keys			{LOG(debug,"Symbol for KEYS."); return _KEYS_;}

\[			{LOG(debug,"Opening bracket.\n"); return '[';}
]			{LOG(debug,"Closing bracket.\n"); return ']';}
,			{LOG(debug,"Symbol sequence delimiter.\n"); return ',';}
\-?{DIGIT}+		{
				LOG(debug,"An integer: %s\n", yytext); 
				DELETE_lval.dval= atof (yytext);
				return REAL;
			}
\-?{DIGIT}*"."{DIGIT}*	{
				LOG(debug,"A float: %s\n", yytext); 
				DELETE_lval.dval= atof (yytext);
				return REAL;
			}
{ID}			{
				LOG(debug,"An identifier: %s\n", yytext); 
				DELETE_lval.str = strdup (yytext);
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

