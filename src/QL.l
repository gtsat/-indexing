
%{
	#include<math.h>
	#include<string.h>
	#include"QL.tab.h"
	#include"defs.h"
%}

%option noyywrap
%option stack
%array 

%x		SUBQ
%x		BODY

DIGIT 		[0-9]
ID		[_a-zA-Z][_a-zA-Z0-9\.]*

%%

			yy_push_state(0);

\/			{LOG(0,"\ncSubquery start.\n"); return '/';}
\%			{LOG(0,"\nrSubquery start.\n"); return '%';}
"%25"			{LOG(0,"\nrSubquery start.\n"); return '%';}


=			{LOG(0,"Assignment.\n"); return '=';}
&			{LOG(0,"Concatenation of predicates.\n"); return '&';}
\?			{LOG(0,"Predicates.\n"); return '?';}

key			{LOG(0,"Keyword KEY"); return LOOKUP;}
from			{LOG(0,"Keyword FROM."); return FROM;}
to			{LOG(0,"Keyword TO."); return TO;}
corn			{LOG(0,"Keyword CORNER."); return CORN;}
bound			{LOG(0,"Keyword BOUND."); return BOUND;}

,			{LOG(0,"Symbol sequence delimiter.\n"); return ',';}
\(			{LOG(0,"Start of key-sequence operator.\n");}
\)			{LOG(0,"End of key-sequence operator.\n");}
[OoIi]+			{
				LOG(0,"A bitfield: %s \n", yytext); 
				yylval.str = strdup (yytext);
				return BITFIELD;
			}
\-?{DIGIT}+		{
				LOG(0,"An integer: %s (%d)\n", yytext, atoi(yytext)); 
				yylval.ival= atoi (yytext);
				return INTEGER;
			}
\-?{DIGIT}*"."{DIGIT}*	{
				LOG(0,"A float: %s (%g)\n", yytext, atof(yytext)); 
				yylval.dval= atof (yytext);
				return REAL;
			}
{ID}			{
				LOG(0,"An identifier: %s\n", yytext); 
				yylval.str = strdup (yytext);
				return ID; /* heapfile name */
			}

\;			{LOG(0,"Termination.\n"); return ';';}
[:blank:]+		/* eat it up */
.			{LOG(0,"Unrecognized character: %s\n", yytext);}

%%

/*
int main (int  argc, char* argv[])
{
	++argv, --argc;
	yyin = argc > 0 ? fopen (argv[0],"r") : stdin;
	yylex();
}
*/

