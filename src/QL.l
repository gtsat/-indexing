
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

\/			{LOG(debug,"\ncSubquery start.\n"); return '/';}
\%			{LOG(debug,"\nrSubquery start.\n"); return '%';}
"%25"			{LOG(debug,"\nrSubquery start.\n"); return '%';}


=			{LOG(debug,"Assignment.\n"); return '=';}
&			{LOG(debug,"Concatenation of predicates.\n"); return '&';}
\?			{LOG(debug,"Predicates.\n"); return '?';}

key			{LOG(debug,"Keyword KEY"); return LOOKUP;}
from			{LOG(debug,"Keyword FROM."); return FROM;}
to			{LOG(debug,"Keyword TO."); return TO;}
corn			{LOG(debug,"Keyword CORNER."); return CORN;}
bound			{LOG(debug,"Keyword BOUND."); return BOUND;}

,			{LOG(debug,"Symbol sequence delimiter.\n"); return ',';}
\(			{LOG(debug,"Start of key-sequence operator.\n");}
\)			{LOG(debug,"End of key-sequence operator.\n");}
[OoIi]+			{
				LOG(debug,"A bitfield: %s \n", yytext); 
				yylval.str = strdup (yytext);
				return BITFIELD;
			}
\-?{DIGIT}+		{
				LOG(debug,"An integer: %s (%d)\n", yytext, atoi(yytext)); 
				yylval.ival= atoi (yytext);
				return INTEGER;
			}
\-?{DIGIT}*"."{DIGIT}*	{
				LOG(debug,"A float: %s (%g)\n", yytext, atof(yytext)); 
				yylval.dval= atof (yytext);
				return REAL;
			}
{ID}			{
				LOG(debug,"An identifier: %s\n", yytext); 
				yylval.str = strdup (yytext);
				return ID; /* heapfile name */
			}

\;			{LOG(debug,"Termination.\n"); return ';';}
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

