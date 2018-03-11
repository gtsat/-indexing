%{
	#include<string.h>
	#include<stdlib.h>
	#include<stdio.h>
	#include"queue.h"
	#include"stack.h"
	#include"defs.h"
	#include"QL.tab.h"
	#include"lex.QL_.h"

	#define YYERROR_VERBOSE
	//#define YYLEX_PARAM scanner
	//#define YYPARSE_PARAM yyscan_t scanner

	void yyerror (yyscan_t,char*);


	extern int QL_lex (YYSTYPE *yylval_param, yyscan_t yyscanner);
	//int QL_parse (lifo_t *const, double[]);
/*
	lifo_t* stack;
	double varray [BUFSIZ];

	unsigned vindex = 0;
	unsigned key_cardinality = 0;
	unsigned predicates_cardinality = 0;
*/
%}

%expect 0
%token_table
%pure_parser
%name-prefix="QL_"
%lex-param {yyscan_t scanner}
%parse-param {yyscan_t scanner}

%union{
	char* str;
	double dval;
	int ival;
}

%type <str> QUERY
%type <str> COMMANDS COMMAND 
%type <str> rCOMMAND 
%type <str> cSUBQUERY rSUBQUERY
%type <str> SUBQUERY
%type <str> PREDICATE

%type <str> rKEY DJOIN_PRED CP_PRED
%type <str> KEY

%token <str> ID LOOKUP FROM TO BOUND CORN
%token <str> BITFIELD
%token <int> INTEGER
%token <double> REAL

%token ';'
%token '/' '%' '?' '='
%right ',' '&'

%start QUERY

%%

QUERY :
	  COMMAND ';'	{
						LOG (debug,"SINGLE COMMAND ';' ENCOUNTERED. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = 0;
					}
	| COMMAND '/' ';' {
						LOG (debug,"SINGLE COMMAND '/;' ENCOUNTERED. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = 0;
					}
	| COMMANDS DJOIN_PRED ';' {
						LOG (debug,"DISTANCE JOIN. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>3;
					}
	| COMMANDS DJOIN_PRED '/' ';' {
						LOG (debug,"DISTANCE JOIN/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>3;
					}
	| COMMANDS CP_PRED ';'	{
						LOG (debug,"CLOSEST PAIRS. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)0xffffffffffffffff);
						insert_into_stack (stack,';');
						varray [vindex++] = $<ival>3;
					}
	| COMMANDS CP_PRED '/' ';'	{
						LOG (debug,"CLOSEST PAIRS/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)0xffffffffffffffff);
						insert_into_stack (stack,';');
						varray [vindex++] = $<ival>3;
					}
	| error 			{
						LOG (error,"Erroneous command... \n");
						yyclearin;
						yyerrok;
						YYABORT;
					}
;

COMMANDS :
		COMMAND COMMAND {LOG (debug,"PAIR OF COMMANDS. \n");}
		| COMMANDS COMMAND {LOG (debug,"COMMAND ADDED IN COMMAND SEQUENCE. \n");}

COMMAND :
	  cSUBQUERY		{LOG (debug,"cSUBQUERY PARSED. \n");}
	| rCOMMAND rKEY {
						LOG (debug,"REVERSE NN. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)'%');
					}
;

rCOMMAND :
	 cSUBQUERY rSUBQUERY		{LOG (debug,"FIRST rSUBQUERY PARSED. \n");}
	| rCOMMAND rSUBQUERY		{LOG (debug,"NEW rSUBQUERY PARSED. \n");}
;

rSUBQUERY :
	'%' rSUBQUERY		{LOG (debug,"More slashes preceding rsubquery. \n");}
	| '%' SUBQUERY	{
						LOG (debug,"Put together rsubquery. \n");
						insert_into_stack (stack,(void*)'%');
					}
;

cSUBQUERY:
	'/' cSUBQUERY	{LOG (debug,"More slashes preceding csubquery. \n");}
	| '/' SUBQUERY	{
						LOG (debug,"Put together csubquery. \n");
						insert_into_stack (stack,(void*)'/');
					}
;

SUBQUERY :
	  ID 			{
						LOG (debug,"Single identifier subquery. \n");
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,$<str>1);
					}
	| ID '?' PREDICATES {
						LOG (debug,"Parsed subquery. \n")
						insert_into_stack (stack,(void*)predicates_cardinality);
						insert_into_stack (stack,$<str>1);
					}
;

PREDICATES :
	  PREDICATES '&' PREDICATE	{
						LOG (debug,"Yet another predicate in the collection... \n");
						predicates_cardinality++;
					}
	| PREDICATE			{
						LOG (debug,"First query predicate encountered. \n");
						predicates_cardinality = 1;
					}
;

PREDICATE :
	  LOOKUP '=' KEY		{
						LOG (debug,"LOOKUP. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)LOOKUP);
					}
	| FROM '=' KEY	{
						LOG (debug,"FROM. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)FROM);
					}
	| TO '=' KEY		{
						LOG (debug,"TO. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)TO);
					}
	| BOUND '=' KEY	{
						LOG (debug,"BOUND. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)BOUND);
					}
	| CORN '=' BITFIELD {
						LOG (debug,"SKYLINE. \n");
						insert_into_stack (stack,$<str>3);
						insert_into_stack (stack,1);
						insert_into_stack (stack,(void*)CORN);
					}
;

rKEY :
	 '%' rKEY		{}
	| '%' KEY		{LOG (debug,"rKEY encountered.\n");}
;

DJOIN_PRED :
	 '/' DJOIN_PRED	{}
	| '/' REAL		{LOG (debug,"Distance join predicate encountered.\n");}
;

CP_PRED :
	 '/' CP_PRED		{}
	| '/' INTEGER	{LOG (debug,"Closest pairs predicate encountered.\n");}
;

KEY : 
	  KEY ',' REAL	{
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<dval>3;
						key_cardinality++;
					}
	| KEY ',' INTEGER {
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<ival>3;
						key_cardinality++;
					}
	| REAL			{
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<dval>1;
						key_cardinality = 1;
					}
	| INTEGER		{
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<ival>1;
						key_cardinality = 1;
					}
;

%%

/***
int main (int argc, char* argv[]) {
	stack = new_stack();
	yyparse();
}
}
***/
void yyerror (yyscan_t scanner, char* description) {
	LOG (error," %s\n", description);
}

