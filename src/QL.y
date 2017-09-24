%{
	#include<string.h>
	#include<stdlib.h>
	#include<stdio.h>
	#include"queue.h"
	#include"stack.h"
	#include"defs.h"

	#define YYERROR_VERBOSE

	void yyerror (char*);

	int QL_lex (void);
	int QL_parse (lifo_t *const, double[]);
/*
	lifo_t* stack;
	double varray [BUFSIZ];

	unsigned vindex;
	unsigned key_cardinality;
	unsigned predicates_cardinality;
*/
%}

%expect 0
%token_table
/* %pure_parser */
%name-prefix="QL_"

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
						LOG (info,"SINGLE COMMAND ';' ENCOUNTERED. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = 0;
					}
	| COMMAND '/' ';' {
						LOG (info,"SINGLE COMMAND '/;' ENCOUNTERED. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = 0;
					}
	| COMMANDS DJOIN_PRED ';' {
						LOG (info,"DISTANCE JOIN. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>3;
					}
	| COMMANDS DJOIN_PRED '/' ';' {
						LOG (info,"DISTANCE JOIN/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>3;
					}
	| COMMANDS CP_PRED ';'	{
						LOG (info,"CLOSEST PAIRS. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)ULONG_MAX);
						insert_into_stack (stack,';');
						varray [vindex++] = $<ival>3;
					}
	| COMMANDS CP_PRED '/' ';'	{
						LOG (info,"CLOSEST PAIRS/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)ULONG_MAX);
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
		COMMAND COMMAND {LOG (info,"PAIR OF COMMANDS. \n");}
		| COMMANDS COMMAND {LOG (info,"COMMAND ADDED IN COMMAND SEQUENCE. \n");}

COMMAND :
	  cSUBQUERY		{LOG (info,"cSUBQUERY PARSED. \n");}
	| rCOMMAND rKEY {
						LOG (info,"REVERSE NN. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)'%');
					}
;

rCOMMAND :
	 cSUBQUERY rSUBQUERY		{LOG (info,"FIRST rSUBQUERY PARSED. \n");}
	| rCOMMAND rSUBQUERY		{LOG (info,"NEW rSUBQUERY PARSED. \n");}
;

rSUBQUERY :
	'%' rSUBQUERY		{LOG (info,"More slashes preceding rsubquery. \n");}
	| '%' SUBQUERY	{
						LOG (info,"Put together rsubquery. \n");
						insert_into_stack (stack,(void*)'%');
					}
;

cSUBQUERY:
	'/' cSUBQUERY	{LOG (info,"More slashes preceding csubquery. \n");}
	| '/' SUBQUERY	{
						LOG (info,"Put together csubquery. \n");
						insert_into_stack (stack,(void*)'/');
					}
;

SUBQUERY :
	  ID 			{
						LOG (info,"Single identifier subquery. \n");
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,$<str>1);
					}
	| ID '?' PREDICATES {
						LOG (info,"Parsed subquery. \n")
						insert_into_stack (stack,(void*)predicates_cardinality);
						insert_into_stack (stack,$<str>1);
					}
;

PREDICATES :
	  PREDICATES '&' PREDICATE	{
						LOG (info,"Yet another predicate in the collection... \n");
						predicates_cardinality++;
					}
	| PREDICATE			{
						LOG (info,"First query predicate encountered. \n");
						predicates_cardinality = 1;
					}
;

PREDICATE :
	  LOOKUP '=' KEY		{
						LOG (info,"LOOKUP. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)LOOKUP);
					}
	| FROM '=' KEY	{
						LOG (info,"FROM. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)FROM);
					}
	| TO '=' KEY		{
						LOG (info,"TO. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)TO);
					}
	| BOUND '=' KEY	{
						LOG (info,"BOUND. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)BOUND);
					}
	| CORN '=' BITFIELD {
						LOG (info,"SKYLINE. \n");
						insert_into_stack (stack,$<str>3);
						insert_into_stack (stack,1);
						insert_into_stack (stack,(void*)CORN);
					}
;

rKEY :
	 '%' rKEY		{}
	| '%' KEY		{LOG (info,"rKEY encountered.\n");}
;

DJOIN_PRED :
	 '/' DJOIN_PRED	{}
	| '/' REAL		{LOG (info,"Distance join predicate encountered.\n");}
;

CP_PRED :
	 '/' CP_PRED		{}
	| '/' INTEGER	{LOG (info,"Closest pairs predicate encountered.\n");}
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
void yyerror (char* description) {
	LOG (error," %s\n", description);
}

