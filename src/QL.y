
%{
	#include<string.h>
	#include<stdlib.h>
	#include<stdio.h>
	#include"queue.h"
	#include"stack.h"

	int yylex (void);
	void yyerror (char*);

	void unroll (void);

	double varray [BUFSIZ];

	unsigned key_cardinality;
	unsigned predicates_cardinality;

	extern lifo_t* stack;
%}

%expect 0
%token_table
/* %pure_parser */

%union{
	char* str;
	double dval;
	int ival;
}

%type <str> COMMAND SUBQUERY
%type <str> PREDICATE
%type <str> KEY

%token <str> ID LOOKUP
%token <str> FROM TO 
%token <str> BOUND 
%token <str> CORN

%token <str> BITFIELD
%token <int> INTEGER
%token <double> REAL

%token ';'
%left '/' '?'
%left '&'
%token '='
%right ','

%start COMMANDS

%%

COMMANDS :
					{
						LOG (info,"EMPTY COMMANDS ENCOUNTERED. \n");
					}
	| COMMANDS ';'			{
						LOG (info,"EMPTY COMMAND ';' ENCOUNTERED. \n");
					}
	| COMMANDS '/' ';'		{
						LOG (info,"EMPTY COMMAND '/;' ENCOUNTERED. \n");
					}
	| COMMANDS COMMAND ';'		{
						LOG (info,"YET ANOTHER COMMAND ADDED. \n"); 
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = INDEX_T_MAX;
					}
	| COMMANDS COMMAND '/' ';'	{
						LOG (info,"YET ANOTHER COMMAND/ ADDED. \n"); 
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = INDEX_T_MAX;
					}
	| COMMANDS COMMAND '/' REAL ';'		{
						LOG (info,"DISTANCE JOIN. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>4;
					}
	| COMMANDS COMMAND '/' REAL '/' ';'	{
						LOG (info,"DISTANCE JOIN/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>4;
					}
	| COMMANDS COMMAND '/' INTEGER ';'	{
						LOG (info,"CLOSEST PAIRS. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)ULONG_MAX);
						insert_into_stack (stack,';');
						varray [vindex++] = $<ival>4;
					}
	| COMMANDS COMMAND '/' INTEGER '/' ';'	{
						LOG (info,"CLOSEST PAIRS/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)ULONG_MAX);
						insert_into_stack (stack,';');
						varray [vindex++] = $<ival>4;
					}
;

COMMAND : 
	COMMAND cSUBQUERY		{
						LOG (info,"NEW SUBQUERY PARSED. \n");
						insert_into_stack (stack,(void*)'/');
					}
	| cSUBQUERY			{
						LOG (info,"FIRST SUBQUERY PARSED. \n");
						insert_into_stack (stack,(void*)'/');
					}
;

cSUBQUERY:
	'/' cSUBQUERY			{	LOG (info,"More slashes preceding subquery. \n");}
	| '/' SUBQUERY			{	LOG (info,"Put together subquery. \n");}
;

SUBQUERY :
	  ID 				{
						LOG (info,"Single identifier subquery. \n");
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,$<str>1);
					}
	| ID '?' PREDICATES 		{
						LOG (info,"Parsed subquery. \n")
						insert_into_stack (stack,(void*)predicates_cardinality);
						insert_into_stack (stack,$<str>1);
					}
	| error 			{
						LOG (error,"Erroneous subquery... \n"); 
						yyerrok;
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
	| FROM '=' KEY			{
						LOG (info,"FROM. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)FROM);
					}
	| TO '=' KEY			{
						LOG (info,"TO. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)TO);
					}
	| BOUND '=' KEY			{
						LOG (info,"BOUND. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)BOUND);
					}
	| CORN '=' BITFIELD		{
						LOG (info,"SKYLINE. \n");
						insert_into_stack (stack,$<str>3);
						insert_into_stack (stack,1);
						insert_into_stack (stack,(void*)CORN);
					}
;

KEY : 
	  KEY ',' REAL			{
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<dval>3;
						key_cardinality++;
					}
	| KEY ',' INTEGER		{
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<ival>3;
						key_cardinality++;
					}
	| REAL				{
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<dval>1;
						key_cardinality = 1;
					}
	| INTEGER			{
						insert_into_stack (stack,varray+vindex);
						varray [vindex++] = $<ival>1;
						key_cardinality = 1;
					}
;

%%

/*
int main (int argc, char* argv[]) {
	stack = new_stack();
	yyparse();
}
*/

void unroll (void) {
	LOG (info,"UNROLLING COMMANDS NOW... \n\n\n");

	while (stack->size) {

		if (remove_from_stack (stack) != (void*)';') {
			LOG (error,"WAS EXPECTING THE START OF A NEW COMMAND... \n");
			abort ();
		}

		if (remove_from_stack (stack) == NULL) {
			LOG (info,"DISTANCE JOIN OPERATION... \n");
		}else{
			LOG (info,"CLOSEST PAIRS OPERATION... \n");
		}

		double threshold = *((double*)remove_from_stack (stack));

		LOG (info,"Threshold parameter is equal to %lf. \n",threshold);


subquery:
		if (remove_from_stack (stack) != (void*)'/') {
			LOG (error,"WAS EXPECTING THE START OF A NEW SUBQUERY... \n");
			abort ();
		}

		LOG (info,"UNROLLING NEW SUBQUERY... \n");

		char const*const heapfile = remove_from_stack (stack);
		LOG (info,"HEAPFILE: '%s'. \n",heapfile);

		unsigned const pcardinality = remove_from_stack (stack);

		for (unsigned j=0; j<pcardinality; ++j) {
			unsigned const operation = remove_from_stack (stack);
			switch (operation) {
				case LOOKUP:
					LOG (info,"LOOKUP ");
					break;
				case FROM:
					LOG (info,"FROM ");
					break;
				case TO:
					LOG (info,"TO ");
					break;
				case CORN:
					LOG (info,"SKYLINE ");
					break;
				default:
					LOG (error,"Unknown operation...\n");
			}

			unsigned const kcardinality = remove_from_stack (stack);

			if (operation == CORN) {
				fprintf (stderr,"BITFIELD: '%s'",((char*const)remove_from_stack (stack)));
			}else{
				for (unsigned i=0; i<kcardinality; ++i) {
					double* tmp = remove_from_stack (stack);
					fprintf (stderr,"%lf ",*tmp);
				}
			}

			fprintf (stderr,"\n");

			if (stack->size && peek_at_stack(stack)==(void*)'/') {
				goto subquery;
			}
		}
	}

	for (unsigned i=0; yytname[i]!=0; ++i) {
		printf ("%s \n", yytname[i]);
	}
}

void yyerror (char* description) {
	LOG (error," %s\n", description);
}

