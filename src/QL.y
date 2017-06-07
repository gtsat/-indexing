
%{
	#include<string.h>
	#include<stdlib.h>
	#include<stdio.h>
	#include"queue.h"
	#include"stack.h"

	#define YYERROR_VERBOSE

	void yyerror (char*);
	//void unroll (void);


	/** the following are passed on yyparse() **/

	lifo_t* stack;

	double varray [BUFSIZ];

	unsigned vindex;
	unsigned key_cardinality;
	unsigned predicates_cardinality;
%}

%expect 0
%token_table
/* %pure_parser */


%union{
	char* str;
	double dval;
	int ival;
}

%type <str> COMMAND 
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
%left '/' '%'
%left '?' '&'
%token '='
%left ','

%start COMMANDS

%%

COMMANDS :
					{
						LOG (info,"EMPTY COMMANDS ENCOUNTERED. \n");
					}
	| COMMANDS ';'				{
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
	| COMMANDS COMMAND DJOIN_PRED ';' {
						LOG (info,"DISTANCE JOIN. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>4;
					}
	| COMMANDS COMMAND DJOIN_PRED '/' ';' {
						LOG (info,"DISTANCE JOIN/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,NULL);
						insert_into_stack (stack,';');
						varray [vindex++] = $<dval>4;
					}
	| COMMANDS COMMAND CP_PRED ';'	{
						LOG (info,"CLOSEST PAIRS. \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)ULONG_MAX);
						insert_into_stack (stack,';');
						varray [vindex++] = $<ival>4;
					}
	| COMMANDS COMMAND CP_PRED '/' ';'	{
						LOG (info,"CLOSEST PAIRS/ . \n");
						insert_into_stack (stack,varray+vindex);
						insert_into_stack (stack,(void*)ULONG_MAX);
						insert_into_stack (stack,';');
						varray [vindex++] = $<ival>4;
					}
;

COMMAND :
	  COMMAND cSUBQUERY		{
						LOG (info,"NEW cSUBQUERY PARSED. \n");
					}
	| cSUBQUERY			{
						LOG (info,"FIRST cSUBQUERY PARSED. \n");
					}
	| rCOMMAND rKEY			{
						LOG (info,"REVERSE NN. \n");
						insert_into_stack (stack,(void*)key_cardinality);
						insert_into_stack (stack,(void*)'%');
					}
	| error 			{
						LOG (error,"Erroneous command... \n");
						yyclearin;
						yyerrok;
						YYABORT;
					}
;

rCOMMAND :
	 cSUBQUERY rSUBQUERY		{
						LOG (info,"FIRST rSUBQUERY PARSED. \n");
					}
	| rCOMMAND rSUBQUERY		{
						LOG (info,"NEW rSUBQUERY PARSED. \n");
					}
;

rSUBQUERY :
	'%' rSUBQUERY			{
						LOG (info,"More slashes preceding rsubquery. \n");
					}
	| '%' SUBQUERY			{
						LOG (info,"Put together rsubquery. \n");
						insert_into_stack (stack,(void*)'%');
					}
;

cSUBQUERY:
	'/' cSUBQUERY			{
						LOG (info,"More slashes preceding csubquery. \n");
					}
	| '/' SUBQUERY			{
						LOG (info,"Put together csubquery. \n");
						insert_into_stack (stack,(void*)'/');
					}
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

rKEY :
	 '%' rKEY			{}
	| '%' KEY			{
						LOG (info,"rKEY encountered.\n");
					}
;

DJOIN_PRED :
	 '/' DJOIN_PRED			{}
	| '/' REAL			{
						LOG (info,"Distance join predicate encountered.\n");
					}
;

CP_PRED :
	 '/' CP_PRED			{}
	| '/' INTEGER			{
						LOG (info,"Closest pairs predicate encountered.\n");
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

/***
int main (int argc, char* argv[]) {
	stack = new_stack();
	yyparse();
}
***

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
***/
void yyerror (char* description) {
	LOG (error," %s\n", description);
}

