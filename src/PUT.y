%{
	#include<string.h>
	#include<stdlib.h>
	#include<stdio.h>
	#include"queue.h"
	#include"stack.h"
	#include"defs.h"
	#include"PUT.tab.h"
	#include"lex.PUT_.h"

	#define YYERROR_VERBOSE
	//#define YYLEX_PARAM scanner
	//#define YYPARSE_PARAM yyscan_t scanner

	void yyerror (yyscan_t scanner, char const *msg);
	void stack_insertion (lifo_t *const, index_t const[], unsigned const, object_t const);

	extern int PUT_lex (YYSTYPE * yylval_param ,yyscan_t yyscanner);
	//int PUT_parse (lifo_t *const, index_t[], char[]);
/*
	index_t varray [BUFSIZ];
	lifo_t *insertions = NULL;
	char* heapfile = NULL;

	unsigned vindex = 0;
*/
%}

%expect 0
%token_table
%pure_parser
%name-prefix="PUT_"
%lex-param {yyscan_t scanner}
%parse-param {yyscan_t scanner}

%union{
	char* str;
	double dval;
}

%type <str> REQUEST

%type <str> HEAPFILE_DEF DATA_DEF
%type <str> DATA
%type <str> DATUM
%type <str> KEY
%token <str> _HEAPFILE_ _DATA_ _KEY_ _OBJECT_
%token <dval> REAL
%token <str> ID

%token '{' '}'
%token '[' ']'
%token ':'
%right ','
%token '"'
%token null

%start REQUEST

%%

REQUEST :
	'{' HEAPFILE_DEF '}'			{LOG (debug,"Parsed no data request.\n");}
	| '{' HEAPFILE_DEF ',' DATA_DEF  '}'	{LOG (debug,"Parsed request.\n");}
	| '{' DATA_DEF ',' HEAPFILE_DEF '}' 	{LOG (debug,"Parsed request.\n")}
	| error 				{
						LOG (error,"Erroneous request... \n");
						yyclearin;
						yyerrok;
						YYABORT;
						}
;

HEAPFILE_DEF :
	'"' _HEAPFILE_ '"' ':' '"' ID '"'	{
						LOG (debug,"Heapfile definition containing identifier: %s\n",$6);
						strcpy(heapfile,$6);
						free($6);
						}
;

DATA_DEF:
	'"' _DATA_ '"' ':' null			{LOG (debug,"Data nulled sequence definition.\n");}
	| '"' _DATA_ '"' ':' '[' DATA ']'	{LOG (debug,"Data sequence definition.\n");}
;

DATA :
	DATUM					{LOG (debug,"Initiating data list.\n");}
	| DATA ',' DATUM			{LOG (debug,"Adding another data item in the list.\n");}
;

DATUM :
      '{' '"' _KEY_ '"' ':' '[' KEY ']' ',' '"' _OBJECT_ '"' ':' REAL '}'	{
										LOG (debug,"Data entry encountered.\n");
										stack_insertion (insertions,varray,vindex,(object_t)$<dval>14);
										}
      '{' '"' _OBJECT_ '"' ':' REAL ',' '"' _KEY_ '"' ':' '[' KEY ']' '}'	{
										LOG (debug,"Data entry encountered.\n");
										stack_insertion (insertions,varray,vindex,(object_t)$<dval>6);
										}
;

KEY : 
	  KEY ',' REAL				{varray [vindex++] = $3;}
	| REAL					{
						*varray = $1;
						vindex = 1;
						}
;

%%

void stack_insertion (lifo_t *const insertions, index_t const varray[], unsigned const vindex, object_t const object) {
	data_pair_t *const new_pair = (data_pair_t *const) malloc (sizeof(data_pair_t));
	new_pair->key = (index_t*) malloc ((vindex)*sizeof(index_t));
	for (unsigned i=0; i<vindex; ++i) {
		new_pair->key[i] = varray[i];
	}
	new_pair->object = object;
	new_pair->dimensions = vindex;
	insert_into_stack (insertions,new_pair);
}
/***
int main (int argc, char* argv[]) {
	insertions= new_stack();
	PUT_parse();
}
***/
void yyerror (yyscan_t scanner, char const *description) {
	LOG (error," %s\n", description);
}

