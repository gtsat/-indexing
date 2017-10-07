%{
	#include<string.h>
	#include<stdlib.h>
	#include<stdio.h>
	#include"stack.h"
	#include"defs.h"
	#include"DELETE.tab.h"
	#include"lex.DELETE_.h"

	#define YYERROR_VERBOSE
	//#define YYLEX_PARAM scanner
	//#define YYPARSE_PARAM yyscan_t scanner

	void yyerror (yyscan_t scanner, char const *msg);
	void stack_deletion (lifo_t *const, index_t const varray[], unsigned const vindex);

	extern int DELETE_lex (YYSTYPE * yylval_param ,yyscan_t yyscanner);
//	int DELETE_parse (lifo_t *const, index_t[], char[]);
/*
	index_t varray [BUFSIZ];
	lifo_t *deletions = NULL;
	char* heapfile = NULL;

	unsigned vindex = 0;
*/
%}

%expect 0
%token_table
%pure_parser
%name-prefix="DELETE_"
%lex-param {yyscan_t scanner}
%parse-param {yyscan_t scanner}

%union{
	char* str;
	double dval;
}

%type <str> REQUEST

%type <str> HEAPFILE_DEF KEYS_DEF 
%type <str> KEY_SEQ 
%type <str> KEY
%token <str> _HEAPFILE_ _KEYS_
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
	| '{' HEAPFILE_DEF ',' KEYS_DEF  '}'	{LOG (debug,"Parsed request.\n");}
	| '{' KEYS_DEF ',' HEAPFILE_DEF '}' 	{LOG (debug,"Parsed request.\n")}
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

KEYS_DEF:
	'"' _KEYS_ '"' ':' '[' ']'		{LOG (debug,"KEYS empty sequence definition.\n");}
	|'"' _KEYS_ '"' ':' null		{LOG (debug,"KEYS nulled sequence definition.\n");}
	| '"' _KEYS_ '"' ':' '[' KEY_SEQ ']'	{LOG (debug,"KEYS sequence definition.\n");}
;

KEY_SEQ :
	'[' KEY ']'				{
						LOG (debug,"First key assembled.\n");
						stack_deletion (deletions,varray,vindex);
						}
	| KEY_SEQ ',' '[' KEY ']'		{
						LOG (debug,"Another key assembled.\n");
						stack_deletion (deletions,varray,vindex);
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

void stack_deletion (lifo_t *const deletions, index_t const varray[], unsigned const vindex) {
	data_pair_t *const new_pair = (data_pair_t *const) malloc (sizeof(data_pair_t));
	new_pair->key = (index_t*) malloc ((vindex)*sizeof(index_t));
	for (unsigned i=0; i<vindex; ++i) {
		new_pair->key [i] = varray[i];
	}
	new_pair->dimensions = vindex;
	insert_into_stack (deletions,new_pair);
}
/***
int main (int argc, char* argv[]) {
	deletions = new_stack();
	DELETE_parse();
}
***/
void yyerror (yyscan_t scanner, char const *description) {
	LOG (error," %s\n", description);
}

