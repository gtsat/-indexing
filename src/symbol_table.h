#ifndef __SYMBOL_TABLE__
#define __SYMBOL_TABLE__

#include "defs.h"

symbol_table_t* new_symbol_table_primitive (value_t const value);
symbol_table_t* new_symbol_table (value_t const, int (*)(key__t const, key__t const));

void delete_symbol_table (symbol_table_t *const);
void clear_symbol_table (symbol_table_t *const);

void set (symbol_table_t *const, key__t const, value_t const);
value_t unset (symbol_table_t *const, key__t const key);

value_t get (symbol_table_t const*const, key__t const);
fifo_t* get_keys (symbol_table_t const*const);
fifo_t* get_values (symbol_table_t const*const);
fifo_t* get_entries (symbol_table_t const*const);

fifo_t* key_range (symbol_table_t const*const, key__t const, key__t const);

key__t max_key (symbol_table_t const*const);
value_t remove_max (symbol_table_t *const rbtree);

void map_keys (symbol_table_t const*const, key__t(*)(key__t const,va_list),...);

fifo_t* filter_keys (symbol_table_t const*const, boolean (*)(key__t const));

#endif /* __SYMBOL_TABLE__ */
