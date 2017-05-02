#ifndef __RTREE_H__
#define __RTREE_H__

#include "defs.h"

tree_t* load_rtree (char const[]);
tree_t* new_rtree (char const[], unsigned const pagesize, unsigned const dims);

void delete_rtree (tree_t *const);

object_t delete_from_rtree (tree_t *const, index_t const[]);
void insert_into_rtree (tree_t *const, index_t const[], object_t const);

void insert_records_from_textfile (tree_t *const, char const[]);
void delete_records_from_textfile (tree_t *const, char const[]);

page_t* load_rtree_page (tree_t *const, unsigned const);
int flush_rtree (tree_t *const);

#endif /* __RTREE_H__ */
