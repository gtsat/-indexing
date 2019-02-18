#ifndef __NTREE_H__
#define __NTREE_H__

#include "defs.h"

tree_t* new_ntree (char const[], uint32_t const pagesize);

fifo_t* find_arcs_in_ntree (tree_t *const tree, object_t const from);
arc_weight_t find_arc_in_ntree (tree_t *const tree, object_t const from, object_t const to);

arc_weight_t delete_arc_from_ntree (tree_t *const tree, object_t const from, object_t const to);

void insert_into_ntree (tree_t *const, object_t const, object_t const, arc_weight_t const);
object_t delete_from_ntree (tree_t *const, object_t const, object_t const);

void insert_arcs_from_edgelist (tree_t *const, char const[]);
void delete_arcs_from_edgelist (tree_t *const, char const[]);

#endif /* __NTREE_H__ */
