#ifndef __COMMONS_H__
#define __COMMONS_H__

#include <stdint.h>
#include "defs.h"

void new_root (tree_t *const tree);
page_t* new_leaf (tree_t const*const tree);
page_t* new_internal (tree_t const*const tree);
void delete_rtree_page (page_t *const page);
void delete_ntree_page (page_t *const page);

page_t* load_page (tree_t *const tree, uint64_t const position);

uint64_t flush_tree (tree_t *const tree);
uint64_t flush_page (tree_t *const tree, uint64_t const page_id);
uint64_t low_level_write_of_page_to_disk (tree_t *const tree, page_t *const page, uint64_t const position);

uint64_t compute_page_priority (tree_t *const tree, uint64_t const page_id);

fifo_t* transpose_subsumed_pages (tree_t *const tree, uint64_t const from, uint64_t const to);
uint64_t anchor (tree_t const*const tree, uint64_t id);

void update_rootbox (tree_t *const tree);

#endif
