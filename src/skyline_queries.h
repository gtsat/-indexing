#ifndef _SKYLINE_QUERIES_H_
#define _SKYLINE_QUERIES_H_

#include "defs.h"

fifo_t* skyline (tree_t *const, boolean const corner[], uint32_t proj_dimensions);
fifo_t* skyline_constrained (tree_t *const, boolean const corner[], index_t const lo[], index_t const hi[], uint32_t proj_dimensions);

fifo_t* multiskyline (lifo_t *const trees, boolean const corner[]);
fifo_t* multiskyline_join (lifo_t *const trees, boolean const corner[]);
fifo_t* multiskyline_sort_merge (lifo_t *const trees, boolean const corner[]);

fifo_t* multiskyline_diversified (lifo_t *const trees, boolean const corner[], uint32_t k);

fifo_t* skyline_diversified (tree_t *const, boolean const[], uint32_t const k, boolean const in_disk);
fifo_t* skyline_diversified_constrained (tree_t *const, boolean const[],
                                        index_t const[], index_t const[],
                                        uint32_t const, boolean const);

#endif /* _SKYLINE_QUERIES_H_ */

