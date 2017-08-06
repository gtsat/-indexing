#ifndef _SPATIAL_STANDARD_QUERIES_H_
#define _SPATIAL_STANDARD_QUERIES_H_

#include "defs.h"


fifo_t* find_all_in_rtree (tree_t *const tree, index_t const key[], uint32_t proj_dimensions);
object_t find_any_in_rtree (tree_t *const tree, index_t const key[], uint32_t proj_dimensions);

fifo_t* range (tree_t *const, index_t const lo[], index_t const hi[], uint32_t proj_dimensions);
fifo_t* nearest (tree_t *const, index_t const center[], uint32_t const k);
fifo_t* bounded_search (tree_t *const, index_t const lo[], index_t const hi[], index_t const center[], uint32_t const k, uint32_t proj_dimensions);

fifo_t* distance_join_ordered (double const theta,
				boolean const less_than_theta,
				boolean const use_avg,
				tree_t *const tree0,...);

fifo_t* distance_join_pairwise (double const theta,
				boolean const less_than_theta,
				boolean const use_avg,
				tree_t *const tree0,...);

fifo_t* distance_join (double theta, boolean const less_than, boolean const pairwise, boolean const use_avg, lifo_t *const trees);

fifo_t* closest_tuples_ordered (uint32_t const k, boolean const use_avg, tree_t *const,...);
fifo_t* closest_tuples_pairwise (uint32_t const k, boolean const use_avg, tree_t *const,...);

fifo_t* farthest_tuples_ordered (uint32_t const k, boolean const use_avg, tree_t *const,...);
fifo_t* farthest_tuples_pairwise (uint32_t const k, boolean const use_avg, tree_t *const,...);

fifo_t* x_tuples (uint32_t const k, boolean const closest, boolean const use_avg, boolean const pairwise, lifo_t *const trees);

fifo_t* multichromatic_reverse_nearest_neighbors (index_t const[], tree_t *const data_tree, lifo_t *const feature_trees, uint32_t proj_dimensions);

#endif /* _SPATIAL_STANDARD_QUERIES_H_ */
