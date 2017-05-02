#ifndef _SPATIAL_STANDARD_QUERIES_H_
#define _SPATIAL_STANDARD_QUERIES_H_

#include "defs.h"


fifo_t* find_all_in_rtree (tree_t *const tree, index_t const key[]);
object_t find_any_in_rtree (tree_t *const tree, index_t const key[]);

fifo_t* range (tree_t *const, index_t const lo[], index_t const hi[]);
fifo_t* nearest (tree_t *const, index_t const center[], unsigned const k);
fifo_t* bounded_search (tree_t *const, index_t const lo[], index_t const hi[], index_t const center[], unsigned const k, unsigned const dimensions);

fifo_t* distance_join_ordered (double const theta,
				boolean const less_than_theta,
				boolean const use_avg,
				tree_t *const tree0,...);

fifo_t* distance_join_pairwise (double const theta,
				boolean const less_than_theta,
				boolean const use_avg,
				tree_t *const tree0,...);

fifo_t* distance_join (double theta, boolean const less_than, boolean const pairwise, boolean const use_avg, lifo_t *const trees);

fifo_t* closest_tuples_ordered (unsigned const k, boolean const use_avg, tree_t *const,...);
fifo_t* closest_tuples_pairwise (unsigned const k, boolean const use_avg, tree_t *const,...);

fifo_t* farthest_tuples_ordered (unsigned const k, boolean const use_avg, tree_t *const,...);
fifo_t* farthest_tuples_pairwise (unsigned const k, boolean const use_avg, tree_t *const,...);

fifo_t* multichromatic_reverse_nearest_neighbors (index_t const[], tree_t *const data_tree, lifo_t *const feature_trees);


fifo_t* x_tuples (unsigned const k, boolean const closest, boolean const use_avg, boolean const pairwise, lifo_t *const trees);

#endif /* _SPATIAL_STANDARD_QUERIES_H_ */
