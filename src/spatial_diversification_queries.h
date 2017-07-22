#ifndef _SPATIAL_DIVERSIFICATION_QUERIES_H_
#define _SPATIAL_DIVERSIFICATION_QUERIES_H_

#include "defs.h"


fifo_t* diversified_join (lifo_t *const trees,tree_t *const attractors,tree_t *const repellers,
			boolean const pull_apart_results, boolean const bring_together_results,
			double const lambda_rel, double const lambda_diss, double const lambda_clust);

/**
 * Uses average distance from attractors and can choose
 * between max-min and max-avg for repellers. May choose
 * to add incrementally the so far selected results to
 * the repellers so as to force the results to appear
 * as far from each other as possible.
 */

fifo_t* hotspots (tree_t *const tree,
			fifo_t const*const attractors,
			fifo_t const*const repellers,
			unsigned const k,
			boolean const pull_apart_results,
			boolean const bring_together_results,
			double const lambda_rel,
			double const lambda_diss,
			double const lambda_clust);

fifo_t* hotspots_constrained (tree_t *const tree,
			fifo_t const*const attractors,
			fifo_t const*const repellers,
			index_t const lo[],
			index_t const hi[],
			unsigned const k,
			boolean const pull_apart_results,
			boolean const bring_together_results,
			double const lambda_rel,
			double const lambda_diss,
			double const lambda_clust);

fifo_t* hotspots_join (tree_t *const tree,
			tree_t *const attractors,
			tree_t *const repellers,
			unsigned const k,
			boolean const pull_apart_results,
			boolean const bring_together_results,
			double const lambda_rel,
			double const lambda_diss,
			double const lambda_clust);

fifo_t* hotspots_join_constrained (tree_t *const tree,
				tree_t *const attractors,
				tree_t *const repellers,
				index_t const lo[],
				index_t const hi[],
				unsigned const k,
				boolean const pull_apart_results,
				boolean const bring_together_results,
				double const lambda_rel,
				double const lambda_diss,
				double const lambda_clust);

#endif /* _SPATIAL_DIVERSIFICATION_QUERIES_H_ */
