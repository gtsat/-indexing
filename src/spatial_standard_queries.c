/**
 *  Copyright (C) 2016 George Tsatsanifos <gtsatsanifos@gmail.com>
 *
 *  #indexing is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <pthread.h>
#include "rtree.h"
#include "spatial_standard_queries.h"
#include "priority_queue.h"
#include "symbol_table.h"
#include "common.h"
#include "queue.h"
#include "stack.h"
#include "defs.h"


object_t find_any_in_rtree (tree_t *const tree, index_t const key[]) {

	load_page(tree,0);
	pthread_rwlock_rdlock (&tree->tree_lock);
	if (!key_enclosed_by_box(key,tree->root_box,tree->dimensions)) {
		pthread_rwlock_unlock (&tree->tree_lock);

		LOG (warn,"Query key ( ");
		if (logging <= warn) {
			for (uint32_t i=0; i < tree->dimensions; ++i) {
				fprintf (stderr, "%12lf ", (double)key[i]);
			}
			fprintf (stderr,") does not belong in the indexed area...\n");
		}

		LOG (warn,"Root-box: \n");
		if (logging <= warn) {
			for (uint32_t i=0; i < tree->dimensions; ++i) {
				fprintf (stderr, "\t\t( %12lf %12lf )\n", (double)tree->root_box[i].start, (double)tree->root_box[i].end);
			}
			fprintf (stderr,"\n");
		}

		return -1;
	}else{
		pthread_rwlock_unlock (&tree->tree_lock);

		fifo_t *const browse = new_queue();

		reset_search_operation:
		insert_at_tail_of_queue (browse,0);

		while (browse->size) {
			uint64_t const page_id = remove_head_of_queue (browse);

			page_t const*const page = load_page(tree,page_id);

			pthread_rwlock_rdlock (&tree->tree_lock);
			pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);

			if (pthread_rwlock_tryrdlock (page_lock)) {
				clear_queue (browse);
				goto reset_search_operation;
			}else{
				if (page->header.is_leaf) {
					for (register uint32_t i=0; i<page->header.records; ++i) {
						if (equal_keys (page->node.leaf.keys+i*tree->dimensions,key,tree->dimensions)) {
							delete_queue (browse);
							return page->node.leaf.objects[i];
						}
					}
				}else{
					for (register uint32_t i=0; i<page->header.records; ++i) {
						if (key_enclosed_by_box(key,page->node.internal.BOX(i),tree->dimensions)) {
							insert_at_tail_of_queue (browse,CHILD_ID(page_id,i));
						}
					}
				}

				pthread_rwlock_unlock (page_lock);
			}
		}

		delete_queue (browse);

		LOG (warn,"Unable to retrieve any record associated with key ( ");
		if (logging <= warn) {
			for (uint32_t i=0; i<tree->dimensions; ++i)
				fprintf (stderr,"%12lf ",(double)key[i]);
			fprintf (stderr,")...\n");
		}

		return -1;
	}
}


fifo_t* find_all_in_rtree (tree_t *const tree, index_t const key[]) {

	load_page(tree,0);
	pthread_rwlock_rdlock (&tree->tree_lock);
	if (!key_enclosed_by_box(key,tree->root_box,tree->dimensions)) {
		pthread_rwlock_unlock (&tree->tree_lock);

		LOG (warn,"Query key ( ");
		if (logging <= warn) {
			for (uint32_t i=0; i < tree->dimensions; ++i) {
				fprintf (stderr, "%12lf ", (double)key[i]);
			}
			fprintf (stderr,") does not belong in the indexed area...\n");
		}

		LOG (warn,"Root-box: \n");
		if (logging <= warn) {
			for (uint32_t i=0; i < tree->dimensions; ++i) {
				fprintf (stderr, "\t\t( %12lf %12lf )\n", (double)tree->root_box[i].start, (double)tree->root_box[i].end);
			}
			fprintf (stderr,"\n");
		}

		return new_queue();
	}else pthread_rwlock_unlock (&tree->tree_lock);

	fifo_t *const result = new_queue();
	fifo_t *const browse = new_queue();

	reset_search_operation:
	insert_at_tail_of_queue (browse,0);

	while (browse->size) {
		uint64_t const page_id = remove_head_of_queue (browse);

		page_t const*const page = load_page(tree,page_id);

		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (page_lock != NULL);

		if (pthread_rwlock_tryrdlock (page_lock)) {
			clear_queue (browse);
			goto reset_search_operation;
		}else{
			if (page->header.is_leaf) {
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (equal_keys (page->node.leaf.keys+i*tree->dimensions,key,tree->dimensions)) {
						insert_at_tail_of_queue (result,page->node.leaf.objects[i]);
					}
				}
			}else{
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (key_enclosed_by_box(key,page->node.internal.BOX(i),tree->dimensions)) {
						insert_at_tail_of_queue (browse,CHILD_ID(page_id,i));
					}
				}
			}

			pthread_rwlock_unlock (page_lock);
		}
	}

	delete_queue (browse);

	if (!result->size) {
		LOG (warn,"Unable to retrieve any records associated with key ( ");
		if (logging <= warn) {
			for (uint32_t i=0; i<tree->dimensions; ++i) {
				fprintf (stderr,"%12lf ",(double)key[i]);
			}
			fprintf (stderr,")...\n");
		}
	}

	return result;
}


fifo_t* range (tree_t *const tree, index_t const lo[], index_t const hi[]) {
	interval_t query [tree->dimensions];
	for (uint32_t j=0; j<tree->dimensions; ++j) {
		if (lo[j]>hi[j]) {
			LOG (error,"Erroneous range query specified...\n");
			return NULL;
		}
		query[j].start = lo[j];
		query[j].end = hi[j];
	}

	pthread_rwlock_rdlock (&tree->tree_lock);
	if (!overlapping_boxes (query,tree->root_box,tree->dimensions)){
		pthread_rwlock_unlock (&tree->tree_lock);

		LOG (warn,"Query does not overlap with indexed area...\n");
		return new_queue();
	}else pthread_rwlock_unlock (&tree->tree_lock);

	fifo_t* result = new_queue();
	fifo_t* browse = new_queue();

	reset_search_operation:
	insert_at_tail_of_queue (browse,0);

	while (browse->size) {
		uint64_t const page_id = remove_head_of_queue (browse);
		page_t const*const page = load_page(tree,page_id);

		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (page_lock != NULL);

		if (pthread_rwlock_tryrdlock (page_lock)) {
			clear_queue (browse);
			goto reset_search_operation;
		}else{
			if (page->header.is_leaf) {
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (key_enclosed_by_box (page->node.leaf.KEY(i),query,tree->dimensions)) {
						data_pair_t* pair = (data_pair_t*) malloc (sizeof(data_pair_t));

						pair->key = (index_t*) malloc (sizeof(index_t)*tree->dimensions);
						memcpy (pair->key,page->node.leaf.KEY(i),sizeof(index_t)*tree->dimensions);

						pair->object = page->node.leaf.objects[i];
						pair->dimensions = tree->dimensions;

						insert_at_tail_of_queue (result,pair);
					}
				}
			}else{
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (overlapping_boxes (query,page->node.internal.BOX(i),tree->dimensions)) {
						insert_at_tail_of_queue (browse,CHILD_ID(page_id,i));
					}
				}
			}

			pthread_rwlock_unlock (page_lock);
		}
	}

	delete_queue (browse);

	return result;
}



fifo_t* bounded_search (tree_t *const tree,
		index_t const lo[], index_t const hi[],
		index_t const center[], uint32_t const k,
		uint32_t const dimensions) {

	if (k==0) return new_queue();
	interval_t query [tree->dimensions];
	for (uint32_t j=0; j<tree->dimensions; ++j) {
		if (lo[j]>hi[j]) {
			LOG (error,"Erroneous range query specified...\n");
			return NULL;
		}
		query[j].start = lo[j];
		query[j].end = hi[j];
	}

	priority_queue_t* browse = new_priority_queue(&mincompare_containers);
	priority_queue_t* data = new_priority_queue(&maxcompare_containers);

	reset_search_operation:;

	box_container_t* container = (box_container_t*) malloc (sizeof(box_container_t));

	container->box = tree->root_box;
	container->sort_key = 0;
	container->id = 0;

	insert_into_priority_queue (browse,container);

	for (index_t threshold=INDEX_T_MAX;browse->size;) {
		container = remove_from_priority_queue (browse);

		if (container->sort_key > threshold) {
			free (container);
			while (browse->size) {
				free (remove_from_priority_queue (browse));
			}
			break;
		}

		uint64_t const page_id = container->id;
		page_t const*const page = load_page(tree,page_id);

		free (container);

		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (page_lock != NULL);


		if (pthread_rwlock_tryrdlock (page_lock)) {
			while (browse->size) {
				free (remove_from_priority_queue (browse));
			}
			goto reset_search_operation;
		}else{
			if (page->header.is_leaf) {
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (key_enclosed_by_box (page->node.leaf.KEY(i),query,tree->dimensions)) {
						data_container_t* data_container = (data_container_t*) malloc (sizeof(data_container_t));

						data_container->key = (index_t*) malloc (sizeof(index_t)*tree->dimensions);
						memcpy (data_container->key,page->node.leaf.KEY(i),sizeof(index_t)*tree->dimensions);

						data_container->object = page->node.leaf.objects[i];

						data_container->sort_key = key_to_key_distance (center,page->node.leaf.KEY(i),dimensions);

						if (data->size < k) {
							data_container->dimensions = tree->dimensions;
							insert_into_priority_queue (data,data_container);

							if (data->size == k) {
								threshold = ((data_container_t*)peek_priority_queue(data))->sort_key;
							}
						}else if (data_container->sort_key
								< ((data_container_t*)peek_priority_queue(data))->sort_key) {
							data_container_t* temp = remove_from_priority_queue (data);

							free (temp->key);
							free (temp);

							data_container->dimensions = tree->dimensions;
							insert_into_priority_queue (data,data_container);

							threshold = ((data_container_t*)peek_priority_queue(data))->sort_key;
						}else{
							free (data_container->key);
							free (data_container);
						}
					}
				}
			}else{
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (overlapping_boxes (query,page->node.internal.BOX(i),tree->dimensions)) {
						container = (box_container_t*) malloc (sizeof(box_container_t));

						container->id = CHILD_ID(page_id,i);
						container->box = page->node.internal.BOX(i);
						container->sort_key = key_to_box_mindistance (center,page->node.internal.BOX(i),dimensions);

						if (container->sort_key < threshold) {
							insert_into_priority_queue (browse,container);
						}
					}
				}
			}

			pthread_rwlock_unlock (page_lock);
		}
	}

	fifo_t* result = new_queue();
	while (data->size) {
		insert_at_head_of_queue (result,remove_from_priority_queue(data));
	}

	delete_priority_queue (browse);
	delete_priority_queue (data);

	return result;
}

fifo_t* nearest (tree_t *const tree, index_t const query[], uint32_t const k) {
	index_t from [tree->dimensions];
	index_t to [tree->dimensions];
	for (uint32_t i=0; i<tree->dimensions; ++i) {
		from [i] = -INDEX_T_MAX;
		to [i] = INDEX_T_MAX;
	}
	return bounded_search (tree, from, to, query, k, tree->dimensions);
}



/**
 * It dynamically constructs a Voronoi cell around the query point for each of the
 * feature-trees, the intersection of which is applied over the data-tree as a
 * polygon query, and the results are returned to the user. The query-center does
 * not necessarily have to be indexed in the database in this.
 */

fifo_t* multichromatic_reverse_nearest_neighbors (index_t const query[],
						tree_t *const data_tree,
						lifo_t *const feature_trees) {
	lifo_t* cells[feature_trees->size];

	priority_queue_t* browse = new_priority_queue (&mincompare_containers);

	for (uint32_t itree=0; itree<feature_trees->size; ++itree) {
		tree_t *const tree = (tree_t *const) feature_trees->buffer[itree];

		cells[itree] = new_stack();

		reset_inner_search_operation:;

		box_container_t* container = (box_container_t*) malloc (sizeof(box_container_t));

		container->box = tree->root_box;
		container->sort_key = 0;
		container->id = 0;

		assert (!browse->size);

		insert_into_priority_queue (browse,container);


		while (browse->size) {
			container = remove_from_priority_queue (browse);
			uint64_t const page_id = container->id;

			free (container);

			page_t const*const page = load_page(tree,page_id);

			pthread_rwlock_rdlock (&tree->tree_lock);
			pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);


			if (pthread_rwlock_tryrdlock (page_lock)) {
				while (browse->size) {
					free (remove_from_priority_queue (browse));
				}
				goto reset_inner_search_operation;
			}else{
				if (page->header.is_leaf) {
					for (register uint32_t i=0; i<page->header.records; ++i) {
						data_container_t* data_container = (data_container_t*) malloc (sizeof(data_container_t));

						data_container->key = (index_t*) malloc (tree->dimensions*sizeof(index_t));
						memcpy (data_container->key,page->node.leaf.KEY(i),tree->dimensions*sizeof(index_t));

						data_container->object = page->node.leaf.objects[i];
						data_container->sort_key = key_to_key_distance (query,data_container->key,tree->dimensions);

						boolean is_obscured_data_item = false;
						for (uint32_t c=0;c<=itree;++c) {
							for (uint32_t j=0; j<cells[c]->size; ++j) {
								data_container_t* adjacent_cell_center = (data_container_t*) cells[c]->buffer[j];
								if (data_container->sort_key
									>= key_to_key_distance
									(data_container->key,adjacent_cell_center->key,tree->dimensions)) {

									is_obscured_data_item = true;
									goto data_double_break;
								}
							}
						}

						data_double_break:
						if (!is_obscured_data_item) {
							insert_into_stack (cells[itree],data_container);
						}else{
							free (data_container->key);
							free (data_container);
						}
					}
				}else{
					uint32_t i = page->header.records-1;
					do{
						container = (box_container_t*) malloc (sizeof(box_container_t));

						container->id = CHILD_ID(page_id,i);
						container->sort_key = key_to_box_mindistance (query,page->node.internal.BOX(i),tree->dimensions);

						boolean is_obscured_box = false;
						for (uint32_t c=0;c<=itree;++c) {
							for (uint32_t j=0; j<cells[c]->size; ++j) {
								data_container_t* adjacent_cell_center = (data_container_t*) cells[c]->buffer[j];
								if (container->sort_key
									>= key_to_box_maxdistance
									(adjacent_cell_center->key,page->node.internal.BOX(i),tree->dimensions)) {

									is_obscured_box = true;
									goto browse_double_break;
								}
							}
						}

						browse_double_break:
						if (!is_obscured_box)
							insert_into_priority_queue (browse,container);
						else free (container);

						if (i) --i;
						else break;
					}while(true);
				}

				pthread_rwlock_unlock (page_lock);
			}
		}
	}

	fifo_t *const result_browse = new_queue();
	fifo_t *const result = new_queue();
	tree_t *const tree = data_tree;

	reset_search_operation:;

	insert_at_tail_of_queue (result_browse,0);
	while (result_browse->size) {
		uint64_t const page_id = remove_head_of_queue (result_browse);
		page_t const*const page = load_page(tree,page_id);

		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (page_lock != NULL);

		if (pthread_rwlock_tryrdlock (page_lock)) {
			clear_queue (result_browse);
			goto reset_search_operation;
		}else{
			if (page->header.is_leaf) {
				for (register uint32_t i=0; i<page->header.records; ++i) {
					data_pair_t* data_container = (data_pair_t*) malloc (sizeof(data_pair_t));

					data_container->key = (index_t*) malloc (sizeof(index_t)*tree->dimensions);
					memcpy (data_container->key,page->node.leaf.KEY(i),sizeof(index_t)*tree->dimensions);

					data_container->object = page->node.leaf.objects[i];

					index_t sort_key = key_to_key_distance (query,data_container->key,tree->dimensions);

					boolean is_obscured_data_item = false;
					for (uint32_t c=0;c<feature_trees->size;++c) {
						for (uint32_t j=0; j<cells[c]->size; ++j) {
							data_container_t* adjacent_cell_center = (data_container_t*) cells[c]->buffer[j];
							if (sort_key > key_to_key_distance
									(data_container->key,adjacent_cell_center->key,tree->dimensions)) {

								is_obscured_data_item = true;
								goto result_data_double_break;
							}
						}
					}

					result_data_double_break:
					if (!is_obscured_data_item) {
						data_container->dimensions = tree->dimensions;
						insert_at_tail_of_queue (result,data_container);
					}else{
						free (data_container->key);
						free (data_container);
					}
				}
			}else{
				uint32_t i = page->header.records-1;
				do{
					index_t box_distance = key_to_box_mindistance (query,page->node.internal.BOX(i),tree->dimensions);

					boolean is_obscured_box = false;
					for (uint32_t c=0;c<feature_trees->size;++c) {
						for (uint32_t j=0; j<cells[c]->size; ++j) {
							data_container_t* other_center = (data_container_t*) cells[c]->buffer[j];
							if (box_distance > key_to_box_maxdistance (other_center->key,page->node.internal.BOX(i),tree->dimensions)) {
								is_obscured_box = true;
								goto result_browse_double_break;
							}
						}
					}

					result_browse_double_break:
					if (!is_obscured_box) {
						insert_at_tail_of_queue (result_browse,CHILD_ID(page_id,i));
					}

					if (i) --i;
					else break;
				}while(true);
			}

			pthread_rwlock_unlock (page_lock);
		}
	}

	for (uint32_t i=0; i<feature_trees->size; ++i) {
		delete_stack (cells[i]);
	}

	delete_queue (result_browse);

	return result;
}



fifo_t* distance_join_ordered (double const theta,
				boolean const less_than_theta,
				boolean const use_avg,
				tree_t *const tree0,...) {

	if (less_than_theta && theta < 0) {
		LOG (warn,"No point for negative distances in predicate joins...\n");
		return new_queue();
	}

	va_list args;
	va_start (args,tree0);

	lifo_t *const trees = new_stack();
	insert_into_stack (trees,tree0);
	for (tree_t* tree = tree0;
		(tree=va_arg (args,tree_t *const))&& tree!=NULL;
		insert_into_stack (trees,tree))
		;

	va_end (args);

	boolean const pairwise = false;
	fifo_t *const result = distance_join (theta,less_than_theta,pairwise,use_avg,trees);

	delete_stack (trees);
	return result;
}


fifo_t* distance_join_pairwise (double const theta,
				boolean const less_than_theta,
				boolean const use_avg,
				tree_t *const tree0,...) {

	if (less_than_theta && theta < 0) {
		LOG (warn,"No point for negative distances in predicate joins...\n");
		return new_queue();
	}

	va_list args;
	va_start (args,tree0);

	lifo_t *const trees = new_stack();
	insert_into_stack (trees,tree0);
	for (tree_t* tree = tree0;
		(tree=va_arg (args,tree_t *const))&& tree!=NULL;
		insert_into_stack (trees,tree))
		;

	va_end (args);

	boolean const pairwise = true;
	fifo_t *const result = distance_join (theta,less_than_theta,pairwise,use_avg,trees);

	delete_stack (trees);
	return result;
}



fifo_t* distance_join (double const theta,
			boolean const less_than_theta,
			boolean const pairwise,
			boolean const use_avg,
			lifo_t *const trees) {

	if (!trees->size) return new_queue();
	if (less_than_theta && theta < 0) {
		LOG (warn,"No point for negative distances in predicate joins...\n");
		return new_queue();
	}

	uint32_t const cardinality = trees->size;
	uint32_t dimensions = UINT_MAX;
	for (uint32_t i=0; i<trees->size; ++i) {
		if (((tree_t *const)trees->buffer[i])->dimensions < dimensions) {
			dimensions = ((tree_t *const)trees->buffer[i])->dimensions;
		}
	}

	lifo_t* browse = new_stack();
	priority_queue_t* data_combinations = new_priority_queue (less_than_theta?&maxcompare_multicontainers:&mincompare_multicontainers);

	reset_search_operation:;
	multibox_container_t* container = (multibox_container_t*) malloc (sizeof(multibox_container_t));

	container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));
	container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
	container->cardinality = cardinality;
	container->dimensions = dimensions;

	bzero (container->page_ids,cardinality*sizeof(uint64_t));

	for (uint32_t i=0; i<cardinality; ++i) {
		pthread_rwlock_rdlock (&TREE(i)->tree_lock);

		memcpy (container->boxes+i*dimensions,
				TREE(i)->root_box,
				dimensions*sizeof(interval_t));

		pthread_rwlock_unlock (&TREE(i)->tree_lock);
	}

	insert_into_stack (browse,container);

	while (browse->size) {
		container = remove_from_stack (browse);

		boolean all_leaves = true;
		for (uint32_t i=0; i<container->cardinality; ++i) {
			uint64_t const page_id = container->page_ids[i];
			page_t const*const page = load_page (TREE(i),page_id);

			pthread_rwlock_rdlock (&TREE(i)->tree_lock);
			pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) get(TREE(i)->page_locks,page_id);
			pthread_rwlock_unlock (&TREE(i)->tree_lock);

			assert (page_lock != NULL);

			if (pthread_rwlock_tryrdlock (page_lock)) {
				while (browse->size) {
					multibox_container_t* temp = remove_from_stack (browse);

					free (temp->page_ids);
					free (temp->boxes);
					free (temp);
				}

				for (uint32_t j=0; j<i; ++j) {
					pthread_rwlock_rdlock (&TREE(j)->tree_lock);
					pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get(TREE(j)->page_locks,page_id);
					pthread_rwlock_unlock (&TREE(j)->tree_lock);

					pthread_rwlock_unlock (previous_lock);
				}

				goto reset_search_operation;
			}else{
				if (!page->header.is_leaf) {
					all_leaves = false;

					uint32_t j = page->header.records-1;
					do{
						multibox_container_t* new_container = (multibox_container_t*) malloc (sizeof(multibox_container_t));

						new_container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
						memcpy (new_container->page_ids,container->page_ids,cardinality*sizeof(uint64_t));
						new_container->page_ids[i] = page_id*TREE(i)->internal_entries+j+1;

						new_container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));
						memcpy (new_container->boxes,container->boxes,cardinality*dimensions*sizeof(interval_t));
						memcpy (new_container->boxes+i*dimensions,
								page->node.internal.intervals+j*dimensions,
								dimensions*sizeof(interval_t));

						new_container->cardinality = cardinality;
						new_container->dimensions = dimensions;

						if ((less_than_theta && theta >= (use_avg?
									(pairwise?
									avg_mindistance_pairwise_multibox(new_container,0)
									:avg_mindistance_ordered_multibox(new_container,0))
									 :(pairwise?
									max_mindistance_pairwise_multibox(new_container,0)
									:max_mindistance_ordered_multibox(new_container,0))))
						|| (!less_than_theta && theta <= (use_avg?
									(pairwise?
									avg_maxdistance_pairwise_multibox(new_container,0)
									:avg_maxdistance_ordered_multibox(new_container,0))
									 :(pairwise?
									min_maxdistance_pairwise_multibox(new_container,0)
									:min_maxdistance_ordered_multibox(new_container,0))))){

								insert_into_stack (browse,new_container);
						}else{
							free (new_container->page_ids);
							free (new_container->boxes);
							free (new_container);
						}

						if (j) --j;
						else break;
					}while(true);

					pthread_rwlock_unlock (page_lock);
					break;
				}else pthread_rwlock_unlock (page_lock);
			}
		}

		if (all_leaves) {
			/******************************************/
			page_t const* pages [cardinality];
			pthread_rwlock_t* page_locks [cardinality];

			for (uint32_t i=0; i<cardinality; ++i) {
				page_t const*const page = load_page (TREE(i),container->page_ids[i]);

				pthread_rwlock_rdlock (&TREE(i)->tree_lock);
				pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) get(TREE(i)->page_locks,container->page_ids[i]);
				pthread_rwlock_unlock (&TREE(i)->tree_lock);

				assert (page_lock != NULL);

				assert (page != NULL);
				assert (page->header.is_leaf);

				page_locks[i] = page_lock;
				pages[i] = page;

				if (pthread_rwlock_tryrdlock (page_lock)) {
					for (uint32_t j=0; j<i; ++j) {
						pthread_rwlock_unlock (page_locks[j]);
					}
					goto reset_search_operation;
				}
			}

			uint64_t offsets [cardinality];
			bzero (offsets,cardinality*sizeof(uint64_t));

			for (uint64_t j=0,i=0;;++j) {
				if (offsets[i] >= pages[i]->header.records) {
					offsets[i++] = 0;
					if (i >= cardinality)
						break;
				}else{
					multidata_container_t* data_container = (multidata_container_t*) malloc (sizeof(multidata_container_t));

					data_container->keys = (index_t*) malloc (cardinality*dimensions*sizeof(index_t));
					data_container->objects = (object_t*) malloc (cardinality*sizeof(object_t));
					data_container->cardinality = cardinality;
					data_container->dimensions = dimensions;

					for (uint32_t offset=0; offset<cardinality; ++offset) {
						data_container->objects[offset] = pages[offset]->node.leaf.objects[offsets[offset]];

						memcpy (data_container->keys+offset*dimensions,
								pages[offset]->node.leaf.keys+offsets[offset]*dimensions,
								dimensions*sizeof(index_t));
					}

					data_container->sort_key = use_avg?
							(pairwise?avgdistance_pairwise_multikey(data_container,0):avgdistance_ordered_multikey(data_container,0))
							:(less_than_theta?
							(pairwise?maxdistance_pairwise_multikey(data_container,0):maxdistance_ordered_multikey(data_container,0))
							:(pairwise?mindistance_pairwise_multikey(data_container,0):mindistance_ordered_multikey(data_container,0)));

					if (less_than_theta ? data_container->sort_key <= theta : data_container->sort_key >= theta) {
							insert_into_priority_queue (data_combinations,data_container);
					}else{
							free (data_container->objects);
							free (data_container->keys);
							free (data_container);
					}
					i=0;
				}
				++offsets[i];
			}

			for (uint32_t i=0; i<cardinality; ++i) {
				pthread_rwlock_unlock (page_locks[i]);
			}

			free (container->page_ids);
			free (container->boxes);
			free (container);
			/******************************************/
		}else{
			free (container->page_ids);
			free (container->boxes);
			free (container);
		}
	}

	delete_stack (browse);

	fifo_t* result = new_queue();
	while (data_combinations->size) {
		insert_at_tail_of_queue(result,remove_from_priority_queue(data_combinations));
	}

	delete_priority_queue (data_combinations);

	return result;
}


fifo_t* x_tuples (uint32_t const k, boolean const closest, boolean const use_avg, boolean const pairwise, lifo_t *const trees) {
	if (!k || !trees->size) return new_queue();

	index_t threshold = closest ? INDEX_T_MAX : -INDEX_T_MAX;
	uint32_t const cardinality = trees->size;
	uint32_t dimensions = UINT_MAX;

	for (uint32_t i=0; i<trees->size; ++i) {
		if (((tree_t*)trees->buffer[i])->dimensions < dimensions) {
			dimensions = ((tree_t*)trees->buffer[i])->dimensions;
		}
	}

	priority_queue_t *const data_combinations = new_priority_queue (closest?&maxcompare_multicontainers:&mincompare_multicontainers);
	priority_queue_t *const browse = new_priority_queue (closest?&mincompare_multicontainers:&maxcompare_multicontainers);

	reset_search_operation:;
	multibox_container_t* container = (multibox_container_t*) malloc (sizeof(multibox_container_t));
	container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));
	container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
	bzero (container->page_ids,cardinality*sizeof(uint64_t));

	container->sort_key = closest ? 0 : DBL_MAX;
	container->cardinality = cardinality;
	container->dimensions = dimensions;

	for (uint32_t i=0; i<cardinality; ++i) {
		pthread_rwlock_rdlock (&TREE(i)->tree_lock);

		memcpy (container->boxes+i*dimensions,
				TREE(i)->root_box,
				dimensions*sizeof(interval_t));

		pthread_rwlock_unlock (&TREE(i)->tree_lock);
	}

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		container = remove_from_priority_queue (browse);

		if (closest ?
			container->sort_key > threshold
			:container->sort_key < threshold) {

			free (container->page_ids);
			free (container->boxes);
			free (container);

			while (browse->size) {
				container = remove_from_priority_queue (browse);

				free (container->page_ids);
				free (container->boxes);
				free (container);
			}

			break;
		}

		boolean all_leaves = true;
		for (uint32_t i=0; i<container->cardinality; ++i) {
			uint64_t const page_id = container->page_ids[i];
			page_t const*const page = load_page (TREE(i),page_id);

			pthread_rwlock_rdlock (&TREE(i)->tree_lock);
			pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) get(TREE(i)->page_locks,page_id);
			pthread_rwlock_unlock (&TREE(i)->tree_lock);

			assert (page_lock != NULL);

			if (pthread_rwlock_tryrdlock (page_lock)) {
				while (browse->size) {
					multibox_container_t* temp = remove_from_priority_queue (browse);

					free (temp->page_ids);
					free (temp->boxes);
					free (temp);
				}

				for (uint32_t j=0; j<i; ++j) {
					pthread_rwlock_rdlock (&TREE(j)->tree_lock);
					pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get(TREE(j)->page_locks,page_id);
					pthread_rwlock_unlock (&TREE(j)->tree_lock);

					pthread_rwlock_unlock (previous_lock);
				}

				goto reset_search_operation;
			}

			if (!page->header.is_leaf) {
				all_leaves = false;
				for (register uint32_t j=0; j<page->header.records; ++j) {
					multibox_container_t* new_container = (multibox_container_t*) malloc (sizeof(multibox_container_t));

					new_container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
					memcpy (new_container->page_ids,container->page_ids,cardinality*sizeof(uint64_t));
					new_container->page_ids[i] = page_id*TREE(i)->internal_entries+j+1;

					new_container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));
					memcpy (new_container->boxes,container->boxes,cardinality*dimensions*sizeof(interval_t));
					memcpy (new_container->boxes+i*dimensions,
							page->node.internal.intervals+j*dimensions,
							dimensions*sizeof(interval_t));

					new_container->cardinality = cardinality;
					new_container->dimensions = dimensions;
					new_container->sort_key = closest ?
							(use_avg?
							(pairwise?avg_mindistance_pairwise_multibox(new_container,0):avg_mindistance_ordered_multibox(new_container,0))
							:(pairwise?max_mindistance_pairwise_multibox(new_container,0):max_mindistance_ordered_multibox(new_container,0)))
							:(use_avg?
							(pairwise?avg_maxdistance_pairwise_multibox(new_container,0):avg_maxdistance_ordered_multibox(new_container,0))
							:(pairwise?min_maxdistance_pairwise_multibox(new_container,0):min_maxdistance_ordered_multibox(new_container,0)));

					if (closest?
						new_container->sort_key < threshold
						:new_container->sort_key > threshold) {

						insert_into_priority_queue (browse,new_container);
					}
				}

				pthread_rwlock_unlock (page_lock);
				break;
			}else pthread_rwlock_unlock (page_lock);
		}

		if (all_leaves) {
			page_t const* pages [cardinality];
			pthread_rwlock_t * page_locks [cardinality];

			for (uint32_t i=0; i<cardinality; ++i) {
				page_t const* page = load_page(TREE(i),container->page_ids[i]);

				pthread_rwlock_rdlock (&TREE(i)->tree_lock);
				pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) get(TREE(i)->page_locks,container->page_ids[i]);
				pthread_rwlock_unlock (&TREE(i)->tree_lock);

				assert (page_lock != NULL);

				assert (page != NULL);
				assert (page->header.is_leaf);

				page_locks[i] = page_lock;
				pages[i] = page;

				if (pthread_rwlock_tryrdlock (page_lock)) {

					while (browse->size) {
						multibox_container_t* temp = remove_from_priority_queue (browse);

						free (temp->page_ids);
						free (temp->boxes);
						free (temp);
					}

					for (uint32_t j=0; j<i; ++j) {
						pthread_rwlock_unlock (page_locks[j]);
					}

					goto reset_search_operation;
				}
			}

			uint64_t offsets [cardinality];
			bzero (offsets,cardinality*sizeof(uint64_t));

			for (uint64_t j=0,i=0;;++j) {
				if (offsets[i] >= pages[i]->header.records) {
					offsets[i++] = 0;
					if (i >= cardinality)
						break;
				}else{
					multidata_container_t *const data_container = (multidata_container_t *const) malloc (sizeof(multidata_container_t));
					data_container->keys = (index_t*) malloc (cardinality*dimensions*sizeof(index_t));
					data_container->objects = (object_t*) malloc (cardinality*sizeof(object_t));

					for (uint32_t offset=0; offset<cardinality; ++offset) {
						data_container->objects[offset] = pages[offset]->node.leaf.objects[offsets[offset]];
						memcpy (data_container->keys+offset*dimensions,
								pages[offset]->node.leaf.keys+offsets[offset]*dimensions,
								dimensions*sizeof(index_t));
					}

					data_container->cardinality = cardinality;
					data_container->dimensions = dimensions;
					data_container->sort_key = closest ?
							(use_avg?
							(pairwise?
							avgdistance_pairwise_multikey(data_container,0)
							:avgdistance_ordered_multikey(data_container,0))
							:(pairwise?
							maxdistance_pairwise_multikey(data_container,0)
							:maxdistance_ordered_multikey(data_container,0)))
							:(use_avg?
							(pairwise?
							avgdistance_pairwise_multikey(data_container,0)
							:avgdistance_ordered_multikey(data_container,0))
							:(pairwise?
							mindistance_pairwise_multikey(data_container,0)
							:mindistance_ordered_multikey(data_container,0)));

					if (data_combinations->size < k) {
						insert_into_priority_queue (data_combinations,data_container);
					}else if (closest ?
							data_container->sort_key < threshold
							:data_container->sort_key > threshold) {

						multidata_container_t* temp = remove_from_priority_queue(data_combinations);

						free (temp->objects);
						free (temp->keys);
						free (temp);

						insert_into_priority_queue (data_combinations,data_container);
					}

					if (data_combinations->size == k) {
						threshold = ((multidata_container_t*) peek_priority_queue(data_combinations))->sort_key;
					}
					i=0;
				}
				++offsets[i];
			}

			for (uint32_t i=0; i<cardinality; ++i) {
				pthread_rwlock_unlock (page_locks[i]);
			}
		}

		free (container->page_ids);
		free (container->boxes);
		free (container);
	}

	delete_priority_queue (browse);

	fifo_t* result = new_queue();
	while (data_combinations->size) {
		insert_at_head_of_queue(result,remove_from_priority_queue(data_combinations));
	}

	delete_priority_queue (data_combinations);

	return result;
}

fifo_t* closest_tuples_ordered  (uint32_t const k,
				boolean const use_avg,
				tree_t *const tree0,...) {
	va_list args;
	va_start (args,tree0);

	lifo_t *const trees = new_stack();
	insert_into_stack (trees,tree0);
	for (tree_t* tree = tree0;
		(tree=va_arg (args,tree_t*)) && tree!=NULL;
		insert_into_stack (trees,tree))
		;

	va_end (args);

	fifo_t *const result = x_tuples (k,true,use_avg,false,trees);
	delete_stack (trees);
	return result;
}

fifo_t* farthest_tuples_ordered (uint32_t const k,
				boolean const use_avg,
				tree_t *const tree0,...) {
	va_list args;
	va_start (args,tree0);

	lifo_t *const trees = new_stack();
	insert_into_stack (trees,tree0);
	for (tree_t* tree = tree0;
		(tree=va_arg (args,tree_t*)) && tree!=NULL;
		insert_into_stack (trees,tree))
		;

	va_end (args);

	fifo_t *const result = x_tuples (k,false,use_avg,false,trees);
	delete_stack (trees);
	return result;
}

fifo_t* closest_tuples_pairwise  (uint32_t const k,
				boolean const use_avg,
				tree_t *const tree0,...) {
	va_list args;
	va_start (args,tree0);

	lifo_t *const trees = new_stack();
	insert_into_stack (trees,tree0);
	for (tree_t* tree = tree0;
		(tree=va_arg (args,tree_t*)) && tree!=NULL;
		insert_into_stack (trees,tree))
		;

	va_end (args);

	fifo_t *const result = x_tuples (k,true,use_avg,true,trees);
	delete_stack (trees);
	return result;
}

fifo_t* farthest_tuples_pairwise (uint32_t const k,
				boolean const use_avg,
				tree_t *const tree0,...) {
	va_list args;
	va_start (args,tree0);

	lifo_t *const trees = new_stack();
	insert_into_stack (trees,tree0);
	for (tree_t* tree = tree0;
		(tree=va_arg (args,tree_t*)) && tree!=NULL;
		insert_into_stack (trees,tree))
		;

	va_end (args);

	fifo_t *const result = x_tuples (k,false,use_avg,true,trees);
	delete_stack (trees);
	return result;
}
