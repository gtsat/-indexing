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

#include <math.h>
#include <pthread.h>
#include "spatial_standard_queries.h"
#include "priority_queue.h"
#include "symbol_table.h"
#include "common.h"
#include "queue.h"
#include "stack.h"
#include "rtree.h"
#include "defs.h"
#include "skyline_queries.h"

static boolean validate_skyline (fifo_t const*const queue, boolean const corner[], uint32_t const dimensions) {
	for (uint64_t i=0; i<queue->size; ++i) {
		data_pair_t const*const i_entry = (data_pair_t const*const) queue->buffer[i];
		for (uint64_t j=i+1; j<queue->size; ++j) {
			data_pair_t const*const j_entry = (data_pair_t const*const) queue->buffer[j];
			if (dominated_key (i_entry->key,j_entry->key,corner,dimensions)) {
				LOG (error," X Object %lu should not dominate on object %lu...\n",j_entry->object,i_entry->object);
				return false;
			}else if (dominated_key (j_entry->key,i_entry->key,corner,dimensions)) {
				LOG (error," X Object %lu should not dominate on object %lu...\n",i_entry->object,j_entry->object);
				return false;
			}
		}
	}
	return true;
}

/**
 * I have slightly changed Donald's skyline definition so as to be able to
 * orient the skyline by any corner of the key-space using the input bitfield.
 */

fifo_t* skyline (tree_t *const tree, boolean const corner[], uint32_t proj_dimensions) {
	index_t lo [tree->dimensions];
	index_t hi [tree->dimensions];
	for (uint32_t j=0; j<tree->dimensions; ++j) {
		lo [j] = tree->root_box[j].start;
		hi [j] = tree->root_box[j].end;
	}
	return skyline_constrained (tree,corner,lo,hi,proj_dimensions);
}


fifo_t* skyline_constrained (tree_t *const tree, boolean const corner[],
							 index_t const lo[], index_t const hi[],
							 uint32_t proj_dimensions) {
	if (tree->dimensions < proj_dimensions) {
		proj_dimensions = tree->dimensions;
	}

	interval_t query [proj_dimensions];
	for (uint32_t j=0; j<proj_dimensions; ++j) {
		if (lo[j]>hi[j]) {
			LOG (error,"Erroneous range query specified...\n");
			return NULL;
		}
		query[j].start = lo[j];
		query[j].end = hi[j];
	}

	pthread_rwlock_rdlock (&tree->tree_lock);

	if (!overlapping_boxes (query,tree->root_box,proj_dimensions)){
		pthread_rwlock_unlock (&tree->tree_lock);

		LOG (warn,"Query does not overlap with indexed area...\n");
		return new_queue();
	}else pthread_rwlock_unlock (&tree->tree_lock);

	load_page (tree,0);
	index_t reference_point [proj_dimensions];

	pthread_rwlock_rdlock (&tree->tree_lock);

	for (uint32_t j=0; j<proj_dimensions; ++j) {
		if (corner[j]) reference_point[j] = query[j].end;
		else reference_point[j] = query[j].start;
	}

	pthread_rwlock_unlock (&tree->tree_lock);

	priority_queue_t *const candidates = new_priority_queue (&mincompare_containers);
	priority_queue_t *const browse = new_priority_queue (&mincompare_containers);
	lifo_t* skyline = new_stack ();

	reset_search_operation:;

	box_container_t* container = (box_container_t*) malloc (sizeof(box_container_t));

	container->box = tree->root_box;
	container->sort_key = 0;
	container->id = 0;

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		container = remove_from_priority_queue(browse);
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
					if (!key_enclosed_by_box (page->node.leaf.KEY(i),query,proj_dimensions)) {
						continue;
					}

					data_container_t *const leaf_entry = (data_container_t *const) malloc (sizeof(data_container_t));

					leaf_entry->object = page->node.leaf.objects[i];
					leaf_entry->key = page->node.leaf.keys+i*tree->dimensions;
					leaf_entry->sort_key = key_to_key_distance (reference_point,leaf_entry->key,proj_dimensions);

					insert_into_priority_queue (candidates,leaf_entry);
				}

	            while (candidates->size) {
					data_container_t *const leaf_entry = (data_container_t *const) remove_from_priority_queue (candidates);

	                boolean is_dominated = false;
	                for (register uint64_t j=0; j<skyline->size;) {
	                        data_pair_t *const pair = (data_pair_t *const) skyline->buffer[j];
	                        if (dominated_key (pair->key,leaf_entry->key,corner,proj_dimensions)) {
	                                if (j < skyline->size-1) {
	                                		skyline->buffer[j] = skyline->buffer[skyline->size-1];
	                                }
	                                skyline->size--;

	                                free (pair->key);
	                                free (pair);
	                        }else if (dominated_key (leaf_entry->key,pair->key,corner,proj_dimensions)) {
	                                is_dominated = true;
	                                break;
	                        }else{
	                                ++j;
	                        }
	                }

	                if (is_dominated) {
	                		free (leaf_entry);
	                }else{
	                		data_pair_t *const pair = (data_pair_t *const) malloc (sizeof(data_pair_t));
	                		pair->key = (index_t *const) malloc (tree->dimensions*sizeof(index_t));
	                		memcpy (pair->key,leaf_entry->key,tree->dimensions*sizeof(index_t));
	                		pair->object = leaf_entry->object;

	                		insert_into_stack (skyline,pair);
	                }
				}
			}else{
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (!overlapping_boxes (query,page->node.internal.BOX(i),proj_dimensions)) {
						continue;
					}

					boolean is_dominated = false;
					for (register uint64_t j=0; j<skyline->size; ++j) {
						if (dominated_box (page->node.internal.BOX(i),
											((data_pair_t const*const)skyline->buffer[j])->key,
											corner,proj_dimensions)) {
							is_dominated = true;
							break;
						}
					}

					if (!is_dominated) {
						container = (box_container_t*) malloc (sizeof(box_container_t));

						container->id = CHILD_ID(page_id,i);
						container->box = page->node.internal.BOX(i);
						container->sort_key = key_to_box_mindistance(reference_point,container->box,proj_dimensions);

						insert_into_priority_queue (browse,container);
					}
				}
			}

			pthread_rwlock_unlock (page_lock);
		}
	}

	fifo_t *const result = transform_into_queue (skyline);
	assert (validate_skyline(result,corner,proj_dimensions));

	delete_priority_queue (candidates);
	delete_priority_queue (browse);

	return result;
}



static int cmp_object_ids (void const*const x,void const*const y) {
	return ((data_pair_t const*const)x)->object - ((data_pair_t const*const)y)->object;
}

fifo_t* multiskyline_sort_merge (lifo_t *const trees, boolean const corner[]) {
	unsigned cardinality = trees->size;

	unsigned full_dimensionality = 0;
	unsigned dimensions_offset [cardinality];

	priority_queue_t *sorted_ids [cardinality];
	priority_queue_t *const multiskyline = new_priority_queue (&mincompare_containers);

	for (unsigned i=0; i<cardinality; ++i) {
		fifo_t *const skyline_i = skyline (TREE(i),corner,TREE(i)->dimensions);
		if (!skyline_i->size) {
			break;
		}

		priority_queue_t *const sorted_ids_i = new_priority_queue (&cmp_object_ids);
		while (skyline_i->size) {
			insert_into_priority_queue (sorted_ids_i,remove_tail_of_queue (skyline_i));
		}
		sorted_ids[i] = sorted_ids_i;
		delete_queue (skyline_i);

		dimensions_offset [i] = full_dimensionality;
		full_dimensionality+=TREE(i)->dimensions;
	}


	index_t reference_point [full_dimensionality];
	for (unsigned i=0; i<cardinality; ++i) {
		pthread_rwlock_rdlock (&TREE(i)->tree_lock);
		for (uint16_t j=0; j<TREE(i)->dimensions; ++j) {
			if (corner[j]) reference_point [dimensions_offset[i]+j] = TREE(i)->root_box[j].end;
			else reference_point [dimensions_offset[i]+j] = TREE(i)->root_box[j].start;
		}
		pthread_rwlock_unlock (&TREE(i)->tree_lock);
	}

	sort_merge:
	while (sorted_ids[0]->size) {
		object_t next_id = ((data_pair_t const*const)peek_priority_queue (sorted_ids[0]))->object;
		for (unsigned i=1; i<cardinality; ++i) {
			if (!sorted_ids[i]->size) {
				goto finalize;
			}
			object_t object_id = ((data_pair_t const*const)peek_priority_queue (sorted_ids[i]))->object;
			if (object_id > next_id) {
				next_id = object_id;
			}
		}

		for (unsigned i=0; i<cardinality; ++i) {
			while (sorted_ids[i]->size && next_id > ((data_pair_t const*const)peek_priority_queue (sorted_ids[i]))->object) {
				data_pair_t *const pair = remove_from_priority_queue (sorted_ids[i]);

				free (pair->key);
				free (pair);
			}
			if (!sorted_ids[i]->size) {
				goto finalize;
			}
			if (next_id < ((data_pair_t const*const)peek_priority_queue (sorted_ids[i]))->object) {
				remove_from_priority_queue (sorted_ids[0]);
				goto sort_merge;
			}
		}

		data_container_t *const multiskyline_tuple = (data_container_t *const) malloc (sizeof(data_container_t));
		multiskyline_tuple->key = (index_t*) malloc (full_dimensionality*sizeof(index_t));
		multiskyline_tuple->sort_key = 0;

		for (unsigned i=0; i<cardinality; ++i) {
			data_pair_t *const pair = remove_from_priority_queue (sorted_ids[i]);
			memcpy (multiskyline_tuple->key+dimensions_offset[i],pair->key,TREE(i)->dimensions*sizeof(index_t));

			free (pair->key);
			free (pair);
		}
		multiskyline_tuple->object = next_id;
		multiskyline_tuple->sort_key = key_to_key_distance(multiskyline_tuple->key,reference_point,full_dimensionality);

		insert_into_priority_queue (multiskyline,multiskyline_tuple);
	}


	finalize:;
	fifo_t *const result = new_queue();
	while (multiskyline->size) {
		data_container_t *const multiskyline_tuple = remove_from_priority_queue (multiskyline);

		data_pair_t *const result_tuple = (data_pair_t*) malloc (sizeof(data_pair_t));
		result_tuple->object = multiskyline_tuple->object;
		result_tuple->key = multiskyline_tuple->key;

		insert_at_tail_of_queue (result,result_tuple);

		fprintf (stdout," !! Retrieved object %u with distance %lf.\n",multiskyline_tuple->object,multiskyline_tuple->sort_key);

		free (multiskyline_tuple);
	}
	delete_priority_queue (multiskyline);

	for (unsigned i=0; i<cardinality; ++i) {
		while (sorted_ids[i]->size) {
			data_pair_t *const pair = remove_from_priority_queue (sorted_ids[i]);
			free (pair->key);
			free (pair);
		}
		delete_priority_queue (sorted_ids[i]);
	}

	assert (validate_skyline(result,corner,full_dimensionality));

	return result;
}



fifo_t* multiskyline_join (lifo_t *const trees, boolean const corner[]) {
	if (!trees->size) return new_queue();

	priority_queue_t *const browse = new_priority_queue (&mincompare_multicontainers);
	priority_queue_t *const candidates = new_priority_queue (&mincompare_containers);
	lifo_t *const multiskyline = new_stack();

	uint32_t const cardinality = trees->size;
	uint32_t dimensions_offset [cardinality];
	uint32_t full_dimensionality = 0;
	for (unsigned i=0; i<cardinality; ++i) {
		dimensions_offset [i] = full_dimensionality;
		full_dimensionality += TREE(i)->dimensions;
	}

	index_t reference_point [full_dimensionality];
	for (unsigned i=0; i<cardinality; ++i) {
		pthread_rwlock_rdlock (&TREE(i)->tree_lock);
		for (uint16_t j=0; j<TREE(i)->dimensions; ++j) {
			if (corner[j]) reference_point [dimensions_offset[i]+j] = TREE(i)->root_box[j].end;
			else reference_point [dimensions_offset[i]+j] = TREE(i)->root_box[j].start;
		}
		pthread_rwlock_unlock (&TREE(i)->tree_lock);
	}

	reset_search_operation:;
	multibox_container_t* container = (multibox_container_t*) malloc (sizeof(multibox_container_t));
	container->boxes = (interval_t *const) malloc (cardinality*full_dimensionality*sizeof(interval_t));
	container->page_ids = (uint64_t *const) malloc (cardinality*sizeof(uint64_t));
	bzero (container->page_ids,cardinality*sizeof(uint64_t));

	container->dimensions = full_dimensionality;
	container->cardinality = cardinality;
	container->sort_key = 0;

	for (uint32_t i=0; i<cardinality; ++i) {
		pthread_rwlock_rdlock (&TREE(i)->tree_lock);
		memcpy (container->boxes+dimensions_offset[i],TREE(i)->root_box,TREE(i)->dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&TREE(i)->tree_lock);
	}

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		boolean all_leaves = true;
		container = remove_from_priority_queue (browse);
		for (uint32_t i=0; i<container->cardinality; ++i) {
			uint64_t const page_id = container->page_ids[i];
			page_t const*const page = load_page (TREE(i),page_id);

			pthread_rwlock_rdlock (&TREE(i)->tree_lock);
			pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) get(TREE(i)->page_locks,page_id);
			pthread_rwlock_unlock (&TREE(i)->tree_lock);

			assert (page_lock != NULL);

			if (pthread_rwlock_tryrdlock (page_lock)) {
				while (browse->size) {
					multibox_container_t* tmp = remove_from_priority_queue (browse);
					free (tmp->page_ids);
					free (tmp->boxes);
					free (tmp);
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
					multibox_container_t *const new_container = (multibox_container_t *const) malloc (sizeof(multibox_container_t));

					new_container->page_ids = (uint64_t *const) malloc (cardinality*sizeof(uint64_t));
					memcpy (new_container->page_ids,container->page_ids,cardinality*sizeof(uint64_t));
					new_container->page_ids[i] = page_id*TREE(i)->internal_entries+j+1;

					new_container->boxes = (interval_t *const) malloc (full_dimensionality*sizeof(interval_t));
					memcpy (new_container->boxes,container->boxes,full_dimensionality*sizeof(interval_t));
					memcpy (new_container->boxes+dimensions_offset[i],
							page->node.internal.intervals+j*TREE(i)->dimensions,
							TREE(i)->dimensions*sizeof(interval_t));

					new_container->dimensions = full_dimensionality;
					new_container->cardinality = cardinality;

					index_t artifical_box_closest_point [full_dimensionality];
					for (uint32_t k=0; k<cardinality; ++k) {
						for (uint16_t d=0; d<TREE(k)->dimensions; ++d) {
							if (corner[k]) {
								*(artifical_box_closest_point+dimensions_offset[k]+d) = (new_container->boxes+dimensions_offset[k]+d)->end;
							}else{
								*(artifical_box_closest_point+dimensions_offset[k]+d) = (new_container->boxes+dimensions_offset[k]+d)->start;
							}
						}
					}

					boolean is_dominated = false;
					for (register uint64_t j=0; j<multiskyline->size; ++j) {
						if (dominated_key (artifical_box_closest_point,
											((data_pair_t const*const)multiskyline->buffer[j])->key,
											corner,full_dimensionality)) {
							is_dominated = true;
							break;
						}
					}

					if (!is_dominated) {
						new_container->sort_key = key_to_key_distance (reference_point,artifical_box_closest_point,full_dimensionality);
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

			page_t* min_loaded_page = NULL;
			uint32_t min_load = UINT_MAX;
			uint32_t min_loaded_page_index = 0;
			for (uint32_t i=0; i<cardinality; ++i) {
				page_t const* page = load_page(TREE(i),container->page_ids[i]);

				pthread_rwlock_rdlock (&TREE(i)->tree_lock);
				pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) get(TREE(i)->page_locks,container->page_ids[i]);
				pthread_rwlock_unlock (&TREE(i)->tree_lock);

				page_locks[i] = page_lock;
				pages[i] = page;

				if (pthread_rwlock_tryrdlock (page_lock)) {
					while (browse->size) {
						multibox_container_t* tmp = remove_from_priority_queue (browse);
						free (tmp->page_ids);
						free (tmp->boxes);
						free (tmp);
					}
					for (uint32_t j=0; j<i; ++j) {
						pthread_rwlock_unlock (page_locks[j]);
					}
					goto reset_search_operation;
				}

				if (page->header.records < min_load) {
					min_load = page->header.records;
					min_loaded_page_index = i;
					min_loaded_page = page;
				}
			}

			for (uint32_t i=0; i<min_loaded_page->header.records; ++i) {
				data_container_t *const skytuple_candidate = (data_container_t *const) malloc (sizeof(data_container_t));
				skytuple_candidate->key = (index_t*) malloc (full_dimensionality*sizeof(index_t));
				skytuple_candidate->object = min_loaded_page->node.leaf.objects[i];
				boolean fully_matches = true;

				for (uint32_t j=0; j<cardinality; ++j) {
					boolean matches = false;
					if (j != min_loaded_page_index) {
						for (uint32_t k=0; k<pages[j]->header.records; ++k) {
							if (pages[j]->node.leaf.objects[k] == min_loaded_page->node.leaf.objects[i]) {
								memcpy (skytuple_candidate->key+dimensions_offset[j],pages[j]->node.leaf.keys+k*TREE(j)->dimensions,TREE(j)->dimensions*sizeof(index_t));
								matches = true;
								break;
							}
						}
					}else{
						memcpy (skytuple_candidate->key+dimensions_offset[j],pages[j]->node.leaf.keys+i*TREE(j)->dimensions,TREE(j)->dimensions*sizeof(index_t));
						matches = true;
					}

					if (!matches) {
						fully_matches = false;
						break;
					}
				}

				if (fully_matches) {
					skytuple_candidate->dimensions = full_dimensionality;
					skytuple_candidate->sort_key = key_to_key_distance (skytuple_candidate->key,reference_point,full_dimensionality);
					insert_into_priority_queue (candidates,skytuple_candidate);
				}
			}

            while (candidates->size) {
				data_container_t *const leaf_entry = (data_container_t *const) remove_from_priority_queue (candidates);

                boolean is_dominated = false;
                for (register uint64_t j=0; j<multiskyline->size;) {
                        data_pair_t *const pair = (data_pair_t *const) multiskyline->buffer[j];
                        if (dominated_key (pair->key,leaf_entry->key,corner,full_dimensionality)) {
                                if (j < multiskyline->size-1) {
                                		multiskyline->buffer[j] = multiskyline->buffer[multiskyline->size-1];
                                }
                                multiskyline->size--;

                                free (pair->key);
                                free (pair);
                        }else if (dominated_key (leaf_entry->key,pair->key,corner,full_dimensionality)) {
                                is_dominated = true;
                                break;
                        }else{
                                ++j;
                        }
                }

                if (is_dominated) {
                		free (leaf_entry->key);
                		free (leaf_entry);
                }else{
                		insert_into_stack (multiskyline,leaf_entry);
                }
			}

			for (uint32_t i=0; i<cardinality; ++i) {
				pthread_rwlock_unlock (page_locks[i]);
			}
		}

		free (container->page_ids);
		free (container->boxes);
		free (container);
	}

	fifo_t *const result = transform_into_queue (multiskyline);
	assert (validate_skyline(result,corner,full_dimensionality));

	delete_priority_queue (candidates);
	delete_priority_queue (browse);

	return result;
}


static
index_t compute_lower_bound (multidata_container_t const*const container, priority_queue_t const*const browse[]) {
	index_t lower_bound_i = container->sort_key;
	for (unsigned j=0; j<container->cardinality; ++j) {
		if (container->keys[j] < 0) {
			if (browse[j]->size) {
				lower_bound_i += ((multidata_container_t const*const)peek_priority_queue(browse[j]))->sort_key;
			}else{
				return INDEX_T_MAX;
			}
		}
	}
	return lower_bound_i;
}

#define C 5

static
index_t compute_best_lower_bound (fifo_t const*const sorted_list, priority_queue_t const*const browse[]) {

	index_t best_lower_bound = INDEX_T_MAX;
	for (register uint64_t i=0; i<C && i<sorted_list->size; ++i) {
		index_t const lower_bound_i = compute_lower_bound 
					(((multidata_container_t const*const)sorted_list->buffer[i]),browse);
		if (lower_bound_i < best_lower_bound) {
			best_lower_bound = lower_bound_i;
		}
	}

	for (register uint64_t i=C; i<sorted_list->size; ++i) {
		multidata_container_t *const container = ((multidata_container_t const*const)sorted_list->buffer[i]);
		if (best_lower_bound > container->sort_key) {
			index_t const lower_bound_i = compute_lower_bound (container,browse);
			if (lower_bound_i < best_lower_bound) {
				best_lower_bound = lower_bound_i;
			}
		}else{
			break;
		}
	}

	return best_lower_bound;
}

/**
 * UNDER CONSTRUCTION
 *
 *
 *
fifo_t* multiskyline (lifo_t *const trees, boolean const corner[]) {
	unsigned const cardinality = trees->size;

	priority_queue_t *const leaf_entries = new_priority_queue (&mincompare_containers);

	priority_queue_t* browse [cardinality];
	index_t* reference_point [cardinality];
	lifo_t* skyline [cardinality];

	priority_queue_t *const complete_distance_objects = new_priority_queue (&mincompare_containers);

	symbol_table_t *const data_pairs = new_symbol_table_primitive (NULL);
	symbol_table_t *const dist_pairs = new_symbol_table_primitive (NULL);

	fifo_t *const sorted_object_list = new_queue();

	unsigned offset = 0;
	unsigned dimensions_offset [cardinality];
	for (unsigned i=0; i<cardinality; ++i) {
		//offset += TREE(i)->dimensions;
		dimensions_offset [i] = offset;
		offset += TREE(i)->dimensions;

		browse [i] = new_priority_queue (&mincompare_containers);

		index_t *const reference_point_i = (index_t *const) malloc (TREE(i)->dimensions*sizeof(index_t));

		pthread_rwlock_rdlock (&TREE(i)->tree_lock);
		for (uint16_t j=0; j<TREE(i)->dimensions; ++j) {
			if (corner[j]) reference_point_i [j] = TREE(i)->root_box[j].end;
			else reference_point_i [j] = TREE(i)->root_box[j].start;
		}
		pthread_rwlock_unlock (&TREE(i)->tree_lock);

		reference_point [i] = reference_point_i;
		skyline [i] = new_stack();
	}

	reset_search_operation:
	for (unsigned i=0; i<cardinality; ++i) {
		box_container_t *const container = (box_container_t *const) malloc (sizeof(box_container_t));

		container->box = TREE(i)->root_box;
		container->sort_key = INDEX_T_MAX;
		container->id = 0;

		insert_into_priority_queue (browse[i],container);
	}

	while (true) {
		for (unsigned i=0; i<cardinality; ++i) {
			index_t *const reference_point_i = reference_point [i];
			priority_queue_t *const browse_i = browse [i];
			lifo_t *const skyline_i = skyline [i];

			if (browse_i->size) {
				box_container_t* container = remove_from_priority_queue (browse_i);
				uint64_t const page_id = container->id;
				free (container);

				page_t const*const page = load_rtree_page(TREE(i),page_id);

				pthread_rwlock_rdlock (&TREE(i)->tree_lock);
				pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const)get(TREE(i)->page_locks,page_id);
				pthread_rwlock_unlock (&TREE(i)->tree_lock);

				assert (page_lock != NULL);

				if (pthread_rwlock_tryrdlock (page_lock)) {
					for (unsigned j=0; j<cardinality; ++j) {
						while (browse[j]->size) {
							free (remove_from_priority_queue (browse[j]));
						}
					}
					goto reset_search_operation;
				}else{
					if (page->header.is_leaf) {
						for (register uint32_t j=0; j<page->header.records; ++j) {
							data_container_t* leaf_entry = (data_container_t*) malloc (sizeof(data_container_t));

							leaf_entry->key = page->node.leaf.keys+j*TREE(i)->dimensions;
							leaf_entry->object = page->node.leaf.objects[j];
							leaf_entry->sort_key = key_to_key_distance(reference_point_i,leaf_entry->key,TREE(i)->dimensions);

							insert_into_priority_queue (leaf_entries,leaf_entry);
						}

						while (leaf_entries->size) {
							data_container_t *const leaf_entry = (data_container_t *const) remove_from_priority_queue (leaf_entries);

							boolean is_dominated = false;
							for (register uint64_t k=0; k<skyline_i->size; ++k) {
								if (dominated_key (leaf_entry->key,
											((data_pair_t*)skyline_i->buffer[k])->key,
											corner,TREE(i)->dimensions)) {
									is_dominated = true;
									break;
								}
							}

							if (!is_dominated) {
								object_t object = leaf_entry->object;

								data_pair_t* data_pair = (data_pair_t *const) malloc (sizeof(data_pair_t));
								data_pair->key = (index_t*) malloc (sizeof(index_t)*TREE(i)->dimensions);
								memcpy (data_pair->key,leaf_entry->key,sizeof(index_t)*TREE(i)->dimensions);
								data_pair->object = object;

								insert_into_stack (skyline_i,data_pair);

								data_container_t* partial_dists = get (dist_pairs,object);

								boolean first_seen_object = false;
								if (partial_dists == NULL) {
									partial_dists = (data_pair_t*) malloc (sizeof(data_pair_t));
									partial_dists->key = (index_t*) malloc (cardinality*sizeof(double));
									for (unsigned j=0; j<cardinality; ++j) {
										partial_dists->key[j] = -DBL_MAX;
									}
									partial_dists->object = object;
									partial_dists->dimensions = cardinality;
									set (dist_pairs,object,partial_dists);

									assert (get(data_pairs,object) == NULL);
									data_pair = (data_pair_t*) malloc (sizeof(data_pair_t));
									data_pair->key = (index_t*) malloc (offset*sizeof(index_t));
									data_pair->object = object;
									set (data_pairs,object,data_pair);

									first_seen_object = true;
								}else{
									data_pair = get (data_pairs,object);
									assert (data_pair != NULL);
								}

								if (partial_dists->key[i] < 0) {
									memcpy (data_pair->key+dimensions_offset[i],leaf_entry->key,TREE(i)->dimensions*sizeof(index_t));
									partial_dists->key[i] = leaf_entry->sort_key;

									boolean is_object_encountered_in_all_domains = true;

									index_t summed_dists = 0;
									for (unsigned k=0; k<cardinality; ++k) {
										if (partial_dists->key[k] > 0) {
											summed_dists += partial_dists->key[k];
										}else{
											is_object_encountered_in_all_domains = false;
										}
									}

									partial_dists->sort_key = summed_dists;

									if (is_object_encountered_in_all_domains) {
//fprintf (stdout," ++ Retrieved object %u with distance %f.\n",partial_dists->objects,partial_dists->sort_key);
										insert_into_priority_queue (complete_distance_objects,partial_dists);
									}else{
										unsigned container_position = 0;
										if (!first_seen_object) {
											container_position = find_position_in_sorted_queue
											(sorted_object_list,partial_dists,&mincompare_containers);

											for (data_container_t *const ptr = (multidata_container_t*) get_queue_element (sorted_object_list,container_position);
													ptr != partial_dists && container_position < sorted_object_list->size;
													++container_position)
												;

											assert (container_position < sorted_object_list->size);
											remove_queue_element (sorted_object_list,container_position);
										}

										container_position = find_position_in_sorted_queue
											(sorted_object_list,partial_dists,&mincompare_containers);

										insert_queue_element(sorted_object_list,container_position,partial_dists);
									}
								}
							}
							free (leaf_entry);
						}
					}else{
						for (register uint32_t j=0; j<page->header.records; ++j) {
							boolean is_dominated = false;
							for (register uint64_t k=0; k<skyline_i->size; ++k) {
								if (dominated_box (page->node.internal.intervals+j*TREE(i)->dimensions,
											((data_pair_t*)skyline_i->buffer[k])->key,
											corner,TREE(i)->dimensions)) {
									is_dominated = true;
									break;
								}
							}

							if (!is_dominated) {
								uint64_t subsumed_page_id = page_id*TREE(i)->internal_entries+j+1;
								box_container_t *const subcontainer = (box_container_t *const) malloc (sizeof(box_container_t));
								if (subcontainer == NULL) {
									LOG (fatal,"Unable to allocate additional memory in multiskyline_indisk() to expand the branch from block %u.\n",subsumed_page_id);
									abort();
								}

								subcontainer->box = page->node.internal.intervals+j*TREE(i)->dimensions;
								subcontainer->id = subsumed_page_id;
								subcontainer->sort_key = key_to_box_mindistance
											(reference_point_i,subcontainer->box,TREE(i)->dimensions);

								insert_into_priority_queue (browse_i,subcontainer);
							}
						}
					}
					pthread_rwlock_unlock (page_lock);
				}
			}else{
				goto double_break;
			}
		}
	}

	double_break:;
	assert (data_pairs->size == dist_pairs->size);

	fifo_t *const multiskyline = new_queue();
	while (complete_distance_objects->size) {
		data_container_t *const multiskyline_tuple = remove_from_priority_queue (complete_distance_objects);

		insert_at_tail_of_queue (multiskyline,get(data_pairs,(object_t)multiskyline_tuple->object));

fprintf (stdout," !! Retrieved object %u with distance %f.\n",multiskyline_tuple->object,multiskyline_tuple->sort_key);

		free (multiskyline_tuple->key);
		free (multiskyline_tuple);
	}

	while (sorted_object_list->size) {
		multidata_container_t *const tmp0 = remove_tail_of_queue (sorted_object_list);
		multidata_container_t *const tmp1 = get (dist_pairs, (object_t)tmp0->objects);

		assert (tmp1 != NULL);
		assert (tmp1 == tmp0);

		//free (tmp0->keys); ////////////////////////////////
		//free (tmp0); //////////////////////////////////////
	}

	delete_queue (sorted_object_list);
	delete_symbol_table (data_pairs);
	delete_symbol_table (dist_pairs);


	for (unsigned i=0; i<cardinality; ++i) {
		while (browse[i]->size) {
			free (remove_from_priority_queue (browse[i]));
		}
		delete_priority_queue (browse[i]);

		while (skyline[i]->size) {
			data_pair_t *const data_pair = remove_from_stack (skyline[i]);
			free (data_pair->key);
			free (data_pair);
		}
		delete_stack (skyline[i]);

		free (reference_point[i]);
	}

	delete_priority_queue (complete_distance_objects);
	delete_priority_queue (leaf_entries);

	return multiskyline;
}
*/

