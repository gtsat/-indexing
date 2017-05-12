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

fifo_t* skyline (tree_t *const tree, boolean const corner[]) {
	index_t lo [tree->dimensions];
	index_t hi [tree->dimensions];
	for (uint32_t j=0; j<tree->dimensions; ++j) {
		lo [j] = tree->root_box[j].start;
		hi [j] = tree->root_box[j].end;
	}
	return skyline_constrained (tree,corner,lo,hi);
}


fifo_t* skyline_constrained (tree_t *const tree, boolean const corner[],
							 index_t const lo[], index_t const hi[]) {

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

	load_rtree_page (tree,0);
	index_t reference_point [tree->dimensions];

	pthread_rwlock_rdlock (&tree->tree_lock);

	for (uint32_t j=0;j<tree->dimensions;++j) {
		if (corner[j]) reference_point[j] = query[j].end;
		else reference_point[j] = query[j].start;
	}

	pthread_rwlock_unlock (&tree->tree_lock);

	priority_queue_t* candidates = new_priority_queue (&mincompare_containers);
	priority_queue_t* browse = new_priority_queue (&mincompare_containers);
	lifo_t* skyline = new_stack ();

	reset_search_operation:;

	box_container_t* container = (box_container_t*) malloc (sizeof(box_container_t));

	container->box = tree->root_box;
	container->id = 0;
	container->sort_key = 0;

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		container = remove_from_priority_queue(browse);
		uint64_t const page_id = container->id;
		page_t const*const page = load_rtree_page(tree,page_id);

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
					if (!key_enclosed_by_box (page->node.leaf.KEY(i),query,tree->dimensions)) {
						continue;
					}

					data_container_t *const leaf_entry = (data_container_t*) malloc (sizeof(data_container_t));

					leaf_entry->key = page->node.leaf.keys+i*tree->dimensions;
					leaf_entry->object = page->node.leaf.objects[i];
					leaf_entry->sort_key = key_to_key_distance (reference_point,leaf_entry->key,tree->dimensions);

					insert_into_priority_queue (candidates,leaf_entry);
				}

	            while (candidates->size) {
					data_container_t *const leaf_entry = (data_container_t *const) remove_from_priority_queue (candidates);

	                boolean is_dominated = false;
	                for (register uint64_t j=0; j<skyline->size;) {
	                        data_pair_t *const pair = (data_pair_t *const) skyline->buffer[j];
	                        if (dominated_key (pair->key,leaf_entry->key,corner,tree->dimensions)) {
	                                if (j < skyline->size-1) {
	                                	skyline->buffer[j] = skyline->buffer[skyline->size-1];
	                                }
	                                skyline->size--;

	                                free (pair->key);
	                                free (pair);
	                        }else if (dominated_key (leaf_entry->key,pair->key,corner,tree->dimensions)) {
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
	                	pair->key = (index_t *) malloc (tree->dimensions*sizeof(index_t));
	                	memcpy (pair->key,leaf_entry->key,tree->dimensions*sizeof(index_t));
	                	pair->object = leaf_entry->object;

	                	insert_into_stack (skyline,pair);
	                }
				}
			}else{
				for (register uint32_t i=0; i<page->header.records; ++i) {
					if (!overlapping_boxes (query,page->node.internal.BOX(i),tree->dimensions)) {
						continue;
					}

					boolean is_dominated = false;
					for (register uint64_t j=0; j<skyline->size; ++j) {
						if (dominated_box (page->node.internal.BOX(i),
											((data_pair_t const*const)skyline->buffer[j])->key,
											corner,tree->dimensions)) {
							is_dominated = true;
							break;
						}
					}

					if (!is_dominated) {
						container = (box_container_t*) malloc (sizeof(box_container_t));

						container->box = page->node.internal.BOX(i);
						container->id = CHILD_ID(page_id,i);
						container->sort_key = key_to_box_mindistance
												(reference_point,container->box,tree->dimensions);

						insert_into_priority_queue (browse,container);
					}
				}
			}

			pthread_rwlock_unlock (page_lock);
		}
	}

	fifo_t *const result = transform_into_queue (skyline);
	assert (validate_skyline(result,corner,tree->dimensions));

	delete_priority_queue (candidates);
	delete_priority_queue (browse);

	return result;
}


