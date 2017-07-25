/**
 *  Copyright (C) 2017 George Tsatsanifos <gtsatsanifos@gmail.com>
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
#include "spatial_diversification_queries.h"
#include "priority_queue.h"
#include "symbol_table.h"
#include "queue.h"
#include "stack.h"
#include "rtree.h"
#include "defs.h"
#include "common.h"


static void augment_set_with_hotspots (tree_t *const tree,
					interval_t const query[],
					multidata_container_t *const result_container,
					fifo_t const*const attractors,
					fifo_t const*const repellers,
					//symbol_table_t const*const matched_attractors,
					//symbol_table_t const*const matched_repellers,
					unsigned const k,
					boolean const pull_apart_results,
					boolean const bring_together_results,
					double const lambda_rel,
					double const lambda_diss,
					double const lambda_clust) {

	assert (!pull_apart_results || !bring_together_results);

	pthread_rwlock_rdlock (&tree->tree_lock);
	if (!overlapping_boxes (query,tree->root_box,tree->dimensions)) {
		pthread_rwlock_unlock (&tree->tree_lock);
		return;
	}else pthread_rwlock_unlock (&tree->tree_lock);

	uint16_t const dimensions = tree->dimensions;

	priority_queue_t* candidates = new_priority_queue (&mincompare_containers);
	priority_queue_t* browse = new_priority_queue (&maxcompare_containers);

	reset_search_operation:;

	box_container_t* container = (box_container_t*) malloc (sizeof(box_container_t));

	container->box = tree->root_box;
	container->sort_key = 0;
	container->id = 0;

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		container = remove_from_priority_queue (browse);

		if (candidates->size >= k &&
			container->sort_key <=
			((data_container_t*)peek_priority_queue(candidates))->sort_key) {

			free (container);
			while (browse->size) {
				free (remove_from_priority_queue (browse));
			}
			break;
		}

		uint64_t const page_id = container->id;
		page_t const*const page = load_page(tree,page_id);
		assert (page != NULL);

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
				for (uint32_t i=0; i<page->header.records; ++i) {
					if (!key_enclosed_by_box (page->node.leaf.KEY(i),query,dimensions)) {
						continue;
					}

					boolean is_obscured = false;

					double relevance = attractors->size ? DBL_MAX : 0;
					for (uint64_t j=0; j<attractors->size; ++j) {
						double key_distance = key_to_key_distance (
										attractors->buffer[j],
										page->node.leaf.KEY(i),
										dimensions);
						if (key_distance < relevance) {
							relevance = key_distance;
						}
					}

					relevance *= lambda_rel;


					double dissimilarity = repellers->size ? DBL_MAX : 0;
					for (uint64_t j=0; j<repellers->size; ++j) {
						double key_distance = key_to_key_distance (
										repellers->buffer[j],
										page->node.leaf.KEY(i),
										dimensions);

						if (key_distance < dissimilarity) {
							if (!pull_apart_results
								&& !bring_together_results
								&& candidates->size >= k
								&& dissimilarity - relevance
								<= ((data_container_t*)peek_priority_queue(candidates))->sort_key) {

								is_obscured = true;
								break;
							}else{
								dissimilarity = key_distance;
							}
						}
					}

					dissimilarity *= lambda_diss;

					double clustering = result_container->cardinality ? DBL_MAX : 0;
					if (pull_apart_results) {
						for (uint16_t j=0; j<result_container->cardinality; ++j) {
							double key_distance = page->node.leaf.objects[i] != result_container->objects[j]?
													key_to_key_distance (
														result_container->keys+j*dimensions,
														page->node.leaf.KEY(i),
														dimensions):-DBL_MAX;

							key_distance *= lambda_clust;

							if (key_distance < clustering) {
								if (candidates->size >= k
									&& key_distance + dissimilarity - relevance
									<= ((data_container_t*)peek_priority_queue(candidates))->sort_key) {

									is_obscured = true;
									break;
								}else{
									clustering = key_distance;
								}
							}
						}
					}else if (bring_together_results) {
						clustering = result_container->cardinality ? -DBL_MAX : 0;
						for (uint16_t j=0; j<result_container->cardinality; ++j) {
							double key_distance = page->node.leaf.objects[i] != result_container->objects[j]?
													key_to_key_distance (
													result_container->keys+j*dimensions,
													page->node.leaf.KEY(i),
													dimensions):DBL_MAX;

							key_distance *= lambda_clust;

							if (key_distance > clustering) {
								if (candidates->size >= k
									&& - key_distance + dissimilarity - relevance
									<= ((data_container_t*)peek_priority_queue(candidates))->sort_key) {

									is_obscured = true;
									break;
								}else{
									clustering = key_distance;
								}
							}
						}
					}


					if (candidates->size < k) {
						data_container_t* candidate = (data_container_t*) malloc (sizeof(data_container_t));

						candidate->key = (index_t*) malloc (dimensions*sizeof(index_t));
						memcpy (candidate->key,page->node.leaf.KEY(i),dimensions*sizeof(index_t));

						candidate->object = page->node.leaf.objects[i];
						candidate->sort_key = dissimilarity - relevance;
						if (pull_apart_results) candidate->sort_key += clustering;
						else if (bring_together_results) candidate->sort_key -= clustering;

						insert_into_priority_queue (candidates,candidate);

					}else if (!is_obscured) {
						index_t tmp_score = dissimilarity - relevance;
						if (pull_apart_results) tmp_score += clustering;
						else if (bring_together_results) tmp_score -= clustering;

						if (tmp_score > ((data_container_t*)peek_priority_queue(candidates))->sort_key) {
							data_container_t* candidate = remove_from_priority_queue (candidates);

							memcpy (candidate->key,page->node.leaf.KEY(i),dimensions*sizeof(index_t));
							candidate->object = page->node.leaf.objects[i];
							candidate->sort_key = tmp_score;

							insert_into_priority_queue (candidates,candidate);
						}
					}
				}
			}else{
				for (uint32_t i=0; i<page->header.records; ++i) {
					if (!overlapping_boxes (query,page->node.internal.BOX(i),dimensions)) {
						continue;
					}

					boolean is_obscured = false;

					double relevance = attractors->size ? DBL_MAX : 0;
					for (uint64_t j=0; j<attractors->size; ++j) {
						double box_distance = key_to_box_mindistance (
											attractors->buffer[j],
											page->node.internal.BOX(i),
											dimensions);

						if (box_distance < relevance) {
							relevance = box_distance;
						}
					}

					relevance *= lambda_rel;


					double dissimilarity = repellers->size ? DBL_MAX : 0;
					for (uint64_t j=0; j<repellers->size; ++j) {
						double box_distance = key_to_box_maxdistance (
											repellers->buffer[j],
											page->node.internal.BOX(i),
											dimensions);

						if (box_distance < dissimilarity) {
							if (!pull_apart_results
								&& !bring_together_results
								&& candidates->size >= k
								&& dissimilarity - relevance
								<= ((data_container_t*)peek_priority_queue(candidates))->sort_key) {

								is_obscured = true;
								break;
							}else{
								dissimilarity = box_distance;
							}
						}
					}

					dissimilarity *= lambda_diss;

					double clustering = result_container->cardinality ? DBL_MAX : 0;
					if (pull_apart_results) {
						for (uint16_t j=0; j<result_container->cardinality; ++j) {

							double box_distance = key_to_box_maxdistance (
												result_container->keys+j*dimensions,
												page->node.internal.BOX(i),
												dimensions);

							box_distance *= lambda_clust;

							if (box_distance < clustering) {
								if (candidates->size >= k
									&& box_distance + dissimilarity - relevance
									<= ((data_container_t*)peek_priority_queue(candidates))->sort_key) {

									is_obscured = true;
									break;
								}else{
									clustering = box_distance;
								}
							}
						}
					}else if (bring_together_results) {
						clustering = -DBL_MAX;
						for (uint16_t j=0; j<result_container->cardinality; ++j) {

							double box_distance = key_to_box_maxdistance (
												result_container->keys+j*dimensions,
												page->node.internal.BOX(i),
												dimensions);

							box_distance *= lambda_clust;

							if (box_distance > clustering) {
								if (candidates->size >= k
									&& - box_distance + dissimilarity - relevance
									<= ((data_container_t*)peek_priority_queue(candidates))->sort_key) {

									is_obscured = true;
									break;
								}else{
									clustering = box_distance;
								}
							}
						}
					}


					if (!is_obscured) {
						index_t tmp_score = dissimilarity - relevance;
						if (pull_apart_results) tmp_score += clustering;
						else if (bring_together_results) tmp_score -= clustering;

						if (candidates->size < k
						|| tmp_score > ((data_container_t*)peek_priority_queue(candidates))->sort_key) {

							box_container_t* new_container = (box_container_t*) malloc (sizeof(box_container_t));
							new_container->id = CHILD_ID(page_id,i);
							new_container->sort_key = tmp_score;

							insert_into_priority_queue (browse,new_container);
						}
					}
				}
			}

			pthread_rwlock_unlock (page_lock);

			free (container);
		}
	}

	assert (!browse->size);

	delete_priority_queue (browse);

	assert (candidates->size <= k);

	while (candidates->size) {
		data_container_t* candidate = remove_from_priority_queue (candidates);

		result_container->objects [result_container->cardinality] = candidate->object;

		memcpy (result_container->keys+result_container->cardinality*dimensions,
				candidate->key,sizeof(index_t)*dimensions);

		result_container->cardinality++;
/*
		data_pair_t *const matched_repeller = (data_pair_t *const) malloc (sizeof(data_pair_t));

		matched_repeller->key = (index_t*) malloc (tree->dimensions*sizeof(index_t));
		memcpy (matched_repeller->key, candidate->key, tree->dimensions*sizeof(index_t));

		matched_repeller->object = candidate->object;

		set (matched_repellers, candidate->object, matched_repeller);
*/
		printf ("(%lf) %lu \n",
				candidate->sort_key,
				candidate->object);

		free (candidate->key);
		free (candidate);
	}

	delete_priority_queue (candidates);
}


static void augment_set_with_joined_hotspots (tree_t *const tree,
						interval_t const query[],
						multidata_container_t *const result_container,
						tree_t *const attractors,
						tree_t *const repellers,
						//symbol_table_t *const matched_attractors,
						//symbol_table_t *const matched_repellers,
						unsigned const k,
						boolean const pull_apart_results,
						boolean const bring_together_results,
						double const lambda_rel,
						double const lambda_diss,
						double const lambda_clust) {

	assert (!pull_apart_results || !bring_together_results);

	pthread_rwlock_rdlock (&tree->tree_lock);
	if (!overlapping_boxes (query,tree->root_box,tree->dimensions)) {
		pthread_rwlock_unlock (&tree->tree_lock);
		return;
	}else pthread_rwlock_unlock (&tree->tree_lock);

	uint16_t const combination_offset = (attractors!=NULL && attractors->indexed_records?1:0)
							+ (repellers!=NULL && repellers->indexed_records?1:0);
	uint16_t const cardinality = 1 + combination_offset;
	uint16_t dimensions = tree->dimensions;

	if (attractors!=NULL && attractors->indexed_records && attractors->dimensions < dimensions) {
		dimensions = attractors->dimensions;
	}

	if (repellers!=NULL && repellers->indexed_records && repellers->dimensions < dimensions) {
		dimensions = repellers->dimensions;
	}

	priority_queue_t* data_combinations = new_priority_queue (&maxcompare_containers);
	priority_queue_t* browse = new_priority_queue (&mincompare_containers);

	reset_search_operation:;

	multibox_container_t* container = (multibox_container_t*) malloc (sizeof(multibox_container_t));

	container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));

	if (attractors!=NULL && attractors->indexed_records) {
		pthread_rwlock_rdlock (&attractors->tree_lock);
		memcpy (container->boxes, attractors->root_box, dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&attractors->tree_lock);
	}

	if (repellers!=NULL && repellers->indexed_records) {
		pthread_rwlock_rdlock (&repellers->tree_lock);
		memcpy (container->boxes+(combination_offset-1)*dimensions, repellers->root_box, dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&repellers->tree_lock);
	}

	pthread_rwlock_rdlock (&tree->tree_lock);
	memcpy (container->boxes+combination_offset*dimensions, tree->root_box, dimensions*sizeof(interval_t));
	pthread_rwlock_unlock (&tree->tree_lock);

	container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
	bzero (container->page_ids,cardinality*sizeof(uint64_t));

	container->cardinality = cardinality;
	container->dimensions = dimensions;
	container->sort_key = 0;

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		container = remove_from_priority_queue (browse);

		if (data_combinations->size >= k
			&& container->sort_key >=
			((multidata_container_t*)peek_priority_queue(data_combinations))->sort_key) {

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
		for (uint16_t i=0; i<container->cardinality; ++i) {
			uint64_t const page_id = container->page_ids[i];

			page_t const* page = NULL;
			pthread_rwlock_t* page_lock = NULL;

			uint16_t idimensions = 0;
			if (i>=combination_offset) {
				idimensions = tree->dimensions;
				page = load_page (tree,page_id);
				pthread_rwlock_rdlock (&tree->tree_lock);
				page_lock = (pthread_rwlock_t*) get (tree->page_locks,page_id);
				pthread_rwlock_unlock (&tree->tree_lock);
			}else if (!i && attractors!=NULL && attractors->indexed_records) {
				idimensions = attractors->dimensions;
				page = load_page (attractors,page_id);
				pthread_rwlock_rdlock (&attractors->tree_lock);
				page_lock = (pthread_rwlock_t*) get (attractors->page_locks,page_id);
				pthread_rwlock_unlock (&attractors->tree_lock);
			}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
				idimensions = repellers->dimensions;
				page = load_page (repellers,page_id);
				pthread_rwlock_rdlock (&repellers->tree_lock);
				page_lock = (pthread_rwlock_t*) get (repellers->page_locks,page_id);
				pthread_rwlock_unlock (&repellers->tree_lock);
			}else{
				LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
				abort();
			}

			assert (page_lock != NULL);

			if (pthread_rwlock_tryrdlock (page_lock)) {
				while (browse->size) {
					multibox_container_t* temp = remove_from_priority_queue (browse);

					free (temp->page_ids);
					free (temp->boxes);
					free (temp);
				}

				if (i>=combination_offset) {
					pthread_rwlock_rdlock (&tree->tree_lock);
					pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get(tree->page_locks,page_id);
					pthread_rwlock_unlock (&tree->tree_lock);
					pthread_rwlock_unlock (previous_lock);
				}else if (!i && attractors!=NULL && attractors->indexed_records) {
					pthread_rwlock_rdlock (&attractors->tree_lock);
					pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get (attractors->page_locks,page_id);
					pthread_rwlock_unlock (&attractors->tree_lock);
					pthread_rwlock_unlock (previous_lock);
				}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
					pthread_rwlock_rdlock (&repellers->tree_lock);
					pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get (repellers->page_locks,page_id);
					pthread_rwlock_unlock (&repellers->tree_lock);
					pthread_rwlock_unlock (previous_lock);
				}else{
					LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
					abort();
				}
				goto reset_search_operation;
			}

			assert (page != NULL);

			if (!page->header.is_leaf) {
				all_leaves = false;
				for (register uint32_t j=0; j<page->header.records; ++j) {
					multibox_container_t* new_container = (multibox_container_t*) malloc (sizeof(multibox_container_t));

					new_container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
					memcpy (new_container->page_ids,container->page_ids,cardinality*sizeof(uint64_t));

					if (i>=combination_offset) {
						new_container->page_ids[i] = page_id*tree->internal_entries+j+1;
					}else if (!i && attractors!=NULL && attractors->indexed_records) {
						*new_container->page_ids = page_id*attractors->internal_entries+j+1;
					}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
						new_container->page_ids[i] = page_id*repellers->internal_entries+j+1;
					}else{
						LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
						abort();
					}

					new_container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));
					memcpy (new_container->boxes,container->boxes,cardinality*dimensions*sizeof(interval_t));
					memcpy (new_container->boxes+i*dimensions,
							page->node.internal.intervals+j*idimensions,
							dimensions*sizeof(interval_t));

					new_container->cardinality = cardinality;
					new_container->dimensions = dimensions;

					if (i>=combination_offset) {
						double rank_score = 0;
						if (attractors!=NULL && attractors->indexed_records) {
							rank_score = lambda_rel 
								* box_to_box_mindistance (new_container->boxes,new_container->boxes+combination_offset*dimensions,dimensions);
						}
						if (repellers!=NULL && repellers->indexed_records) {
							rank_score -= lambda_diss 
								* box_to_box_maxdistance (new_container->boxes+(attractors!=NULL && attractors->indexed_records?dimensions:0),new_container->boxes+combination_offset*dimensions,dimensions);
						}

						if (new_container->cardinality) {
							if (pull_apart_results) {
								rank_score -= lambda_clust
										* min_maxdistance_pairwise_multibox (new_container,combination_offset*dimensions);
							}else if (bring_together_results) {
								rank_score += lambda_clust
										* max_mindistance_pairwise_multibox (new_container,combination_offset*dimensions);
							}
						}

						if (data_combinations->size < k
						|| rank_score < ((multidata_container_t*)peek_priority_queue(data_combinations))->sort_key) {

							new_container->sort_key = rank_score;
							insert_into_priority_queue (browse,new_container);
						}else{
							free (new_container->page_ids);
							free (new_container->boxes);
							free (new_container);
						}
					}else{
						new_container->sort_key = 0;
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

			for (uint16_t i=0; i<cardinality; ++i) {
				pthread_rwlock_t* page_lock = NULL;
				page_t const* page = NULL;

				if (i>=combination_offset) {
					page = load_page (tree,container->page_ids[i]);
					pthread_rwlock_rdlock (&tree->tree_lock);
					page_lock = (pthread_rwlock_t*) get (tree->page_locks,container->page_ids[i]);
					pthread_rwlock_unlock (&tree->tree_lock);
				}else if (!i && attractors!=NULL && attractors->indexed_records) {
					page = load_page (attractors,*container->page_ids);
					pthread_rwlock_rdlock (&attractors->tree_lock);
					page_lock =  (pthread_rwlock_t*) get (attractors->page_locks,*container->page_ids);
					pthread_rwlock_unlock (&attractors->tree_lock);
				}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
					page = load_page (repellers,container->page_ids[1]);
					pthread_rwlock_rdlock (&repellers->tree_lock);
					page_lock = (pthread_rwlock_t*) get (repellers->page_locks,container->page_ids[1]);
					pthread_rwlock_unlock (&repellers->tree_lock);
				}else{
					LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
					abort();
				}

				assert (page != NULL);
				assert (page_lock != NULL);
				assert (page->header.is_leaf);

				page_locks[i] = page_lock;
				pages[i] = page;

				if (pthread_rwlock_tryrdlock (page_lock)) {
					while (browse->size) {
						multibox_container_t *const temp = (multibox_container_t *const) remove_from_priority_queue (browse);

						free (temp->page_ids);
						free (temp->boxes);
						free (temp);
					}
					for (uint16_t j=0; j<i; ++j) {
						pthread_rwlock_unlock (page_locks[j]);
					}
					goto reset_search_operation;
				}
			}

			uint32_t offsets [cardinality];
			bzero (offsets,cardinality*sizeof(uint32_t));
			for (uint32_t j=0,i=0;;++j) {
				if (offsets[i] >= pages[i]->header.records) {
					offsets[i++] = 0;
					if (i >= cardinality) {
						break;
					}
				}else{
					boolean has_duplicates = false;
					for (uint16_t ii=0; ii<result_container->cardinality; ++ii) {
						if (pages[combination_offset]->node.leaf.objects[offsets[combination_offset]] == result_container->objects[ii]) {
							has_duplicates = true;
							break;
						}
					}

					if (!has_duplicates) {
						multidata_container_t* data_container = (multidata_container_t*) malloc (sizeof(multidata_container_t));
						data_container->keys = (index_t*) malloc (cardinality*dimensions*sizeof(index_t));
						data_container->objects = (object_t*) malloc (cardinality*sizeof(object_t));
						for (uint16_t offset=0; offset<cardinality; ++offset) {
							data_container->objects[offset] = pages[offset]->node.leaf.objects[offsets[offset]];
							memcpy (data_container->keys+offset*dimensions,
									pages[offset]->node.leaf.keys+offsets[offset]*dimensions,
									dimensions*sizeof(index_t));
						}

						data_container->cardinality = cardinality;
						data_container->dimensions = dimensions;

						double rank_score = 0;
						if (attractors!=NULL && attractors->indexed_records) {
							rank_score = lambda_rel * key_to_key_distance 
								(data_container->keys,data_container->keys+combination_offset*dimensions,dimensions);
						}

						if (repellers!=NULL && repellers->indexed_records) {
							rank_score -= lambda_diss * key_to_key_distance 
								(data_container->keys+(attractors->indexed_records?dimensions:0),
								data_container->keys+combination_offset*dimensions,dimensions);
						}

						if (data_container->cardinality && (pull_apart_results || bring_together_results)) {
							if (pull_apart_results) {
								//rank_score -= lambda_clust
								//	*mindistance_pairwise_multikey (data_container,combination_offset*dimensions);
							}else if (bring_together_results) {
								rank_score += lambda_clust
									*maxdistance_pairwise_multikey (data_container,combination_offset*dimensions);
							}
						}

						data_container->sort_key = rank_score;

						if (data_combinations->size < k) {
							insert_into_priority_queue (data_combinations,data_container);
						}else if (rank_score < ((multidata_container_t*)peek_priority_queue(data_combinations))->sort_key) {
							multidata_container_t* temp = remove_from_priority_queue(data_combinations);

							free (temp->objects);
							free (temp->keys);
							free (temp);

							insert_into_priority_queue (data_combinations,data_container);
						}else{
							free (data_container->objects);
							free (data_container->keys);
							free (data_container);
						}
					}

					i=0;
				}
				++offsets[i];
			}

			for (uint16_t i=0; i<cardinality; ++i) {
				pthread_rwlock_unlock (page_locks[i]);
			}
		}

		free (container->page_ids);
		free (container->boxes);
		free (container);
	}

	assert (!browse->size);

	delete_priority_queue (browse);

	assert (data_combinations->size <= k);

	while (data_combinations->size) {
		multidata_container_t* data_container = remove_from_priority_queue (data_combinations);

		boolean has_duplicates = false;
		for (uint16_t i=0; i<result_container->cardinality; ++i) {
			if (result_container->objects [i] == data_container->objects [combination_offset]) {
				has_duplicates = true;
				break;
			}
		}

		if (!has_duplicates) {
			result_container->sort_key = data_container->sort_key;

			result_container->objects [result_container->cardinality] = data_container->objects [combination_offset];

			memcpy (result_container->keys+result_container->cardinality*dimensions,
					data_container->keys+combination_offset*dimensions,sizeof(index_t)*dimensions);

/*
			if (repellers!=NULL && repellers->indexed_records) {
				data_pair_t *const matched_repeller = (data_pair_t *const) malloc (sizeof(data_pair_t));

				matched_repeller->key = (index_t*) malloc (dimensions*sizeof(index_t));
				memcpy (matched_repeller->key,data_container->keys
						+(attractors!=NULL && attractors->indexed_records ? 1 : 0)*dimensions,
						sizeof(index_t)*dimensions);

				matched_repeller->object = data_container->objects [combination_offset - 1];

				set (matched_repellers, data_container->objects [combination_offset], matched_repeller);
			}

			if (attractors!=NULL && attractors->indexed_records) {
				data_pair_t *const matched_attractor = (data_pair_t *const) malloc (sizeof(data_pair_t));

				matched_attractor->key = (index_t*) malloc (dimensions*sizeof(index_t));
				memcpy (matched_attractor->key,data_container->keys,sizeof(index_t)*dimensions);

				matched_attractor->object = *data_container->objects;

				set (matched_attractors, data_container->objects [combination_offset], matched_attractor);
			}
*/
			result_container->cardinality++;


			printf ("(%lf) %lu %lu %lu\n",
					data_container->sort_key,
					data_container->objects [combination_offset],
					*data_container->objects,
					data_container->objects [combination_offset-1]
					);
					//((data_pair_t*) get (matched_attractors, result_container->objects [result_container->cardinality-1]))->object,
					//((data_pair_t*) get (matched_repellers, result_container->objects [result_container->cardinality-1]))->object);
		}

		free (data_container->objects);
		free (data_container->keys);
		free (data_container);
	}

	delete_priority_queue (data_combinations);
}


static index_t compute_hotspot_score (multidata_container_t const*const result,
					symbol_table_t const*const matched_attractors,
					symbol_table_t const*const matched_repellers,
					boolean const pull_apart_results,
					boolean const bring_together_results,
					double const lambda_rel,
					double const lambda_diss,
					double const lambda_clust) {

	assert (!pull_apart_results || !bring_together_results);

	double score = 0;
	for (uint16_t i=0; i<result->cardinality; ++i) {
		if (get (matched_attractors, result->objects[i]) == NULL) {
			LOG (error,"Unknown attractor for object %lu.\n", result->objects[i]);
			abort();
		}
		if (get (matched_repellers, result->objects[i]) == NULL) {
			LOG (error,"Unknown repeller for object %lu.\n", result->objects[i]);
			abort();
		}
		double relevance = lambda_rel 
				* key_to_key_distance(result->keys+i*result->dimensions,((data_pair_t*) get (matched_attractors, result->objects[i]))->key,result->dimensions);
		double dissimilarity = lambda_diss 
				* key_to_key_distance(result->keys+i*result->dimensions,((data_pair_t*) get (matched_repellers, result->objects[i]))->key,result->dimensions);
		score += dissimilarity - relevance;
	}

	if (pull_apart_results) {
		score += lambda_clust * mindistance_pairwise_multikey (result,0);
	}else if (bring_together_results) {
		score -= lambda_clust * mindistance_pairwise_multikey (result,0);
	}

	return score;
}


static boolean augment_priority_queue_with_hotspots (tree_t const*const tree,
							multidata_container_t *const result_container,
							priority_queue_t *const subsets_heap,
							unsigned const max_size,
							symbol_table_t const*const matched_attractors,
							symbol_table_t const*const matched_repellers,
							boolean const pull_apart_results,
							boolean const bring_together_results,
							double const lambda_rel,
							double const lambda_diss,
							double const lambda_clust) {

	assert (!pull_apart_results || !bring_together_results);

	boolean is_updated = false;
	uint16_t const cardinality = result_container->cardinality;

	for (uint16_t i=0; i<cardinality-1; ++i) {
		multidata_container_t* subset_container = (multidata_container_t*) malloc (sizeof(multidata_container_t));

		subset_container->keys = (index_t*) malloc (cardinality*tree->dimensions*sizeof(index_t));
		subset_container->objects = (object_t*) malloc (cardinality*sizeof(object_t));

		memcpy (subset_container->keys,result_container->keys,(cardinality-1)*tree->dimensions*sizeof(index_t));
		memcpy (subset_container->objects,result_container->objects,(cardinality-1)*sizeof(object_t));

		memcpy (subset_container->keys+i,result_container->keys+cardinality-1,tree->dimensions*sizeof(index_t));
		subset_container->objects[i] = result_container->objects[cardinality-1];

		subset_container->dimensions = result_container->dimensions;
		subset_container->cardinality = cardinality-1;

		subset_container->sort_key = compute_hotspot_score (subset_container,
						matched_attractors,matched_repellers,
						pull_apart_results,bring_together_results,
						lambda_rel,lambda_diss,lambda_clust);

		if (subsets_heap->size < max_size) {
			insert_into_priority_queue (subsets_heap,subset_container);
		}else if (subset_container->sort_key > ((multidata_container_t*)peek_priority_queue(subsets_heap))->sort_key) {
			multidata_container_t* top = remove_from_priority_queue (subsets_heap);
			insert_into_priority_queue (subsets_heap,subset_container);

			free (top->objects);
			free (top->keys);
			free (top);

			is_updated = true;
		}
	}
	{
		multidata_container_t* subset_container = (multidata_container_t*) malloc (sizeof(multidata_container_t));

		subset_container->keys = (index_t*) malloc (cardinality*tree->dimensions*sizeof(index_t));
		subset_container->objects = (object_t*) malloc (cardinality*sizeof(object_t));

		memcpy (subset_container->keys,result_container->keys,(cardinality-1)*tree->dimensions*sizeof(index_t));
		memcpy (subset_container->objects,result_container->objects,(cardinality-1)*sizeof(object_t));

		subset_container->dimensions = result_container->dimensions;
		subset_container->cardinality = cardinality-1;

		subset_container->sort_key = compute_hotspot_score (subset_container,
						matched_attractors,matched_repellers,
						pull_apart_results,bring_together_results,
						lambda_rel,lambda_diss,lambda_clust);

		if (subsets_heap->size < max_size) {
			insert_into_priority_queue (subsets_heap,subset_container);
		}else if (subset_container->sort_key > ((multidata_container_t*)peek_priority_queue(subsets_heap))->sort_key) {
			multidata_container_t* top = remove_from_priority_queue (subsets_heap);
			insert_into_priority_queue (subsets_heap,subset_container);

			free (top->objects);
			free (top->keys);
			free (top);

			is_updated = true;
		}
	}

	return is_updated;
}


fifo_t* hotspots (tree_t *const tree,
			fifo_t const*const attractors,
			fifo_t const*const repellers,
			unsigned const k,
			boolean const pull_apart_results,
			boolean const bring_together_results,
			double const lambda_rel,
			double const lambda_diss,
			double const lambda_clust) {

	index_t lo [tree->dimensions];
	index_t hi [tree->dimensions];

	for (uint16_t j=0; j<tree->dimensions; ++j) {
		lo [j] = tree->root_box[j].start;
		hi [j] = tree->root_box[j].end;
	}

	return hotspots_constrained (tree,attractors,repellers,lo,hi,k,
					pull_apart_results,bring_together_results,
					lambda_rel,lambda_diss,lambda_diss);
}

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
				double const lambda_clust) {

	if (pull_apart_results && bring_together_results) {
		LOG (error,"Should try to either bring together OR pull apart the results. Not both...");
		return NULL;
	}

	if (k==0) return new_queue();

	interval_t query [tree->dimensions];
	for (uint16_t j=0; j<tree->dimensions; ++j) {
		query[j].start = lo[j];
		query[j].end = hi[j];
	}

	uint16_t const dimensions = tree->dimensions;

	/***** Initialization of necessary containers *****/
	//symbol_table_t *const matched_attractors = new_symbol_table_primitive (NULL);
	//symbol_table_t *const matched_repellers = new_symbol_table_primitive (NULL);


	multidata_container_t* result_container = (multidata_container_t*) malloc (sizeof(multidata_container_t));

	result_container->keys = (index_t*) malloc (k*dimensions*sizeof(index_t));
	result_container->objects = (object_t*) malloc (k*sizeof(object_t));
	result_container->dimensions = dimensions;
	result_container->cardinality = 0;

	/******* PHASE 1 --- AUGMENT THE RESULT-SET INCREMENTALLY *******/
	if (pull_apart_results || bring_together_results) {
		while (result_container->cardinality < k) {
				augment_set_with_hotspots (tree,
							query,
							result_container,
							attractors,repellers,
							//matched_attractors,
							//matched_repellers,
							1,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);
		}
	}else{
		augment_set_with_hotspots (tree,query,
							result_container,
							attractors,repellers,
							//matched_attractors,
							//matched_repellers,
							k,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);
	}

	/*** PHASE 2: ITERATE THROUGH THE POSSIBLE MUTATIONS OF THE RESULT-SET ***
	if (pull_apart_results || bring_together_results) {
		priority_queue_t* subsets_heap = new_priority_queue (&maxcompare_multicontainers);

		augment_priority_queue_with_hotspots (tree,
							result_container,subsets_heap,
							DIVERSIFICATION_HEAP_SIZE,
							matched_attractors,
							matched_repellers,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);

		for (unsigned counter=0; subsets_heap->size && counter<MAX_ITERATIONS_NUMBER; ++counter) {
			multidata_container_t* subset_container = remove_from_priority_queue (subsets_heap);

			augment_set_with_hotspots (tree,query,
							subset_container,
							attractors,repellers,
							matched_attractors,
							matched_repellers,
							1,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);
			if (compute_hotspot_score (subset_container,
							matched_attractors,matched_repellers,
							pull_apart_results,bring_together_results,
							lambda_rel,lambda_diss,lambda_clust)
			<= compute_hotspot_score (result_container,
							matched_attractors,matched_repellers,
							pull_apart_results,bring_together_results,
							lambda_rel,lambda_diss,lambda_clust)) {

					//free (subset_container->objects);
					//free (subset_container->keys);
					//free (subset_container);
					//break;
			}else{
				augment_priority_queue_with_hotspots (tree,
							subset_container,subsets_heap,
							DIVERSIFICATION_HEAP_SIZE,
							matched_attractors,
							matched_repellers,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);

				multidata_container_t* temp_container = result_container;

				result_container = subset_container;
				subset_container = temp_container;
			}

			free (subset_container->objects);
			free (subset_container->keys);
			free (subset_container);
		}
		delete_priority_queue (subsets_heap);
	}

	/*** Construct returned result-set ***/
	fifo_t* result = new_queue();
	for (uint16_t i=0; i<result_container->cardinality; ++i) {
		data_pair_t* pair = (data_pair_t*) malloc (sizeof(data_pair_t));

		pair->object = result_container->objects[i];
		pair->key = (index_t*) malloc (sizeof(index_t)*dimensions);
		memcpy (pair->key,result_container->keys+i*dimensions,sizeof(index_t)*dimensions);

		insert_at_tail_of_queue (result,pair);
	}

	free (result_container->objects);
	free (result_container->keys);
	free (result_container);

	return result;
}

fifo_t* hotspots_join (tree_t *const tree,
			tree_t *const attractors,
			tree_t *const repellers,
			unsigned const k,
			boolean const pull_apart_results,
			boolean const bring_together_results,
			double const lambda_rel,
			double const lambda_diss,
			double const lambda_clust) {

	index_t lo [tree->dimensions];
	index_t hi [tree->dimensions];

	for (uint16_t j=0; j<tree->dimensions; ++j) {
		lo [j] = tree->root_box[j].start;
		hi [j] = tree->root_box[j].end;
	}

	return hotspots_join_constrained (tree,attractors,repellers,lo,hi,k,
						pull_apart_results,bring_together_results,
						lambda_rel,lambda_diss,lambda_clust);
}

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
				double const lambda_clust) {

	if (pull_apart_results && bring_together_results) {
		LOG (error,"Should try to either bring together OR pull apart the results. Not both...");
		return NULL;
	}

	if (k==0) return new_queue();

	interval_t query [tree->dimensions];
	for (uint16_t j=0; j<tree->dimensions; ++j) {
		query[j].start = lo[j];
		query[j].end = hi[j];
	}

	//symbol_table_t *const matched_repellers = new_symbol_table_primitive (NULL);
	//symbol_table_t *const matched_attractors = new_symbol_table_primitive (NULL);

	multidata_container_t* result_container = (multidata_container_t*) malloc (sizeof(multidata_container_t));
	result_container->keys = (index_t*) malloc (k*tree->dimensions*sizeof(index_t));
	result_container->objects = (object_t*) malloc (k*sizeof(object_t));
	result_container->dimensions = tree->dimensions;
	result_container->cardinality = 0;

	/******* PHASE 1 --- AUGMENT THE RESULT-SET INCREMENTALLY *******/
	while (result_container->cardinality < k) {
		if (pull_apart_results || bring_together_results) {
			augment_set_with_joined_hotspots (tree,query,
							result_container,
							attractors,repellers,
							//matched_attractors,
							//matched_repellers,
							1,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);
		}else{
			augment_set_with_joined_hotspots (tree,query,
							result_container,
							attractors,repellers,
							//matched_attractors,
							//matched_repellers,
							k,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);
		}
	}

	/*** PHASE 2: ITERATE THROUGH THE POSSIBLE MUTATIONS OF THE RESULT-SET ***
	if (pull_apart_results || bring_together_results) {
		priority_queue_t* subsets_heap = new_priority_queue (&maxcompare_multicontainers);

		augment_priority_queue_with_hotspots (tree,
							result_container,
							subsets_heap,
							DIVERSIFICATION_HEAP_SIZE,
							matched_attractors,
							matched_repellers,
							pull_apart_results,
							bring_together_results,
							lambda_rel,
							lambda_diss,
							lambda_clust);

		for (unsigned counter=0; subsets_heap->size && counter<MAX_ITERATIONS_NUMBER; ++counter) {
			multidata_container_t* subset_container = remove_from_priority_queue (subsets_heap);

			augment_set_with_joined_hotspots (tree,query,
						subset_container,
						attractors,repellers,
						matched_attractors,
						matched_repellers,
						1,
						pull_apart_results,
						bring_together_results,
						lambda_rel,
						lambda_diss,
						lambda_clust);

			if (compute_hotspot_score (subset_container,matched_attractors,matched_repellers,
							pull_apart_results,bring_together_results,
							lambda_rel,lambda_diss,lambda_clust)
					<= compute_hotspot_score (result_container,matched_attractors,matched_repellers,
							pull_apart_results,bring_together_results,
							lambda_rel,lambda_diss,lambda_clust)) {
					//free (subset_container->objects);
					//free (subset_container->keys);
					//free (subset_container);
					//break;
			}else{
				augment_priority_queue_with_hotspots (tree,
								subset_container,
								subsets_heap,
								DIVERSIFICATION_HEAP_SIZE,
								matched_attractors,
								matched_repellers,
								pull_apart_results,
								bring_together_results,
								lambda_rel,
								lambda_diss,
								lambda_clust);

				multidata_container_t* temp_container = result_container;

				result_container = subset_container;
				subset_container = temp_container;
			}

			free (subset_container->objects);
			free (subset_container->keys);
			free (subset_container);
		}
		delete_priority_queue (subsets_heap);
	}
*/

	/*** Construct returned result-set ***/
	fifo_t* result = new_queue();
	for (uint16_t i=0; i<result_container->cardinality; ++i) {
		data_pair_t* pair = (data_pair_t*) malloc (sizeof(data_pair_t));

		pair->object = result_container->objects[i];
		pair->key = (index_t*) malloc (sizeof(index_t)*tree->dimensions);
		memcpy (pair->key,result_container->keys+i*tree->dimensions,sizeof(index_t)*tree->dimensions);

		insert_at_tail_of_queue (result,pair);
	}

/*
	fifo_t *const matched_attractors_entries = get_values (matched_attractors);
	while (matched_attractors_entries->size) {
		data_pair_t *const entry = (data_pair_t*) remove_tail_of_queue (matched_attractors_entries);
		free (entry->key);
		free (entry);
	}

	delete_queue (matched_attractors_entries);
	delete_symbol_table (matched_attractors);


	fifo_t *const matched_repellers_entries = get_values (matched_repellers);
	while (matched_repellers_entries->size) {
		data_pair_t *const entry = (data_pair_t*) remove_tail_of_queue (matched_repellers_entries);
		free (entry->key);
		free (entry);
	}

	delete_queue (matched_repellers_entries);
	delete_symbol_table (matched_repellers);
*/

	free (result_container->objects);
	free (result_container->keys);
	free (result_container);

	return result;
}


fifo_t* diversified_join (lifo_t *const trees, tree_t *const attractors, tree_t *const repellers,
						boolean const pull_apart_results, boolean const bring_together_results,
						double const lambda_rel, double const lambda_diss, double const lambda_clust) {

	if (pull_apart_results && bring_together_results) {
		LOG (error,"Should try to either bring together OR pull apart the results. Not both...");
		return NULL;
	}

	unsigned const k = 1;

	unsigned dimensions = UINT_MAX;
	for (uint64_t i=0; i<trees->size; ++i) {
		if (TREE(i)->dimensions < dimensions) {
			dimensions = TREE(i)->dimensions;
		}
	}

	if (attractors!=NULL && attractors->indexed_records && attractors->dimensions < dimensions) {
		dimensions = attractors->dimensions;
	}

	if (repellers!=NULL && repellers->indexed_records && repellers->dimensions < dimensions) {
		dimensions = repellers->dimensions;
	}

	uint16_t const combination_offset = (attractors!=NULL && attractors->indexed_records?1:0) + (repellers!=NULL && repellers->indexed_records?1:0);
	unsigned const cardinality = trees->size + combination_offset;

	priority_queue_t* data_combinations = new_priority_queue (&maxcompare_multicontainers);
	priority_queue_t* browse = new_priority_queue (&mincompare_multicontainers);

	reset_search_operation:;
	multibox_container_t* container = (multibox_container_t*) malloc (sizeof(multibox_container_t));

	container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));
	container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
	container->cardinality = cardinality;
	container->dimensions = dimensions;
	container->sort_key = 0;

	bzero (container->page_ids,cardinality*sizeof(uint64_t));

	if (attractors!=NULL && attractors->indexed_records) {
		pthread_rwlock_rdlock (&attractors->tree_lock);
		memcpy (container->boxes,attractors->root_box,dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&attractors->tree_lock);
	}

	if (repellers!=NULL && repellers->indexed_records) {
		pthread_rwlock_rdlock (&repellers->tree_lock);
		memcpy (container->boxes + (attractors!=NULL && attractors->indexed_records ? dimensions : 0),
				repellers->root_box,dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&repellers->tree_lock);
	}

	for (uint64_t i=0; i<trees->size; ++i) {
		pthread_rwlock_rdlock (&TREE(i)->tree_lock);
		memcpy (container->boxes+(i+combination_offset)*dimensions,TREE(i)->root_box,dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&TREE(i)->tree_lock);
	}

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		container = remove_from_priority_queue (browse);

		if (data_combinations->size == k && container->sort_key >=
			((multidata_container_t*)peek_priority_queue(data_combinations))->sort_key) {

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
		for (uint16_t i=0; i<container->cardinality; ++i) {
			uint64_t const page_id = container->page_ids[i];

			page_t const* page = NULL;
			pthread_rwlock_t* page_lock = NULL;

			uint16_t idimensions = 0;
			if (i>=combination_offset) {
				idimensions = TREE(i)->dimensions;
				page = load_page (TREE(i-combination_offset),page_id);
				pthread_rwlock_rdlock (&TREE(i-combination_offset)->tree_lock);
				page_lock = (pthread_rwlock_t*) get (TREE(i-combination_offset)->page_locks,page_id);
				pthread_rwlock_unlock (&TREE(i-combination_offset)->tree_lock);
			}else if (!i && attractors!=NULL && attractors->indexed_records) {
				idimensions = attractors->dimensions;
				page = load_page (attractors,page_id);
				pthread_rwlock_rdlock (&attractors->tree_lock);
				page_lock = (pthread_rwlock_t*) get (attractors->page_locks,page_id);
				pthread_rwlock_unlock (&attractors->tree_lock);
			}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
				idimensions = repellers->dimensions;
				page = load_page (repellers,page_id);
				pthread_rwlock_rdlock (&repellers->tree_lock);
				page_lock = (pthread_rwlock_t*) get (repellers->page_locks,page_id);
				pthread_rwlock_unlock (&repellers->tree_lock);
			}else{
				LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
				abort();
			}

			assert (page_lock != NULL);
			if (pthread_rwlock_tryrdlock (page_lock)) {
				while (browse->size) {
					multibox_container_t* temp = remove_from_priority_queue (browse);

					free (temp->page_ids);
					free (temp->boxes);
					free (temp);
				}

				if (i>=combination_offset) {
					for (uint16_t j=0; j<i-combination_offset; ++j) {
						pthread_rwlock_rdlock (&TREE(j)->tree_lock);
						pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get(TREE(j)->page_locks,page_id);
						pthread_rwlock_unlock (&TREE(j)->tree_lock);
						pthread_rwlock_unlock (previous_lock);
					}
				}else if (attractors!=NULL && attractors->indexed_records) {
					pthread_rwlock_rdlock (&attractors->tree_lock);
					pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get (attractors->page_locks,page_id);
					pthread_rwlock_unlock (&attractors->tree_lock);
					pthread_rwlock_unlock (previous_lock);
				}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
					pthread_rwlock_rdlock (&repellers->tree_lock);
					pthread_rwlock_t *const previous_lock = (pthread_rwlock_t *const) get (repellers->page_locks,page_id);
					pthread_rwlock_unlock (&repellers->tree_lock);
					pthread_rwlock_unlock (previous_lock);
				}else{
					LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
					abort();
				}

				goto reset_search_operation;
			}

			assert (page != NULL);

			if (!page->header.is_leaf) {
				all_leaves = false;
				for (register uint32_t j=0; j<page->header.records; ++j) {
					multibox_container_t* new_container = (multibox_container_t*) malloc (sizeof(multibox_container_t));

					new_container->page_ids = (uint64_t*) malloc (cardinality*sizeof(uint64_t));
					memcpy (new_container->page_ids,container->page_ids,cardinality*sizeof(uint64_t));
					if (i>=combination_offset) {
						new_container->page_ids[i] = page_id*TREE(i-combination_offset)->internal_entries+j+1;
					}else if (!i && attractors!=NULL && attractors->indexed_records) {
						new_container->page_ids[i] = page_id*attractors->internal_entries+j+1;
					}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
						new_container->page_ids[i] = page_id*repellers->internal_entries+j+1;
					}else{
						LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
						abort();
					}

					new_container->boxes = (interval_t*) malloc (cardinality*dimensions*sizeof(interval_t));
					memcpy (new_container->boxes,container->boxes,cardinality*dimensions*sizeof(interval_t));
					memcpy (new_container->boxes+i*dimensions,
							page->node.internal.intervals+j*idimensions,
							dimensions*sizeof(interval_t));

					new_container->cardinality = cardinality;
					new_container->dimensions = dimensions;

					if (i>=combination_offset) {
						double rank_score = 0;
						for (uint16_t x=combination_offset; x<cardinality; ++x) {
							if (attractors!=NULL && attractors->indexed_records) {
								rank_score += lambda_rel 
									* box_to_box_mindistance (new_container->boxes,new_container->boxes+x*dimensions,dimensions);
							}
							if (repellers!=NULL && repellers->indexed_records) {
								rank_score -= lambda_diss 
									* box_to_box_maxdistance (new_container->boxes+(attractors->indexed_records?dimensions:0),
									 new_container->boxes+x*dimensions,dimensions);
							}
						}

						rank_score /= (cardinality-combination_offset);

						if (pull_apart_results) {
							rank_score -= lambda_clust
								* min_maxdistance_pairwise_multibox (new_container,combination_offset*dimensions);
						}else if (bring_together_results) {
							rank_score += lambda_clust
								* max_mindistance_pairwise_multibox (new_container,combination_offset*dimensions);
						}

						if (data_combinations->size < k ||
							rank_score < ((multidata_container_t*)peek_priority_queue(data_combinations))->sort_key) {

							new_container->sort_key = rank_score;
							insert_into_priority_queue (browse,new_container);
						}else{
							free (new_container->page_ids);
							free (new_container->boxes);
							free (new_container);
						}
					}else{
						new_container->sort_key = 0;
						insert_into_priority_queue (browse,new_container);
					}
				}
				pthread_rwlock_unlock (page_lock);
				break;
			}else{
				pthread_rwlock_unlock (page_lock);
			}
		}

		if (all_leaves) {
			page_t const* pages [cardinality];
			pthread_rwlock_t * page_locks [cardinality];

			for (uint16_t i=0; i<cardinality; ++i) {
				pthread_rwlock_t* page_lock = NULL;
				page_t const* page = NULL;

				if (i>=combination_offset) {
					page = load_page(TREE(i-combination_offset),container->page_ids[i]);
					pthread_rwlock_rdlock (&TREE(i-combination_offset)->tree_lock);
					page_lock = (pthread_rwlock_t*) get (TREE(i-combination_offset)->page_locks,container->page_ids[i]);
					pthread_rwlock_unlock (&TREE(i-combination_offset)->tree_lock);
				}else if (!i && attractors!=NULL && attractors->indexed_records) {
					page = load_page(attractors,*container->page_ids);
					pthread_rwlock_rdlock (&attractors->tree_lock);
					page_lock =  (pthread_rwlock_t*) get (attractors->page_locks,*container->page_ids);
					pthread_rwlock_unlock (&attractors->tree_lock);
				}else if (i==combination_offset-1 && repellers!=NULL && repellers->indexed_records) {
					page = load_page(repellers,container->page_ids[1]);
					pthread_rwlock_rdlock (&repellers->tree_lock);
					page_lock = (pthread_rwlock_t*) get (repellers->page_locks,container->page_ids[1]);
					pthread_rwlock_unlock (&repellers->tree_lock);
				}else{
					LOG(error,"%u %lu %lu\n",i,attractors->indexed_records,repellers->indexed_records);
					abort();
				}

				assert (page_lock != NULL);

				assert (page != NULL);
				assert (page->header.is_leaf);

				page_locks[i] = page_lock;
				pages[i] = page;

				if (pthread_rwlock_tryrdlock (page_lock)) {
					while (browse->size) {
						multibox_container_t *const temp = (multibox_container_t *const) remove_from_priority_queue (browse);

						free (temp->page_ids);
						free (temp->boxes);
						free (temp);
					}
					for (uint16_t j=0; j<i; ++j) {
						pthread_rwlock_unlock (page_locks[j]);
					}
					goto reset_search_operation;
				}
			}

			uint32_t offsets [cardinality];
			bzero (offsets,cardinality*sizeof(uint32_t));
			for (uint32_t j=0,i=0;;++j) {
				if (offsets[i] >= pages[i]->header.records) {
					offsets[i++] = 0;
					if (i >= cardinality) {
						break;
					}
				}else{
					multidata_container_t* data_container = (multidata_container_t*) malloc (sizeof(multidata_container_t));
					data_container->keys = (index_t*) malloc (cardinality*dimensions*sizeof(index_t));
					data_container->objects = (object_t*) malloc (cardinality*sizeof(object_t));
					for (uint16_t offset=0; offset<cardinality; ++offset) {
						data_container->objects[offset] = pages[offset]->node.leaf.objects[offsets[offset]];

						uint16_t offset_dimensions = 0;
						if (offset >= combination_offset) {
							offset_dimensions = TREE(offset-combination_offset)->dimensions;
						}else if (!offset && attractors != NULL && attractors->indexed_records) {
							offset_dimensions = attractors->dimensions;
						}else if (offset == combination_offset - 1 && repellers != NULL && repellers->indexed_records) {
							offset_dimensions = repellers->dimensions;
						}else{
							LOG(error,"Critical error in diversified join...\n");
							abort();
						}
						memcpy (data_container->keys+offset*dimensions,
								pages[offset]->node.leaf.keys+offsets[offset]*offset_dimensions, 
								dimensions*sizeof(index_t));
					}

					data_container->cardinality = cardinality;
					data_container->dimensions = dimensions;

					double rank_score = 0;
					for (uint16_t x=combination_offset; x<cardinality; ++x) {
						if (attractors!=NULL && attractors->indexed_records) {
							rank_score += lambda_rel 
								* key_to_key_distance(data_container->keys,data_container->keys+x*dimensions,dimensions);
						}

						if (repellers!=NULL && repellers->indexed_records) {
							rank_score -= lambda_diss 
								* key_to_key_distance (data_container->keys+(attractors!=NULL && attractors->indexed_records?dimensions:0),data_container->keys+x*dimensions,dimensions);
						}
					}

					rank_score /= (cardinality-combination_offset);

					if (pull_apart_results) {
						rank_score -= lambda_clust
								* mindistance_pairwise_multikey (data_container,combination_offset*dimensions);
					}else if (bring_together_results) {
						rank_score += lambda_clust
								* maxdistance_pairwise_multikey (data_container,combination_offset*dimensions);
					}


					data_container->sort_key = rank_score;

					if (data_combinations->size < k) {
						insert_into_priority_queue (data_combinations,data_container);
					}else if (rank_score < ((multidata_container_t*)peek_priority_queue(data_combinations))->sort_key) {
						multidata_container_t* temp = remove_from_priority_queue(data_combinations);

						free (temp->objects);
						free (temp->keys);
						free (temp);

						insert_into_priority_queue (data_combinations,data_container);
					}else{
						no_duplicates_allowed_flag:

						free (data_container->objects);
						free (data_container->keys);
						free (data_container);
					}

					//LOG (warn,"data combination threshold: %f \n",((multidata_container_t*)peek_priority_queue(data_combinations))->sort_key);

					i=0;
				}
				++offsets[i];
			}

			for (uint16_t i=0; i<cardinality; ++i) {
				pthread_rwlock_unlock (page_locks[i]);
			}
		}

		free (container->page_ids);
		free (container->boxes);
		free (container);
	}

	delete_priority_queue (browse);

	assert (data_combinations->size <= k);

	fifo_t* result = new_queue();

	multidata_container_t* final = (multidata_container_t*) remove_from_priority_queue(data_combinations);
	for (uint16_t i=combination_offset; i<final->cardinality; ++i) {
		data_pair_t* pair = (data_pair_t*) malloc (sizeof(data_pair_t));

		pair->object = final->objects[i];
		pair->key = (index_t*) malloc (sizeof(index_t));
		memcpy (pair->key,final->keys+i*dimensions,dimensions*sizeof(index_t));

		insert_at_head_of_queue(result,pair);
	}

	assert (!data_combinations->size);
	delete_priority_queue (data_combinations);

	return result;
}

