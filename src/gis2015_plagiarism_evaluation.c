#include "defs.h"
#include "queue.h"
#include "stack.h"
#include "rtree.h"
#include "common.h"
#include "symbol_table.h"
#include "priority_queue.h"
#include "spatial_diversification_queries.h"
#include <stdlib.h>
#include <pthread.h>


static
boolean mbb_qualifies_by_criterion2 (interval_t const mbb[], uint16_t const dimensions,
							double const threshold,
							fifo_t const*const attractors, fifo_t const*const repellers,
							double const lambda_rel, double const lambda_diss) {

	for (uint16_t i=0; i<(1<<dimensions); ++i) {
		index_t point [dimensions];
		for (uint16_t j=0; j<dimensions; ++j) {
			point[j] = (i%(j+1)>>j) ? mbb[j].end : mbb[j].start ;
		}

		double relevance = attractors->size ? DBL_MAX : 0;
		for (uint64_t j=0; j<attractors->size; ++j) {
			double key_distance = key_to_key_distance (
							attractors->buffer[j],
							point,
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
							point,
							dimensions);

			if (key_distance < dissimilarity) {
				dissimilarity = key_distance;
			}
		}
		dissimilarity *= lambda_diss;

		if (dissimilarity - relevance < threshold) {
			return false;
		}
	}
	return true;
}

static
data_container_t* most_diversified_tuple (tree_t *const tree,
			fifo_t const*const attractors,
			fifo_t const*const repellers,
			double const lambda_rel,
			double const lambda_diss
		) {

	double tau = -DBL_MAX;
	fifo_t* leaves = new_queue();
	fifo_t* queue = new_queue();

	data_container_t *const optimal = (data_container_t *const) malloc (sizeof(data_container_t));
	optimal->key = (index_t *const) malloc (sizeof(index_t)*tree->dimensions);
	optimal->sort_key = -DBL_MAX;

	reset_search_operation:;
	box_container_t* container = (box_container_t*) malloc (sizeof(box_container_t));
	container->box = tree->root_box;
	container->sort_key = 0;
	container->id = 0;

	boolean flag = false;
	while (container != NULL) {
		uint64_t const page_id = container->id;
		page_t const*const page = load_page(tree,page_id);
		assert (page != NULL);

		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
		pthread_rwlock_unlock (&tree->tree_lock);
		assert (page_lock != NULL);

		if (pthread_rwlock_tryrdlock (page_lock)) {
			while (queue->size) {
				free (remove_tail_of_queue (queue));
			}
			goto reset_search_operation;
		}else{
			if (page->header.is_leaf) {
				for (uint32_t i=0; i<page->header.records; ++i) {
					double relevance = attractors->size ? DBL_MAX : 0;
					for (uint64_t j=0; j<attractors->size; ++j) {
						double key_distance = key_to_key_distance (
									attractors->buffer[j],
									page->node.leaf.KEY(i),
									tree->dimensions);
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
									tree->dimensions);

						if (key_distance < dissimilarity) {
							dissimilarity = key_distance;
						}
					}
					dissimilarity *= lambda_diss;

					double tuple_score = dissimilarity - relevance;
					if (tuple_score > optimal->sort_key) {
						optimal->sort_key = tuple_score;
						optimal->object = page->node.leaf.objects[i];
						memcpy (optimal->key,page->node.leaf.KEY(i),tree->dimensions*sizeof(index_t));
					}
				}


				if (queue->size) {
					fifo_t *const next_queue = new_queue();
					data_container_t* next_container = NULL;
					for (uint64_t i=0; i<queue->size; ++i) {
						box_container_t *const icontainer = (box_container_t*)queue->buffer[i];
						if (container != icontainer) {
							insert_at_tail_of_queue (next_queue,icontainer);

							if (next_container == NULL || icontainer->sort_key > next_container->sort_key) {
								next_container = icontainer;
							}
						}
					}

					delete_queue (queue);
					queue = next_queue;

					free (container);
					container = next_container;
				}else{
					pthread_rwlock_unlock (page_lock);
					goto double_break;
				}
			}else{
				for (uint32_t i=0; i<page->header.records; ++i) {
					double relevance_lo = attractors->size ? DBL_MAX : 0;
					double relevance_hi = attractors->size ? DBL_MAX : 0;

					for (uint64_t j=0; j<attractors->size; ++j) {
						double box_mindistance = key_to_box_mindistance (
											attractors->buffer[j],
											page->node.internal.BOX(i),
											tree->dimensions);

						if (box_mindistance < relevance_lo) {
							relevance_lo = box_mindistance;
						}

						double box_maxdistance = key_to_box_maxdistance (
											attractors->buffer[j],
											page->node.internal.BOX(i),
											tree->dimensions);

						if (box_maxdistance < relevance_hi) {
							relevance_hi = box_maxdistance;
						}
					}
					relevance_lo *= lambda_rel;
					relevance_hi *= lambda_rel;

					double dissimilarity_lo = repellers->size ? DBL_MAX : 0;
					double dissimilarity_hi = repellers->size ? DBL_MAX : 0;
					for (uint64_t j=0; j<repellers->size; ++j) {
						double box_mindistance = key_to_box_mindistance (
											repellers->buffer[j],
											page->node.internal.BOX(i),
											tree->dimensions);

						if (box_mindistance < dissimilarity_lo) {
							dissimilarity_lo = box_mindistance;
						}

						double box_maxdistance = key_to_box_maxdistance (
											repellers->buffer[j],
											page->node.internal.BOX(i),
											tree->dimensions);

						if (box_maxdistance < dissimilarity_hi) {
							dissimilarity_hi = box_maxdistance;
						}
					}
					dissimilarity_lo *= lambda_diss;
					dissimilarity_hi *= lambda_diss;

					double lower_bound = dissimilarity_lo - relevance_hi;
					double upper_bound = dissimilarity_hi - relevance_lo;

					box_container_t *const new_container = (box_container_t *const) malloc (sizeof(box_container_t));
					new_container->box = page->node.internal.BOX(i);
					new_container->id = CHILD_ID(page_id,i);
					new_container->sort_key = upper_bound;
					insert_at_tail_of_queue (queue,new_container);

					if (lower_bound > tau) {
						tau = lower_bound;
						flag = true;
					}
				}

				data_container_t* next_container = NULL;
				fifo_t *const next_queue = new_queue();
				if (flag) {
					flag = false;
					for (uint64_t i=0; i<queue->size; ++i) {
						box_container_t *const icontainer = (box_container_t*)queue->buffer[i];
						if (container == icontainer) {
							continue;
						}else if (icontainer->sort_key > tau) {
							insert_at_tail_of_queue (next_queue,icontainer);
						}else if (lambda_rel == lambda_diss
								&& mbb_qualifies_by_criterion2(icontainer->box,tree->dimensions,tau,attractors,repellers,lambda_rel,lambda_diss)) {
							insert_at_tail_of_queue (next_queue,icontainer);
						}else{
							free (icontainer);
						}
						if (next_container == NULL || icontainer->sort_key > next_container->sort_key) {
							next_container = icontainer;
						}
					}
				}else if (queue->size) {
					for (uint64_t i=0; i<queue->size; ++i) {
						box_container_t *const icontainer = (box_container_t*)queue->buffer[i];
						if (container != icontainer) {
							insert_at_tail_of_queue (next_queue,icontainer);

							if (next_container == NULL || icontainer->sort_key > next_container->sort_key) {
								next_container = icontainer;
							}
						}
					}
				}

				delete_queue (queue);
				queue = next_queue;

				free (container);
				container = next_container;
			}
			pthread_rwlock_unlock (page_lock);
		}
	}
	double_break:
	while (queue->size) {
		free (remove_tail_of_queue(queue));
	}
	delete_queue (queue);
	return optimal;
}


static
data_container_t* augment_set_with_hotspots_minimal (tree_t *const tree,
					fifo_t const*const attractors,
					fifo_t const*const repellers,
					double const lambda_rel,
					double const lambda_diss) {

	uint16_t const dimensions = tree->dimensions;

	data_container_t *const optimal = (data_container_t *const) malloc (sizeof(data_container_t));
	optimal->key = (index_t *const) malloc (sizeof(index_t)*tree->dimensions);
	optimal->dimensions = tree->dimensions;
	optimal->sort_key = -DBL_MAX;

	priority_queue_t *const browse = new_priority_queue (&maxcompare_containers);

	reset_search_operation:;

	box_container_t* container = (box_container_t*) malloc (sizeof(box_container_t));

	container->box = tree->root_box;
	container->sort_key = 0;
	container->id = 0;

	insert_into_priority_queue (browse,container);

	while (browse->size) {
		container = remove_from_priority_queue (browse);

		if (container->sort_key <= optimal->sort_key) {
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
			free (container);
			while (browse->size) {
				free (remove_from_priority_queue (browse));
			}
			goto reset_search_operation;
		}else{
			if (page->header.is_leaf) {
				for (uint32_t i=0; i<page->header.records; ++i) {
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
							if (dissimilarity - relevance <= optimal->sort_key) {
								is_obscured = true;
								break;
							}else{
								dissimilarity = key_distance;
							}
						}
					}

					dissimilarity *= lambda_diss;


					double tuple_score = dissimilarity - relevance;
					if (tuple_score > optimal->sort_key) {
						memcpy (optimal->key,page->node.leaf.KEY(i),dimensions*sizeof(index_t));
						optimal->object = page->node.leaf.objects[i];
						optimal->sort_key = tuple_score;
					}
				}
			}else{
				for (uint32_t i=0; i<page->header.records; ++i) {
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
							if (dissimilarity - relevance <= optimal->sort_key) {
								is_obscured = true;
								break;
							}else{
								dissimilarity = box_distance;
							}
						}
					}

					dissimilarity *= lambda_diss;

					if (!is_obscured) {
						double tmp_score = dissimilarity - relevance;
						if (tmp_score > optimal->sort_key) {
							box_container_t *const new_container = (box_container_t *const) malloc (sizeof(box_container_t));
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
	delete_priority_queue (browse);
	return optimal;
}


static
index_t* new_random_point (interval_t const domain[], uint16_t dimensions) {
	index_t *const new_point = (index_t *const) malloc (sizeof(index_t)*dimensions);
	for (uint16_t i=0; i<dimensions; ++i) {
		new_point[i] = drand48() * (domain[i].end - domain[i].start) + domain[i].start;
		fprintf (stderr," %lf",new_point[i]);
	}
	fprintf (stderr,"\n");
	return new_point;
}


static
void print_usage (char const*const program) {
	printf (" ** Usage:\n\t %s [heapfile] [number of random attractors] [number of random repelers] [relevance tradeoff] [dissimilarity tradeoff] [executions]\n\n", program);
}

int main (int argc, char* argv[]) {
	if (argc < 7) {
		print_usage(*argv);
		return EXIT_FAILURE;
	}
	tree_t *const tree = load_rtree (argv[1]);

	uint64_t attractors_cardinality = atol(argv[2]);
	uint64_t repellers_cardinality = atol(argv[3]);

	double lambda_rel = atof(argv[4]);
	double lambda_diss = atof(argv[5]);

	unsigned executions = atol(argv[6]);

	fifo_t* attractors = new_queue();
	fifo_t* repellers = new_queue();

	srand48(time(NULL));

	double sum_ioC=0, sum_ioH=0, sum_ioM=0;
	double sum_timeC=0, sum_timeH=0, sum_timeM=0;
	double sum_scoreC=0, sum_scoreH=0, sum_scoreM=0;

	for (unsigned x=0; x<executions; ++x) {

		for (register uint64_t i=0; i<attractors_cardinality; ++i) {
			index_t *const attractor = new_random_point (tree->root_box,tree->dimensions);
			insert_at_tail_of_queue (attractors,attractor);
		}

		LOG (warn,"Attractors-set now consists of %lu points.\n",attractors->size);

		for (register uint64_t i=0; i<repellers_cardinality; ++i) {
			index_t *const repeller = new_random_point (tree->root_box,tree->dimensions);
			insert_at_tail_of_queue (repellers,repeller);
		}

		LOG (warn,"Repellers-set now consists of %lu points.\n",repellers->size);


		/**
		 * One way...
		 */
		clock_t startC = clock();
		data_container_t* competitor = most_diversified_tuple (tree,attractors,repellers,lambda_rel,lambda_diss);
		clock_t diffC = clock() - startC;
		uint64_t msecC = diffC * 1000 / CLOCKS_PER_SEC;
		LOG (warn,"COMPETITOR (minimal implementation using the pseudo-code from the paper) retrieved object %lu, achieving score %lf, in %lu msec.\n",competitor->object,competitor->sort_key,msecC);
		uint64_t ioC = tree->io_counter;
		sum_scoreC += competitor->sort_key;
		sum_timeC += msecC;
		sum_ioC += ioC;
		tree->io_counter = 0;


		/**
		 * or another
		 */
		clock_t startH = clock();
		fifo_t *const result = hotspots (tree,attractors,repellers,1,false,false,lambda_rel,lambda_diss,0);
		clock_t diffH = clock() - startH;
		uint64_t msecH = diffH * 1000 / CLOCKS_PER_SEC;
		uint64_t ioH = tree->io_counter;
		sum_timeH += msecH;
		sum_ioH += ioH;
		tree->io_counter = 0;

		LOG (warn,"HOMEGROWN built a list of %u hotspots (not averaged distances) using the deluxe, everything included implementation in %lu msec.\n",result->size,msecH);
		while (result->size) {
			data_pair_t* data = remove_tail_of_queue (result);
			LOG (warn,"(%f,%f): %u \n",data->key[0],data->key[1],data->object);

			free (data->key);
			free (data);
		}
		delete_queue (result);


		/**
		 * and yet, another!
		 */
		clock_t startM = clock();
		data_container_t *const homegrown = augment_set_with_hotspots_minimal(tree,attractors,repellers,lambda_rel,lambda_diss);
		clock_t diffM = clock() - startM;
		uint64_t msecM = diffM * 1000 / CLOCKS_PER_SEC;
		uint64_t ioM = tree->io_counter;
		sum_scoreM += homegrown->sort_key;
		sum_timeM += msecM;
		sum_ioM += ioM;
		tree->io_counter = 0;

		LOG (warn,"HOMEGROWN retrieved a solution (not averaged distances) using the minimal implementation in %lu msec.\n",msecM);

		/**
		 * Unallocating resources
		 */
		while (attractors->size) {
			free (remove_tail_of_queue (attractors));
		}

		while (repellers->size) {
			free (remove_tail_of_queue (repellers));
		}

		free (competitor->key);
		free (competitor);

		free (homegrown->key);
		free (homegrown);
	}

	LOG (warn,"And this is what in HOMEGROWN we call bull-shit; but not in GIS 2015, they loved that kind of shit!\n");
	LOG (warn,"Donno, maybe someone forgot to add a line or two in his source code...\n\n");

	sum_ioC/=executions; sum_ioH/=executions; sum_ioM/=executions;
	sum_timeC/=executions; sum_timeH/=executions; sum_timeM/=executions;
	sum_scoreC/=executions; sum_scoreH/=executions; sum_scoreM/=executions;
	LOG (10,"[%lu %lu %lu %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf]\n\n",
			tree->page_size,attractors_cardinality,repellers_cardinality,
			lambda_rel,lambda_diss,
			sum_timeC,sum_timeH,sum_timeM,
			sum_ioC,sum_ioH,sum_ioM,
			sum_scoreC,sum_scoreM);

	delete_queue (attractors);
	delete_queue (repellers);
	delete_rtree (tree);

	return EXIT_SUCCESS;
}

