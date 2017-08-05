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

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<limits.h>
#include<unistd.h>
#include"QL.tab.h"
#include"symbol_table.h"
#include"skyline_queries.h"
#include"spatial_standard_queries.h"
#include"priority_queue.h"
#include"stack.h"
#include"rtree.h"
#include"defs.h"

#define MEMORY_BOUND 1<<30

extern FILE* yyin;
extern FILE* yyout;

symbol_table_t* server_trees = NULL;
pthread_rwlock_t server_lock = PTHREAD_RWLOCK_INITIALIZER;

static fifo_t* process_command (lifo_t *const, char const folder[], char message[], uint64_t *const io_blocks_counter, double *const io_mb_counter);
static tree_t* process_reverse_NN_query (lifo_t *const, char const folder[], char message[], uint64_t *const io_blocks_counter, double *const io_mb_counter);
static tree_t* process_subquery (lifo_t *const, char const folder[], char message[], uint64_t *const io_blocks_counter);
static fifo_t* top_level_in_mem_closest_pairs (uint32_t const k, boolean const less_than_theta, boolean const pairwise, boolean const use_avg, lifo_t *const partial_results, boolean const has_tail);
static fifo_t* top_level_in_mem_distance_join (double const theta, boolean const less_than_theta, boolean const pairwise, boolean const use_avg, lifo_t *const partial_results, boolean const has_tail);
static tree_t* create_temp_rtree (fifo_t *const partial_result, uint32_t const page_size, uint32_t const dimensions);
static tree_t* get_rtree (char const*const filepath);
static int strcompare (key__t x, key__t y) {
	return strcmp ((char const*const)x,(char const*const)y);
}


char* qprocessor (char command[], char const folder[], char message[], uint64_t *const io_blocks_counter, double *const io_mb_counter, int fd) {
	get_rtree (NULL);
	unlink ("/tmp/cached_command.txt");
	FILE* fptr = fopen ("/tmp/cached_command.txt","w+");
	//char command[] = "/NY.rtree?key=41.127369,-73.529746;";
	//char command[] = "/NY.rtree?from=41.1,-73.6&to=41.2,-73.5;";
	//char command[] = "/NY.rtree?from=41.1,-73.6&to=41.2,-73.5&corn=II;";
	//char command[] = "/NY.rtree?from=41.1,-73.6&to=41.2,-73.5/BAY.rtree?corn=II/-10;";
	//char command[] = "/NY.rtree?corn=oo/BAY.rtree?corn=oo/NW.rtree?corn=oo/5;"

	LOG (info,"Will now initiate the processing of command '%s'.\n",command);

	fprintf (fptr,"%s",command);
	rewind (fptr);

	yyin = fptr;

	lifo_t *const stack = new_stack();
	double varray [BUFSIZ];
	int parser_rval = yyparse(stack,varray);
	fclose (fptr);

	if (parser_rval) {
		LOG (error,"Syntax error; unable to parse query: '%s'\n",command);
		strcat (message,"Syntax error; unable to parse query.");
		char *null_string = "null,\n";
		if (fd) {
			if (write (fd,null_string,sizeof(null_string)) < sizeof(null_string)) {
				LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
			}
		}
		return NULL;
	}

	//unroll (stack); exit (EXIT_SUCCESS);

	srand48(time(NULL));
	char *buffer = NULL;
	//pthread_rwlock_init (&server_lock,NULL);
	while (stack->size) {
		fifo_t *const result = process_command (stack,folder,message,io_blocks_counter,io_mb_counter);

		if (result == NULL) {
			char *null_string = "null,\n";
			if (fd) {
				if (write (fd,null_string,sizeof(null_string)) < sizeof(null_string)) {
					LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
				}
			}
			LOG (error,"Early return from query processor due to bad command...\n");
			delete_stack (stack);
			return NULL;
		}

		uint64_t buffer_size = BUFSIZ<<1;
		buffer = (char *const) malloc (sizeof(char)*buffer_size);
		char *guard = buffer + buffer_size;
		char* result_string = buffer;

		uint64_t rid = 0;
		*result_string = '\0';
		result_string = strcat (result_string,"[ \n");
		result_string += strlen(result_string);

		if (strchr(command,'/') == strrchr(command,'/')) {
			LOG (info,"Processed query returned %lu tuples. \n",result->size);
			while (result->size) {
				data_pair_t *const tuple = remove_tail_of_queue (result);

				if (guard - result_string < BUFSIZ) {
					uint64_t resultlen = strlen(buffer);
					if (fd) {
						if (write (fd,buffer,resultlen*sizeof(char)) < resultlen*sizeof(char)) {
							LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
						}
						bzero (buffer,sizeof(buffer));
						result_string = buffer;
						*result_string = '\0';
					}else{
						char *const old_buffer = buffer;
						buffer_size <<= 1;
						buffer = (char *const) malloc (sizeof(char)*buffer_size);
						guard = buffer + buffer_size;

						strcpy (buffer,old_buffer);
						free (old_buffer);

						result_string = buffer + resultlen;
					}
				}

				sprintf (result_string,"\t{ \"rid\": %lu, ",rid++);
				result_string += strlen(result_string);

				sprintf (result_string,"\"objects\": [%lu], ",tuple->object);
				result_string += strlen(result_string);

				result_string = strcat (result_string,"\"keys\": [[");
				result_string += strlen (result_string);

				for (uint32_t j=0; j<tuple->dimensions; ++j) {
					sprintf (result_string,"%12lf,",(double)tuple->key[j]);
					result_string += strlen(result_string);
				}

				result_string--;
				*result_string = ']';
				result_string++;
				*result_string = ']';
				result_string++;
				*result_string = ',';
				result_string++;
				*result_string = '\0';


				result_string = strcat (result_string," \"mindistance_ordered\": 0.0,");
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"mindistance_pairwise\": 0.0,");
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"avgdistance_ordered\": 0.0,");
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"avgdistance_pairwise\": 0.0,");
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"maxdistance_ordered\": 0.0,");
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"maxdistance_pairwise\": 0.0");
				result_string += strlen (result_string);

				result_string = strcat (result_string," },\n");
				result_string += strlen (result_string);


				free (tuple->key);
				free (tuple);
			}
		}else{
			LOG (info,"Processed join returned %lu tuples. \n",result->size);
			while (result->size) {
				multidata_container_t *const tuple = remove_tail_of_queue (result);

				if (guard - result_string < BUFSIZ) {
					uint64_t resultlen = strlen(buffer);
					if (fd) {
						if (write (fd,buffer,resultlen*sizeof(char)) < resultlen*sizeof(char)) {
							LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
						}
						bzero (buffer,sizeof(buffer));
						result_string = buffer;
						*result_string = '\0';
					}else{
						char *const old_buffer = buffer;
						buffer_size <<= 1;
						buffer = (char *const) malloc (sizeof(char)*buffer_size);
						guard = buffer + buffer_size;

						strcpy (buffer,old_buffer);
						free (old_buffer);

						result_string = buffer + resultlen;
					}
				}

				sprintf (result_string,"\t{ \"rid\": %lu, ",rid++);
				result_string += strlen(result_string);

				result_string = strcat (result_string,"\"objects\": [");
				result_string += strlen(result_string);
				for (uint32_t i=0; i<tuple->cardinality; ++i) {
					sprintf (result_string,"%lu,",tuple->objects[i]);
					result_string += strlen(result_string);
				}

				result_string--;
				*result_string = '\0';
				result_string = strcat (result_string,"], \"keys\": [");
				result_string += strlen (result_string);

				for (uint32_t i=0; i<tuple->cardinality; ++i) {
					*result_string = '[';
					result_string++;

					for (uint32_t j=0; j<tuple->dimensions; ++j) {
						sprintf (result_string,"%12lf,",(double)tuple->keys[i*tuple->dimensions+j]);
						result_string += strlen(result_string);
					}

					result_string--;
					*result_string = ']';
					result_string++;
					*result_string = ',';
					result_string++;
				}

				result_string--;
				*result_string = '\0';
				result_string = strcat (result_string,"],");
				result_string += strlen (result_string);


				result_string = strcat (result_string," \"mindistance_ordered\": ");
				result_string += strlen (result_string);
				sprintf (result_string,"%8lf,",mindistance_ordered_multikey(tuple,0));
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"mindistance_pairwise\": ");
				result_string += strlen (result_string);
				sprintf (result_string,"%8lf,",mindistance_pairwise_multikey(tuple,0));
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"avgdistance_ordered\": ");
				result_string += strlen (result_string);
				sprintf (result_string,"%8lf,",avgdistance_ordered_multikey(tuple,0));
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"avgdistance_pairwise\": ");
				result_string += strlen (result_string);
				sprintf (result_string,"%8lf,",avgdistance_pairwise_multikey(tuple,0));
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"maxdistance_ordered\": ");
				result_string += strlen (result_string);
				sprintf (result_string,"%8lf,",maxdistance_ordered_multikey(tuple,0));
				result_string += strlen (result_string);

				result_string = strcat (result_string," \"maxdistance_pairwise\": ");
				result_string += strlen (result_string);
				sprintf (result_string,"%8lf",maxdistance_pairwise_multikey(tuple,0));
				result_string += strlen (result_string);

				result_string = strcat (result_string," },\n");
				result_string += strlen (result_string);


				free (tuple->objects);
				free (tuple->keys);
				free (tuple);
			}
		}

		result_string -= 2;
		*result_string = '\0';
		result_string = strcat (result_string,"\n\t]\n");
		result_string += strlen (result_string);
		*result_string = '\0';

		LOG (info,"RESPONSE:\n%s",buffer);
	}

	delete_stack (stack);
/*
	fifo_t *const server_tree_entries = get_entries (server_trees);
	while (server_tree_entries->size) {
		symbol_table_entry_t *const entry = remove_tail_of_queue (server_tree_entries);
		delete_rtree (entry->value);
		free (entry->key);
		free (entry);
	}
	delete_symbol_table (server_trees);

	pthread_rwlock_destroy (&server_lock);
*/
	if (buffer != NULL) {
		strcpy (message,"Successful operation.");
		if (fd) {
			uint64_t resultlen = strlen(buffer);
			if (write (fd,buffer,resultlen*sizeof(char)) < resultlen*sizeof(char)) {
				LOG (error,"Error while sending data using file-descriptor %u.\n",fd);
			}
			bzero (buffer,sizeof(buffer));
			*buffer = '\0';
		}
	}else{
		strcpy (message,"Syntax error.");
	}
	return buffer;
}


static
fifo_t* process_command (lifo_t *const stack, char const folder[], char message[], uint64_t *const io_blocks_counter, double *const io_mb_counter) {
	if (stack->size) {
		if (remove_from_stack (stack) != (void*)';') {
			LOG (error,"Syntax error: Command was not ended properly.\n");
			strcpy (message,"Syntax error: Command was not ended properly.");
			clear_stack(stack);
			return NULL;
		}

		boolean is_closest_pairs_operation = false;
		if (remove_from_stack (stack) != NULL) {
			is_closest_pairs_operation = true;
		}

		double threshold = *((double*)remove_from_stack (stack));
		LOG (info,"Threshold parameter is equal to %lf. \n",threshold);


		/**
		 * Compute sub-queries.
		 */
		lifo_t *const subq_trees = new_stack();
		while (stack->size) {
			tree_t *subq_tree = NULL;
			uint64_t sub_io_blocks_counter = 0;
			if (peek_at_stack (stack) == (void*)'/') {
				subq_tree = process_subquery (stack,folder,message,&sub_io_blocks_counter);
			}else if (peek_at_stack (stack) == (void*)'%') {
				subq_tree = process_reverse_NN_query (stack,folder,message,&sub_io_blocks_counter,io_mb_counter);
			}else{
				break;
			}

			if (subq_tree != NULL) {
				*io_mb_counter += (sub_io_blocks_counter * subq_tree->page_size)/((double)(1<<20));
				*io_blocks_counter += sub_io_blocks_counter;

				insert_into_stack (subq_trees,subq_tree);
				LOG (info,"Processed subquery returned %lu tuples. \n",subq_tree->indexed_records);
			}else{
				delete_stack (subq_trees);
				return NULL;
			}
		}


		/**
		 * Join the results from all subqueries.
		 */
		fifo_t *result = NULL;
		if (subq_trees->size > 1) {
			boolean closest = true;
			boolean use_avg = false;
			boolean pairwise = false;

			if (threshold < 0) {
				threshold = -threshold;
				closest = false;
			}


			lifo_t* partial_results = new_stack();
			boolean has_tail = false;
			do{
				uint64_t cartesian_size = 0;
				lifo_t *const to_be_joined = new_stack();

				while (subq_trees->size && 
					(to_be_joined->size < 2 
					|| MEMORY_BOUND >= cartesian_size 
						+ ((tree_t *const)peek_at_stack (subq_trees))->indexed_records 
						* (((tree_t *const)peek_at_stack (subq_trees))->dimensions)
						* sizeof(index_t)+sizeof(object_t))) {

					tree_t *const top = remove_from_stack (subq_trees);
					cartesian_size += top->dimensions * top->indexed_records;
					insert_into_stack (to_be_joined,top);
				}

				LOG (info,"Executing join \#%lu...\n",partial_results->size);

				fifo_t *const partial_result = is_closest_pairs_operation
					? x_tuples (threshold,closest,use_avg,pairwise,to_be_joined)
					: distance_join (threshold,closest,pairwise,use_avg,to_be_joined);

				while (to_be_joined->size) {
					tree_t *const joined_tree = remove_from_stack(to_be_joined);

					if (joined_tree != NULL) {
						pthread_rwlock_wrlock (&joined_tree->tree_lock);
						*io_blocks_counter += joined_tree->io_counter;
						*io_mb_counter += (joined_tree->io_counter * joined_tree->page_size)/((double)(1<<20));
						joined_tree->io_counter = 0;
						pthread_rwlock_unlock (&joined_tree->tree_lock);
						
						delete_rtree (joined_tree);
					}else{
						LOG (error,"Error while finalizing join operands.\n");
						strcat (message,"Error while finalizing join operands.");
					}
				}
				delete_stack (to_be_joined);

				insert_into_stack (partial_results,partial_result);

				if (subq_trees->size == 1) {
					tree_t *const remaining_tree = remove_from_stack(subq_trees);
					index_t from [remaining_tree->dimensions];
					index_t to [remaining_tree->dimensions];
					for (uint32_t i=0; i<remaining_tree->dimensions; ++i) {
						from[i] = -INDEX_T_MAX;
						to[i] = INDEX_T_MAX;
					}
					insert_into_stack (partial_results,range(remaining_tree,from,to,remaining_tree->dimensions));
					delete_rtree (remaining_tree);
					has_tail = true;
				}
			}while (subq_trees->size);

			//printf ("partial_results->size: %lu\n\n",partial_results->size);

			fifo_t* top_level_list = NULL;
			if (partial_results->size > 1) {
				top_level_list = is_closest_pairs_operation
						? top_level_in_mem_closest_pairs (threshold,closest,pairwise,use_avg,partial_results,has_tail)
						: top_level_in_mem_distance_join (threshold,closest,pairwise,use_avg,partial_results,has_tail);
				while (partial_results->size) {
					fifo_t *const partial_result = remove_from_stack (partial_results);
					while (partial_result->size) {
						if (partial_results->size && has_tail) {
							data_pair_t *const tuple = remove_tail_of_queue (partial_result);
							free (tuple->key);
							free (tuple);
						}else{
							multidata_container_t *const tuple = remove_tail_of_queue (partial_result);
							free (tuple->objects);
							free (tuple->keys);
							free (tuple);
						}
					}
					delete_queue (partial_result);
				}
			}else{
				assert (partial_results->size);
				top_level_list = remove_from_stack (partial_results);
			}

			delete_stack (partial_results);
			delete_stack (subq_trees);

			assert (!subq_trees->size);

			return top_level_list;
		}

		assert (subq_trees->size);
		assert (subq_trees->size == 1);

		tree_t *const subq_tree = remove_from_stack (subq_trees);
		index_t from [subq_tree->dimensions];
		index_t to [subq_tree->dimensions];
		for (uint32_t i=0; i<subq_tree->dimensions; ++i) {
			from [i] = -INDEX_T_MAX;
			to [i] = INDEX_T_MAX;
		}

		result = range (subq_tree,from,to,subq_tree->dimensions);
		assert (result->size == subq_tree->indexed_records);


		while (subq_trees->size) {
			delete_rtree (remove_from_stack(subq_trees));
		}
		delete_stack (subq_trees);

		return result;
	}else{
		strcpy (message,"Syntax error: No query has been parsed to be processed.");
		LOG (error,"Syntax error: Command has not been parsed to be processed.\n");
		return NULL;
	}
}


static
fifo_t* top_level_in_mem_closest_pairs (uint32_t const k, boolean const less_than_theta, boolean const pairwise, boolean const use_avg, lifo_t *const partial_results, boolean const has_tail) {
	if (k <= 0) {
		LOG (warn,"No point for zero or negative result-sizes in joins...\n");
		return new_queue();
	}

	uint64_t const outer_cardinality = partial_results->size;
	uint64_t inner_cardinality = 0;
	uint32_t dimensions = UINT_MAX;

	for (uint64_t i=0; i<partial_results->size; ++i) {
		fifo_t *const partial_result = partial_results->buffer[i];
		if (partial_result->size) {
			if (has_tail && i == partial_results->size-1) {
				++inner_cardinality;
			}else{
				inner_cardinality += ((multidata_container_t *const)partial_result->buffer[partial_result->head])->cardinality;
				uint32_t const dimensionality_i = ((multidata_container_t *const)partial_result->buffer[partial_result->head])->dimensions;
				if (dimensionality_i < dimensions) {
					dimensions = dimensionality_i;
				}
			}
		}else{
			LOG (error,"Partial result-set %lu contains no records...\n",i);
			return new_queue();
		}
	}

	priority_queue_t* data_combinations = new_priority_queue (less_than_theta?&maxcompare_multicontainers:&mincompare_multicontainers);

	uint64_t offsets [outer_cardinality];
	bzero (offsets,outer_cardinality*sizeof(uint64_t));

	for (uint64_t j=0,i=0;;++j) {
		if (offsets[i] >= ((fifo_t *const)partial_results->buffer[i])->size) {
			offsets[i++] = 0;
			if (i >= outer_cardinality)
				break;
		}else{
			multidata_container_t *const dest_container = (multidata_container_t *const) malloc (sizeof(multidata_container_t));

			dest_container->keys = (index_t*) malloc (inner_cardinality*dimensions*sizeof(index_t));
			dest_container->objects = (object_t*) malloc (inner_cardinality*sizeof(object_t));
			dest_container->dimensions = dimensions;
			dest_container->cardinality = 0;

			for (uint32_t offset=0; offset<outer_cardinality; ++offset) {
				assert (offset < partial_results->size);
				assert (offsets[offset] < ((fifo_t *const)partial_results->buffer[offset])->size);

				if (has_tail && offset == outer_cardinality - 1) {
					data_pair_t const*const src_container = get_queue_element((fifo_t *const)partial_results->buffer[offset],offsets[offset]);
					dest_container->objects[dest_container->cardinality] = src_container->object;

					memcpy (dest_container->keys+dest_container->cardinality*dimensions,
						src_container->key,
						dimensions*sizeof(index_t));

					dest_container->cardinality++;
				}else{
					multidata_container_t const*const src_container = get_queue_element((fifo_t *const)partial_results->buffer[offset],offsets[offset]);

					memcpy (dest_container->objects+dest_container->cardinality,
						src_container->objects,
						src_container->cardinality*sizeof(object_t));

					for (uint32_t c=0; c<src_container->cardinality; ++c) {
						memcpy (dest_container->keys+(dest_container->cardinality+c)*dimensions,
							src_container->keys+c*src_container->dimensions,
							dimensions*sizeof(index_t));
					}

					dest_container->cardinality += src_container->cardinality;
				}
			}

			assert (dest_container->cardinality == inner_cardinality);

			dest_container->sort_key = use_avg?
					(pairwise?avgdistance_pairwise_multikey(dest_container,0):avgdistance_ordered_multikey(dest_container,0))
					:(less_than_theta?
					(pairwise?maxdistance_pairwise_multikey(dest_container,0):maxdistance_ordered_multikey(dest_container,0))
					:(pairwise?mindistance_pairwise_multikey(dest_container,0):mindistance_ordered_multikey(dest_container,0)));

			if (data_combinations->size < k) {
				insert_into_priority_queue (data_combinations,dest_container);
			}else{
				multidata_container_t *const top = (multidata_container_t *const) peek_priority_queue (data_combinations);
				if (less_than_theta ? dest_container->sort_key < top->sort_key : dest_container->sort_key > top->sort_key) {
					remove_from_priority_queue (data_combinations);
					insert_into_priority_queue (data_combinations,dest_container);

					free (top->objects);
					free (top->keys);
					free (top);
				}else{
					free (dest_container->objects);
					free (dest_container->keys);
					free (dest_container);
				}
			}
			i=0;
		}
		++offsets[i];
	}

	fifo_t* result = new_queue();
	while (data_combinations->size) {
		insert_at_tail_of_queue(result,remove_from_priority_queue(data_combinations));
	}

	delete_priority_queue (data_combinations);

	return result;
}


static
fifo_t* top_level_in_mem_distance_join (double const theta, boolean const less_than_theta, boolean const pairwise, boolean const use_avg, lifo_t *const partial_results, boolean const has_tail) {
	if (less_than_theta && theta < 0) {
		LOG (warn,"No point for negative distances in joins...\n");
		return new_queue();
	}

	uint64_t const outer_cardinality = partial_results->size;
	uint64_t inner_cardinality = 0;
	uint32_t dimensions = UINT_MAX;

	for (uint64_t i=0; i<partial_results->size; ++i) {
		fifo_t *const partial_result = partial_results->buffer[i];
		if (partial_result->size) {
			if (has_tail && i == partial_results->size-1) {
				++inner_cardinality;
			}else{
				inner_cardinality += ((multidata_container_t *const)partial_result->buffer[partial_result->head])->cardinality;
				uint32_t const dimensionality_i = ((multidata_container_t *const)partial_result->buffer[partial_result->head])->dimensions;
				if (dimensionality_i < dimensions) {
					dimensions = dimensionality_i;
				}
			}
		}else{
			LOG (error,"Partial result-set %lu contains no records...\n",i);
			return new_queue();
		}
	}

	priority_queue_t* data_combinations = new_priority_queue (less_than_theta?&maxcompare_multicontainers:&mincompare_multicontainers);

	uint64_t offsets [outer_cardinality];
	bzero (offsets,outer_cardinality*sizeof(uint64_t));

	for (uint64_t j=0,i=0;;++j) {
		if (offsets[i] >= ((fifo_t *const)partial_results->buffer[i])->size) {
			offsets[i++] = 0;
			if (i >= outer_cardinality)
				break;
		}else{
			multidata_container_t *const dest_container = (multidata_container_t *const) malloc (sizeof(multidata_container_t));

			dest_container->keys = (index_t*) malloc (inner_cardinality*dimensions*sizeof(index_t));
			dest_container->objects = (object_t*) malloc (inner_cardinality*sizeof(object_t));
			dest_container->cardinality = 0;
			dest_container->dimensions = dimensions;

			for (uint32_t offset=0; offset<outer_cardinality; ++offset) {
				if (has_tail && offset == outer_cardinality - 1) {
					data_pair_t const*const src_container = ((fifo_t *const)partial_results->buffer[offset])->buffer[offsets[offset]];
					dest_container->objects[dest_container->cardinality] = src_container->object;

					memcpy (dest_container->keys+dest_container->cardinality*dimensions,
						src_container->key,
						dimensions*sizeof(index_t));

					dest_container->cardinality++;
				}else{
					multidata_container_t const*const src_container = ((fifo_t *const)partial_results->buffer[offset])->buffer[offsets[offset]];

					memcpy (dest_container->objects+dest_container->cardinality,
						src_container->objects,
						src_container->cardinality*sizeof(object_t));

					for (uint32_t c=0; c<src_container->cardinality; ++c) {
						memcpy (dest_container->keys+(dest_container->cardinality+c)*dimensions,
							src_container->keys+c*src_container->dimensions,
							dimensions*sizeof(index_t));
					}

					dest_container->cardinality += src_container->cardinality;
				}
			}

			assert (dest_container->cardinality == inner_cardinality);

			dest_container->sort_key = use_avg?
					(pairwise?avgdistance_pairwise_multikey(dest_container,0):avgdistance_ordered_multikey(dest_container,0))
					:(less_than_theta?
					(pairwise?maxdistance_pairwise_multikey(dest_container,0):maxdistance_ordered_multikey(dest_container,0))
					:(pairwise?mindistance_pairwise_multikey(dest_container,0):mindistance_ordered_multikey(dest_container,0)));

			if (less_than_theta ? dest_container->sort_key <= theta : dest_container->sort_key >= theta) {
					insert_into_priority_queue (data_combinations,dest_container);
			}else{
					free (dest_container->objects);
					free (dest_container->keys);
					free (dest_container);
			}
			i=0;
		}
		++offsets[i];
	}

	fifo_t* result = new_queue();
	while (data_combinations->size) {
		insert_at_tail_of_queue(result,remove_from_priority_queue(data_combinations));
	}

	delete_priority_queue (data_combinations);

	return result;
}


static
tree_t* get_rtree (char const*const filepath) {
	pthread_rwlock_rdlock (&server_lock);
	if (server_trees == NULL) {
		pthread_rwlock_unlock (&server_lock);
		pthread_rwlock_wrlock (&server_lock);
		server_trees = new_symbol_table (NULL,&strcompare);
	}

	if (filepath != NULL) {
		tree_t* tree = get (server_trees,filepath);
		pthread_rwlock_unlock (&server_lock);

		if (tree == NULL) {
			pthread_rwlock_wrlock (&server_lock);
			tree = load_rtree (filepath);
			if (tree != NULL) {
				set (server_trees,filepath,tree);
				LOG (info,"Loaded from the disk R#-Tree: '%s'\n",tree->filename);
			}
			pthread_rwlock_unlock (&server_lock);
		}else{
			LOG (info,"Retrieved R#-Tree: '%s'\n",tree->filename)
		}
		return tree;
	}else{
		pthread_rwlock_unlock (&server_lock);
		return NULL;
	}
}

static
tree_t* process_reverse_NN_query (lifo_t *const stack, char const folder[], char message[], uint64_t *const io_blocks_counter, double *const io_mb_counter) {
	if (remove_from_stack (stack) == (void*)'%') {
		uint32_t kcardinality = remove_from_stack (stack);
		index_t key [kcardinality];
		for (register uint32_t i=kcardinality; i>0; --i) {
			double* tmp = remove_from_stack (stack);
			if (logging <= info) fprintf (stderr,"%lf ",*tmp);
			key [i-1] = *tmp;
		}

		lifo_t *const feature_trees = new_stack ();
		while (peek_at_stack (stack) == (void*)'%') {
			unsigned sub_io_blocks_counter = 0;
			tree_t *const feature_tree = process_subquery (stack,folder,message,&sub_io_blocks_counter);

			if (feature_tree == NULL) {
				while (feature_trees->size) {
					delete_rtree (remove_from_stack(feature_trees));
				}
				delete_stack (feature_trees);

				strcpy (message,"Unable to retrieve feature-tree for the RNN query.");
				LOG (error,"Unable to retrieve the feature-tree for the RNN query.\n");
				return NULL;
			}else
			if (feature_tree->dimensions > kcardinality) {
				while (feature_trees->size) {
					delete_rtree (remove_from_stack(feature_trees));
				}
				delete_stack (feature_trees);

				LOG (error,"RNN key predicate dimensionality should be equal or greater than the dimensionality of any feature-tree.\n");
				strcat (message,"RNN key predicate dimensionality should be equal or greater than the dimensionality of any feature-tree.\n");
				return NULL;
			}

			*io_mb_counter += (sub_io_blocks_counter * feature_tree->page_size)/((double)(1<<20));
			*io_blocks_counter += sub_io_blocks_counter;
		}

		if (peek_at_stack (stack) != (void*)'/') {
			strcpy (message,"Syntax error: RNN query should start with a data-tree.");
			LOG (error,"Syntax error: RNN query should start with a data-tree.\n");
			return NULL;
		}

		uint64_t sub_io_blocks_counter = 0;
		tree_t *const data_tree = process_subquery (stack,folder,message,&sub_io_blocks_counter);

		if (data_tree == NULL) {
			strcpy (message,"Unable to retrieve the data-tree for the RNN query.");
			LOG (error,"Unable to retrieve the data-tree for the RNN query.\n");
			return NULL;
		}else
		if (data_tree->dimensions > kcardinality) {
			LOG (error,"RNN key predicate dimensionality should be equal or greater than the dimensionality of the data-tree.\n");
			strcat (message,"RNN key predicate dimensionality should be equal or greater than the dimensionality of the data-tree.\n");
			return NULL;
		}

		*io_mb_counter += (sub_io_blocks_counter * data_tree->page_size)/((double)(1<<20));
		*io_blocks_counter += sub_io_blocks_counter;


		fifo_t *const result_list = new_queue(); //multichromatic_reverse_nearest_neighbors (key,data_tree,feature_trees);
		tree_t *const result_tree = create_temp_rtree (result_list,data_tree->page_size,data_tree->dimensions);

		delete_stack (feature_trees);
		return result_tree;
	}else{
		LOG (error,"Syntax error: Was expecting the start of a reverse NN subquery.");
		strcpy (message,"Syntax error.");
		clear_stack (stack);
		return NULL;
	}
}

static
tree_t* process_subquery (lifo_t *const stack, char const folder[], char message[], uint64_t *const io_counter) {
	if (peek_at_stack (stack) == (void*)'/' || peek_at_stack (stack) == (void*)'%') {
		char start_symbol = (char) remove_from_stack (stack);
		LOG (0,"UNROLLING NEW SUBQUERY... \n");

		char *const filename = remove_from_stack (stack);
		char *const filepath = (char *const) malloc (sizeof(char)*(strlen(folder)+strlen(filename)+2));
		strcpy (filepath,folder);
		if (folder[strlen(folder)-1]!='/') {
			strcat (filepath,"/");
		}
		LOG (info,"HEAPFILE: '%s'. \n",filename);
		strcat (filepath,filename);
		free (filename);

		tree_t* tree = get_rtree (filepath);
		if (tree == NULL) {
			sprintf (message,"Cannot perform an operation on heapfile '%s' because it does not exist.",filepath);
			LOG (error,"Cannot perform an operation on heapfile '%s' because it does not exist.\n",filepath);
			return NULL;
		}

		index_t from [tree->dimensions];
		index_t to [tree->dimensions];

		boolean corner [tree->dimensions];
		index_t bound [tree->dimensions+1];

		bzero (corner,tree->dimensions*sizeof(boolean));
		for (uint32_t i=0; i<tree->dimensions; ++i) {
			from [i] = -INDEX_T_MAX;
			to [i] = INDEX_T_MAX;
		}

		lifo_t *const lookups = new_stack();


		boolean is_skyline = false;
		uint32_t bounded_dimensionality = 0;

		uint32_t const pcardinality = remove_from_stack (stack);
		if (!pcardinality) {
			return tree;
		}
		for (uint32_t j=0; j<pcardinality; ++j) {
			uint32_t const operation = remove_from_stack (stack);
			uint32_t kcardinality = remove_from_stack(stack);

			switch (operation) {
				case LOOKUP:
					LOG (info,"LOOKUP ");
					index_t *const lookup = (index_t *const) malloc (tree->dimensions*sizeof(index_t));
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					if (tree->dimensions < kcardinality) {
						kcardinality = tree->dimensions;
					}
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					uint32_t i=0;
					for (i=0; i<kcardinality; ++i) {
						double* tmp = remove_from_stack (stack);
						if (logging <= info) fprintf (stderr,"%lf ",*tmp);
						lookup[kcardinality-i-1] = *tmp;
					}

					if (i < tree->dimensions) {
						for (uint32_t k=0; k<i; ++k) {
							if (lookup[k] > from[k]) {
								lookup[k] = from[k];
							}
							if (lookup[k] < to[k]) {
								lookup[k] = to[k];
							}
						}
					}else{
						insert_into_stack (lookups,lookup);
					}

					break;
				case FROM:
					LOG (info,"FROM ");
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					if (tree->dimensions < kcardinality) {
						kcardinality = tree->dimensions;
					}
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					for (uint32_t i=0; i<kcardinality; ++i) {
						double* tmp = remove_from_stack (stack);
						if (logging <= info) fprintf (stderr,"%lf ",*tmp);
						if (*tmp > from[kcardinality-1-i]) {
							from[kcardinality-1-i] = *tmp;
						}
					}
					break;
				case TO:
					LOG (info,"TO ");
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					if (tree->dimensions < kcardinality) {
						kcardinality = tree->dimensions;
					}
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					for (uint32_t i=0; i<kcardinality; ++i) {
							double* tmp = remove_from_stack (stack);
							if (logging <= info) fprintf (stderr,"%lf ",*tmp);
							if (*tmp < to[kcardinality-1-i]) {
								to[kcardinality-1-i] = *tmp;
							}
					}
					break;
				case BOUND:
					LOG (info,"BOUND ");
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					if (tree->dimensions+1 < kcardinality) {
						kcardinality = tree->dimensions+1;
					}
					if (logging <= info) fprintf (stderr," (%u) ",kcardinality);
					bounded_dimensionality = kcardinality;
					for (uint32_t i=0; i<kcardinality; ++i) {
						double* tmp = remove_from_stack (stack);
						if (logging <= info) fprintf (stderr,"%lf ",*tmp);
						bound[kcardinality-1-i] = *tmp;
					}
					break;
				case CORN:
					is_skyline = true;
					char *const bitfield = remove_from_stack (stack);
					LOG (info,"SKYLINE - BITFIELD '%s'",bitfield);

					uint32_t length = strlen (bitfield);
					for (uint32_t i=0; i<length; ++i) {
						if (bitfield[i]=='O' || bitfield[i]=='o') {
							corner[i] = false;
						}else if (bitfield[i]=='I' || bitfield[i]=='i') {
							corner[i] = true;
						}else{
							LOG (error,"Unable to parse bitfield '%s'...\n",bitfield)
							is_skyline = false;
							break;
						}
					}
					break;
				default:
					LOG (error,"Unknown operation...\n");
			}

			if (logging <= info) {
				fprintf (stderr,"\n");
			}
		}


		LOG (info,"Result computation to take place now...\n");

		boolean delete_rtree_flag = false;
		tree_t* result_tree = NULL;
		if (lookups->size) {
			fifo_t *const lookups_result_list = new_queue();
			LOG (info,"lookups stack-size: %lu \n",lookups->size);

			while (lookups->size) {
				index_t *const lookup = remove_from_stack (lookups);
				LOG (info,"lookup-key: ( %12lf %12lf ) \n",(double)lookup[0],(double)lookup[1]);

				fifo_t *const partial = find_all_in_rtree (tree,lookup,tree->dimensions);
				LOG (info,"Adding %lu new tuples in the result...\n",partial->size);

				while (partial->size) {
					data_pair_t *const pair = (data_pair_t*) malloc (sizeof(data_pair_t));
					pair->key = (index_t*) malloc (sizeof(index_t)*tree->dimensions);
					memcpy (pair->key,lookup,sizeof(index_t)*tree->dimensions);

					pair->object = remove_tail_of_queue (partial);
					insert_at_tail_of_queue (lookups_result_list,pair);
				}

				free (lookup);
				delete_queue (partial);
			}

			if (lookups_result_list->size) {
				LOG (info,"Creating materialized view for the result consisting of %lu records... \n",lookups_result_list->size);

				pthread_rwlock_wrlock (&tree->tree_lock);
				*io_counter = tree->io_counter;
				tree->io_counter = 0;
				pthread_rwlock_unlock (&tree->tree_lock);

				tree = create_temp_rtree (lookups_result_list,tree->page_size,tree->dimensions);
				delete_rtree_flag = true;
			}
		}

		if (bounded_dimensionality && !is_skyline) {
			fifo_t *const bounded_result_list = bounded_search (tree,from,to,bound+1,*bound,bounded_dimensionality-1);
			LOG (info,"Bounded search result contains %lu tuples.\n",bounded_result_list->size);

			result_tree = create_temp_rtree (bounded_result_list,tree->page_size,tree->dimensions);
		}else
		if (is_skyline) {
			fifo_t *const skyline_result_list = skyline_constrained (tree,corner,from,to);
			LOG (info,"Skyline result contains %lu tuples.\n",skyline_result_list->size);

			if (bounded_dimensionality) {
				priority_queue_t* max_heap = new_priority_queue(&maxcompare_containers);

				while (skyline_result_list->size) {
					data_pair_t *sky_tuple = remove_tail_of_queue (skyline_result_list);

					data_container_t *const sort_tuple = (data_container_t*) malloc (sizeof(data_container_t));

					sort_tuple->object = sky_tuple->object;
					sort_tuple->key = sky_tuple->key;
					sort_tuple->sort_key = key_to_key_distance (bound+1,sky_tuple->key,bounded_dimensionality-1);

					if (max_heap->size < *bound) {
						sort_tuple->dimensions = tree->dimensions;
						insert_into_priority_queue (max_heap,sort_tuple);
					}else{
						if (sort_tuple->sort_key < ((data_container_t*)peek_priority_queue(max_heap))->sort_key) {
							data_container_t* temp = remove_from_priority_queue (max_heap);
							free (temp->key);
							free (temp);

							sort_tuple->dimensions = tree->dimensions;
							insert_into_priority_queue (max_heap,sort_tuple);
						}else{
							free (sort_tuple->key);
							free (sort_tuple);
						}
					}
					free (sky_tuple);
				}

				clear_queue (skyline_result_list);

				while (max_heap->size) {
					data_container_t *const before = remove_from_priority_queue (max_heap);
					data_pair_t *const after = (data_pair_t *const) malloc (sizeof(data_pair_t));
					insert_at_tail_of_queue (skyline_result_list,after);
					after->dimensions = before->dimensions;
					after->object = before->object;
					after->key = before->key;
					free (before);
				}
				delete_priority_queue (max_heap);
			}

			result_tree = create_temp_rtree (skyline_result_list,tree->page_size,tree->dimensions);
		}else{
			fifo_t *const range_result_list = range (tree,from,to,tree->dimensions);
			LOG (info,"Range query result contains %lu tuples.\n",range_result_list->size);

			result_tree = create_temp_rtree (range_result_list,tree->page_size,tree->dimensions);
		}


		delete_stack (lookups);
		if (delete_rtree_flag) {
			delete_rtree (tree);
		}else{
			pthread_rwlock_wrlock (&tree->tree_lock);
			*io_counter = tree->io_counter;
			tree->io_counter = 0;
			pthread_rwlock_unlock (&tree->tree_lock);
		}
		return result_tree;
	}else{
		LOG (error,"Syntax error: Was expecting the start of a new subquery.");
		strcpy (message,"Syntax error.");
		clear_stack (stack);
		return NULL;
	}
}


/**
 * It returns a spatial structure containing the results of a
 * sub-query to be joined with other results from a complex query.
 */
static
tree_t* create_temp_rtree (fifo_t *const partial_result, uint32_t const page_size, uint32_t const dimensions) {
        char filename[32];
        strcpy (filename,"/tmp/tree.");
        sprintf (filename+10,"%lx",lrand48());
        assert (strlen(filename)<32);

        tree_t *const tree = new_rtree (filename,page_size,dimensions);

        while (partial_result->size) {
                data_pair_t *const data_pair = remove_tail_of_queue (partial_result);
                insert_into_rtree (tree,data_pair->key,data_pair->object);

                free (data_pair->key);
                free (data_pair);
        }
        delete_queue (partial_result);
        return tree;
}


