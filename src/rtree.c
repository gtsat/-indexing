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
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "symbol_table.h"
#include "priority_queue.h"
#include "common.h"
#include "queue.h"
#include "stack.h"
#include "rtree.h"
//#include "ntree.h"
#include "swap.h"
#ifdef __APPLE__
	#include <machine/endian.h>
#else
	#include <endian.h>
#endif

boolean verbose_splits = false;


extern boolean allow_dangling_pages;
extern boolean dump_transposed_pages;


void delete_rtree (tree_t *const tree) {
	if (tree != NULL) {
		if (tree->root_box != NULL)
			free (tree->root_box);

		if (tree->object_range != NULL)
			free (tree->object_range);

		if (tree->heapfile_index != NULL) {
			flush_tree (tree);
			delete_symbol_table (tree->heapfile_index);
			delete_swap (tree->swap);
		}

		if (tree->page_locks != NULL) {
			fifo_t* queue = get_entries (tree->page_locks);
			while (queue->size) {
				pthread_rwlock_t *const page_lock = remove_tail_of_queue(queue);
				pthread_rwlock_destroy (page_lock);
				free (page_lock);
			}
			delete_symbol_table (tree->page_locks);
		}

		int fd = open (tree->filename, O_WRONLY | O_CREAT, PERMS);
		lseek (fd,0,SEEK_SET);
		uint16_t le_tree_dimensions = htole16(tree->dimensions);
		if (write (fd,&le_tree_dimensions,sizeof(uint16_t)) < sizeof(uint16_t)) {
			LOG (error,"[%s] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint16_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint32_t le_tree_page_size = htole32(tree->page_size);
		if (write (fd,&le_tree_page_size,sizeof(uint32_t)) < sizeof(uint32_t)) {
			LOG (error,"[%s] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint32_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint64_t le_tree_tree_size = htole16(tree->tree_size);
		if (write (fd,&le_tree_tree_size,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (error,"[%s] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint64_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint64_t le_tree_indexed_records = htole16(tree->indexed_records);
		if (write (fd,&le_tree_indexed_records,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (error,"[%s] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint64_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		close (fd);

		pthread_rwlock_destroy (&tree->tree_lock);

		free (tree->filename);
		free (tree);
	}
}

static void print_box (boolean stream,tree_t const*const tree, interval_t* box) {
	for (uint32_t j=0; j<tree->dimensions; ++j)
		fprintf(stream?stderr:stdout,"(%12lf,%12lf) ",(double)box[j].start,(double)box[j].end);
	fprintf(stream?stderr:stdout,".\n",tree->filename);
}

/*
static int truncate_heapfile (tree_t const*const tree) {
	uint64_t id = 0;
	while (true) {
		page_t const*const page = load_page (tree,id);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(id);

		assert (page != NULL);
		assert (page_lock != NULL);

		pthread_rwlock_rdlock (page_lock);
		if (page->header.is_leaf) break;
		else id = CHILD_ID(id,page->header.records-1);
		pthread_rwlock_unlock (page_lock);
	}

	pthread_rwlock_rdlock (&tree->tree_lock);
	LOG (info,"[%s] Tree contains %lu data records.\n",tree->filename,tree->indexed_records);
	pthread_rwlock_unlock (&tree->tree_lock);

	LOG (info,"[%s] Truncating heapfile to %lu pages.\n",tree->filename,id+1);

	return truncate (tree->filename,(id+1)*tree->page_size);
}
*/

static boolean update_box (tree_t *const tree, uint64_t const page_id) {
	if (!page_id) return false;

	page_t *const parent = load_page (tree,PARENT_ID(page_id));
	page_t const*const page = load_page (tree,page_id);

	pthread_rwlock_rdlock (&tree->tree_lock);
	pthread_rwlock_t *const parent_lock = LOADED_LOCK(PARENT_ID(page_id));
	pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
	pthread_rwlock_unlock (&tree->tree_lock);

	assert (parent_lock != NULL);
	assert (page_lock != NULL);

	boolean is_updated = false;

	pthread_rwlock_wrlock (parent_lock);
	pthread_rwlock_rdlock (page_lock);

	uint64_t const offset = CHILD_OFFSET(page_id);
	if (page->header.is_leaf) {
		for (register uint32_t i=0; i<page->header.records; ++i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if (page->node.leaf.KEYS(i,j) < (parent->node.internal.BOX(offset)+j)->start) {
					(parent->node.internal.BOX(offset)+j)->start = page->node.leaf.KEYS(i,j);
					parent->header.is_dirty = true;
					is_updated = true;
				}
				if (page->node.leaf.KEYS(i,j) > (parent->node.internal.BOX(offset)+j)->end) {
					(parent->node.internal.BOX(offset)+j)->end = page->node.leaf.KEYS(i,j);
					parent->header.is_dirty = true;
					is_updated = true;
				}
			}
		}
	}else{
		for (register uint32_t i=0; i<page->header.records; ++i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if ((page->node.internal.BOX(i)+j)->start < (parent->node.internal.BOX(offset)+j)->start){
					(parent->node.internal.BOX(offset)+j)->start = (page->node.internal.BOX(i)+j)->start;
					parent->header.is_dirty = true;
					is_updated = true;
				}
				if ((page->node.internal.BOX(i)+j)->end > (parent->node.internal.BOX(offset)+j)->end) {
					(parent->node.internal.BOX(offset)+j)->end =(page->node.internal.BOX(i)+j)->end;
					parent->header.is_dirty = true;
					is_updated = true;
				}
			}
		}
	}

	pthread_rwlock_unlock (parent_lock);
	pthread_rwlock_unlock (page_lock);

	if (is_updated) {
		pthread_rwlock_wrlock (&tree->tree_lock);
		tree->is_dirty = true;
		pthread_rwlock_unlock (&tree->tree_lock);
	}

	return is_updated;
}

static void update_boxes (tree_t *const tree, uint64_t page_id) {
	for (;page_id
		&& update_box(tree,page_id);
		page_id=PARENT_ID(page_id))
		;

	if (!page_id) {
		update_rootbox (tree);
	}
}


tree_t* load_rtree (char const filename[]) {
	umask ( S_IRWXO | S_IWGRP);
	tree_t *const tree = (tree_t *const) malloc (sizeof(tree_t));
	if (tree == NULL) {
		LOG (error,"[%s] Unable to allocate memory for new R-Tree...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	tree->filename = strdup (filename);

	int fd = open (filename,O_RDONLY,0);
	lseek (fd,0,SEEK_SET);
	if (fd < 0) {
		LOG (error,"[%s] Could not find heapfile '%s'... \n",tree->filename,filename);
		return NULL;
	}else{
		if (read (fd,&tree->dimensions,sizeof(uint16_t)) < sizeof(uint16_t)) {
			LOG (error,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint16_t),filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (read (fd,&tree->page_size,sizeof(uint32_t)) < sizeof(uint32_t)) {
			LOG (error,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint32_t),filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (read (fd,&tree->tree_size,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (error,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint64_t),filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (read (fd,&tree->indexed_records,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (error,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint64_t),filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		tree->dimensions = le16toh(tree->dimensions);
		tree->page_size = le32toh(tree->page_size);
		tree->tree_size = le64toh(tree->tree_size);
		tree->indexed_records = le64toh(tree->indexed_records);
	}

	tree->io_counter = 0;
	tree->is_dirty = false;
	close (fd);

	tree->internal_entries = (tree->page_size-sizeof(header_t)) / (sizeof(interval_t)*tree->dimensions);
	tree->leaf_entries = (tree->page_size-sizeof(header_t)) / (sizeof(index_t)*tree->dimensions + sizeof(object_t));


	LOG (info,"[%s] Configuration uses pages of %u bytes.\n",filename,tree->page_size);

	if (fairness_threshold*(tree->internal_entries>>1) < 2) {
		LOG (error,"[%s] Cannot use configuration allowing underflows of just one record per page.\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	LOG (info,"[%s] Heapfile consists of %lu pages and has %lu %u-dimensional records.\n",
				filename,tree->tree_size,tree->indexed_records,tree->dimensions);

	tree->object_range = NULL;
	tree->root_box = (interval_t*) malloc (tree->dimensions*sizeof(interval_t));
	if (tree->root_box == NULL) {
		LOG (error,"[%s] Unable to allocate memory for new tree hierarchy...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	for (uint16_t j=0; j<tree->dimensions; ++j) {
		tree->root_box[j].start = INDEX_T_MAX;
		tree->root_box[j].end = -INDEX_T_MAX;
	}

	tree->heapfile_index = new_symbol_table_primitive (NULL);
	tree->page_locks = new_symbol_table_primitive (NULL);
	tree->swap = new_swap (initial_capacity);

	pthread_rwlock_init (&tree->tree_lock,NULL);

	if (load_page (tree,0) == NULL) new_root(tree);
	else update_rootbox (tree);
	return tree;
}

tree_t* new_rtree (char const filename[], uint32_t const page_size, uint32_t const dimensions) {
	umask ( S_IRWXO | S_IWGRP);
	tree_t *const tree = (tree_t *const) malloc (sizeof(tree_t));
	if (tree == NULL) {
		LOG (error,"[%s] Unable to allocate memory for new R-Tree...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	tree->filename = (char*) malloc ((strlen(filename)+1)*sizeof(char));
	strcpy (tree->filename,filename);

	int fd = open (filename,O_RDONLY,0);
	lseek (fd,0,SEEK_SET);
	if (fd < 0) {
		tree->dimensions = dimensions;
		tree->page_size = page_size;
		tree->indexed_records = 0;
		tree->tree_size = 0;
	}else{
		if (read (fd,&tree->dimensions,sizeof(uint16_t)) < sizeof(uint16_t)) {
			LOG (fatal,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint16_t),filename);
			abort();
		}
		if (read (fd,&tree->page_size,sizeof(uint32_t)) < sizeof(uint32_t)) {
			LOG (fatal,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint32_t),filename);
			abort();
		}
		if (read (fd,&tree->tree_size,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (fatal,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint64_t),filename);
			abort();
		}
		if (read (fd,&tree->indexed_records,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (fatal,"[%s] Read less than %lu bytes from heapfile '%s'...\n",tree->filename,sizeof(uint64_t),filename);
			abort();
		}

		tree->dimensions = le16toh(tree->dimensions);
		tree->page_size = le32toh(tree->page_size);
		tree->tree_size = le64toh(tree->tree_size);
		tree->indexed_records = le64toh(tree->indexed_records);
	}

	close (fd);

	tree->io_counter = 0;
	tree->is_dirty = false;

	tree->internal_entries = (tree->page_size-sizeof(header_t)) / (sizeof(interval_t)*tree->dimensions);
	tree->leaf_entries = (tree->page_size-sizeof(header_t)) / (sizeof(index_t)*tree->dimensions + sizeof(object_t));


	LOG (info,"[%s] Configuration uses pages of %u bytes.\n",filename,tree->page_size);

	if (fairness_threshold*(tree->internal_entries>>1) < 2) {
		LOG (error,"[%s] Cannot use configuration allowing underflows of just one record per page.\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	LOG (info,"[%s] Heapfile consists of %lu pages and has %lu %u-dimensional records.\n",
				filename,tree->tree_size,tree->indexed_records,tree->dimensions);

	tree->object_range = NULL;
	tree->root_box = (interval_t*) malloc (tree->dimensions*sizeof(interval_t));
	if (tree->root_box == NULL) {
		LOG (error,"[%s] Unable to allocate memory for new tree hierarchy...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	for (uint16_t j=0; j<tree->dimensions; ++j) {
		tree->root_box[j].start = INDEX_T_MAX;
		tree->root_box[j].end = -INDEX_T_MAX;
	}

	tree->heapfile_index = new_symbol_table_primitive (NULL);
	tree->page_locks = new_symbol_table_primitive (NULL);
	tree->swap = new_swap (initial_capacity);

	pthread_rwlock_init (&tree->tree_lock,NULL);

	if (load_page (tree,0) == NULL) new_root(tree);
	else update_rootbox (tree);

	return tree;
}


static uint64_t split_internal (tree_t *const tree, uint64_t pos, fifo_t *const inception);

static uint64_t halve_internal (tree_t *const tree, uint64_t position, fifo_t *const inception_queue) {
	if (verbose_splits) {
		puts ("==============================================================");
		LOG (info,"[%s] HALVING INTERNAL NODE AT POSITION: %lu\n",tree->filename,position);
	}

	page_t* overloaded_page = load_page (tree,position);
	page_t* parent = load_page (tree,PARENT_ID(position));

	pthread_rwlock_rdlock (&tree->tree_lock);
	pthread_rwlock_t* page_lock = LOADED_LOCK(position);
	pthread_rwlock_t* parent_lock = LOADED_LOCK(PARENT_ID(position));
	pthread_rwlock_unlock (&tree->tree_lock);

	assert (page_lock != NULL);
	assert (parent_lock != NULL);

	page_t* lo_page;
	page_t* hi_page;

	pthread_rwlock_rdlock (parent_lock);
	if (verbose_splits) {
		print_box(false,tree,parent->node.internal.BOX(CHILD_OFFSET(position)));
	}
	uint32_t parent_records = parent->header.records;
	pthread_rwlock_unlock (parent_lock);

	/** take care of cascading splits here **/
	if (parent_records >= tree->internal_entries) {
		if (position) {
			insert_at_tail_of_queue (inception_queue,position);
			uint64_t const parent_id = split_internal (tree,PARENT_ID(position),inception_queue);
			position = remove_tail_of_queue (inception_queue);

			parent = load_page (tree,PARENT_ID(position));
			overloaded_page = load_page (tree,position);

			pthread_rwlock_rdlock (&tree->tree_lock);
			page_lock = LOADED_LOCK(position);
			parent_lock = LOADED_LOCK(PARENT_ID(position));
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);
			assert (parent_lock != NULL);
		}else{
			position = 1;
			new_root(tree);
			parent = load_page (tree,0);

			pthread_rwlock_rdlock (&tree->tree_lock);
			page_lock = LOADED_LOCK(1);
			parent_lock = LOADED_LOCK(0);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);
			assert (parent_lock != NULL);

			insert_at_tail_of_queue (inception_queue,ULONG_MAX);
			do{
				uint64_t inception = remove_head_of_queue (inception_queue);
				if (inception != ULONG_MAX) {
					LOG (info,"[%s] TRANSPOSING PAGE IDENTIFIER %lu TO %lu.\n",tree->filename,inception,TRANSPOSE_PAGE_POSITION(inception));
					insert_at_tail_of_queue (inception_queue,TRANSPOSE_PAGE_POSITION(inception));
				}else break;
			}while (true);
		}
	}


	pthread_rwlock_rdlock (parent_lock);
	parent_records = parent->header.records;
	pthread_rwlock_unlock (parent_lock);

	uint32_t const lo_offset = CHILD_OFFSET(position);
	uint32_t const hi_offset = parent_records;

	uint64_t const lo_id = position;
	uint64_t const hi_id = CHILD_ID(PARENT_ID(position),hi_offset);

	index_t overlap = INDEX_T_MAX;
	lifo_t* lo_pages = new_stack();
	lifo_t* hi_pages = new_stack();
	priority_queue_t *lo_priority_queue = new_priority_queue(&mincompare_containers);
	priority_queue_t *hi_priority_queue = new_priority_queue(&mincompare_containers);


	pthread_rwlock_rdlock (page_lock);

	for (uint16_t j=0; j<tree->dimensions; ++j) {
		for (register uint32_t i=0; i<overloaded_page->header.records; ++i) {
			box_container_t *const box_container_lo = (box_container_t *const) malloc (sizeof(box_container_t));
			box_container_t *const box_container_hi = (box_container_t *const) malloc (sizeof(box_container_t));

			box_container_lo->box = box_container_hi->box = overloaded_page->node.internal.BOX(i);

			box_container_lo->sort_key = overloaded_page->node.internal.INTERVALS(i,j).start;
			box_container_hi->sort_key = overloaded_page->node.internal.INTERVALS(i,j).end;

			box_container_lo->id = box_container_hi->id = i;

			insert_into_priority_queue (lo_priority_queue,box_container_lo);
			insert_into_priority_queue (hi_priority_queue,box_container_hi);
		}


		lifo_t *const lo_lo_pages = new_stack();
		page_t *const lo_lo_page = new_internal(tree);
		index_t lo_hi_bound = ((box_container_t const*const)peek_priority_queue (lo_priority_queue))->box[j].end;
		while (lo_priority_queue->size > (overloaded_page->header.records>>1)) {
			box_container_t const*const top = (box_container_t const*const) remove_from_priority_queue (lo_priority_queue);
			memcpy(lo_lo_page->node.internal.BOX(lo_lo_page->header.records++),top->box,tree->dimensions*sizeof(interval_t));
			if (top->box[j].end > lo_hi_bound) lo_hi_bound = top->box[j].end;
			insert_into_stack (lo_lo_pages,top->id);
		}

		page_t *const lo_hi_page = new_internal(tree);
		lifo_t *const lo_hi_pages = new_stack();
		index_t lo_lo_bound = ((box_container_t const*const)peek_priority_queue (lo_priority_queue))->box[j].start;
		while (lo_priority_queue->size) {
			box_container_t const*const top = (box_container_t const*const) remove_from_priority_queue (lo_priority_queue);
			memcpy(lo_hi_page->node.internal.BOX(lo_hi_page->header.records++),top->box,tree->dimensions*sizeof(interval_t));
			if (top->box[j].start < lo_lo_bound) lo_lo_bound = top->box[j].end;
			insert_into_stack (lo_hi_pages,top->id);
		}

		index_t lo_overlap = lo_hi_bound > lo_lo_bound ? lo_hi_bound - lo_lo_bound : 0;

		page_t* hi_lo_page = new_internal(tree);
		lifo_t* hi_lo_pages = new_stack();
		index_t hi_hi_bound = ((box_container_t const*const)peek_priority_queue (hi_priority_queue))->box[j].start;
		while (hi_priority_queue->size > (overloaded_page->header.records>>1)) {
			box_container_t const*const top = (box_container_t const*const) remove_from_priority_queue (hi_priority_queue);
			memcpy (hi_lo_page->node.internal.BOX(hi_lo_page->header.records++),top->box,tree->dimensions*sizeof(interval_t));
			if (top->box[j].end > hi_hi_bound) hi_hi_bound = top->box[j].end;
			insert_into_stack (hi_lo_pages,top->id);
		}

		page_t* hi_hi_page = new_internal(tree);
		lifo_t* hi_hi_pages = new_stack();
		index_t hi_lo_bound = ((box_container_t const*const)peek_priority_queue (hi_priority_queue))->box[j].start;
		while (hi_priority_queue->size) {
			box_container_t const*const top = (box_container_t const*const) remove_from_priority_queue (hi_priority_queue);
			memcpy (hi_hi_page->node.internal.BOX(hi_hi_page->header.records++),top->box,tree->dimensions*sizeof(interval_t));
			if (top->box[j].start < hi_lo_bound) hi_lo_bound = top->box[j].end;
			insert_into_stack (hi_hi_pages,top->id);
		}

		index_t hi_overlap = hi_hi_bound > hi_lo_bound ? hi_hi_bound - hi_lo_bound : 0;

		if (hi_overlap < lo_overlap) {
			if (hi_overlap < overlap) {
				overlap = hi_overlap;

				lo_page = hi_lo_page;
				hi_page = hi_hi_page;

				lo_pages = hi_lo_pages;
				hi_pages = hi_hi_pages;
			}else{
				delete_rtree_page (hi_lo_page);
				delete_rtree_page (hi_hi_page);

				hi_lo_page = NULL;
				hi_hi_page = NULL;

				delete_stack (hi_lo_pages);
				delete_stack (hi_hi_pages);

				hi_lo_pages = NULL;
				hi_hi_pages = NULL;
			}
			delete_rtree_page (lo_lo_page);
			delete_rtree_page (lo_hi_page);
			delete_stack (lo_lo_pages);
			delete_stack (lo_hi_pages);
		}else{
			if (lo_overlap < overlap) {
				overlap = lo_overlap;

				lo_page = lo_lo_page;
				hi_page = lo_hi_page;

				lo_pages = lo_lo_pages;
				hi_pages = lo_hi_pages;
			}else{
				delete_rtree_page (lo_lo_page);
				delete_rtree_page (lo_hi_page);
				delete_stack (lo_lo_pages);
				delete_stack (lo_hi_pages);
			}
			delete_rtree_page (hi_lo_page);
			delete_rtree_page (hi_hi_page);

			hi_lo_page = NULL;
			hi_hi_page = NULL;

			delete_stack (hi_lo_pages);
			delete_stack (hi_hi_pages);

			hi_lo_pages = NULL;
			hi_hi_pages = NULL;
		}
	}

	delete_priority_queue (lo_priority_queue);
	delete_priority_queue (hi_priority_queue);

	boolean update_flag = false;

	uint64_t old_child=0, new_child=0, new_id=0;
	insert_at_head_of_queue (inception_queue,ULONG_MAX);
	uint64_t inception = remove_tail_of_queue (inception_queue);

	fifo_t* transposed_ids = new_queue();

	assert (lo_page->header.records == lo_pages->size);
	for (register uint32_t i=lo_page->header.records-1; lo_pages->size; --i) {
		uint64_t const offset = remove_from_stack (lo_pages);
		page_t *const page = load_page (tree,CHILD_ID(position,offset));
		if (page != NULL) {
			new_child = new_id = CHILD_ID(lo_id,i);
			old_child = CHILD_ID(position,offset);
			LOG (info,"[%s] Node with id %lu (compared against %lu) is now under %lu with new id %lu.\n",tree->filename,old_child,inception,lo_id,new_id);
			if (!update_flag && inception == old_child) {
				insert_at_head_of_queue (inception_queue,new_id);
				do{
					inception = remove_tail_of_queue (inception_queue);
					if (inception != ULONG_MAX) {
						new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
						insert_at_head_of_queue (inception_queue,new_id);
					}else break;
				}while (true);
				update_flag = true;
			}
			fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
			assert (!tmp_queue->head && !transposed_ids->head);
			uint64_t new_size = transposed_ids->size + tmp_queue->size;
			while (new_size > transposed_ids->capacity) {
				expand_queue (transposed_ids);
			}

			memcpy (transposed_ids->buffer+transposed_ids->tail,
				tmp_queue->buffer,
				tmp_queue->size*sizeof(value_t));

			transposed_ids->tail = transposed_ids->size = new_size;
			delete_queue (tmp_queue);
		}else{
			LOG (error,"[%s] Unable to retrieve heapfile entry #%lu...\n",tree->filename,CHILD_ID(position,offset));
			exit (EXIT_FAILURE);
		}
	}

	uint64_t const new_position = update_flag ? position : hi_id;

	assert (hi_page->header.records == hi_pages->size);
	for (register uint32_t i=hi_page->header.records-1; hi_pages->size; --i) {
		uint64_t const offset = remove_from_stack (hi_pages);
		page_t *const page = load_page (tree,CHILD_ID(position,offset));
		if (page != NULL) {
			new_child = new_id = CHILD_ID(hi_id,i);
			old_child = CHILD_ID(position,offset);
			LOG (info,"[%s] Node with id %lu (compared against %lu) is now under %lu with new id %lu.\n",tree->filename,old_child,inception,hi_id,new_id);
			if (!update_flag && inception == old_child) {
				insert_at_head_of_queue (inception_queue,new_id);
				do{
					inception = remove_tail_of_queue (inception_queue);
					if (inception != ULONG_MAX) {
						new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
						insert_at_head_of_queue (inception_queue,new_id);
					}else break;
				}while (true);
				update_flag = true;
			}
			fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
			assert (!tmp_queue->head && !transposed_ids->head);
			uint64_t new_size = transposed_ids->size + tmp_queue->size;
			while (new_size > transposed_ids->capacity) {
				expand_queue (transposed_ids);
			}

			memcpy (transposed_ids->buffer+transposed_ids->tail,
				tmp_queue->buffer,
				tmp_queue->size*sizeof(value_t));

			transposed_ids->tail = transposed_ids->size = new_size;
			delete_queue (tmp_queue);
		}else{
			LOG (error,"[%s] Unable to retrieve heapfile entry #%lu...\n",tree->filename,CHILD_ID(position,offset));
			exit (EXIT_FAILURE);
		}
	}

	delete_stack (lo_pages);
	delete_stack (hi_pages);

	uint64_t swapped = ULONG_MAX;
	boolean safety_precaution = unset_priority (tree->swap,position);

	priority_queue_t *const sorted_pages = new_priority_queue (&mincompare_symbol_table_entries);
	while (transposed_ids->size) {
		insert_into_priority_queue (sorted_pages,remove_head_of_queue (transposed_ids));
	}

	while (sorted_pages->size) {
		symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_from_priority_queue (sorted_pages);

		if (dump_transposed_pages) {
			low_level_write_of_page_to_disk (tree,entry->value,entry->key);
			if (((page_t const*const)entry->value)->header.is_leaf) {
				delete_rtree_page (entry->value);
			}else{
				delete_rtree_page (entry->value);
			}
		}else{
			swapped = set_priority (tree->swap,entry->key,compute_page_priority(tree,entry->key));
			assert (is_active_identifier (tree->swap,entry->key));
			assert (swapped != entry->key);
			assert (swapped != lo_id);
			assert (swapped != hi_id);
			if (swapped != ULONG_MAX) {
				LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,entry->key);
				assert (LOADED_PAGE(swapped) != NULL);
				if (flush_page (tree,swapped) != swapped) {
					LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
					exit (EXIT_FAILURE);
				}
			}

			pthread_rwlock_wrlock (&tree->tree_lock);
			SET_PAGE(entry->key,entry->value);

			pthread_rwlock_t *const entry_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
			pthread_rwlock_init (entry_lock,NULL);
			SET_LOCK(entry->key,entry_lock);
			pthread_rwlock_unlock (&tree->tree_lock);
		}
		free (entry);
	}

	delete_queue (transposed_ids);
	delete_priority_queue (sorted_pages);
	pthread_rwlock_unlock (page_lock);

	pthread_rwlock_wrlock (page_lock);
	pthread_rwlock_wrlock (&tree->tree_lock);
	SET_PAGE(lo_id,lo_page);
	SET_PAGE(hi_id,hi_page);
	pthread_rwlock_t *const hi_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
	pthread_rwlock_init (hi_lock,NULL);
	SET_LOCK(hi_id,hi_lock);
	tree->is_dirty = true;
	tree->tree_size++;

	swapped = set_priority (tree->swap,lo_id,compute_page_priority(tree,lo_id));
	assert (is_active_identifier (tree->swap,lo_id));
	assert (swapped != lo_id);
	pthread_rwlock_unlock (&tree->tree_lock);

	if (swapped != ULONG_MAX) {
		LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,lo_id);
		if (flush_page (tree,swapped) != swapped) {
			LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
			exit (EXIT_FAILURE);
		}
	}

	pthread_rwlock_wrlock (&tree->tree_lock);
	swapped = set_priority (tree->swap,hi_id,compute_page_priority(tree,hi_id));
	assert (is_active_identifier (tree->swap,hi_id));
	assert (swapped != hi_id);
	pthread_rwlock_unlock (&tree->tree_lock);

	if (swapped != ULONG_MAX) {
		LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,hi_id);
		assert (LOADED_PAGE(swapped) != NULL);
		if (flush_page (tree,swapped) != swapped) {
			LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
			exit (EXIT_FAILURE);
		}
	}

	LOG (info,"[%s] Node with id %lu is now under %lu.\n",tree->filename,lo_id,PARENT_ID(lo_id));
	LOG (info,"[%s] Node with id %lu is now under %lu.\n",tree->filename,hi_id,PARENT_ID(hi_id));
	LOG (info,"[%s] Halved internal node with new identifier %lu and sibling new node "
			"%lu having %u records into two parts of %u and %u records.\n",tree->filename,
			new_position, hi_id, overloaded_page->header.records,
			lo_page->header.records, hi_page->header.records);

	assert (overloaded_page->header.records == lo_page->header.records + hi_page->header.records);
	assert (update_flag);

	lo_page->header.is_dirty = true;
	hi_page->header.is_dirty = true;
	pthread_rwlock_unlock (page_lock);

	pthread_rwlock_wrlock (parent_lock);
	for (register uint32_t i=0; i<lo_page->header.records; ++i) {
		if (i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if (lo_page->node.internal.INTERVALS(i,j).start
						< parent->node.internal.INTERVALS(lo_offset,j).start)
				parent->node.internal.INTERVALS(lo_offset,j).start
						= lo_page->node.internal.INTERVALS(i,j).start;
				if (lo_page->node.internal.INTERVALS(i,j).end
						> parent->node.internal.INTERVALS(lo_offset,j).end)
				parent->node.internal.INTERVALS(lo_offset,j).end
						= lo_page->node.internal.INTERVALS(i,j).end;
			}
		}else{
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				parent->node.internal.INTERVALS(lo_offset,j).start
						= lo_page->node.internal.INTERVALS(i,j).start;
				parent->node.internal.INTERVALS(lo_offset,j).end
						= lo_page->node.internal.INTERVALS(i,j).end;
			}
		}
	}

	for (register uint32_t i=0; i<hi_page->header.records; ++i) {
		if (i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if (hi_page->node.internal.INTERVALS(i,j).start
						< parent->node.internal.INTERVALS(hi_offset,j).start)
				parent->node.internal.INTERVALS(hi_offset,j).start
						= hi_page->node.internal.INTERVALS(i,j).start;
				if (hi_page->node.internal.INTERVALS(i,j).end
						> parent->node.internal.INTERVALS(hi_offset,j).end)
				parent->node.internal.INTERVALS(hi_offset,j).end
						= hi_page->node.internal.INTERVALS(i,j).end;
			}
		}else{
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				parent->node.internal.INTERVALS(hi_offset,j).start
					= hi_page->node.internal.INTERVALS(i,j).start;
				parent->node.internal.INTERVALS(hi_offset,j).end
					= hi_page->node.internal.INTERVALS(i,j).end;
			}
		}
	}

	if (verbose_splits) {
		print_box(false,tree,parent->node.internal.BOX(lo_offset));
		print_box(false,tree,parent->node.internal.BOX(hi_offset));
		puts ("==============================================================");
	}
	parent->header.records++;

	pthread_rwlock_unlock (parent_lock);

	if (dump_transposed_pages) {
		uint64_t const parent_id = PARENT_ID(new_position);
		if (LOADED_PAGE(parent_id)!=NULL) {
			if (flush_page (tree,parent_id) != parent_id) {
				LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,parent_id);
				exit (EXIT_FAILURE);
			}
		}
		if (flush_page (tree,lo_id) != lo_id) {
			LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,lo_id);
			exit (EXIT_FAILURE);
		}
		if (flush_page (tree,hi_id) != hi_id) {
			LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,hi_id);
			exit (EXIT_FAILURE);
		}
	}

	LOG (info,"[%s] DONE HALVING INTERNAL NODE AT POSITION %lu WITH NEW ID %lu.\n",tree->filename,position,new_position);
	delete_rtree_page (overloaded_page);
	return new_position;
}

static uint64_t split_internal (tree_t *const tree, uint64_t position, fifo_t *const inception_queue) {
	if (verbose_splits) {
		puts ("==============================================================");
		LOG (info,"[%s] SPLITTING INTERNAL NODE AT POSITION: %lu\n",tree->filename,position);
	}

	page_t* overloaded_page = load_page (tree,position);
	page_t* parent = load_page (tree,PARENT_ID(position));
	assert (parent != NULL);

	pthread_rwlock_rdlock (&tree->tree_lock);
	pthread_rwlock_t* page_lock = LOADED_LOCK(position);
	pthread_rwlock_t* parent_lock = LOADED_LOCK(PARENT_ID(position));
	pthread_rwlock_unlock (&tree->tree_lock);

	assert (page_lock != NULL);
	assert (parent_lock != NULL);

	if (verbose_splits) {
		print_box(false,tree,parent->node.internal.BOX(CHILD_OFFSET(position)));
	}

	pthread_rwlock_rdlock (parent_lock);
	uint32_t parent_records = parent->header.records;
	pthread_rwlock_unlock (parent_lock);

	/** take care of cascading splits here **/
	if (parent_records >= tree->internal_entries) {
		if (position) {
			insert_at_tail_of_queue (inception_queue,position);
			uint64_t const parent_id = split_internal (tree,PARENT_ID(position),inception_queue);
			position = remove_tail_of_queue (inception_queue);

			parent = load_page (tree,parent_id);
			overloaded_page = load_page (tree,position);

			pthread_rwlock_rdlock (&tree->tree_lock);
			page_lock = LOADED_LOCK(position);
			parent_lock = LOADED_LOCK(parent_id);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);
			assert (parent_lock != NULL);
		}else{
			position = 1;
			new_root(tree);
			parent = load_page (tree,0);

			pthread_rwlock_rdlock (&tree->tree_lock);
			parent_lock = LOADED_LOCK(0);
			page_lock = LOADED_LOCK(1);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);
			assert (parent_lock != NULL);

			insert_at_tail_of_queue (inception_queue,ULONG_MAX);
			do{
				uint64_t inception = remove_head_of_queue (inception_queue);
				if (inception != ULONG_MAX) {
					uint64_t transposed_inception = TRANSPOSE_PAGE_POSITION(inception);
					LOG (info,"[%s] TRANSPOSING PAGE IDENTIFIER %lu TO %lu.\n",tree->filename,inception,transposed_inception);
					insert_at_tail_of_queue (inception_queue,transposed_inception);
				}else break;
			}while (true);
		}
	}

	pthread_rwlock_rdlock (page_lock);

	float fairness = 0;
	uint32_t splitdim = 0;
	interval_t splitzone = {0,0};
	priority_queue_t  *const priority_queue = new_priority_queue (&mincompare_containers);
	for (register uint16_t j=0; j<tree->dimensions; ++j) {
		for (register uint32_t i=0; i<overloaded_page->header.records; ++i) {
			box_container_t *const box_container_lo = (box_container_t *const) malloc (sizeof(box_container_t));
			box_container_t *const box_container_hi = (box_container_t *const) malloc (sizeof(box_container_t));
			if (box_container_lo == NULL || box_container_hi == NULL) {
				LOG (error,"[%s] Unable to reserve additional memory to split page %lu...\n",tree->filename,position);
				exit (EXIT_FAILURE);
			}

			box_container_lo->id = i;
			box_container_hi->id = i;

			box_container_lo->box = overloaded_page->node.internal.BOX(i);
			box_container_hi->box = overloaded_page->node.internal.BOX(i);

			box_container_lo->sort_key = overloaded_page->node.internal.INTERVALS(i,j).start;
			box_container_hi->sort_key = overloaded_page->node.internal.INTERVALS(i,j).end;

			insert_into_priority_queue (priority_queue,box_container_lo);
			insert_into_priority_queue (priority_queue,box_container_hi);
		}
		assert (priority_queue->size == tree->internal_entries<<1);

		float jfairness = 0;
		interval_t jzone = {0,0};

		interval_t zone = ((box_container_t const*const)peek_priority_queue(priority_queue))->box[j];

		uint32_t lo_records = 0;
		uint32_t hi_records = overloaded_page->header.records;

		for (uint32_t insertions=0,deletions=0; priority_queue->size;) {
			box_container_t *const box_container = (box_container_t *const) remove_from_priority_queue (priority_queue);

			if (box_container->sort_key == box_container->box[j].start) {
				++insertions;
				if (lo_records >= fairness_threshold*(tree->internal_entries>>1)
				 && hi_records >= fairness_threshold*(tree->internal_entries>>1)) {

					float newfairness = ((lo_records + hi_records)*(lo_records + hi_records)>>1)
										/ (float)(lo_records*lo_records + hi_records*hi_records);

					if (newfairness > jfairness) {
						jfairness = newfairness;
						zone.end = box_container->sort_key;
						jzone = zone;
					}
				}
			}else if (box_container->sort_key == box_container->box[j].end) {
				++deletions;
				++lo_records;

				zone.start = box_container->sort_key;
				hi_records = overloaded_page->header.records - lo_records;
			}else{
				LOG (error,"[%s] Invalid dimension %u interval encountered (%12lf,%12lf)...\n",tree->filename,
					j,(double)box_container->box[j].start,(double)box_container->box[j].end);
				exit (EXIT_FAILURE);
			}

			free (box_container);
		}

		assert (lo_records + hi_records == overloaded_page->header.records);

		if (jfairness > fairness) {
			fairness = jfairness;
			splitzone = jzone;
			splitdim = j;
		}
		clear_priority_queue (priority_queue);
	}
	delete_priority_queue (priority_queue);

	LOG (info,"[%s] Selected split-zone is (%12lf,%12lf) along dimension %u achieving fairness: %f.\n",tree->filename,
						(double)splitzone.start,(double)splitzone.end,splitdim,fairness);

	pthread_rwlock_unlock (page_lock);
	if (fairness >= fairness_threshold) {
		pthread_rwlock_wrlock (page_lock);

		uint64_t new_position = position;
		uint64_t const lo_id = position;

		pthread_rwlock_rdlock (parent_lock);
		parent_records = parent->header.records;
		pthread_rwlock_unlock (parent_lock);

		uint64_t const hi_id = CHILD_ID(PARENT_ID(position),parent_records);
		uint64_t old_child = 0, new_child = 0, new_id = 0;

		fifo_t* transposed_ids = new_queue();

		insert_at_head_of_queue (inception_queue,ULONG_MAX);
		uint64_t inception = remove_tail_of_queue (inception_queue);

		boolean update_flag = false;

		page_t* lo_page = new_internal(tree);
		page_t* hi_page = new_internal(tree);

		priority_queue_t* lo_overlap = new_priority_queue(&maxcompare_containers);
		priority_queue_t* hi_overlap = new_priority_queue(&maxcompare_containers);

		for (register uint32_t i=0; i<overloaded_page->header.records; ++i) {
			interval_t* box_ptr = overloaded_page->node.internal.BOX(i);

			old_child = CHILD_ID(position,i);
			if (box_ptr[splitdim].end <= splitzone.start) {
				new_child = new_id = CHILD_ID(lo_id,lo_page->header.records);
				memcpy(lo_page->node.internal.BOX(lo_page->header.records++),box_ptr,tree->dimensions*sizeof(interval_t));
				LOG (info,"[%s] Node with id %lu is now under %lu with new id %lu.\n",tree->filename,old_child,lo_id,new_id);
				if (!update_flag && inception == old_child) {
					insert_at_head_of_queue (inception_queue,new_id);
					do{
						inception = remove_tail_of_queue (inception_queue);
						if (inception != ULONG_MAX) {
							new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
							insert_at_head_of_queue (inception_queue,new_id);
						}else break;
					}while (true);
					update_flag = true;
				}
				fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
				assert (!tmp_queue->head && !transposed_ids->head);
				uint64_t new_size = transposed_ids->size + tmp_queue->size;
				while (new_size > transposed_ids->capacity) {
					expand_queue (transposed_ids);
				}

				memcpy (transposed_ids->buffer+transposed_ids->tail,
					tmp_queue->buffer,
					tmp_queue->size*sizeof(value_t));

				transposed_ids->tail = transposed_ids->size = new_size;
				delete_queue (tmp_queue);
			}else if (box_ptr[splitdim].start >= splitzone.end) {
				new_child = new_id = CHILD_ID(hi_id,hi_page->header.records);
				memcpy(hi_page->node.internal.BOX(hi_page->header.records++),box_ptr,tree->dimensions*sizeof(interval_t));
				LOG (info,"[%s] Node with id %lu is now under %lu with new id %lu.\n",tree->filename,old_child,hi_id,new_id);
				if (!update_flag && inception == old_child) {
					insert_at_head_of_queue (inception_queue,new_id);
					do{
						inception = remove_tail_of_queue (inception_queue);
						if (inception != ULONG_MAX) {
							new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
							insert_at_head_of_queue (inception_queue,new_id);
						}else break;
					}while (true);
					update_flag = true;
					new_position = hi_id;
				}
				fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
				assert (!tmp_queue->head && !transposed_ids->head);
				uint64_t new_size = transposed_ids->size + tmp_queue->size;
				while (new_size > transposed_ids->capacity) {
					expand_queue (transposed_ids);
				}

				memcpy (transposed_ids->buffer+transposed_ids->tail,
					tmp_queue->buffer,
					tmp_queue->size*sizeof(value_t));
				transposed_ids->tail = transposed_ids->size = new_size;
				delete_queue (tmp_queue);
			}else if (box_ptr[splitdim].end - splitzone.start <= splitzone.end - box_ptr[splitdim].start) {
				box_container_t *const box_container = (box_container_t *const) malloc (sizeof(box_container_t));

				box_container->sort_key = box_ptr[splitdim].end - splitzone.start;
				box_container->box = overloaded_page->node.internal.BOX(i);
				box_container->id = i;

				insert_into_priority_queue (lo_overlap,box_container);
			}else if (splitzone.end - box_ptr[splitdim].start <= box_ptr[splitdim].end - splitzone.start) {
				box_container_t *const box_container = (box_container_t *const) malloc (sizeof(box_container_t));

				box_container->sort_key = splitzone.end - box_ptr[splitdim].start;
				box_container->box = overloaded_page->node.internal.BOX(i);
				box_container->id = i;

				insert_into_priority_queue (hi_overlap,box_container);
			}else{
				LOG (error,"[%s] %u \n",tree->filename,box_ptr[splitdim].start >= splitzone.start && box_ptr[splitdim].end <= splitzone.end);
				LOG (error,"[%s] %u \n",tree->filename,box_ptr[splitdim].start < splitzone.start);
				LOG (error,"[%s] %u \n",tree->filename,box_ptr[splitdim].end > splitzone.end);
				LOG (error,"[%s] Unclaimed page #%lu while moving it from page #%lu...\n",tree->filename,old_child,position);
				exit (EXIT_FAILURE);
			}
		}


		if (lo_overlap->size || hi_overlap->size) {
			while (hi_page->header.records+hi_overlap->size < fairness_threshold*(tree->internal_entries>>1)) {
				box_container_t *const box_container =  (box_container_t *const) remove_from_priority_queue (lo_overlap->size?lo_overlap:hi_overlap);

				old_child = CHILD_ID(position,box_container->id);
				interval_t* box_ptr = box_container->box;

				free (box_container);

				new_child = new_id = CHILD_ID(hi_id,hi_page->header.records);
				memcpy(hi_page->node.internal.BOX(hi_page->header.records++),box_ptr,tree->dimensions*sizeof(interval_t));
				LOG (info,"[%s] Node with id %lu is now under %lu with new id %lu.\n",tree->filename,old_child,hi_id,new_id);
				if (!update_flag && inception == old_child) {
					insert_at_head_of_queue (inception_queue,new_id);
					do{
						inception = remove_tail_of_queue (inception_queue);
						if (inception != ULONG_MAX) {
							new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
							insert_at_head_of_queue (inception_queue,new_id);
						}else break;
					}while (true);
					update_flag = true;
					new_position = hi_id;
				}
				fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
				assert (!tmp_queue->head && !transposed_ids->head);
				uint64_t new_size = transposed_ids->size + tmp_queue->size;
				while (new_size > transposed_ids->capacity) {
					expand_queue (transposed_ids);
				}

				memcpy (transposed_ids->buffer+transposed_ids->tail,
					tmp_queue->buffer,
					tmp_queue->size*sizeof(value_t));

				transposed_ids->tail = transposed_ids->size = new_size;
				delete_queue (tmp_queue);
			}

			while (lo_page->header.records+lo_overlap->size < fairness_threshold*(tree->internal_entries>>1)) {
				
				box_container_t *const box_container =  (box_container_t *const) remove_from_priority_queue (hi_overlap->size?hi_overlap:lo_overlap);

				old_child = CHILD_ID(position,box_container->id);
				interval_t* box_ptr = box_container->box;

				free (box_container);

				new_child = new_id = CHILD_ID(lo_id,lo_page->header.records);
				memcpy(lo_page->node.internal.BOX(lo_page->header.records++),box_ptr,tree->dimensions*sizeof(interval_t));
				LOG (info,"[%s] Node with id %lu is now under %lu with new id %lu.\n",tree->filename,old_child,lo_id,new_id);
				if (!update_flag && inception == old_child) {
					insert_at_head_of_queue (inception_queue,new_id);
					do{
						inception = remove_tail_of_queue (inception_queue);
						if (inception != ULONG_MAX) {
							new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
							insert_at_head_of_queue (inception_queue,new_id);
						}else break;
					}while (true);
					update_flag = true;
				}
				fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
				assert (!tmp_queue->head && !transposed_ids->head);
				uint64_t new_size = transposed_ids->size + tmp_queue->size;
				while (new_size > transposed_ids->capacity) {
					expand_queue (transposed_ids);
				}

				memcpy (transposed_ids->buffer+transposed_ids->tail,
					tmp_queue->buffer,
					tmp_queue->size*sizeof(value_t));

				transposed_ids->tail = transposed_ids->size = new_size;
				delete_queue (tmp_queue);
			}
		}

		while (lo_overlap->size) {
			box_container_t *const box_container = (box_container_t *const) remove_from_priority_queue (lo_overlap);

			old_child = CHILD_ID(position,box_container->id);
			interval_t* box_ptr = box_container->box;

			free (box_container);

			new_child = new_id = CHILD_ID(lo_id,lo_page->header.records);
			memcpy(lo_page->node.internal.BOX(lo_page->header.records++),box_ptr,tree->dimensions*sizeof(interval_t));
			LOG (info,"[%s] Node with id %lu is now under %lu with new id %lu.\n",tree->filename,old_child,lo_id,new_id);
			if (!update_flag && inception == old_child) {
				insert_at_head_of_queue (inception_queue,new_id);
				do{
					inception = remove_tail_of_queue (inception_queue);
					if (inception != ULONG_MAX) {
						new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
						insert_at_head_of_queue (inception_queue,new_id);
					}else break;
				}while (true);
				update_flag = true;
			}
			fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
			assert (!tmp_queue->head && !transposed_ids->head);
			uint64_t new_size = transposed_ids->size + tmp_queue->size;
			while (new_size > transposed_ids->capacity) {
				expand_queue (transposed_ids);
			}

			memcpy (transposed_ids->buffer+transposed_ids->tail,
				tmp_queue->buffer,
				tmp_queue->size*sizeof(value_t));

			transposed_ids->tail = transposed_ids->size = new_size;
			delete_queue (tmp_queue);
		}

		while (hi_overlap->size) {
			box_container_t *const box_container = (box_container_t *const) remove_from_priority_queue (hi_overlap);

			old_child = CHILD_ID(position,box_container->id);
			interval_t* box_ptr = box_container->box;

			free (box_container);

			new_child = new_id = CHILD_ID(hi_id,hi_page->header.records);
			memcpy(hi_page->node.internal.BOX(hi_page->header.records++),box_ptr,tree->dimensions*sizeof(interval_t));
			LOG (info,"[%s] Node with id %lu is now under %lu with new id %lu.\n",tree->filename,old_child,hi_id,new_id);
			if (!update_flag && inception == old_child) {
				insert_at_head_of_queue (inception_queue,new_id);
				do{
					inception = remove_tail_of_queue (inception_queue);
					if (inception != ULONG_MAX) {
						new_id = CHILD_ID(new_id,CHILD_OFFSET(inception));
						insert_at_head_of_queue (inception_queue,new_id);
					}else break;
				}while (true);
				update_flag = true;
				new_position = hi_id;
			}
			fifo_t* tmp_queue = transpose_subsumed_pages (tree,old_child,new_child);
			assert (!tmp_queue->head && !transposed_ids->head);
			uint64_t new_size = transposed_ids->size + tmp_queue->size;
			while (new_size > transposed_ids->capacity) {
				expand_queue (transposed_ids);
			}

			memcpy (transposed_ids->buffer+transposed_ids->tail,
				tmp_queue->buffer,
				tmp_queue->size*sizeof(value_t));

			transposed_ids->tail = transposed_ids->size = new_size;
			delete_queue (tmp_queue);
		}

		uint64_t swapped = ULONG_MAX;
		boolean safety_precaution = unset_priority (tree->swap,position);

		priority_queue_t *const sorted_pages = new_priority_queue (&mincompare_symbol_table_entries);
		while (transposed_ids->size) {
			insert_into_priority_queue (sorted_pages,remove_head_of_queue (transposed_ids));
		}

		while (sorted_pages->size) {
			symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_from_priority_queue (sorted_pages);

			if (dump_transposed_pages) {
				low_level_write_of_page_to_disk (tree,entry->value,entry->key);
				if (((page_t const*const)entry->value)->header.is_leaf) {
					delete_rtree_page (entry->value);
				}else{
					delete_rtree_page (entry->value);
				}
			}else{
				pthread_rwlock_rdlock (&tree->tree_lock);
				swapped = set_priority (tree->swap,entry->key,compute_page_priority(tree,entry->key));
				assert (is_active_identifier (tree->swap,entry->key));
				pthread_rwlock_unlock (&tree->tree_lock);

				assert (swapped != entry->key);
				assert (swapped != lo_id);
				assert (swapped != hi_id);
				if (swapped != ULONG_MAX) {
					LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,entry->key);
					if (flush_page (tree,swapped) != swapped) {
						LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
						exit (EXIT_FAILURE);
					}
				}

				pthread_rwlock_wrlock (&tree->tree_lock);
				SET_PAGE(entry->key,entry->value);

				pthread_rwlock_t *const entry_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
				pthread_rwlock_init (entry_lock,NULL);
				SET_LOCK(entry->key,entry_lock);
				pthread_rwlock_unlock (&tree->tree_lock);
			}
			free (entry);
		}

		pthread_rwlock_unlock (page_lock);
		pthread_rwlock_wrlock (page_lock);

		pthread_rwlock_wrlock (&tree->tree_lock);
		SET_PAGE(lo_id,lo_page);
		SET_PAGE(hi_id,hi_page);

		pthread_rwlock_t *const hi_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
		pthread_rwlock_init (hi_lock,NULL);
		SET_LOCK(hi_id,hi_lock);

		tree->is_dirty = true;
		tree->tree_size++;

		swapped = set_priority (tree->swap,lo_id,compute_page_priority(tree,lo_id));
		assert (is_active_identifier (tree->swap,lo_id));
		assert (swapped != lo_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		if (swapped != ULONG_MAX) {
			LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,lo_id);
			if (flush_page (tree,swapped) != swapped) {
				LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
				exit (EXIT_FAILURE);
			}
		}

		pthread_rwlock_wrlock (&tree->tree_lock);
		swapped = set_priority (tree->swap,hi_id,compute_page_priority(tree,hi_id));
		assert (is_active_identifier (tree->swap,hi_id));
		assert (swapped != lo_id);
		assert (swapped != hi_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		if (swapped != ULONG_MAX) {
			LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,hi_id);
			assert (LOADED_PAGE(swapped) != NULL);
			if (flush_page (tree,swapped) != swapped) {
				LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
				exit (EXIT_FAILURE);
			}
		}

		lo_page->header.is_dirty = true;
		hi_page->header.is_dirty = true;

		pthread_rwlock_unlock (page_lock);

		delete_queue (transposed_ids);
		delete_priority_queue (sorted_pages);

		LOG (info,"[%s] Node with id %lu is now under %lu.\n",tree->filename,lo_id,PARENT_ID(lo_id));
		LOG (info,"[%s] Node with id %lu is now under %lu.\n",tree->filename,hi_id,PARENT_ID(hi_id));
		LOG (info,"[%s] Split by dimension %u leaf node with new identifier %lu and sibling new node "
				"%lu having %u records into two parts of %u and %u records.\n",tree->filename,
				splitdim, position, hi_id, overloaded_page->header.records,
				lo_page->header.records, hi_page->header.records);

		assert (overloaded_page->header.records == lo_page->header.records + hi_page->header.records);
		assert (lo_page->header.records >= fairness_threshold*(tree->internal_entries>>1));
		assert (hi_page->header.records >= fairness_threshold*(tree->internal_entries>>1));
		assert (update_flag);

		uint32_t const lo_offset = CHILD_OFFSET(position);
		uint32_t const hi_offset = parent_records;

		pthread_rwlock_wrlock (parent_lock);
		memcpy(parent->node.internal.BOX(lo_offset),lo_page->node.internal.intervals,tree->dimensions*sizeof(interval_t));
		for (register uint32_t i=1; i<lo_page->header.records; ++i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if (parent->node.internal.INTERVALS(lo_offset,j).start > lo_page->node.internal.INTERVALS(i,j).start)
					parent->node.internal.INTERVALS(lo_offset,j).start = lo_page->node.internal.INTERVALS(i,j).start;
				if (parent->node.internal.INTERVALS(lo_offset,j).end < lo_page->node.internal.INTERVALS(i,j).end)
					parent->node.internal.INTERVALS(lo_offset,j).end = lo_page->node.internal.INTERVALS(i,j).end;
			}
		}

		memcpy(parent->node.internal.BOX(hi_offset),hi_page->node.internal.intervals,tree->dimensions*sizeof(interval_t));
		for (register uint32_t i=1; i<hi_page->header.records; ++i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if (parent->node.internal.INTERVALS(hi_offset,j).start > hi_page->node.internal.INTERVALS(i,j).start)
					parent->node.internal.INTERVALS(hi_offset,j).start = hi_page->node.internal.INTERVALS(i,j).start;
				if (parent->node.internal.INTERVALS(hi_offset,j).end < hi_page->node.internal.INTERVALS(i,j).end)
					parent->node.internal.INTERVALS(hi_offset,j).end = hi_page->node.internal.INTERVALS(i,j).end;
			}
		}

		parent->header.records++;

		if (verbose_splits) {
			print_box(false,tree,parent->node.internal.BOX(lo_offset));
			print_box(false,tree,parent->node.internal.BOX(hi_offset));
			puts ("==============================================================");
		}
		pthread_rwlock_unlock (parent_lock);

		if (dump_transposed_pages) {
			uint64_t const parent_id = PARENT_ID(new_position);
			if (LOADED_PAGE(parent_id)!=NULL) {
				if (flush_page (tree,parent_id) != parent_id) {
					LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,parent_id);
					exit (EXIT_FAILURE);
				}
			}
			if (flush_page (tree,lo_id) != lo_id) {
				LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,lo_id);
				exit (EXIT_FAILURE);
			}
			if (flush_page (tree,hi_id) != hi_id) {
				LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,hi_id);
				exit (EXIT_FAILURE);
			}
		}

		LOG (info,"[%s] DONE SPLITTING INTERNAL NODE AT POSITION %lu WITH NEW ID %lu.\n",tree->filename,position,new_position);
		delete_rtree_page (overloaded_page);
		return new_position;
	}else{
		return halve_internal (tree,position,inception_queue);
	}
}

static uint64_t split_leaf (tree_t *const tree, uint64_t position) {
	LOG (info,"[%s] SPLITTING LEAF NODE AT POSITION: %lu\n",tree->filename,position);

	page_t* overloaded_page = load_page (tree,position);
	page_t* parent = load_page (tree,PARENT_ID(position));

	pthread_rwlock_rdlock (&tree->tree_lock);
	pthread_rwlock_t* page_lock = LOADED_LOCK(position);
	pthread_rwlock_t* parent_lock = LOADED_LOCK(PARENT_ID(position));
	pthread_rwlock_unlock (&tree->tree_lock);

	assert (page_lock != NULL);
	assert (parent_lock != NULL);

	if (overloaded_page == NULL) {
		LOG (error,"[%s] Unable to split node corresponding to an invalid page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}
	if (!overloaded_page->header.is_leaf) {
		LOG (error,"[%s] Splitting should start from a leaf-node...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	assert (parent != NULL);

	if (position) {
		assert (page_lock != parent_lock);

		pthread_rwlock_rdlock (parent_lock);
		uint32_t parent_records = parent->header.records;
		pthread_rwlock_unlock (parent_lock);

		if (parent_records >= tree->internal_entries) {
			fifo_t* inception_queue = new_queue();
			insert_at_tail_of_queue (inception_queue,position);
			uint64_t const parent_id = split_internal (tree,PARENT_ID(position),inception_queue);
			position = remove_tail_of_queue (inception_queue);

			assert (!inception_queue->size);
			delete_queue (inception_queue);

			assert (PARENT_ID(position) == parent_id);

			parent = load_page (tree,parent_id);
			overloaded_page = load_page (tree,position);

			pthread_rwlock_rdlock (&tree->tree_lock);
			page_lock = LOADED_LOCK(position);
			parent_lock = LOADED_LOCK(PARENT_ID(position));
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);
			assert (parent_lock != NULL);

			assert (overloaded_page != NULL);
			assert (overloaded_page->header.records == tree->leaf_entries);
		}
	}else{
		parent = new_internal(tree);

		parent->header.records = 1;

		pthread_rwlock_wrlock (&tree->tree_lock);

		SET_PAGE(1,UNSET_PAGE(0));
		SET_PAGE(0,parent);

		SET_LOCK(1,UNSET_LOCK(0));

		parent_lock = (pthread_rwlock_t*) malloc (sizeof(pthread_rwlock_t));
		pthread_rwlock_init (parent_lock,NULL);
		SET_LOCK(0,parent_lock);

		pthread_rwlock_unlock (&tree->tree_lock);

		position = 1;

		update_rootbox(tree);

		pthread_rwlock_wrlock (parent_lock);
		pthread_rwlock_rdlock (&tree->tree_lock);
		memcpy(parent->node.internal.intervals,tree->root_box,tree->dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&tree->tree_lock);
		pthread_rwlock_unlock (parent_lock);
	}

	uint32_t const lo_offset = CHILD_OFFSET(position);

	pthread_rwlock_rdlock (parent_lock);

	if (verbose_splits) {
		puts ("--------------------------------------------------------------");
		print_box(false,tree,parent->node.internal.BOX(lo_offset));
	}

	uint32_t const hi_offset = parent->header.records;
	pthread_rwlock_unlock (parent_lock);

	uint64_t const hi_id = CHILD_ID(PARENT_ID(position),hi_offset);

	page_t *const lo_page = new_leaf(tree);
	page_t *const hi_page = new_leaf(tree);

	pthread_rwlock_rdlock (parent_lock);
	uint32_t splitdim = 0;
	index_t spread = parent->node.internal.intervals[lo_offset].end
					- parent->node.internal.intervals[lo_offset].start;
	for (uint16_t j=1; j<tree->dimensions; ++j) {
		index_t newspread = parent->node.internal.INTERVALS(lo_offset,j).end
							- parent->node.internal.INTERVALS(lo_offset,j).start;
		if (newspread > spread) {
			spread = newspread;
			splitdim = j;
		}
	}
	pthread_rwlock_unlock (parent_lock);

	pthread_rwlock_rdlock (page_lock);
	priority_queue_t* priority_queue = new_priority_queue (&mincompare_containers);
	for (register uint32_t i=0; i<overloaded_page->header.records; ++i) {
		data_container_t *const data_container = (data_container_t *const) malloc (sizeof(data_container_t));

		data_container->sort_key = overloaded_page->node.leaf.KEYS(i,splitdim);
		data_container->key = overloaded_page->node.leaf.keys + i*tree->dimensions;
		data_container->object = overloaded_page->node.leaf.objects[i];

		insert_into_priority_queue (priority_queue,data_container);
	}

	assert (priority_queue->size == overloaded_page->header.records);

	pthread_rwlock_wrlock (parent_lock);
	for (register uint32_t i=0; i<(overloaded_page->header.records>>1); ++i) {
		data_container_t* top = (data_container_t*) remove_from_priority_queue (priority_queue);
		lo_page->node.leaf.objects[lo_page->header.records] = top->object;
		memcpy (lo_page->node.leaf.keys+lo_page->header.records*tree->dimensions,
				top->key,tree->dimensions*sizeof(index_t));

		for (uint16_t j=0; j<tree->dimensions; ++j) {
			if (lo_page->header.records) {
				if (top->key[j] < parent->node.internal.INTERVALS(lo_offset,j).start)
					parent->node.internal.INTERVALS(lo_offset,j).start = top->key[j];
				else if (top->key[j] > parent->node.internal.INTERVALS(lo_offset,j).end)
					parent->node.internal.INTERVALS(lo_offset,j).end = top->key[j];
			}else{
				parent->node.internal.INTERVALS(lo_offset,j).start = top->key[j];
				parent->node.internal.INTERVALS(lo_offset,j).end = top->key[j];
			}
		}
		++lo_page->header.records;
		free (top);
	}
	pthread_rwlock_unlock (page_lock);

	while (priority_queue->size) {
		data_container_t* top = (data_container_t*) remove_from_priority_queue (priority_queue);
		hi_page->node.leaf.objects[hi_page->header.records] = top->object;
		memcpy (hi_page->node.leaf.keys+hi_page->header.records*tree->dimensions,
				top->key,tree->dimensions*sizeof(index_t));

		for (uint16_t j=0; j<tree->dimensions; ++j) {
			if (hi_page->header.records) {
				if (top->key[j] < parent->node.internal.INTERVALS(hi_offset,j).start)
					parent->node.internal.INTERVALS(hi_offset,j).start = top->key[j];
				else if (top->key[j] > parent->node.internal.INTERVALS(hi_offset,j).end)
					parent->node.internal.INTERVALS(hi_offset,j).end = top->key[j];
			}else{
				parent->node.internal.INTERVALS(hi_offset,j).start = top->key[j];
				parent->node.internal.INTERVALS(hi_offset,j).end = top->key[j];
			}
		}
		++hi_page->header.records;

		free (top);
	}
	parent->header.records++;

	if (verbose_splits) {
		print_box(false,tree,parent->node.internal.BOX(lo_offset));
		print_box(false,tree,parent->node.internal.BOX(hi_offset));
		puts ("--------------------------------------------------------------");
	}

	pthread_rwlock_unlock (parent_lock);

	lo_page->header.is_dirty = true;
	hi_page->header.is_dirty = true;

	assert (overloaded_page->header.records == lo_page->header.records + hi_page->header.records);

	LOG (info,"[%s] Split by dimension %u leaf node with new identifier %lu and sibling new node "
			"%lu having %u records into two parts of %u and %u records.\n",tree->filename,
			splitdim, position, hi_id, overloaded_page->header.records,
			lo_page->header.records, hi_page->header.records);

	delete_priority_queue (priority_queue);

	pthread_rwlock_wrlock (&tree->tree_lock);
	SET_PAGE(position,lo_page);
	SET_PAGE(hi_id,hi_page);

	pthread_rwlock_t *const hi_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
	pthread_rwlock_init (hi_lock,NULL);
	SET_LOCK(hi_id,hi_lock);

	tree->is_dirty = true;
	tree->tree_size++;

	uint64_t swapped = set_priority (tree->swap,position,compute_page_priority(tree,position));
	assert (is_active_identifier (tree->swap,position));

	pthread_rwlock_unlock (&tree->tree_lock);

	assert (swapped != position);
	if (swapped != ULONG_MAX) {
		LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,position);
		if (flush_page (tree,swapped) != swapped) {
			LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
			exit (EXIT_FAILURE);
		}
	}

	pthread_rwlock_wrlock (&tree->tree_lock);

	swapped = set_priority (tree->swap,hi_id,compute_page_priority(tree,hi_id));
	assert (is_active_identifier (tree->swap,hi_id));

	pthread_rwlock_unlock (&tree->tree_lock);

	assert (swapped != hi_id);
	if (swapped != ULONG_MAX) {
		LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,hi_id);
		if (flush_page (tree,swapped) != swapped) {
			LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
			exit (EXIT_FAILURE);
		}
	}

	delete_rtree_page (overloaded_page);
	return position;
}


static void cascade_deletion (tree_t *const tree, uint64_t const page_id, uint32_t const offset) {
	LOG(info,"CASCADED DELETION TO PAGE %lu.\n",tree->filename,page_id);

	page_t *const page = load_page (tree,page_id);

	pthread_rwlock_wrlock (&tree->tree_lock);
	pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
	tree->is_dirty = true;
	pthread_rwlock_unlock (&tree->tree_lock);

	assert (page_lock != NULL);

	pthread_rwlock_wrlock (page_lock);

	assert (offset < page->header.records);

	page->header.is_dirty = true;

	/**** Congregate the data subsumed by the removed page ****/
	lifo_t* browse = new_stack();
	lifo_t* leaf_entries = new_stack();

	/***** Cascade deletion upward, or update the root if necessary *****/
	boolean is_current_page_removed = false;
	if (page->header.records >= fairness_threshold*(tree->internal_entries>>1)
		|| (!page_id && page->header.records > 2)) {

		LOG (info,"[%s] Cascaded deletion: CASE 0 (Another page obtains the identifier of the removed page)\n",tree->filename);

		/**
		 * Another page obtains the identifier of the removed child of the root
		 * and the pages under the moved replacement have to change accordingly.
		 */

		uint64_t const deleted_page_id = CHILD_ID(page_id,offset);
		uint64_t const replacement_page_id = CHILD_ID(page_id,page->header.records-1);

		if (deleted_page_id < replacement_page_id) {
			memcpy (page->node.internal.BOX(offset),
					page->node.internal.BOX(page->header.records-1),
					tree->dimensions*sizeof(interval_t));

			fifo_t* transposed_ids = transpose_subsumed_pages (tree,replacement_page_id,deleted_page_id);
			while (transposed_ids->size) {
				symbol_table_entry_t* entry = (symbol_table_entry_t*) remove_head_of_queue (transposed_ids);

				if (dump_transposed_pages) {
					low_level_write_of_page_to_disk (tree,entry->value,entry->key);
					if (((page_t const*const)entry->value)->header.is_leaf) {
						delete_rtree_page (entry->value);
					}else{
						delete_rtree_page (entry->value);
					}
				}else{
					uint64_t swapped = set_priority (tree->swap,entry->key,compute_page_priority(tree,entry->key));
					assert (is_active_identifier (tree->swap,entry->key));
					assert (swapped != entry->key);
					if (swapped != page_id && swapped != ULONG_MAX) {
						LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,entry->key);
						assert (LOADED_PAGE(swapped) != NULL);
						if (flush_page (tree,swapped) != swapped) {
							LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
							exit (EXIT_FAILURE);
						}
					}

					pthread_rwlock_wrlock (&tree->tree_lock);
					SET_PAGE(entry->key,entry->value);

					if (LOADED_LOCK(entry->key)==NULL) {
						pthread_rwlock_t *const entry_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
						pthread_rwlock_init (entry_lock,NULL);
						SET_LOCK(entry->key,entry_lock);
					}
					pthread_rwlock_unlock (&tree->tree_lock);
				}
				free (entry);
			}
			delete_queue (transposed_ids);
		}
	}else if (page_id) {
		LOG (info,"[%s] Cascaded deletion: CASE I (Under-loaded non-root page.)\n",tree->filename);

		/**
		 * Under-loaded non-leaf non-root page.
		 */

		pthread_rwlock_wrlock (&tree->tree_lock);

		UNSET_PAGE(page_id);
		UNSET_LOCK(page_id);

		unset_priority (tree->swap,page_id);

		pthread_rwlock_unlock (&tree->tree_lock);

		for (register uint32_t i=0; i<page->header.records; ++i) {
			if (i!=offset) {
				insert_into_stack (browse,CHILD_ID(page_id,i));
			}
		}

		while (browse->size) {
			uint64_t const subsumed_id = remove_from_stack (browse);
			load_page (tree,subsumed_id);

			pthread_rwlock_wrlock (&tree->tree_lock);
			page_t* subsumed_page = UNSET_PAGE(subsumed_id);
			pthread_rwlock_t* subsumed_lock = UNSET_LOCK(subsumed_id);
			unset_priority (tree->swap,subsumed_id);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (subsumed_lock != NULL);
			assert (subsumed_page != NULL);

			pthread_rwlock_wrlock (subsumed_lock);
			subsumed_page->header.is_dirty = true;
			if (subsumed_page->header.is_leaf) {
				for (register uint32_t i=0; i<subsumed_page->header.records; ++i) {
					data_pair_t *const pair = (data_pair_t *const) malloc (sizeof(data_pair_t));

					pair->key = (index_t*) malloc (sizeof(index_t)*tree->dimensions);
					memcpy (pair->key,subsumed_page->node.leaf.keys+i*tree->dimensions,sizeof(index_t)*tree->dimensions);

					pair->object = subsumed_page->node.leaf.objects[i];

					insert_into_stack (leaf_entries,pair);

					pthread_rwlock_wrlock (&tree->tree_lock);
					tree->indexed_records--;
					pthread_rwlock_unlock (&tree->tree_lock);
				}
				delete_rtree_page (subsumed_page);
			}else{
				for (register uint32_t i=0; i<subsumed_page->header.records; ++i) {
					insert_into_stack (browse,CHILD_ID(subsumed_id,i));
				}
				delete_rtree_page (subsumed_page);
			}

			pthread_rwlock_unlock (subsumed_lock);
			pthread_rwlock_destroy (subsumed_lock);
		}

		delete_stack (browse);


		cascade_deletion (tree,PARENT_ID(page_id),CHILD_OFFSET(page_id));


		/**** Reinsert the data subsumed by the removed page ****/
		while (leaf_entries->size) {
			data_pair_t* pair = (data_pair_t*) remove_from_stack (leaf_entries);
			insert_into_rtree (tree, pair->key, pair->object);

			free (pair->key);
			free (pair);
		}
		delete_stack (leaf_entries);
		/*************************************************/

		delete_rtree_page (page);

		is_current_page_removed = true;

	}else if (page->header.records < 3) {
		LOG (info,"[%s] Cascaded deletion: CASE II (The only child of the root becomes the new root)\n",tree->filename);
		assert (page->header.records == 2);

		/**
		 * The only child of the root becomes the new root.
		 */

		pthread_rwlock_wrlock (&tree->tree_lock);

		UNSET_PAGE(page_id);
		UNSET_LOCK(page_id);

		pthread_rwlock_unlock (&tree->tree_lock);

		fifo_t* transposed_ids = offset?transpose_subsumed_pages(tree,1,0):transpose_subsumed_pages(tree,2,0);

		while (transposed_ids->size) {
			symbol_table_entry_t* entry = (symbol_table_entry_t*) remove_head_of_queue (transposed_ids);

			if (dump_transposed_pages) {
				low_level_write_of_page_to_disk (tree,entry->value,entry->key);
				if (((page_t const*const)entry->value)->header.is_leaf) {
					delete_rtree_page (entry->value);
				}else{
					delete_rtree_page (entry->value);
				}
			}else{
				pthread_rwlock_wrlock (&tree->tree_lock);
				uint64_t swapped = set_priority (tree->swap,entry->key,compute_page_priority(tree,entry->key));
				assert (is_active_identifier (tree->swap,entry->key));
				pthread_rwlock_unlock (&tree->tree_lock);

				assert (swapped != entry->key);
				if (swapped != ULONG_MAX) {
					LOG (info,"[%s] Swapping page %lu for page %lu from the disk.\n",tree->filename,swapped,entry->key);
					if (flush_page (tree,swapped) != swapped) {
						LOG (error,"[%s] Unable to flush page %lu...\n",tree->filename,swapped);
						exit (EXIT_FAILURE);
					}
				}

				pthread_rwlock_wrlock (&tree->tree_lock);
				SET_PAGE(entry->key,entry->value);

				pthread_rwlock_t *const entry_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
				pthread_rwlock_init (entry_lock,NULL);
				SET_LOCK(entry->key,entry_lock);
				pthread_rwlock_unlock (&tree->tree_lock);
			}
			free (entry);
		}


		assert (!page->header.is_leaf);

		pthread_rwlock_wrlock (&tree->tree_lock);
		memcpy (tree->root_box,
			offset?page->node.internal.intervals:page->node.internal.intervals+1,
			tree->dimensions*sizeof(interval_t));
		pthread_rwlock_unlock (&tree->tree_lock);


		delete_rtree_page (page);
		delete_queue (transposed_ids);
		is_current_page_removed = true;
	}else{
		LOG(error,"[%s] Erroneous cascaded deletion to remove page %lu enacted from page %lu...\n",
					tree->filename,page_id,CHILD_ID(page_id,offset));
		exit (EXIT_FAILURE);
	}


	if (is_current_page_removed) {
		pthread_rwlock_unlock (page_lock);
		pthread_rwlock_destroy (page_lock);

		pthread_rwlock_wrlock (&tree->tree_lock);
		tree->tree_size--;
		pthread_rwlock_unlock (&tree->tree_lock);
	}else{
		page->header.records--;
		pthread_rwlock_unlock (page_lock);

		update_boxes (tree,page_id);
	}
}


/**
 * If many occurrences with the same key,
 * will have to be called as many times.
 */

object_t delete_from_rtree (tree_t *const tree, index_t const key[]) {
	/* depth-first search */
	lifo_t* browse = new_stack();
	insert_into_stack (browse,0);

	while (browse->size) {
		uint64_t const page_id = remove_from_stack (browse);

		page_t *const page = load_page (tree,page_id);

		pthread_rwlock_wrlock (&tree->tree_lock);
		pthread_rwlock_t* page_lock = LOADED_LOCK(page_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		pthread_rwlock_rdlock (page_lock);

		if (page->header.is_leaf) {
			for (register uint32_t i=0; i<page->header.records; ++i) {
				if (equal_keys (page->node.leaf.keys+i*tree->dimensions,key,tree->dimensions)) {

					pthread_rwlock_unlock (page_lock);
					pthread_rwlock_wrlock (page_lock);

					object_t const result = page->node.leaf.objects[i];

					boolean is_current_page_removed = false;
					if (page_id && page->header.records < fairness_threshold*(tree->leaf_entries>>1)) {

						/**
						 * Not necessary to update the heapfile too if interested in performance
						 * over disk-utilization. Eventually will be overwritten, or if it is of
						 * subsequent level, search would stop at the first encountered leaf without
						 * proceeding any deeper, and hence, will never be accessed even again.
						 * Normally, the heapfile should be truncated so as to include all pages
						 * up to the last valid page.
						 */

						pthread_rwlock_wrlock (&tree->tree_lock);

						UNSET_PAGE(page_id);
						UNSET_LOCK(page_id);

						unset_priority (tree->swap,page_id);

						tree->is_dirty = true;

						pthread_rwlock_unlock (&tree->tree_lock);


						page->header.is_dirty = true;

						cascade_deletion (tree,PARENT_ID(page_id),CHILD_OFFSET(page_id));


						for (register uint32_t j=0; j<page->header.records; ++j) {
							if (j!=i) {
								pthread_rwlock_wrlock (&tree->tree_lock);
								tree->indexed_records--;
								pthread_rwlock_unlock (&tree->tree_lock);

								insert_into_rtree (tree,
										page->node.leaf.keys+j*tree->dimensions,
										page->node.leaf.objects[j]);
							}
						}

						delete_rtree_page (page);

						is_current_page_removed = true;
					}else if (i < page->header.records-1) {
						page->node.leaf.objects[i] = page->node.leaf.objects[page->header.records-1];
						memcpy (page->node.leaf.keys+i*tree->dimensions,
								page->node.leaf.keys+(page->header.records-1)*tree->dimensions,
								tree->dimensions*sizeof(index_t));
					}

					pthread_rwlock_wrlock (&tree->tree_lock);
					tree->indexed_records--;
					pthread_rwlock_unlock (&tree->tree_lock);


					if (is_current_page_removed) {
						pthread_rwlock_unlock (page_lock);
						pthread_rwlock_destroy (page_lock);

						pthread_rwlock_wrlock (&tree->tree_lock);
						tree->tree_size--;
						pthread_rwlock_unlock (&tree->tree_lock);
					}else{
						page->header.records--;
						pthread_rwlock_unlock (page_lock);

						update_boxes(tree,page_id);
					}

					delete_stack (browse);

					return result;
				}
			}
		}else{
			for (register uint32_t i=0; i<page->header.records; ++i) {
				if (key_enclosed_by_box (key,page->node.internal.BOX(i),tree->dimensions)) {
					insert_into_stack (browse,CHILD_ID(page_id,i));
				}
			}
		}

		pthread_rwlock_unlock (page_lock);
	}

	LOG (warn,"[%s] Attempted to delete non-existent data entry...\n",tree->filename);
	delete_stack (browse);

	return -1;
}


static void process_records_from_textfile (tree_t *const tree, char const filename[], boolean const insert) {
	FILE *const fptr = fopen (filename,"r");
	if (fptr == NULL) {
		LOG (error,"[%s] Cannot open file '%s' for reading...\n",tree->filename,filename);
	}else{
		index_t coordinates [tree->dimensions];
		object_t id;

		rewind (fptr);

		while (!feof(fptr)) {
			if (sizeof(object_t) == sizeof(short)) {
				fscanf (fptr,"%h ",&id);
			}else if (sizeof(object_t) == sizeof(int)) {
				fscanf (fptr,"%d ",&id);
			}else if (sizeof(object_t) == sizeof(long)) {
				fscanf (fptr,"%ld ",&id);
			}

			if (insert) {
				LOG(info,"Indexing value '%ld' by key: (",id);
			}else{
				LOG(info,"Deleting value '%ld' indexed by key: (",id);
			}

			for (uint16_t i=0; i<tree->dimensions; ++i) {
				if (sizeof(index_t) == sizeof(short)) {
					fscanf (fptr,"%h ",coordinates+i);
				}else if (sizeof(index_t) == sizeof(float)) {
					fscanf (fptr,"%f ",coordinates+i);
				}else if (sizeof(index_t) == sizeof(double)) {
					fscanf (fptr,"%lf ",coordinates+i);
				}

				if (logging <= info) {
					fprintf (stderr,"%12lf ",(double)coordinates[i]);
				}
			}

			if (logging <= info) {
				fprintf (stderr,").\n",tree->filename);
			}

			if (insert) insert_into_rtree (tree,coordinates,id);
			else delete_from_rtree (tree,coordinates);
		}

		if (ferror(fptr)) {
			LOG (error,"[%s] Error occurred while accessing file '%s...\n",tree->filename,filename);
			exit (EXIT_FAILURE);
		}
	}

	fclose(fptr);
}

void insert_records_from_textfile (tree_t *const tree, char const filename[]) {
	process_records_from_textfile (tree,filename,true);
}

void delete_records_from_textfile (tree_t *const tree, char const filename[]) {
	process_records_from_textfile (tree,filename,false);
}


static index_t expansion_volume (index_t const key[], interval_t const box[], uint32_t const dimensions) {
	index_t volume = 0;
	for (uint32_t i=0; i<dimensions; ++i) {
		index_t increment = 0;
		if (key[i] < box[i].start) {
			increment = box[i].start - key[i];
		}else if (key[i] > box[i].end) {
			increment = key[i] - box[i].end;
		}

		if (increment > 0) {
			for (uint32_t j=0; j<dimensions; ++j) {
				if (j != i) {
					increment *= box[j].end - box[j].start;
				}
			}
			volume += increment;
		}
	}
	return volume;
}

typedef struct {
	tree_t const* tree;
	index_t const* key;
	uint64_t page_id;
} dummy_t;

static int compare_expansion_dummy (void const*const xcontainer, void const*const ycontainer) {
	tree_t const*const tree=((dummy_t*)xcontainer)->tree;
	index_t const*const key = ((dummy_t*)xcontainer)->key;

	uint64_t const x = ((dummy_t*)xcontainer)->page_id;
	uint64_t const y = ((dummy_t*)ycontainer)->page_id;

	//assert (load_page (tree,PARENT_ID(x))->header.is_valid);
	//assert (load_page (tree,PARENT_ID(y))->header.is_valid);

	index_t volx = expansion_volume (key,x==0?tree->root_box:load_page ((tree_t *const)tree,PARENT_ID(x))->node.internal.BOX(CHILD_OFFSET(x)),tree->dimensions);
	index_t voly = expansion_volume (key,y==0?tree->root_box:load_page ((tree_t *const)tree,PARENT_ID(y))->node.internal.BOX(CHILD_OFFSET(y)),tree->dimensions);

	if (volx<voly) return -1;
	else if (volx>voly) return 1;
	else return 0;
}


static void insert_into_leaf (tree_t const*const tree, page_t *const leaf,
								index_t const key[], object_t const value) {

	assert (leaf != NULL);
	assert (leaf->header.is_leaf);

	memcpy (leaf->node.leaf.keys+leaf->header.records*tree->dimensions,
			key,tree->dimensions*sizeof(index_t));
	leaf->node.leaf.objects [leaf->header.records] = value;
	leaf->header.records++;
}

void insert_into_rtree (tree_t *const tree, index_t const key[], object_t const value) {
	if (load_page (tree,0) == NULL) new_root(tree);

	pthread_rwlock_wrlock (&tree->tree_lock);
	tree->is_dirty = true;
	tree->indexed_records++;
	pthread_rwlock_unlock (&tree->tree_lock);

	uint64_t minload = ULONG_MAX;
	uint64_t minpos = 0;
	uint64_t minexp = 0;
	page_t* minleaf = NULL;
	index_t minvol = INDEX_T_MAX;

	lifo_t* browse = new_stack();
	insert_into_stack (browse,0);

	while (browse->size) {
		uint64_t const position = remove_from_stack (browse);
		page_t *const page = load_page (tree,position);

		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(position);
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (page_lock != NULL);

		pthread_rwlock_rdlock (page_lock);

		if (page->header.is_leaf) {
			if (page->header.records < minload) {
				minload = page->header.records;
				minpos = position;
				minleaf = page;
			}
		}else{
			for (register uint32_t i=0; i<page->header.records; ++i) {
				if (key_enclosed_by_box(key,page->node.internal.BOX(i),tree->dimensions)) {
					insert_into_stack (browse,CHILD_ID(position,i));
				}else{
					index_t tempvol = expansion_volume(key,page->node.internal.BOX(i),tree->dimensions);
					if (tempvol < minvol && LOADED_PAGE(CHILD_ID(position,i)) != NULL) {
						minexp = CHILD_ID(position,i);
						minvol = tempvol;
					}
				}
			}
		}

		pthread_rwlock_unlock (page_lock);
	}

	delete_stack (browse);


	if (minleaf != NULL) {
		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t* minleaf_lock = LOADED_LOCK(minpos);
		pthread_rwlock_unlock (&tree->tree_lock);

		pthread_rwlock_rdlock (minleaf_lock);
		uint32_t minleaf_records = minleaf->header.records;
		pthread_rwlock_unlock (minleaf_lock);

		if (minleaf_records >= tree->leaf_entries) {
			//page_t *const nca = load_page (tree,PARENT_ID(minexp));
			minpos = split_leaf (tree,minpos);
			uint64_t const sibling = CHILD_ID(PARENT_ID(minpos),load_page (tree,PARENT_ID(minpos))->header.records-1);

			boolean former = key_enclosed_by_box(key,MBB(minpos),tree->dimensions);
			boolean latter = key_enclosed_by_box(key,MBB(sibling),tree->dimensions);

			if (former && latter)
				minpos = load_page (tree,minpos)->header.records < load_page (tree,sibling)->header.records ? minpos : sibling;
			else if (latter) minpos = sibling;
			else if (former) ;
			else{
				index_t tempvol = expansion_volume(key,load_page (tree,PARENT_ID(minpos))->node.internal.BOX(CHILD_OFFSET(minpos)),tree->dimensions);
				if (tempvol < minvol && load_page (tree,minpos) != NULL) {
					minexp = minpos;
					minvol = tempvol;
				}

				tempvol = expansion_volume(key,load_page (tree,PARENT_ID(sibling))->node.internal.BOX(CHILD_OFFSET(sibling)),tree->dimensions);
				if (tempvol < minvol && load_page (tree,sibling) != NULL) {
					minexp = sibling;
					minvol = tempvol;
				}

				LOG (info,"[%s] EXPANDED HERE PAGE #%lu...\n",tree->filename,minexp);

				goto expand;
			}
		}

		minleaf = load_page (tree,minpos);

		pthread_rwlock_rdlock (&tree->tree_lock);
		minleaf_lock = LOADED_LOCK(minpos);
		pthread_rwlock_unlock (&tree->tree_lock);

		pthread_rwlock_wrlock (minleaf_lock);
		minleaf->header.is_dirty = true;
		insert_into_leaf (tree,minleaf,key,value);
		pthread_rwlock_unlock (minleaf_lock);

		if (!minpos) update_rootbox (tree);
	}else{
		expand:;
		priority_queue_t* volume_expansion_priority_queue = new_priority_queue (&compare_expansion_dummy);

		dummy_t *const container = (dummy_t *const) malloc (sizeof(dummy_t));
		container->tree = tree;
		container->key = key;
		container->page_id = 0; // PARENT_ID(minexp); ///////////////////////////////////////////////////////////

		insert_into_priority_queue (volume_expansion_priority_queue,container);

		boolean is_inserted = false;
		assert (volume_expansion_priority_queue->size);
		while (!is_inserted && volume_expansion_priority_queue->size) {
			dummy_t *const container = (dummy_t *const) remove_from_priority_queue (volume_expansion_priority_queue);
			uint64_t position = container->page_id;

			free (container);

			page_t* page = load_page (tree,position);

			pthread_rwlock_rdlock (&tree->tree_lock);
			pthread_rwlock_t* page_lock = LOADED_LOCK(position);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);

			pthread_rwlock_rdlock (page_lock);

			if (page->header.is_leaf) {
				if (page->header.records >= tree->leaf_entries) {
					pthread_rwlock_unlock (page_lock);

					position = split_leaf (tree,position);

					uint64_t const sibling = CHILD_ID(PARENT_ID(position),load_page (tree,PARENT_ID(position))->header.records-1);

					boolean former = key_enclosed_by_box(key,MBB(position),tree->dimensions);
					boolean latter = key_enclosed_by_box(key,MBB(sibling),tree->dimensions);

					if (former && latter)
						position = load_page (tree,position)->header.records < load_page (tree,sibling)->header.records ? position : sibling;
					else if (latter) position = sibling;

					page = load_page (tree,position);

					pthread_rwlock_rdlock (&tree->tree_lock);
					page_lock = LOADED_LOCK(position);
					pthread_rwlock_unlock (&tree->tree_lock);

					assert (page_lock != NULL);

					pthread_rwlock_rdlock (page_lock);
				}

				pthread_rwlock_unlock (page_lock);
				pthread_rwlock_wrlock (page_lock);

				insert_into_leaf (tree,page,key,value);
				page->header.is_dirty = true;
				is_inserted = true;

				pthread_rwlock_unlock (page_lock);

				for (;position;position=PARENT_ID(position)) {
					page_t *const parent = load_page (tree,PARENT_ID(position));

					pthread_rwlock_rdlock (&tree->tree_lock);
					pthread_rwlock_t *const parent_lock = LOADED_LOCK(PARENT_ID(position));
					pthread_rwlock_unlock (&tree->tree_lock);

					assert (parent_lock != NULL);
					assert (page_lock != parent_lock);

					uint32_t const offset = CHILD_OFFSET(position);

					pthread_rwlock_wrlock (parent_lock);

					if (key_enclosed_by_box (key,
									parent->node.internal.BOX(offset),
									tree->dimensions)) {

						pthread_rwlock_unlock (parent_lock);
						break;
					}

					for (uint16_t j=0; j<tree->dimensions; ++j) {
						if (key[j] < parent->node.internal.INTERVALS(offset,j).start)
							parent->node.internal.INTERVALS(offset,j).start = key[j];
						else if (parent->node.internal.INTERVALS(offset,j).end < key[j])
							parent->node.internal.INTERVALS(offset,j).end = key[j];
					}
					parent->header.is_dirty = true;

					pthread_rwlock_unlock (parent_lock);
				}

				pthread_rwlock_rdlock (page_lock);

				pthread_rwlock_wrlock (&tree->tree_lock);

				if (!position) {
					if (page->header.records) {
						for (uint16_t j=0; j<tree->dimensions; ++j) {
							if (key[j] < tree->root_box[j].start) {
								tree->root_box[j].start = key[j];
							}else if (tree->root_box[j].end < key[j]) {
								tree->root_box[j].end = key[j];
							}
						}
					}else{
						for (uint16_t j=0; j<tree->dimensions; ++j) {
							tree->root_box[j].start = key[j];
							tree->root_box[j].end = key[j];
						}
					}
				}

				pthread_rwlock_unlock (&tree->tree_lock);
				pthread_rwlock_unlock (page_lock);

			}else{
				for (register uint32_t i=0;i<page->header.records;++i) {
					dummy_t *const container = (dummy_t *const) malloc (sizeof(dummy_t));
					container->tree = tree;
					container->key = key;
					container->page_id = CHILD_ID(position,i);

					insert_into_priority_queue (volume_expansion_priority_queue,container);
				}

				pthread_rwlock_unlock (page_lock);
			}
		}

		delete_priority_queue (volume_expansion_priority_queue);

		if (!is_inserted) {
			LOG (error,"[%s] Unable to locate a page for the new tuple...\n",tree->filename);
			exit (EXIT_FAILURE);
		}
	}
}

