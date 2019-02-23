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

#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "symbol_table.h"
#include "priority_queue.h"
#include "common.h"
#include "queue.h"
#include "stack.h"
#include "swap.h"
#include "defs.h"
#include "rtree.h"
#include "ntree.h"


static
page_t* new_rtree_leaf (tree_t const*const tree) {
	page_t *const page = (page_t *const) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s][new_rtree_leaf()] Unable to allocate additional memory for a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = true;
	page->header.is_dirty = true;

	page->node.leaf.objects = (object_t *const) malloc (tree->leaf_entries*sizeof(object_t));
	if (page->node.leaf.objects == NULL) {
		LOG (fatal,"[%s][new_rtree_leaf()] Unable to allocate additional memory for the objects of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.leaf.keys = (index_t *const) malloc (tree->leaf_entries*tree->dimensions*sizeof(index_t));
	if (page->node.leaf.keys == NULL) {
		LOG (fatal,"[%s][new_rtree_leaf()] Unable to allocate additional memory for the keys of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

static
page_t* new_rtree_internal (tree_t const*const tree) {
	page_t *const page = (page_t *const) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s][new_rtree_internal()] Unable to allocate additional memory for a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = false;
	page->header.is_dirty = true;

	page->node.internal.intervals = (interval_t*) malloc (tree->dimensions*tree->internal_entries*sizeof(interval_t));
	if (page->node.internal.intervals == NULL) {
		LOG (fatal,"[%s][new_rtree_internal()] Unable to allocate additional memory for the entries of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

static
page_t* new_ntree_leaf (tree_t const*const tree) {
	page_t *const page = (page_t*) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s][new_ntree_leaf()] Unable to allocate additional memory for a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = true;
	page->header.is_dirty = true;

	page->node.subgraph.from = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));
	if (page->node.subgraph.from == NULL) {
		LOG (fatal,"[%s][new_ntree_leaf()] Unable to allocate additional memory for the arc origins of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.subgraph.pointers = (arc_pointer_t*) malloc (tree->leaf_entries*sizeof(arc_pointer_t));
	if (page->node.subgraph.pointers == NULL) {
		LOG (fatal,"[%s][new_ntree_leaf()] Unable to allocate additional memory for the arc pointers of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.subgraph.to = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));
	if (page->node.subgraph.to == NULL) {
		LOG (fatal,"[%s][new_ntree_leaf()] Unable to allocate additional memory for the arc targets of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.subgraph.weights = (arc_weight_t*) malloc (tree->leaf_entries*sizeof(arc_weight_t));
	if (page->node.subgraph.weights == NULL) {
		LOG (fatal,"[%s][new_ntree_leaf()] Unable to allocate additional memory for the arc weights of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

static
page_t* new_ntree_internal (tree_t const*const tree) {
	page_t *const page = (page_t*) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s][new_ntree_internal()] Unable to allocate additional memory for a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = false;
	page->header.is_dirty = true;

	page->node.group.ranges = (object_range_t*) malloc (tree->internal_entries*sizeof(object_range_t));
	if (page->node.group.ranges == NULL) {
		LOG (fatal,"[%s][new_ntree_internal()] Unable to allocate additional memory for the lookup list of a new block...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

page_t* new_leaf (tree_t const*const tree) {
	return tree->root_range == NULL ? new_rtree_leaf (tree) : new_ntree_leaf (tree);
}

page_t* new_internal (tree_t const*const tree) {
	return tree->root_range == NULL ? new_rtree_internal (tree) : new_ntree_internal (tree);
}

/**
 * Change this with caution in order to update swapping policy.
 */

uint64_t counter = 0;
uint64_t compute_page_priority (tree_t *const tree, uint64_t const page_id) {
	//return 0xffffffffffffffff - log2(page_id) / log2(tree->internal_entries); // Lowest Level First (LLF)
	return counter++; //time(0); // LRU
}

uint64_t anchor (tree_t const*const tree, uint64_t id) {
	uint64_t sum = 0;
	uint64_t product = 1;
	while (sum <= id) {
		sum += product;
		product *= tree->internal_entries;
	}
	return sum - product/tree->internal_entries;
}

fifo_t* transpose_subsumed_pages (tree_t *const tree, uint64_t const from, uint64_t const to) {
	assert (tree->root_range == NULL || tree->root_box == NULL);
	assert (tree->root_range != NULL || tree->root_box != NULL);

	fifo_t *const changes = new_queue();
	fifo_t *const original = new_queue();
	fifo_t *const transposed = new_queue();

	insert_at_tail_of_queue (original,from);
	insert_at_tail_of_queue (transposed,to);

	while (original->size) {
		assert (original->size == transposed->size);

		uint64_t const original_id = remove_head_of_queue (original);
		uint64_t const transposed_id = remove_head_of_queue (transposed);

		if (original_id == transposed_id) continue;

		LOG (info,"[%s][transpose_subsumed_pages()] Block at position %lu will be transposed to position %lu.\n",tree->filename,original_id,transposed_id);

		page_t *const page = load_page (tree,original_id);
		assert (page != NULL);

		symbol_table_entry_t *const change = (symbol_table_entry_t *const) malloc (sizeof(symbol_table_entry_t));
		change->key = transposed_id;
		change->value = page;
		insert_at_tail_of_queue (changes,change);

		pthread_rwlock_rdlock (&tree->tree_lock);
		pthread_rwlock_t *const page_lock = LOADED_LOCK(original_id);
		pthread_rwlock_unlock (&tree->tree_lock);
		assert (page_lock != NULL);

		pthread_rwlock_wrlock (&tree->tree_lock);
		UNSET_PAGE(original_id);
		UNSET_LOCK(original_id);
		assert (LOADED_PAGE(original_id) == NULL);
		assert (LOADED_LOCK(original_id) == NULL);
		boolean rval = UNSET_PRIORITY (original_id);
		assert (!is_active_identifier(tree->swap,original_id));
		pthread_rwlock_unlock (&tree->tree_lock);

		pthread_rwlock_wrlock (page_lock);
		page->header.is_dirty = true;
		if (!page->header.is_leaf) {
			for (register uint32_t offset=0;offset<page->header.records;++offset) {
				insert_at_tail_of_queue (original,CHILD_ID(original_id,offset));
				insert_at_tail_of_queue (transposed,CHILD_ID(transposed_id,offset));
			}
		}
		pthread_rwlock_unlock (page_lock);
		pthread_rwlock_destroy (page_lock);
		free (page_lock);
	}
	assert (!original->size);
	assert (!transposed->size);

	delete_queue (original);
	delete_queue (transposed);

	return changes;
}
/**
static uint64_t transpose_page_position (uint64_t page_id,va_list args) {
	tree_t const*const tree = va_arg (args,tree_t const*const);
	uint64_t anchorage =  anchor(tree,page_id);
	return CHILD_ID(anchorage,page_id-anchorage);
}
**/
void update_rootbox (tree_t *const tree) {
	page_t const*const page = load_page (tree,0);

	pthread_rwlock_wrlock (&tree->tree_lock);
	pthread_rwlock_t *const page_lock = LOADED_LOCK(0);

	assert (page_lock != NULL);
	pthread_rwlock_rdlock (page_lock);

	assert (page != NULL);
	if (page->header.is_leaf) {
		for (register uint32_t i=0; i<page->header.records; ++i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if (page->node.leaf.KEYS(i,j) < tree->root_box[j].start){
					tree->root_box[j].start = page->node.leaf.KEYS(i,j);
					tree->is_dirty = true;
				}
				if (page->node.leaf.KEYS(i,j) > tree->root_box[j].end){
					tree->root_box[j].end = page->node.leaf.KEYS(i,j);
					tree->is_dirty = true;
				}
			}
		}
	}else{
		for (register uint32_t i=0; i<page->header.records; ++i) {
			for (uint16_t j=0; j<tree->dimensions; ++j) {
				if ((page->node.internal.BOX(i)+j)->start < tree->root_box[j].start){
					tree->root_box[j].start = (page->node.internal.BOX(i)+j)->start;
					tree->is_dirty = true;
				}
				if ((page->node.internal.BOX(i)+j)->end > tree->root_box[j].end){
					tree->root_box[j].end =(page->node.internal.BOX(i)+j)->end;
					tree->is_dirty = true;
				}
			}
		}
	}
	pthread_rwlock_unlock (page_lock);
	pthread_rwlock_unlock (&tree->tree_lock);
}

void update_rootrange (tree_t *const tree) {
	page_t const*const page = load_page (tree,0);
	assert (page != NULL);

	pthread_rwlock_wrlock (&tree->tree_lock);
	pthread_rwlock_t *const page_lock = LOADED_LOCK(0);
	assert (page_lock != NULL);

	pthread_rwlock_rdlock (page_lock);
	if (page->header.is_leaf) {
		for (register uint32_t i=0; i<page->header.records; ++i) {
			if (page->node.subgraph.from[i] < tree->root_range->start) {
				tree->root_range->start = page->node.subgraph.from[i];
				tree->is_dirty = true;
			}
			if (page->node.subgraph.from[i] > tree->root_range->end) {
				tree->root_range->end = page->node.subgraph.from[i];
				tree->is_dirty = true;
			}
		}
	}else{
		for (register uint32_t i=0; i<page->header.records; ++i) {
			if (page->node.group.ranges[i].start < tree->root_range->start){
				tree->root_range->start = page->node.group.ranges[i].start;
				tree->is_dirty = true;
			}
			if (page->node.group.ranges[i].end > tree->root_range->end){
				tree->root_range->end = page->node.group.ranges[i].end;
				tree->is_dirty = true;
			}
		}
	}
	pthread_rwlock_unlock (page_lock);
	pthread_rwlock_unlock (&tree->tree_lock);
}

void new_root (tree_t *const tree) {
	LOG (info,"[%s][new_root()] NEW ROOT!\n",tree->filename);
	page_t* new_root = NULL;

	pthread_rwlock_wrlock (&tree->tree_lock);

	if (tree->heapfile_index->size) {
		//map_keys (tree->heapfile_index,&transpose_page_position,tree);

		pthread_rwlock_unlock (&tree->tree_lock);
		fifo_t* transposed_ids = transpose_subsumed_pages (tree,0,1);
		clear_swap (tree->swap);

		priority_queue_t *const sorted_pages = new_priority_queue (&mincompare_symbol_table_entries);
		while (transposed_ids->size) {
			insert_into_priority_queue (sorted_pages,remove_head_of_queue (transposed_ids));
		}

		while (sorted_pages->size) {
			symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_from_priority_queue (sorted_pages);

			low_level_write_of_page_to_disk (tree,entry->value,entry->key);
			if (((page_t*)entry->value)->header.is_leaf) {
				assert (((page_t*)entry->value)->header.records <= tree->leaf_entries);
			}else{
				assert (((page_t*)entry->value)->header.records <= tree->internal_entries);
			}
			if (tree->root_range == NULL) delete_rtree_page (entry->value);
			else delete_ntree_page (entry->value);

			free (entry);
		}
		pthread_rwlock_wrlock (&tree->tree_lock);

		delete_priority_queue (sorted_pages);
		delete_queue (transposed_ids);

		new_root = new_internal(tree);
		new_root->header.records = 1;

		if (tree->root_range == NULL) {
			memcpy (new_root->node.internal.intervals,
					tree->root_box,
					tree->dimensions*sizeof(index_t));
		}else{
			//*(new_root->node.group.ranges) = *(tree->root_range);
			memcpy (new_root->node.group.ranges,
					tree->root_range,
					sizeof(object_range_t));
		}
	}else{
		new_root = new_leaf (tree);
		new_root->header.records = 0;
	}

	uint64_t swapped = SET_PRIORITY (0);
	assert (is_active_identifier (tree->swap,0));
	assert (swapped);
	if (swapped != 0xffffffffffffffff) {
		LOG (info,"[%s][new_root()] Swapping block %lu for block %lu from the disk.\n",tree->filename,swapped,0L);
		assert (LOADED_PAGE(swapped) != NULL);
		assert (LOADED_LOCK(swapped) != NULL);
		pthread_rwlock_unlock (&tree->tree_lock);
		if (flush_page (tree,swapped) != swapped) {
			LOG (fatal,"[%s][new_root()] Unable to flush block %lu...\n",tree->filename,swapped);
			exit (EXIT_FAILURE);
		}
		assert (LOADED_PAGE(swapped) == NULL);
		assert (LOADED_LOCK(swapped) == NULL);
		pthread_rwlock_wrlock (&tree->tree_lock);
	}

	SET_PAGE(0,new_root);

	pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
	pthread_rwlock_init (page_lock,NULL);
	SET_LOCK(0,page_lock);

	tree->tree_size++;

	pthread_rwlock_unlock (&tree->tree_lock);
}


static
page_t* load_rtree_page (tree_t *const tree, uint64_t const position) {
	pthread_rwlock_rdlock (&tree->tree_lock);
	page_t* page = LOADED_PAGE(position);
	pthread_rwlock_t* page_lock = LOADED_LOCK(position);
	pthread_rwlock_unlock (&tree->tree_lock);

	if (page_lock != NULL) {
		if (page != NULL) {
			//assert (is_active_identifier(tree->swap,position));
			pthread_rwlock_wrlock (&tree->tree_lock);
			uint64_t swapped = SET_PRIORITY (position);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (swapped != position);
			if (swapped != 0xffffffffffffffff) {
				LOG (info,"[%s][load_rtree_page()] Swapping block %lu for block %lu from the disk.\n",tree->filename,swapped,position);
				if (flush_page (tree,swapped) != swapped) {
					LOG (fatal,"[%s][load_rtree_page()] Unable to flush block %lu...\n",tree->filename,swapped);
					exit (EXIT_FAILURE);
				}
			}
			return page;
		}else{
			LOG (fatal,"[%s][load_rtree_page()] Inconsistency in block/lock %lu...\n",tree->filename,position);
			exit (EXIT_FAILURE);
		}
	}else if (page == NULL) {
		if (tree->filename == NULL) {
			LOG (info,"[%s][load_rtree_page()] No binary file was provided...\n",tree->filename);
			return page;
		}
		int fd = open (tree->filename,O_RDONLY,0);
		if (fd < 0) {
			LOG (warn,"[%s][load_rtree_page()] Cannot open file '%s' for reading...\n",tree->filename,tree->filename);
			return page;
		}

		if (lseek (fd,(1+position)*tree->page_size,SEEK_SET) < 0) {
			LOG (error,"[%s][load_rtree_page()] There are less than %lu blocks in file '%s'...\n",tree->filename,position+1,tree->filename);
			close (fd);
			return page;
		}

		page = (page_t*) malloc (sizeof(page_t));
		if (page == NULL) {
			LOG (fatal,"[%s][load_rtree_page()] Unable to reserve additional memory to load block %lu from the external memory...\n",tree->filename,position);
			close (fd);
			exit (EXIT_FAILURE);
		}

		void *const buffer = (void *const) malloc (tree->page_size), *ptr;
		if (buffer == NULL) {
			LOG (fatal,"[%s][load_rtree_page()] Unable to buffer block %lu from the external memory...\n",tree->filename,position);
			abort ();
		}
		if (read (fd,buffer,tree->page_size) < tree->page_size) {
			LOG (warn,"[%s][load_rtree_page()] Read less than %u bytes for block %lu in '%s'...\n",tree->filename,tree->page_size,position,tree->filename);
		}

		memcpy (&page->header,buffer,sizeof(header_t));
		page->header.records = le32toh (page->header.records);

		ptr = buffer + sizeof(header_t);

		if (page->header.is_leaf) {
			page->node.leaf.objects = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));
			if (page->node.leaf.objects == NULL) {
				LOG (fatal,"[%s][load_rtree_page()] Unable to allocate additional memory for the objects of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			page->node.leaf.keys = (index_t*) malloc (tree->dimensions*tree->leaf_entries*sizeof(index_t));
			if (page->node.leaf.keys == NULL) {
				LOG (fatal,"[%s][load_rtree_page()] Unable to allocate additional memory for the keys of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			memcpy (page->node.leaf.keys,ptr,sizeof(index_t)*tree->dimensions*page->header.records);
			if (sizeof(index_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = page->node.leaf.keys;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = page->node.leaf.keys;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = page->node.leaf.keys;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_rtree_page()] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(index_t)*tree->dimensions*page->header.records;

			memcpy (page->node.leaf.objects,ptr,sizeof(object_t)*page->header.records);
			if (sizeof(object_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = page->node.leaf.objects;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = page->node.leaf.objects;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = page->node.leaf.objects;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_rtree_page()] Unable to deserialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
		}else{
			page->node.internal.intervals = (interval_t*) malloc (tree->dimensions*tree->internal_entries*sizeof(interval_t));
			if (page->node.internal.intervals == NULL) {
				LOG (fatal,"[%s][load_rtree_page()] Unable to allocate additional memory for the entries of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			memcpy (page->node.internal.intervals,ptr,sizeof(interval_t)*tree->dimensions*page->header.records);
			if (sizeof(index_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = page->node.internal.intervals;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records<<1; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = page->node.internal.intervals;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records<<1; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = page->node.internal.intervals;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records<<1; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_rtree_page()] Unable to deserialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
		}
		close (fd);
		free (buffer);
		page->header.is_dirty = false;

		pthread_rwlock_wrlock (&tree->tree_lock);
		++tree->io_counter;
		SET_PAGE(position,page);
		assert (page_lock == NULL);
		page_lock = (pthread_rwlock_t*) malloc (sizeof(pthread_rwlock_t));
		pthread_rwlock_init (page_lock,NULL);
		SET_LOCK(position,page_lock);
		pthread_rwlock_unlock (&tree->tree_lock);

		if (!position) update_rootbox (tree);

		LOG (info,"[%s][load_rtree_page()] Loaded from '%s' block %lu with %u records from the disk.\n",tree->filename,
								tree->filename,position,page->header.records);

		pthread_rwlock_wrlock (&tree->tree_lock);
		uint64_t swapped = SET_PRIORITY (position);
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (swapped != position);
		if (swapped != 0xffffffffffffffff) {
			LOG (info,"[%s][load_rtree_page()] Swapping block %lu for block %lu from the disk.\n",tree->filename,swapped,position);
			if (flush_page (tree,swapped) != swapped) {
				LOG (fatal,"[%s] Unable to flush block %lu...\n",tree->filename,swapped);
				exit (EXIT_FAILURE);
			}
		}
	}else{
		LOG (fatal,"[%s][load_rtree_page()] Inconsistency in block/lock %lu...\n",tree->filename,position);
		exit (EXIT_FAILURE);
	}
	return page;
}

static
page_t* load_ntree_page (tree_t *const tree, uint64_t const position) {
	pthread_rwlock_rdlock (&tree->tree_lock);
	page_t* page = LOADED_PAGE(position);
	pthread_rwlock_t* page_lock = LOADED_LOCK(position);
	pthread_rwlock_unlock (&tree->tree_lock);

	if (page_lock != NULL) {
		if (page != NULL) {
			//assert (is_active_identifier(tree->swap,position));
			pthread_rwlock_wrlock (&tree->tree_lock);
			uint64_t swapped = SET_PRIORITY (position);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (swapped != position);
			if (swapped != 0xffffffffffffffff) {
				LOG (info,"[%s][load_ntree_page()] Swapping block %lu for block %lu from the disk.\n",tree->filename,swapped,position);
				if (flush_page (tree,swapped) != swapped) {
					LOG (fatal,"[%s][load_ntree_page()] Unable to flush block %lu...\n",tree->filename,swapped);
					exit (EXIT_FAILURE);
				}
			}
			return page;
		}else{
			LOG (fatal,"[%s][load_ntree_page()] Inconsistency in block/lock %lu...\n",tree->filename,position);
			exit (EXIT_FAILURE);
		}
	}else if (page == NULL) {
		if (tree->filename == NULL) {
			LOG (info,"[%s][load_ntree_page()] No binary file was provided...\n",tree->filename);
			return page;
		}
		int fd = open (tree->filename,O_RDONLY,0);
		if (fd < 0) {
			LOG (warn,"[%s][load_ntree_page()] Cannot open file '%s' for reading...\n",tree->filename,tree->filename);
			return page;
		}

		if (lseek (fd,(1+position)*tree->page_size,SEEK_SET) < 0) {
			LOG (error,"[%s][load_ntree_page()] There are less than %lu blocks in file '%s'...\n",tree->filename,position+1,tree->filename);
			close (fd);
			return page;
		}

		page = (page_t*) malloc (sizeof(page_t));
		if (page == NULL) {
			LOG (fatal,"[%s][load_ntree_page()] Unable to reserve additional memory to load block %lu from the external memory...\n",tree->filename,position);
			close (fd);
			exit (EXIT_FAILURE);
		}

		void *const buffer = (void *const) malloc (tree->page_size), *ptr;
		if (buffer == NULL) {
			LOG (fatal,"[%s][load_ntree_page()] Unable to buffer block %lu from the external memory...\n",tree->filename,position);
			abort ();
		}
		if (read (fd,buffer,tree->page_size) < tree->page_size) {
			LOG (warn,"[%s][load_ntree_page()] Read less than %u bytes for block %lu in '%s'...\n",tree->filename,tree->page_size,position,tree->filename);
		}

		memcpy (&page->header,buffer,sizeof(header_t));
		page->header.records = le32toh (page->header.records);
		ptr = buffer + sizeof(header_t);
		if (page->header.is_leaf) {
			assert (page->header.records <= tree->leaf_entries);
			page->node.subgraph.from = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));
			if (page->node.subgraph.from == NULL) {
				LOG (fatal,"[%s][load_ntree_page()] Unable to allocate additional memory for the sources of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			page->node.subgraph.to = (object_t*) malloc (tree->page_size);
			if (page->node.subgraph.to == NULL) {
				LOG (fatal,"[%s][load_ntree_page()] Unable to allocate additional memory for the targets of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			page->node.subgraph.pointers = (arc_pointer_t*) malloc (tree->leaf_entries*sizeof(arc_pointer_t));
			if (page->node.subgraph.pointers == NULL) {
				LOG (fatal,"[%s][load_ntree_page()] Unable to allocate additional memory for the offsets of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			page->node.subgraph.weights = (arc_weight_t*) malloc (tree->page_size);
			if (page->node.subgraph.weights == NULL) {
				LOG (fatal,"[%s][load_ntree_page()] Unable to allocate additional memory for the arc-weights of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			memcpy (page->node.subgraph.from,ptr,sizeof(object_t)*page->header.records);
			if (sizeof(object_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = ptr;
				for (register uint64_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_ntree_page()] 1.Unable to deserialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(object_t)*page->header.records;

			memcpy (page->node.subgraph.pointers,ptr,sizeof(arc_pointer_t)*page->header.records);
			if (sizeof(arc_pointer_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(arc_pointer_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(arc_pointer_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_ntree_page()] 2.Unable to deserialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(arc_pointer_t)*page->header.records;

			uint64_t total_arcs_number = 0;
			for (register uint32_t i=0; i<page->header.records; ++i) {
				total_arcs_number += page->node.subgraph.pointers[i];
			}
			LOG (debug,"[%s][load_ntree_page()] Deserializing %lu bytes.\n",tree->filename,
					(sizeof(header_t)+sizeof(object_t)+sizeof(arc_pointer_t))*page->header.records+(sizeof(object_t)+sizeof(arc_weight_t))*total_arcs_number);

			memcpy (page->node.subgraph.to,ptr,sizeof(object_t)*total_arcs_number);
			if (sizeof(object_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_ntree_page()] 3.Unable to deserialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(object_t)*total_arcs_number;

			memcpy (page->node.subgraph.weights,ptr,sizeof(arc_weight_t)*total_arcs_number);
			if (sizeof(arc_weight_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(arc_weight_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(arc_weight_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_ntree_page()] 4.Unable to deserialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
		}else{
			assert (page->header.records <= tree->internal_entries);
			page->node.group.ranges = (object_range_t*) malloc (tree->page_size-sizeof(header_t));
			if (page->node.group.ranges == NULL) {
				LOG (fatal,"[%s][load_ntree_page()] Unable to allocate additional memory for the run-length sequence of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			memcpy (page->node.group.ranges,ptr,page->header.records*sizeof(object_range_t));
			LOG (debug,"[%s][load_ntree_page()] About to deserialize %lu bytes.\n",tree->filename,sizeof(header_t)+sizeof(object_range_t)*page->header.records);
			if (sizeof(object_range_t) == sizeof(uint16_t)<<1) {
				uint16_t* le_ptr = buffer;
				for (register uint32_t i=0; i<page->header.records<<1; ++i) {
					le_ptr[i] = le16toh (le_ptr[i]);
				}
			}else if (sizeof(object_range_t) == sizeof(uint32_t)<<1) {
				uint32_t* le_ptr = buffer;
				for (register uint32_t i=0; i<page->header.records<<1; ++i) {
					le_ptr[i] = le32toh (le_ptr[i]);
				}
			}else if (sizeof(object_range_t) == sizeof(uint64_t)<<1) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<page->header.records<<1; ++i) {
					le_ptr[i] = le64toh (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][load_ntree_page()] Unable to deserialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
		}
		close (fd);
		free (buffer);
		page->header.is_dirty = false;

		pthread_rwlock_wrlock (&tree->tree_lock);
		++tree->io_counter;
		SET_PAGE(position,page);
		assert (page_lock == NULL);
		page_lock = (pthread_rwlock_t*) malloc (sizeof(pthread_rwlock_t));
		pthread_rwlock_init (page_lock,NULL);
		SET_LOCK(position,page_lock);
		pthread_rwlock_unlock (&tree->tree_lock);

		if (!position) update_rootrange (tree);

		LOG (info,"[%s][load_ntree_page()] Loaded from '%s' block %lu with %u records from the disk.\n",tree->filename,
								tree->filename,position,page->header.records);

		pthread_rwlock_wrlock (&tree->tree_lock);
		uint64_t swapped = SET_PRIORITY (position);
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (swapped != position);
		if (swapped != 0xffffffffffffffff) {
			LOG (info,"[%s][load_ntree_page()] Swapping block %lu for block %lu from the disk.\n",tree->filename,swapped,position);
			if (flush_page (tree,swapped) != swapped) {
				LOG (fatal,"[%s] Unable to flush block %lu...\n",tree->filename,swapped);
				exit (EXIT_FAILURE);
			}
		}
	}else{
		LOG (fatal,"[%s][load_ntree_page()] Inconsistency in block/lock %lu...\n",tree->filename,position);
		exit (EXIT_FAILURE);
	}
	return page;
}

page_t* load_page (tree_t *const tree, uint64_t const position) {
	return tree->root_range == NULL ?
			load_rtree_page (tree,position)
			:load_ntree_page (tree,position);
}

static
void delete_rtree_leaf (page_t *const page) {
	if (page != NULL) {
		if (page->node.leaf.keys != NULL)
			free (page->node.leaf.keys);
		if (page->node.leaf.objects != NULL)
			free (page->node.leaf.objects);

		free (page);
	}
}

static
void delete_rtree_internal (page_t *const page) {
	if (page != NULL) {
		if (page->node.internal.intervals != NULL)
			free (page->node.internal.intervals);

		free (page);
	}
}

void delete_rtree_page (page_t *const page) {
	page->header.is_leaf ? delete_rtree_leaf (page) : delete_rtree_internal (page);
}


static
void delete_ntree_leaf (page_t *const page) {
	if (page != NULL) {
		if (page->node.subgraph.from != NULL) {
			free (page->node.subgraph.from);
		}
		if (page->node.subgraph.to != NULL) {
			free (page->node.subgraph.to);
		}
		if (page->node.subgraph.weights != NULL) {
			free (page->node.subgraph.weights);
		}
		if (page->node.subgraph.pointers != NULL) {
			free (page->node.subgraph.pointers);
		}

		free (page);
	}
}

static
void delete_ntree_internal (page_t *const page) {
	if (page != NULL) {
		if (page->node.group.ranges != NULL) {
			free (page->node.group.ranges);
		}
		free (page);
	}
}

void delete_ntree_page (page_t *const page) {
	page->header.is_leaf ? delete_ntree_leaf (page) : delete_ntree_internal (page);
}

uint64_t flush_page (tree_t *const tree, uint64_t const page_id) {
	pthread_rwlock_rdlock (&tree->tree_lock);
	page_t *const page = LOADED_PAGE(page_id);
	pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
	pthread_rwlock_unlock (&tree->tree_lock);

	if ((page != NULL && page_lock == NULL)
		|| (page == NULL && page_lock != NULL)) {
		LOG (fatal,"[%s][flush_page()] Block/Lock inconsistency while flushing block: %lu...\n",tree->filename,page_id);
		abort();
	}else if (page == NULL && page_lock == NULL) {
		LOG (warn,"[%s][flush_page()] Block %lu has already been flushed!\n",tree->filename,page_id);
		return 0xffffffffffffffff;
	}

	pthread_rwlock_wrlock (page_lock);
	if (page->header.is_dirty) {
		low_level_write_of_page_to_disk (tree,page,page_id);
	}
	if (tree->root_range == NULL) delete_rtree_page (page);
	else delete_ntree_page (page);

	pthread_rwlock_wrlock (&tree->tree_lock);
	UNSET_PAGE(page_id);
	UNSET_LOCK(page_id);
	pthread_rwlock_unlock (&tree->tree_lock);

	pthread_rwlock_wrlock (&tree->tree_lock);
	boolean rval = UNSET_PRIORITY (page_id);
	pthread_rwlock_unlock (&tree->tree_lock);

	pthread_rwlock_unlock (page_lock);
	pthread_rwlock_destroy (page_lock);
	free (page_lock);

	return page_id;
}

static
uint64_t low_level_write_of_rtree_page_to_disk (tree_t *const tree, page_t *const page, uint64_t const position) {
	int fd = open (tree->filename, O_WRONLY | O_CREAT, PERMS);
	if (fd < 0) {
		LOG (error,"[%s][low_level_write_of_rtree_page_to_disk()] Cannot open file '%s' for writing...\n",tree->filename,tree->filename);
		return 0xffffffffffffffff;
	}else{
		if (lseek (fd,(1+position)*tree->page_size,SEEK_SET) < 0) {
			LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Cannot dump block at position %lu in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		page->header.is_dirty = false;

		void *const buffer = (void *const) malloc (tree->page_size), *ptr;
		if (buffer == NULL) {
			LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Unable to allocate enough memory so as to dump block...\n",tree->filename);
			exit (EXIT_FAILURE);
		}
		bzero (buffer,tree->page_size);

		ptr = buffer;
		memcpy (ptr,&page->header,sizeof(header_t));
		((header_t *const)ptr)->records = htole32(((header_t *const)ptr)->records);
		ptr += sizeof(header_t);
		if (page->header.is_leaf) {
			LOG (info,"[%s][low_level_write_of_rtree_page_to_disk()] Dumping leaf-block at position %lu with %u records.\n",tree->filename,position,page->header.records);
			memcpy (ptr,page->node.leaf.keys,sizeof(index_t)*tree->dimensions*page->header.records);
			LOG (debug,"[%s][low_level_write_of_rtree_page_to_disk()] About to dump %lu bytes of %u-dimensional keys.\n",tree->filename,sizeof(index_t)*tree->dimensions*page->header.records,tree->dimensions);
			if (sizeof(index_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = ptr;
				for (register uint64_t i=0; i<tree->dimensions*page->header.records; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(index_t)*tree->dimensions*page->header.records;

			memcpy (ptr,page->node.leaf.objects,sizeof(object_t)*page->header.records);
			LOG (debug,"[%s][low_level_write_of_rtree_page_to_disk()] About to dump %lu bytes of identifiers.\n",tree->filename,sizeof(object_t)*page->header.records);
			if (sizeof(object_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(object_t)*page->header.records;
		}else{
			LOG (info,"[%s][low_level_write_of_rtree_page_to_disk()] Dumping non-leaf block at position %lu with %u children.\n",tree->filename,position,page->header.records);
			memcpy (ptr,page->node.leaf.keys,sizeof(interval_t)*tree->dimensions*page->header.records);
			LOG (debug,"[%s][low_level_write_of_rtree_page_to_disk()] About to dump %lu bytes of %u-dimensional boxes.\n",tree->filename,sizeof(interval_t)*tree->dimensions*page->header.records,tree->dimensions);
			if (sizeof(index_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = buffer;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records<<1; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = buffer;
				for (register uint32_t i=0; i<tree->dimensions*page->header.records<<1; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(index_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<tree->dimensions*page->header.records<<1; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(interval_t)*tree->dimensions*page->header.records;
		}
		uint64_t bytelength = ptr - buffer;
		if (bytelength > tree->page_size) {
			LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Over-flown block at position %lu occupying %lu bytes when block-size is %u...\n",tree->filename,position,bytelength,tree->page_size);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (write (fd,buffer,tree->page_size) != tree->page_size) {
			LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Unable to dump block at position %lu in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}else{
			LOG (debug,"[%s][low_level_write_of_rtree_page_to_disk()] Done dumping %lu bytes of binary data.\n",tree->filename,bytelength);
		}
		free (buffer);
	}
	close(fd);
	return position;
}

static
uint64_t low_level_write_of_ntree_page_to_disk (tree_t *const tree, page_t *const page, uint64_t const position) {
	int fd = open (tree->filename, O_WRONLY | O_CREAT, PERMS);
	if (fd < 0) {
		LOG (error,"[%s][low_level_write_of_ntree_page_to_disk()] Cannot open file '%s' for writing...\n",tree->filename,tree->filename);
		return 0xffffffffffffffff;
	}else{
		if (lseek (fd,(1+position)*tree->page_size,SEEK_SET) < 0) {
			LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] Cannot write block %lu at the appropriate position in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		page->header.is_dirty = false;

		void *const buffer = (void *const) malloc (tree->page_size), *ptr;
		if (buffer == NULL) {
			LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] Unable to allocate enough memory so as to dump block...\n",tree->filename);
			exit (EXIT_FAILURE);
		}
		bzero (buffer,tree->page_size);

		ptr = buffer;
		memcpy (ptr,&page->header,sizeof(header_t));
		((header_t *const)ptr)->records = htole32(((header_t *const)ptr)->records);
		ptr += sizeof(header_t);
		if (page->header.is_leaf) {
			LOG (info,"[%s][low_level_write_of_ntree_page_to_disk()] Flushing leaf-block %lu with %u records.\n",tree->filename,position,page->header.records);
			uint64_t total_arcs_number = 0;
			for (register uint32_t i=0; i<page->header.records; ++i) {
				total_arcs_number += page->node.subgraph.pointers[i];
			}
			LOG (debug,"[%s][low_level_write_of_ntree_page_to_disk()] About to dump %lu bytes.\n",tree->filename,
					sizeof(header_t)+(sizeof(object_t)+sizeof(arc_pointer_t))*page->header.records+(sizeof(object_t)+sizeof(arc_weight_t))*total_arcs_number);
			memcpy (ptr,page->node.subgraph.from,sizeof(object_t)*page->header.records);
			if (sizeof(object_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = ptr;
				for (register uint64_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] 1.Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(object_t)*page->header.records;

			memcpy (ptr,page->node.subgraph.pointers,sizeof(arc_pointer_t)*page->header.records);
			if (sizeof(arc_pointer_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(arc_pointer_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(arc_pointer_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<page->header.records; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] 2.Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(arc_pointer_t)*page->header.records;

			memcpy (ptr,page->node.subgraph.to,sizeof(object_t)*total_arcs_number);
			if (sizeof(object_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(object_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] 3.Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(object_t)*total_arcs_number;

			memcpy (ptr,page->node.subgraph.weights,sizeof(arc_weight_t)*total_arcs_number);
			if (sizeof(arc_weight_t) == sizeof(uint16_t)) {
				uint16_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(arc_weight_t) == sizeof(uint32_t)) {
				uint32_t* le_ptr = ptr;
				for (register uint32_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(arc_weight_t) == sizeof(uint64_t)) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<total_arcs_number; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] 4.Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(arc_weight_t)*total_arcs_number;
		}else{
			LOG (info,"[%s][low_level_write_of_ntree_page_to_disk()] Flushing non-leaf block %lu with %u children.\n",tree->filename,position,page->header.records);
			memcpy (ptr,page->node.group.ranges,sizeof(object_range_t)*page->header.records);
			LOG (debug,"[%s][low_level_write_of_ntree_page_to_disk()] About to dump %lu bytes.\n",tree->filename,sizeof(header_t)+sizeof(object_range_t)*page->header.records);
			if (sizeof(object_range_t) == sizeof(uint16_t)<<1) {
				uint16_t* le_ptr = buffer;
				for (register uint32_t i=0; i<page->header.records<<1; ++i) {
					le_ptr[i] = htole16 (le_ptr[i]);
				}
			}else if (sizeof(object_range_t) == sizeof(uint32_t)<<1) {
				uint32_t* le_ptr = buffer;
				for (register uint32_t i=0; i<page->header.records<<1; ++i) {
					le_ptr[i] = htole32 (le_ptr[i]);
				}
			}else if (sizeof(object_range_t) == sizeof(uint64_t)<<1) {
				uint64_t* le_ptr = buffer;
				for (register uint64_t i=0; i<page->header.records<<1; ++i) {
					le_ptr[i] = htole64 (le_ptr[i]);
				}
			}else{
				LOG (fatal,"[%s][low_level_write_of_rtree_page_to_disk()] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			ptr += sizeof(object_range_t)*page->header.records;
		}
		uint64_t bytelength = ptr - buffer;
		if (bytelength > tree->page_size) {
			LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] Over-flown block at position %lu occupying %lu bytes when block-size is %u...\n",tree->filename,position,bytelength,tree->page_size);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (write (fd,buffer,tree->page_size) != tree->page_size) {
			LOG (fatal,"[%s][low_level_write_of_ntree_page_to_disk()] Unable to dump block at position %lu in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}else{
			LOG (debug,"[%s][low_level_write_of_ntree_page_to_disk()] Done dumping %lu bytes of binary data.\n",tree->filename,bytelength);
		}
		free (buffer);
	}
	close(fd);
	return position;
}

uint64_t low_level_write_of_page_to_disk (tree_t *const tree, page_t *const page, uint64_t const position) {
	return tree->root_range == NULL ?
			low_level_write_of_rtree_page_to_disk (tree,page,position)
			:low_level_write_of_ntree_page_to_disk (tree,page,position);
}

void delete_tree (tree_t *const tree) {
	if (tree != NULL) {
		if (tree->root_box != NULL) {
			free (tree->root_box);
		}

		if (tree->root_range != NULL) {
			free (tree->root_range);
		}

		flush_tree (tree);

		delete_symbol_table (tree->heapfile_index);
		delete_symbol_table (tree->page_locks);
		delete_swap (tree->swap);

		pthread_rwlock_destroy (&tree->tree_lock);
		free (tree->filename);
		free (tree);
	}
}

uint64_t flush_tree (tree_t *const tree) {
	LOG (warn,"[%s][flush_tree()] Now flushing tree hierarchy. Overall %lu entries are indexed overall.\n",tree->filename,tree->indexed_records);

	uint64_t count_dirty_pages = 0;
	pthread_rwlock_rdlock (&tree->tree_lock);
	if (tree->is_dirty) {
		int fd = open (tree->filename, O_WRONLY | O_CREAT, PERMS);
		if (fd < 0) {
			LOG (error,"[%s][flush_tree()] Cannot open file '%s' for writing...\n",tree->filename,tree->filename);
			return -1;
		}
		lseek (fd,0,SEEK_SET);
		uint16_t le_tree_dimensions = htole16(tree->dimensions);
		if (write (fd,&le_tree_dimensions,sizeof(uint16_t)) < sizeof(uint16_t)) {
			LOG (fatal,"[%s][flush_tree()] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint16_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint32_t le_tree_page_size = htole32(tree->page_size);
		if (write (fd,&le_tree_page_size,sizeof(uint32_t)) < sizeof(uint32_t)) {
			LOG (fatal,"[%s][flush_tree()] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint32_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint64_t le_tree_tree_size = htole64(tree->tree_size);
		if (write (fd,&le_tree_tree_size,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (fatal,"[%s][flush_tree()] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint64_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint64_t le_tree_indexed_records = htole64(tree->indexed_records);
		if (write (fd,&le_tree_indexed_records,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (fatal,"[%s][flush_tree()] Wrote less than %lu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint64_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		close (fd);
	}

	boolean allow_dump = tree->indexed_records > 0;
	pthread_rwlock_unlock (&tree->tree_lock);

	if (allow_dump) {
		pthread_rwlock_rdlock (&tree->tree_lock);
		fifo_t* queue = get_entries (tree->heapfile_index);
		pthread_rwlock_unlock (&tree->tree_lock);

		priority_queue_t *const sorted_pages = new_priority_queue (&mincompare_symbol_table_entries);
		while (queue->size) {
			insert_into_priority_queue (sorted_pages,remove_head_of_queue(queue));
		}
		delete_queue (queue);

		while (sorted_pages->size) {
			symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_from_priority_queue (sorted_pages);
			page_t *const page = entry->value;

			pthread_rwlock_rdlock (&tree->tree_lock);
			pthread_rwlock_t *const page_lock = LOADED_LOCK(entry->key);

			UNSET_PAGE (entry->key);
			UNSET_LOCK (entry->key);
			UNSET_PRIORITY (entry->key);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);
			pthread_rwlock_wrlock (page_lock);
			if (page->header.is_dirty) {
				low_level_write_of_page_to_disk (tree,page,entry->key);
				++count_dirty_pages;
			}
			if (tree->root_range==NULL) delete_rtree_page (page);
			else delete_ntree_page (page);
			free (entry);

			pthread_rwlock_unlock (page_lock);
			pthread_rwlock_destroy (page_lock);
			free (page_lock);
		}
		delete_priority_queue (sorted_pages);
	}else{
		LOG (warn,"[%s][flush_tree()] Deleting heapfile for it indexes no data anymore!\n",tree->filename);
		unlink (tree->filename);
	}
	LOG (warn,"[%s][flush_tree()] Done flushing tree hierarchy. Overall %lu dirty blocks were found!\n",tree->filename,count_dirty_pages);

	pthread_rwlock_wrlock (&tree->tree_lock);
	tree->is_dirty = false;
	pthread_rwlock_unlock (&tree->tree_lock);

	return count_dirty_pages;
}


static
boolean update_box (tree_t *const tree, uint64_t const page_id) {
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

void update_root_range (tree_t *const tree) {
	page_t const*const page = load_page (tree,0);
	assert (page != NULL);

	pthread_rwlock_wrlock (&tree->tree_lock);
	pthread_rwlock_t *const page_lock = LOADED_LOCK(0);
	assert (page_lock != NULL);

	pthread_rwlock_rdlock (page_lock);
	if (page->header.is_leaf) {
		for (register uint32_t i=0; i<page->header.records; ++i) {
			if (page->node.subgraph.from[i] < tree->root_range->start) {
				tree->root_range->start = page->node.subgraph.from[i];
				tree->is_dirty = true;
			}
			if (page->node.subgraph.from[i] > tree->root_range->end) {
				tree->root_range->end = page->node.subgraph.from[i];
				tree->is_dirty = true;
			}
		}
	}else{
		for (register uint32_t i=0; i<page->header.records; ++i) {
			if (page->node.group.ranges[i].start < tree->root_range->start){
				tree->root_range->start = page->node.group.ranges[i].start;
				tree->is_dirty = true;
			}
			if (page->node.group.ranges[i].end > tree->root_range->end){
				tree->root_range->end = page->node.group.ranges[i].end;
				tree->is_dirty = true;
			}
		}
	}
	pthread_rwlock_unlock (page_lock);
	pthread_rwlock_unlock (&tree->tree_lock);
}

static
boolean update_internal_range (tree_t *const tree, uint64_t const page_id) {
	if (!page_id) return false;
	page_t const*const page = load_page (tree,page_id);
	page_t *const parent = load_page (tree,PARENT_ID(page_id));
	assert (page != NULL);
	assert (parent != NULL);

	pthread_rwlock_rdlock (&tree->tree_lock);
	pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
	pthread_rwlock_t *const parent_lock = LOADED_LOCK(PARENT_ID(page_id));
	pthread_rwlock_unlock (&tree->tree_lock);

	assert (parent_lock != NULL);
	assert (page_lock != NULL);

	pthread_rwlock_rdlock (page_lock);
	pthread_rwlock_wrlock (parent_lock);
	uint32_t const offset = CHILD_OFFSET(page_id);
	boolean is_updated = false;
	if (page->header.is_leaf) {
		for (register uint32_t i=0; i<page->header.records; ++i) {
			if (page->node.subgraph.from[i] < parent->node.group.ranges[offset].start) {
				parent->node.group.ranges[offset].start = page->node.subgraph.from[i];
				parent->header.is_dirty = true;
				is_updated = true;
			}
			if (page->node.subgraph.from[i] > parent->node.group.ranges[offset].end) {
				parent->node.group.ranges[offset].end = page->node.subgraph.from[i];
				parent->header.is_dirty = true;
				is_updated = true;
			}
		}
	}else{
		for (register uint32_t i=0; i<page->header.records; ++i) {
			if (page->node.group.ranges[i].start < parent->node.group.ranges[offset].start) {
				parent->node.group.ranges[offset].start = page->node.group.ranges[i].start;
				parent->header.is_dirty = true;
				is_updated = true;
			}
			if (page->node.group.ranges[i].end > parent->node.group.ranges[offset].end) {
				parent->node.group.ranges[offset].end = page->node.group.ranges[i].end;
				parent->header.is_dirty = true;
				is_updated = true;
			}
		}
	}
	pthread_rwlock_unlock (parent_lock);
	pthread_rwlock_unlock (page_lock);

	if (is_updated) {
		LOG(debug,"[%s][update_internal_range()] Updated internal range corresponding to block %lu: [%lu,%lu]\n",tree->filename,page_id,parent->node.group.ranges[offset].start,parent->node.group.ranges[offset].end);
		pthread_rwlock_wrlock (&tree->tree_lock);
		tree->is_dirty = true;
		pthread_rwlock_unlock (&tree->tree_lock);
	}
	return is_updated;
}

void update_upwards (tree_t *const tree, uint64_t page_id) {
	for (;page_id; page_id = PARENT_ID(page_id)) {
		if (tree->root_range == NULL) {
			if (!update_box (tree,page_id)) {
				return;
			}
		}else{
			if (!update_internal_range (tree,page_id)) {
				return;
			}
		}
	}

	if (!page_id) {
		if (tree->root_range == NULL) {
			update_rootbox (tree);
		}else{
			update_root_range (tree);
		}
	}
}

void cascade_deletion (tree_t *const tree, uint64_t const page_id, uint32_t const offset) {
	LOG(info,"[%s][cascade_deletion()] CASCADED DELETION TO BLOCK %lu.\n",tree->filename,page_id);

	page_t *const page = load_page (tree,page_id);

	pthread_rwlock_wrlock (&tree->tree_lock);
	pthread_rwlock_t *const page_lock = LOADED_LOCK(page_id);
	tree->is_dirty = true;
	pthread_rwlock_unlock (&tree->tree_lock);

	assert (page_lock != NULL);

	pthread_rwlock_wrlock (page_lock);

	assert (offset < page->header.records);

	page->header.is_dirty = true;

	/***** Cascade deletion upward, or update the root if necessary *****/
	boolean is_current_page_removed = false;
	if (page->header.records >= fairness_threshold*(tree->internal_entries>>1)
		|| (!page_id && page->header.records > 2)) {

		LOG (info,"[%s][cascade_deletion()] Cascaded deletion: CASE 0 (Another block obtains the identifier of the removed block)\n",tree->filename);

		/**
		 * Another block obtains the identifier of the removed child of the root
		 * and the block under the moved replacement have to change accordingly.
		 */

		uint64_t const deleted_page_id = CHILD_ID(page_id,offset);
		uint64_t const replacement_page_id = CHILD_ID(page_id,page->header.records-1);

		if (deleted_page_id < replacement_page_id) {
			if (tree->root_range == NULL) {
				memcpy (page->node.internal.BOX(offset),
						page->node.internal.BOX(page->header.records-1),
						tree->dimensions*sizeof(interval_t));
			}else{
				//page->node.group.ranges [offset] = page->node.group.ranges [page->header.records-1];
				memcpy (page->node.group.ranges+offset,
						page->node.group.ranges+page->header.records-1,
						sizeof(object_range_t));
			}

			fifo_t* transposed_ids = transpose_subsumed_pages (tree,replacement_page_id,deleted_page_id);
			priority_queue_t *const sorted_pages = new_priority_queue (&mincompare_symbol_table_entries);
			while (transposed_ids->size) {
				insert_into_priority_queue (sorted_pages,remove_head_of_queue (transposed_ids));
			}
			delete_queue (transposed_ids);

			while (sorted_pages->size) {
				symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_from_priority_queue (sorted_pages);

				assert (UNSET_PAGE(entry->key) == NULL);
				assert (UNSET_LOCK(entry->key) == NULL);
				assert (!is_active_identifier (tree->swap,entry->key));
				assert (!UNSET_PRIORITY (entry->key));

				low_level_write_of_page_to_disk (tree,entry->value,entry->key);
				delete_rtree_page (entry->value);
				free (entry);
			}
			delete_priority_queue (sorted_pages);
		}
	}else if (page_id) {
		LOG (info,"[%s][cascade_deletion()] Cascaded deletion: CASE I (Under-loaded non-root block.)\n",tree->filename);

		/**
		 * Under-loaded non-leaf non-root block.
		 */

		pthread_rwlock_wrlock (&tree->tree_lock);
		UNSET_PAGE(page_id);
		UNSET_LOCK(page_id);
		UNSET_PRIORITY (page_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		lifo_t* leaf_entries = new_stack();
		lifo_t* browse = new_stack();
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
			UNSET_PRIORITY (subsumed_id);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (subsumed_lock != NULL);
			assert (subsumed_page != NULL);

			pthread_rwlock_wrlock (subsumed_lock);
			subsumed_page->header.is_dirty = true;
			if (subsumed_page->header.is_leaf) {
				if (tree->root_range == NULL) {
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
					uint32_t start=0, end=0;
					for (register uint32_t i=0; i<subsumed_page->header.records; ++i) {
							start = end;
							end += subsumed_page->node.subgraph.pointers[i];

							for (uint32_t j=start; j<end; ++j) {
								arc_t* arc = (arc_t*) malloc (sizeof(arc_t));

								arc->from = subsumed_page->node.subgraph.from[i];
								arc->to = subsumed_page->node.subgraph.to[j];
								arc->weight = subsumed_page->node.subgraph.weights[j];

								insert_into_stack (leaf_entries,arc);
							}
					}
					delete_ntree_page (subsumed_page);
				}
			}else{
				for (register uint32_t i=0; i<subsumed_page->header.records; ++i) {
					insert_into_stack (browse,CHILD_ID(subsumed_id,i));
				}
				if (tree->root_range == NULL) {
					delete_rtree_page (subsumed_page);
				}else{
					delete_ntree_page (subsumed_page);
				}
			}

			pthread_rwlock_unlock (subsumed_lock);
			pthread_rwlock_destroy (subsumed_lock);
			free (subsumed_lock);
		}
		delete_stack (browse);

		cascade_deletion (tree,PARENT_ID(page_id),CHILD_OFFSET(page_id));

		/**** Reinsert the data subsumed by the removed block ****/
		while (leaf_entries->size) {
			data_pair_t* pair = (data_pair_t*) remove_from_stack (leaf_entries);
			if (tree->root_range == NULL) {
				insert_into_rtree (tree, pair->key, pair->object);
				free (pair->key);
				free (pair);
			}else{
				arc_t* arc = (arc_t*) remove_from_stack (leaf_entries);
				insert_into_ntree (tree,arc->from,arc->to,arc->weight);
				free (arc);
			}
		}
		delete_stack (leaf_entries);
		/*************************************************/

		if (tree->root_range == NULL) {
			delete_rtree_page (page);
		}else{
			delete_ntree_page (page);
		}
		is_current_page_removed = true;
	}else if (page->header.records < 3) {
		LOG (info,"[%s][cascade_deletion()] Cascaded deletion: CASE II (The only child of the root becomes the new root)\n",tree->filename);
		assert (page->header.records == 2);

		/**
		 * The only child of the root becomes the new root.
		 */

		pthread_rwlock_wrlock (&tree->tree_lock);
		UNSET_PAGE(page_id);
		UNSET_LOCK(page_id);
		pthread_rwlock_unlock (&tree->tree_lock);

		fifo_t* transposed_ids = offset?transpose_subsumed_pages(tree,1,0):transpose_subsumed_pages(tree,2,0);
		priority_queue_t *const sorted_pages = new_priority_queue (&mincompare_symbol_table_entries);
		while (transposed_ids->size) {
			insert_into_priority_queue (sorted_pages,remove_head_of_queue (transposed_ids));
		}
		delete_queue (transposed_ids);

		while (sorted_pages->size) {
			symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_from_priority_queue (sorted_pages);

			while (UNSET_PRIORITY (entry->key) != NULL) {
				LOG (error,"[%s][cascade_deletion()] Transposed block %lu is still in swap...\n",tree->filename,entry->key);
			}
			assert (UNSET_PAGE(entry->key) == NULL);
			assert (UNSET_LOCK(entry->key) == NULL);
			assert (!is_active_identifier (tree->swap,entry->key));
			assert (!UNSET_PRIORITY (entry->key));

			low_level_write_of_page_to_disk (tree,entry->value,entry->key);
			if (tree->root_range == NULL) {
				delete_rtree_page (entry->value);
			}else{
				delete_ntree_page (entry->value);
			}
			free (entry);
		}
		delete_priority_queue (sorted_pages);

		assert (!page->header.is_leaf);

		pthread_rwlock_wrlock (&tree->tree_lock);
		if (tree->root_range == NULL) {
			memcpy (tree->root_box,
					offset?page->node.internal.intervals:page->node.internal.intervals+1,
					tree->dimensions*sizeof(interval_t));
		}else{
			memcpy (tree->root_range,
					offset?page->node.group.ranges:page->node.group.ranges+1,
					sizeof(object_range_t));
		}
		pthread_rwlock_unlock (&tree->tree_lock);

		if (tree->root_range == NULL) {
			delete_rtree_page (page);
		}else{
			delete_ntree_page (page);
		}
		is_current_page_removed = true;
	}else{
		LOG(fatal,"[%s][cascade_deletion()] Erroneous cascaded deletion to remove block %lu enacted from block %lu...\n",
					tree->filename,page_id,CHILD_ID(page_id,offset));
		exit (EXIT_FAILURE);
	}

	if (is_current_page_removed) {
		pthread_rwlock_unlock (page_lock);
		pthread_rwlock_destroy (page_lock);
		free (page_lock);

		pthread_rwlock_wrlock (&tree->tree_lock);
		tree->tree_size--;
		pthread_rwlock_unlock (&tree->tree_lock);
	}else{
		page->header.records--;
		pthread_rwlock_unlock (page_lock);

		update_upwards(tree,page_id);
	}
}
