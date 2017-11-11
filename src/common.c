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
#include "common.h"
#include "queue.h"
#include "swap.h"
#include "defs.h"


boolean allow_dangling_pages = true;
boolean dump_transposed_pages = false;


static
page_t* new_rtree_leaf (tree_t const*const tree) {
	page_t *const page = (page_t *const) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = true;
	page->header.is_dirty = true;

	page->node.leaf.objects = (object_t *const) malloc (tree->leaf_entries*sizeof(object_t));
	if (page->node.leaf.objects == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the objects of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.leaf.keys = (index_t *const) malloc (tree->leaf_entries*tree->dimensions*sizeof(index_t));
	if (page->node.leaf.keys == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the keys of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

static
page_t* new_rtree_internal (tree_t const*const tree) {
	page_t *const page = (page_t *const) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = false;
	page->header.is_dirty = true;

	page->node.internal.intervals = (interval_t*) malloc (tree->dimensions*tree->internal_entries*sizeof(interval_t));
	if (page->node.internal.intervals == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the entries of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

static
page_t* new_ntree_leaf (tree_t const*const tree) {

	page_t *const page = (page_t*) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = true;
	page->header.is_dirty = true;

	page->node.subgraph.from = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));
	if (page->node.subgraph.from == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the arc origins of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.subgraph.pointers = (arc_pointer_t*) malloc (tree->leaf_entries*sizeof(arc_pointer_t));
	if (page->node.subgraph.pointers == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the arc pointers of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.subgraph.to = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));
	if (page->node.subgraph.to == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the arc targets of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->node.subgraph.weights = (arc_weight_t*) malloc (tree->leaf_entries*sizeof(arc_weight_t));
	if (page->node.subgraph.weights == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the arc weights of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

static
page_t* new_ntree_internal (tree_t const*const tree) {
	page_t *const page = (page_t*) malloc (sizeof(page_t));
	if (page == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	page->header.records = 0;
	page->header.is_leaf = false;
	page->header.is_dirty = true;

	page->node.group.ranges = (object_range_t*) malloc (tree->internal_entries*sizeof(object_range_t));
	if (page->node.group.ranges == NULL) {
		LOG (fatal,"[%s] Unable to allocate additional memory for the lookup list of a new page...\n",tree->filename);
		exit (EXIT_FAILURE);
	}

	return page;
}

page_t* new_leaf (tree_t const*const tree) {
	return tree->object_range == NULL ? new_rtree_leaf (tree) : new_ntree_leaf (tree);
}

page_t* new_internal (tree_t const*const tree) {
	return tree->object_range == NULL ? new_rtree_internal (tree) : new_ntree_internal (tree);
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
	assert (tree->object_range == NULL || tree->root_box == NULL);
	assert (tree->object_range != NULL || tree->root_box != NULL);

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

		LOG (info,"[%s] Block at position %llu will be transposed to position %llu.\n",tree->filename,original_id,transposed_id);

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
		boolean rval = unset_priority (tree->swap,original_id);
		//assert (!is_active_identifier(tree->swap,original_id));
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


void new_root (tree_t *const tree) {
	LOG (info,"[%s] NEW ROOT!\n",tree->filename);
	page_t* new_root = NULL;

	pthread_rwlock_wrlock (&tree->tree_lock);

	if (tree->heapfile_index->size) {
		//map_keys (tree->heapfile_index,&transpose_page_position,tree);

		pthread_rwlock_unlock (&tree->tree_lock);
		fifo_t* transposed_ids = transpose_subsumed_pages (tree,0,1);
		clear_swap (tree->swap);

		while (transposed_ids->size) {
			symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_head_of_queue (transposed_ids);
			if (true) { //!dump_transposed_pages || entry->key==1) {
				pthread_rwlock_wrlock (&tree->tree_lock);
				assert (!is_active_identifier(tree->swap,entry->key));
				uint64_t swapped = set_priority (tree->swap,entry->key,compute_page_priority(tree,entry->key));
				assert (is_active_identifier (tree->swap,entry->key));
				pthread_rwlock_unlock (&tree->tree_lock);

				assert (swapped != entry->key);
				if (swapped != 0xffffffffffffffff) {
					LOG (info,"[%s] Swapping page %llu for page %llu from the disk.\n",tree->filename,swapped,entry->key);
					if (flush_page (tree,swapped) != swapped) {
						LOG (fatal,"[%s] Unable to flush page %llu...\n",tree->filename,swapped);
						exit (EXIT_FAILURE);
					}
				}

				pthread_rwlock_wrlock (&tree->tree_lock);
				SET_PAGE(entry->key,entry->value);

				pthread_rwlock_t *const page_lock = (pthread_rwlock_t *const) malloc (sizeof(pthread_rwlock_t));
				pthread_rwlock_init (page_lock,NULL);
				SET_LOCK(entry->key,page_lock);
				pthread_rwlock_unlock (&tree->tree_lock);
			}else{
				low_level_write_of_page_to_disk (tree,entry->value,entry->key);
				if (tree->object_range == NULL) delete_rtree_page (entry->value);
				else delete_ntree_page (entry->value);
			}
			free (entry);
		}
		pthread_rwlock_wrlock (&tree->tree_lock);

		delete_queue (transposed_ids);

		new_root = new_internal(tree);
		new_root->header.records = 1;
		memcpy (new_root->node.internal.intervals,
				tree->root_box,
				tree->dimensions*sizeof(index_t));
	}else{
		new_root = new_leaf (tree);
		new_root->header.records = 0;
	}

	uint64_t swapped = set_priority (tree->swap,0,compute_page_priority(tree,0));
	assert (is_active_identifier (tree->swap,0));
	assert (swapped);
	if (swapped != 0xffffffffffffffff) {
		LOG (info,"[%s] Swapping page %llu for page %llu from the disk.\n",tree->filename,swapped,0);
		assert (LOADED_PAGE(swapped) != NULL);

		pthread_rwlock_unlock (&tree->tree_lock);
		if (flush_page (tree,swapped) != swapped) {
			LOG (fatal,"[%s] Unable to flush page %llu...\n",tree->filename,swapped);
			exit (EXIT_FAILURE);
		}
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
	/*
	assert (tree->object_range == NULL);
	assert (tree->root_box != NULL);
	*/

	pthread_rwlock_rdlock (&tree->tree_lock);
	page_t* page = LOADED_PAGE(position);
	pthread_rwlock_t* page_lock = LOADED_LOCK(position);
	pthread_rwlock_unlock (&tree->tree_lock);

	if (page_lock != NULL) {
		if (page != NULL) {
			return page;
		}else{
			LOG (fatal,"[%s] Inconsistency in page/lock %llu...\n",tree->filename,position);
			exit (EXIT_FAILURE);
		}
	}else if (page == NULL) {
		if (tree->filename == NULL) {
			LOG (info,"[%s] No binary file was provided...\n",tree->filename);
			return page;
		}
		int fd = open (tree->filename,O_RDONLY,0);
		if (fd < 0) {
			LOG (warn,"[%s] Cannot open file '%s' for reading...\n",tree->filename,tree->filename);
			return page;
		}

		if (lseek (fd,(1+position)*tree->page_size,SEEK_SET) < 0) {
			LOG (error,"[%s] There are less than %llu pages in file '%s'...\n",tree->filename,position+1,tree->filename);
			close (fd);
			return page;
		}

		page = (page_t*) malloc (sizeof(page_t));
		if (page == NULL) {
			LOG (fatal,"[%s] Unable to reserve additional memory to load page %llu from the external memory...\n",tree->filename,position);
			close (fd);
			exit (EXIT_FAILURE);
		}

		void *const buffer = (void *const) malloc (tree->page_size), *ptr;
		if (buffer == NULL) {
			LOG (fatal,"[%s] Unable to buffer page %llu from the external memory...\n",tree->filename,position);
			abort ();
		}
		if (read (fd,buffer,tree->page_size) < tree->page_size) {
			LOG (warn,"[%s] Read less than %u bytes from page %llu in '%s'...\n",tree->filename,tree->page_size,position,tree->filename);
		}

		memcpy (&page->header,buffer,sizeof(header_t));
		page->header.records = le32toh (page->header.records);

		ptr = buffer + sizeof(header_t);

		if (page->header.is_leaf) {
			page->node.leaf.objects = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));

			if (page->node.leaf.objects == NULL) {
				LOG (fatal,"[%s] Unable to allocate additional memory for the objects of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			page->node.leaf.keys = (index_t*) malloc (tree->dimensions*tree->leaf_entries*sizeof(index_t));
			if (page->node.leaf.keys == NULL) {
				LOG (fatal,"[%s] Unable to allocate additional memory for the keys of a disk-page...\n",tree->filename);
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
				LOG (fatal,"[%s] Unable to serialize into a global heapfile format.\n",tree->filename);
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
				LOG (fatal,"[%s] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
		}else{
			page->node.internal.intervals = (interval_t*) malloc (tree->dimensions*tree->internal_entries*sizeof(interval_t));
			if (page->node.internal.intervals == NULL) {
				LOG (fatal,"[%s] Unable to allocate additional memory for the entries of a disk-page...\n",tree->filename);
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
				LOG (fatal,"[%s] Unable to serialize into a global heapfile format.\n",tree->filename);
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

		LOG (info,"[%s] Loaded from '%s' page %llu with %u records from the disk.\n",tree->filename,
								tree->filename,position,page->header.records);

		pthread_rwlock_wrlock (&tree->tree_lock);
		uint64_t swapped = set_priority (tree->swap,position,compute_page_priority(tree,position));
		pthread_rwlock_unlock (&tree->tree_lock);

		assert (swapped != position);
		if (swapped != 0xffffffffffffffff) {
			LOG (info,"[%s] Swapping page %llu for page %llu from the disk.\n",tree->filename,swapped,position);
			if (flush_page (tree,swapped) != swapped) {
				LOG (fatal,"[%s] Unable to flush page %llu...\n",tree->filename,swapped);
				exit (EXIT_FAILURE);
			}
		}
	}else{
		LOG (fatal,"[%s] Inconsistency in page/lock %llu...\n",tree->filename,position);
		exit (EXIT_FAILURE);
	}
	return page;
}

static
page_t* load_ntree_page (tree_t *const tree, uint64_t const page_id) {
	assert (tree->object_range != NULL);
	assert (tree->root_box == NULL);

	pthread_rwlock_rdlock (&tree->tree_lock);
	page_t* page = LOADED_PAGE(page_id);
	pthread_rwlock_t* page_lock = LOADED_LOCK(page_id);
	pthread_rwlock_unlock (&tree->tree_lock);

	if (page_lock != NULL) {
		if (page != NULL) {
			return page;
		}else{
			LOG (error,"[%s] Inconsistency in page/lock %llu...\n",tree->filename,page_id);
			exit (EXIT_FAILURE);
		}
	}else{
		if (tree->filename == NULL) {
			LOG (info,"[%s] No binary file was provided...\n",tree->filename);
			return page;
		}

		int fd = open (tree->filename,O_RDONLY,0);
		if (fd < 0) {
			LOG (error,"[%s] Cannot open file '%s' for reading...\n",tree->filename,tree->filename);
			return page;
		}

		if (lseek (fd,(1+page_id)*tree->page_size,SEEK_SET) < 0) {
			LOG (error,"[%s] There are less than %llu pages in file '%s'...\n",tree->filename,page_id+1,tree->filename);
			close (fd);
			return page;
		}

		page = (page_t*) malloc (sizeof(page));

		if (page == NULL) {
			LOG (fatal,"[%s] Unable to reserve additional memory to load page from the external memory...\n",tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (read (fd,&page->header,sizeof(header_t)) < sizeof(header_t)) {
			LOG (fatal,"[%s] Unable to read the header of page %llu from '%s'...\n",tree->filename,page_id,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		if (page->header.is_leaf) {
			page->node.subgraph.from = (object_t*) malloc (tree->leaf_entries*sizeof(object_t));
			if (page->node.subgraph.from == NULL) {
				LOG (fatal,"[%s] Unable to allocate additional memory for the arc sources of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			page->node.subgraph.pointers = (arc_pointer_t*) malloc (tree->leaf_entries*sizeof(arc_pointer_t));
			if (page->node.subgraph.pointers == NULL) {
				LOG (fatal,"[%s] Unable to allocate additional memory for the arc pointers of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			if (read (fd,page->node.subgraph.from,sizeof(object_t)*page->header.records)
												< sizeof(object_t)*page->header.records) {
				LOG (fatal,"[%s] Unable to read the arc sources of page %llu from '%s'...\n",tree->filename,page_id,tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			if (read (fd,page->node.subgraph.pointers,sizeof(arc_pointer_t)*page->header.records)
													< sizeof(arc_pointer_t)*page->header.records) {
				LOG (fatal,"[%s] Unable to read the arc pointers of page %llu from '%s'...\n",tree->filename,page_id,tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			uint64_t total_arcs_number = 0;
			for (register uint32_t i=0; i<page->header.records; ++i) {
				total_arcs_number += page->node.subgraph.pointers[i];
			}

			if (read (fd,page->node.subgraph.to,sizeof(object_t)*total_arcs_number)
												< sizeof(object_t)*total_arcs_number) {
				LOG (fatal,"[%s] Unable to read the arc targets of dirty page %llu in '%s'...\n",tree->filename,page_id,tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
			if (read (fd,page->node.subgraph.weights,sizeof(arc_weight_t)*total_arcs_number)
													< sizeof(arc_weight_t)*total_arcs_number) {
				LOG (fatal,"[%s] Unable to read the arc weight of dirty page %llu in '%s'...\n",tree->filename,page_id,tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
		}else{
			page->node.group.ranges = (object_range_t*) malloc (tree->internal_entries*tree->leaf_entries*sizeof(object_range_t));
			if (page->node.group.ranges == NULL) {
				LOG (fatal,"[%s] Unable to allocate additional memory for the run-length sequence of a disk-page...\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			if (read (fd, page->node.group.ranges,page->header.records*sizeof(object_range_t))
												< page->header.records*sizeof(object_range_t)) {
				LOG (fatal,"[%s] Unable to read the run-length sequence of page %llu in '%s'...\n",tree->filename,page_id,tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}
		}

		close (fd);

		page->header.is_dirty = false;

		pthread_rwlock_wrlock (&tree->tree_lock);

		SET_PAGE(page_id,page);

		assert (page_lock == NULL);

		page_lock = (pthread_rwlock_t*) malloc (sizeof(pthread_rwlock_t));
		pthread_rwlock_init (page_lock,NULL);
		SET_LOCK(page_id,page_lock);

		pthread_rwlock_unlock (&tree->tree_lock);

		LOG (info,"[%s] Loaded from '%s' page %llu with %u records from the disk.\n",tree->filename,
								tree->filename,page_id,page->header.records);
/**/
		pthread_rwlock_wrlock (&tree->tree_lock);
		uint64_t swapped = set_priority (tree->swap,page_id,compute_page_priority(tree,page_id));
		pthread_rwlock_unlock (&tree->tree_lock);

		if (swapped != 0xffffffffffffffff) {
			LOG (info,"[%s] Swapping page %llu for page %llu from the disk.\n",tree->filename,swapped,page_id);
			if (flush_page (tree,swapped) != swapped) {
				LOG (fatal,"[%s] Unable to flush page %llu...\n",tree->filename,swapped);
				exit (EXIT_FAILURE);
			}
		}
/**/
		return page;
	}
}


page_t* load_page (tree_t *const tree, uint64_t const position) {
	return tree->object_range == NULL ?
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
		LOG (fatal,"[%s] Page/Lock inconsistency while flushing page: %llu...\n",tree->filename,page_id);
		abort();
	}else if (page == NULL && page_lock == NULL) {
		LOG (warn,"[%s] Page %llu has already been flushed!\n",tree->filename,page_id);
		return 0xffffffffffffffff;
	}

	pthread_rwlock_wrlock (page_lock);

	if (page->header.is_dirty) {
		if (!allow_dangling_pages) {
			low_level_write_of_page_to_disk (tree,page,page_id);
			if (tree->object_range == NULL) delete_rtree_page (page);
			else delete_ntree_page (page);
			UNSET_PAGE(page_id);
			UNSET_LOCK(page_id);
		}
	}else{
		if (tree->object_range == NULL) delete_rtree_page (page);
		else delete_ntree_page (page);
		UNSET_PAGE(page_id);
		UNSET_LOCK(page_id);
	}
	pthread_rwlock_unlock (page_lock);

	pthread_rwlock_wrlock (&tree->tree_lock);
	boolean rval = unset_priority (tree->swap,page_id);
	pthread_rwlock_unlock (&tree->tree_lock);
	return page_id;
}

static
uint64_t low_level_write_of_rtree_page_to_disk (tree_t *const tree, page_t *const page, uint64_t const position) {
	int fd = open (tree->filename, O_WRONLY | O_CREAT, PERMS);
	if (fd < 0) {
		LOG (error,"[%s] Cannot open file '%s' for writing...\n",tree->filename,tree->filename);
		return 0xffffffffffffffff;
	}else{
		if (lseek (fd,(1+position)*tree->page_size,SEEK_SET) < 0) {
			LOG (fatal,"[%s] Cannot dump page at position %llu in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		page->header.is_dirty = false;

		void *const buffer = (void *const) malloc (tree->page_size), *ptr;

		if (buffer == NULL) {
			LOG (fatal,"[%s] Unable to allocate enough memory so as to dump page...\n",tree->filename);
			exit (EXIT_FAILURE);
		}

		bzero (buffer,tree->page_size);

		ptr = buffer;
		memcpy (ptr,&page->header,sizeof(header_t));
		((header_t *const)ptr)->records = htole32(((header_t *const)ptr)->records);
		ptr += sizeof(header_t);

		if (page->header.is_leaf) {
			LOG (info,"[%s] Dumping leaf-node at position %llu with %u records.\n",tree->filename,position,page->header.records);

			memcpy (ptr,page->node.leaf.keys,sizeof(index_t)*tree->dimensions*page->header.records);
			LOG (debug,"[%s] About to dump %llu bytes of %u-dimensional keys.\n",tree->filename,sizeof(index_t)*tree->dimensions*page->header.records,tree->dimensions);

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
				LOG (fatal,"[%s] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			ptr += sizeof(index_t)*tree->dimensions*page->header.records;
			memcpy (ptr,page->node.leaf.objects,sizeof(object_t)*page->header.records);
			LOG (debug,"[%s] About to dump %llu bytes of identifiers.\n",tree->filename,sizeof(object_t)*page->header.records);

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
				LOG (fatal,"[%s] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			ptr += sizeof(object_t)*page->header.records;
		}else{
			LOG (info,"[%s] Dumping internal page at position %llu with %u children.\n",tree->filename,position,page->header.records);

			memcpy (ptr,page->node.leaf.keys,sizeof(interval_t)*tree->dimensions*page->header.records);
			LOG (debug,"[%s] About to dump %llu bytes of %u-dimensional boxes.\n",tree->filename,sizeof(interval_t)*tree->dimensions*page->header.records,tree->dimensions);

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
				LOG (fatal,"[%s] Unable to serialize into a global heapfile format.\n",tree->filename);
				close (fd);
				exit (EXIT_FAILURE);
			}

			ptr += sizeof(interval_t)*tree->dimensions*page->header.records;
		}

		uint64_t bytelength = ptr - buffer;
		if (bytelength > tree->page_size) {
			LOG (fatal,"[%s] Over-flown page at position %llu occupying %llu bytes when block-size is %u...\n",tree->filename,position,bytelength,tree->page_size);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (write (fd,buffer,tree->page_size) != tree->page_size) {
			LOG (fatal,"[%s] Unable to dump page at position %llu in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}else{
			LOG (debug,"[%s] Done dumping %llu bytes of serialized data.\n",tree->filename,bytelength);
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
		LOG (error,"[%s] Cannot open file '%s' for writing...\n",tree->filename,tree->filename);
		return 0xffffffffffffffff;
	}else{
		if (lseek (fd,(1+position)*tree->page_size,SEEK_SET) < 0) {
			LOG (fatal,"[%s] Cannot write page %llu at the appropriate position in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		page->header.is_dirty = false;
		if (write (fd,&page->header,sizeof(header_t)) != sizeof(header_t)) {
			LOG (fatal,"[%s] Unable to write the header of dirty page %llu in '%s'...\n",tree->filename,position,tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}
		if (page->header.is_leaf) {
			LOG (info,"[%s] Flushing leaf-node %llu with %u records.\n",tree->filename,position,page->header.records);

			if (tree->object_range != NULL) {
				if (write (fd,page->node.subgraph.from,sizeof(object_t)*page->header.records)
													!= sizeof(object_t)*page->header.records) {
					LOG (fatal,"[%s] Unable to write the arc sources of dirty page %llu in '%s'...\n",tree->filename,position,tree->filename);
					close (fd);
					exit (EXIT_FAILURE);
				}
				if (write (fd,page->node.subgraph.pointers,sizeof(arc_pointer_t)*page->header.records)
														!= sizeof(arc_pointer_t)*page->header.records) {
					LOG (fatal,"[%s] Unable to write the arc pointers of dirty page %llu in '%s'...\n",tree->filename,position,tree->filename);
					close (fd);
					exit (EXIT_FAILURE);
				}

				uint64_t total_arcs_number = 0;
				for (register uint32_t i=0; i<page->header.records; ++i) {
					total_arcs_number += page->node.subgraph.pointers[i];
				}

				if (write (fd,page->node.subgraph.to,sizeof(object_t)*total_arcs_number)
													!= sizeof(object_t)*total_arcs_number) {
					LOG (fatal,"[%s] Unable to write the arc targets of dirty page %llu in '%s'...\n",tree->filename,position,tree->filename);
					close (fd);
					exit (EXIT_FAILURE);
				}
				if (write (fd,page->node.subgraph.weights,sizeof(arc_weight_t)*total_arcs_number)
														!= sizeof(arc_weight_t)*total_arcs_number) {
					LOG (fatal,"[%s] Unable to write the arc weight of dirty page %llu in '%s'...\n",tree->filename,position,tree->filename);
					close (fd);
					exit (EXIT_FAILURE);
				}
			}else{
				LOG (fatal,"[%s] Unable to flush tree hierarchy...\n",tree->filename);
				exit (EXIT_FAILURE);
			}
		}else{
			LOG (info,"[%s] Flushing internal page %llu with %u children.\n",tree->filename,position,page->header.records);

			if (tree->object_range != NULL) {
				if (write (fd,page->node.group.ranges,sizeof(object_range_t)*page->header.records)
													!= sizeof(object_range_t)*page->header.records) {
					LOG (fatal,"[%s] Unable to write the range entries of dirty page %llu in '%s'...\n",tree->filename,position,tree->filename);
					close (fd);
					exit (EXIT_FAILURE);
				}
			}else{
				LOG (fatal,"[%s] Unable to flush tree hierarchy...\n",tree->filename);
				exit (EXIT_FAILURE);
			}
		}
	}
	close(fd);
	return position;
}

uint64_t low_level_write_of_page_to_disk (tree_t *const tree, page_t *const page, uint64_t const position) {
	return tree->object_range == NULL ?
			low_level_write_of_rtree_page_to_disk (tree,page,position)
			:low_level_write_of_ntree_page_to_disk (tree,page,position);
}

uint64_t flush_tree (tree_t *const tree) {
	LOG (warn,"[%s] Now flushing tree hierarchy. Overall %lu entries are indexed overall.\n",tree->filename,tree->indexed_records);

	uint64_t count_dirty_pages = 0;
	pthread_rwlock_rdlock (&tree->tree_lock);
	if (tree->is_dirty) {
		int fd = open (tree->filename, O_WRONLY | O_CREAT, PERMS);
		if (fd < 0) {
			LOG (error,"[%s] Cannot open file '%s' for writing...\n",tree->filename,tree->filename);
			return -1;
		}
		lseek (fd,0,SEEK_SET);
		uint16_t le_tree_dimensions = htole16(tree->dimensions);
		if (write (fd,&le_tree_dimensions,sizeof(uint16_t)) < sizeof(uint16_t)) {
			LOG (fatal,"[%s] Wrote less than %llu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint16_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint32_t le_tree_page_size = htole32(tree->page_size);
		if (write (fd,&le_tree_page_size,sizeof(uint32_t)) < sizeof(uint32_t)) {
			LOG (fatal,"[%s] Wrote less than %llu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint32_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint64_t le_tree_tree_size = htole64(tree->tree_size);
		if (write (fd,&le_tree_tree_size,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (fatal,"[%s] Wrote less than %llu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint64_t),tree->filename);
			close (fd);
			exit (EXIT_FAILURE);
		}

		uint64_t le_tree_indexed_records = htole64(tree->indexed_records);
		if (write (fd,&le_tree_indexed_records,sizeof(uint64_t)) < sizeof(uint64_t)) {
			LOG (fatal,"[%s] Wrote less than %llu bytes in heapfile '%s'...\n",tree->filename,sizeof(uint64_t),tree->filename);
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

		while (queue->size) {
			symbol_table_entry_t *const entry = (symbol_table_entry_t *const) remove_head_of_queue (queue);
			page_t *const page = entry->value;

			pthread_rwlock_rdlock (&tree->tree_lock);
			pthread_rwlock_t *const page_lock = LOADED_LOCK(entry->key);
			pthread_rwlock_unlock (&tree->tree_lock);

			assert (page_lock != NULL);

			pthread_rwlock_wrlock (page_lock);

			if (page->header.is_dirty) {
				low_level_write_of_page_to_disk (tree,page,entry->key);
				++count_dirty_pages;
			}
			delete_rtree_page (page);

			free (entry);

			pthread_rwlock_unlock (page_lock);
			pthread_rwlock_destroy (page_lock);
		}

		delete_queue (queue);
		//truncate_heapfile (tree);
	}else{
		LOG (warn,"[%s] Deleting heapfile for it indexes no data anymore!\n",tree->filename);
		unlink (tree->filename);
	}
	LOG (warn,"[%s] Done flushing tree hierarchy. Overall %llu dirty blocks were found!\n",tree->filename,count_dirty_pages);

	pthread_rwlock_wrlock (&tree->tree_lock);
	clear_symbol_table (tree->heapfile_index);
	clear_symbol_table (tree->page_locks);
	clear_swap (tree->swap);
	tree->is_dirty = false;
	pthread_rwlock_unlock (&tree->tree_lock);

	return count_dirty_pages;
}
