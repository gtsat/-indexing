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

#ifndef __DEFS_H__
#define __DEFS_H__

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <limits.h>
#include <float.h>
#include <pthread.h>

#ifdef __FreeBSD__
  #include "endian.h"
#elif __APPLE__
  #include <libkern/OSByteOrder.h>

  #define htobe16(x) OSSwapHostToBigInt16(x)
  #define htole16(x) OSSwapHostToLittleInt16(x)
  #define be16toh(x) OSSwapBigToHostInt16(x)
  #define le16toh(x) OSSwapLittleToHostInt16(x)

  #define htobe32(x) OSSwapHostToBigInt32(x)
  #define htole32(x) OSSwapHostToLittleInt32(x)
  #define be32toh(x) OSSwapBigToHostInt32(x)
  #define le32toh(x) OSSwapLittleToHostInt32(x)

  #define htobe64(x) OSSwapHostToBigInt64(x)
  #define htole64(x) OSSwapHostToLittleInt64(x)
  #define be64toh(x) OSSwapBigToHostInt64(x)
  #define le64toh(x) OSSwapLittleToHostInt64(x)
#endif  /* __APPLE__ */

/**** BASIC DEFINITIONS BEGIN ****/

#define PERMS 0644

extern uint32_t DIMENSIONS;
extern uint32_t PAGESIZE;

#define THREAD_STACK_SIZE (getpagesize()<<6)


typedef union {
	double fval;
	uint64_t uval;
} serializable_float64_t;

typedef union {
	float fval;
	uint32_t uval;
} serializable_float32_t;


typedef struct {
	void** buffer;

	int (*compare) (void const*const,void const*const);

	uint64_t capacity;
	uint64_t size;
} priority_queue_t;

typedef struct {
	void** buffer;

	uint64_t capacity;
	uint64_t size;

	uint64_t head;
	uint64_t tail;
} fifo_t;

typedef struct {
	void** buffer;

	uint64_t capacity;
	uint64_t size;
} lifo_t;

typedef enum {false=0,true} boolean;

typedef enum {PUT,DELETE} request_t;

static const uint64_t initial_capacity = 5;

/**** BASIC DEFINITIONS END ****/


/***** R-TREE DEFINITIONS BEGIN *****/

#define fairness_threshold 	.5

typedef uint64_t 	object_t;
typedef float		index_t;

#define OBJECT_T_MAX 	0xffffffffffffffff
#define INDEX_T_MAX 	FLT_MAX


typedef struct {
	index_t start;
	index_t end;
} interval_t;

typedef struct {
	interval_t* intervals;
} internal_node_t;

typedef struct {
	index_t* keys;
	object_t* objects;
} leaf_node_t;

/***** R-TREE DEFINITIONS END *****/


/***** N-TREE DEFINITIONS BEGIN *****/

typedef float arc_weight_t;
typedef uint16_t arc_pointer_t;

/**
 * only arc sources information is kept in internal nodes,
 * sinks can be found at the leaf-layer at the end of the pages
 */

typedef struct {
	object_t start;
	object_t end;
} object_range_t;

typedef struct {
	object_range_t* ranges;
} group_node_t;

typedef struct {
	object_t* from;
	object_t* to;
	arc_weight_t* weights;
	arc_pointer_t* pointers;
} subgraph_node_t;

typedef union {
	leaf_node_t leaf;
	internal_node_t internal;

	subgraph_node_t subgraph;
	group_node_t group;
} node_t;

typedef struct {
	uint32_t records;
	unsigned char is_leaf :1;
	unsigned char is_dirty :1;
} header_t;

typedef struct {
	header_t header;
	node_t node;
} page_t;

/***** N-TREE DEFINITIONS END *****/


/*** SYMBOL-TABLE DEFINITIONS BEGIN ***/

typedef uint64_t key__t;
typedef void* value_t;

typedef struct {
	key__t key;
	value_t value;
} symbol_table_entry_t;

typedef enum {black=0,red} color_t;

typedef struct tree_node  {
	struct tree_node* left;
	struct tree_node* right;

	value_t value;
	key__t key;

	uint64_t size;

	color_t color;
} tree_node_t;

typedef struct {
	tree_node_t* root;

	int (*compare) (key__t const, key__t const);

	value_t default_value;
	uint64_t size;
} symbol_table_t;

/*** SYMBOL-TABLE DEFINITIONS END ***/


/*** SWAP DEFINITIONS BEGIN ***/

/**
 * uses a cache-efficient (small) table of just a few identifiers.
 */

typedef struct {
	uint64_t *pq;
	uint64_t *qp;
	double *keys;

	uint64_t *identifiers;

	uint64_t size;
	uint64_t capacity;
} swap_t;

/*** SWAP DEFINITIONS END ***/


/***** R-TREE DEFINITIONS BEGIN *****/

typedef struct {
	object_range_t* root_range;
	interval_t* root_box;

	pthread_rwlock_t tree_lock;
	symbol_table_t* page_locks;

	symbol_table_t* heapfile_index;

	swap_t* swap;

	char* filename;


	uint64_t indexed_records;
	uint64_t tree_size;

	uint32_t leaf_entries;
	uint32_t internal_entries;

	uint64_t io_counter;

	uint32_t page_size;

	uint16_t dimensions;
	boolean is_dirty;
} tree_t;


#define PARENT_ID(child_id)	((child_id)>tree->internal_entries?((child_id-1)/tree->internal_entries):0)
#define CHILD_OFFSET(id)	((id)==0?0:((id+tree->internal_entries-1)%tree->internal_entries))
#define CHILD_ID(id,offset)	((id)*tree->internal_entries+(offset)+1)

#define SET_PAGE(x,y)		set(tree->heapfile_index,(x),(y))
#define UNSET_PAGE(x)		((page_t*)unset(tree->heapfile_index,(x)))
#define LOADED_PAGE(x)		((page_t*)get(tree->heapfile_index,(x)))

#define SET_LOCK(x,y)		set(tree->page_locks,(x),(y))
#define UNSET_LOCK(x)		((pthread_rwlock_t *const)unset(tree->page_locks,(x)))
#define LOADED_LOCK(x)		((pthread_rwlock_t *const)get(tree->page_locks,(x)))

#define KEY(i)			keys+(i)*tree->dimensions
#define KEYS(i,j)		keys[(i)*tree->dimensions+(j)]
#define BOX(i)			intervals+(i)*tree->dimensions
#define INTERVALS(i,j)	intervals[(i)*tree->dimensions+(j)]

#define MBB(id)			(id)?load_page(tree,PARENT_ID(id))->node.internal.BOX(CHILD_OFFSET(id)):tree->root_box

#define TRANSPOSE_PAGE_POSITION(id) CHILD_ID(anchor(tree,(id)),(id)-anchor(tree,(id)))

#define TREE(i)			((tree_t *const)trees->buffer[i])

#define MIN(x,y)		((x)<(y)?(x):(y))
#define MAX(x,y)		((x)>(y)?(x):(y))

/***** R-TREE DEFINITIONS END *****/


/** QUERY PROCESSING DEFINITIONS BEGIN **/

typedef struct {
	object_t from;
	object_t to;

	arc_weight_t weight;
} arc_t;

typedef struct {
	index_t* key;
	object_t object;

	uint16_t dimensions;
} data_pair_t;

typedef struct {
	index_t* key;
	object_t object;

	double sort_key;

	uint16_t dimensions;
} data_container_t ;

typedef struct {
	interval_t* box;
	uint64_t id;

	double sort_key;
} box_container_t;

typedef struct {
	object_range_t* range;
	uint64_t id;

	double sort_key;
} range_container_t;

/**
 * For the three structs above we use the same
 * comparators as the sort-key has exactly the
 * same offset in bytes from its address in memory
 */


boolean equal_keys (index_t const[],index_t const[], uint32_t const);

boolean key_enclosed_by_box (index_t const[],interval_t const[],uint32_t const);
boolean box_enclosed_by_box (interval_t const[],interval_t const[],uint32_t const);

boolean overlapping_boxes  (interval_t const[],interval_t const[],uint32_t const);

int mincompare_symbol_table_entries (void const*const, void const*const);
int mincompare_containers (void const*const, void const*const);
int maxcompare_containers (void const*const, void const*const);

double key_to_key_distance (index_t const[],index_t const[],uint32_t const);
double key_to_box_mindistance (index_t const[],interval_t const[],uint32_t const);
double key_to_box_maxdistance (index_t const[],interval_t const[],uint32_t const);
double box_to_box_mindistance (interval_t const[],interval_t const[],uint32_t const);
double box_to_box_maxdistance (interval_t const[],interval_t const[],uint32_t const);

boolean dominated_key (index_t const[],index_t const[],boolean const[], uint32_t const);
boolean dominated_box (interval_t const[],index_t const[],boolean const[],uint32_t const);


typedef struct {
	interval_t* boxes;
	uint64_t* page_ids;

	double sort_key;

	uint16_t dimensions;
	uint16_t cardinality;
} multibox_container_t;

typedef struct {
	index_t* keys;
	object_t* objects;

	double sort_key;

	uint16_t dimensions;
	uint16_t cardinality;
} multidata_container_t;


int mincompare_multicontainers (void const*const, void const*const);
int maxcompare_multicontainers (void const*const, void const*const);

double mindistance_ordered_multikey (multidata_container_t const*const, uint32_t const);
double maxdistance_ordered_multikey (multidata_container_t const*const, uint32_t const);
double avgdistance_ordered_multikey (multidata_container_t const*const, uint32_t const);

double max_mindistance_ordered_multibox (multibox_container_t const*const, uint32_t const);
double min_maxdistance_ordered_multibox (multibox_container_t const*const, uint32_t const);
double avg_mindistance_ordered_multibox (multibox_container_t const*const, uint32_t const);
double avg_maxdistance_ordered_multibox (multibox_container_t const*const, uint32_t const);

double mindistance_pairwise_multikey (multidata_container_t const*const, uint32_t const);
double maxdistance_pairwise_multikey (multidata_container_t const*const, uint32_t const);
double avgdistance_pairwise_multikey (multidata_container_t const*const, uint32_t const);

double max_mindistance_pairwise_multibox (multibox_container_t const*const, uint32_t const);
double min_maxdistance_pairwise_multibox (multibox_container_t const*const, uint32_t const);
double avg_mindistance_pairwise_multibox (multibox_container_t const*const, uint32_t const);
double avg_maxdistance_pairwise_multibox (multibox_container_t const*const, uint32_t const);

double pairwise_mindistance (fifo_t const*const, uint32_t);


/*
 * Be extremely wary of the effect of this parameter for it allows a broader and
 * more "aware" exploration of the search space that eventually speed up things,
 * at the cost of a small amount of memory overhead... Overall, its effect is
 * immensely more conspicuous than the produced seeds from the first and greedy
 * phase (just one in this implementation).
 */

#define DIVERSIFICATION_HEAP_SIZE 5
#define MAX_ITERATIONS_NUMBER 256

/** QUERY PROCESSING DEFINITIONS END **/


/*** NETWORK DEFINITIONS BEGIN ***/

typedef struct {
	symbol_table_t* id;
	symbol_table_t* size;
} wquf_t;

typedef symbol_table_t network_t;

arc_t* new_arc (object_t const, object_t const, arc_weight_t const);

/*** NETWORK DEFINITIONS END ***/



/*** LOGGING DEFINITIONS BEGIN ***/

enum message_t {debug=1,info,warn,error,fatal};

#define logging info

#define LOG(level,message...)	if (logging<=level){\
				switch (level) {\
				case debug:\
					fprintf(stderr," ** DEBUG - "message);\
					break;\
				case info:\
					fprintf(stderr," ** INFO - "message);\
					break;\
				case warn: \
					fprintf(stderr," ** WARNING - "message);\
					break;\
				case error: \
					fprintf(stderr," ** ERROR - "message);\
					break;\
				case fatal: \
					fprintf(stderr," ** FATAL - "message);\
					break;\
				default: \
					fprintf(stderr," "message);\
				}\
				}

/** LOGGING DEFINITIONS END **/

#endif /* __DEFS_H__ */

