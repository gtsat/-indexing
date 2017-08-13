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

#include <limits.h>
#include "swap.h"

swap_t* new_swap (uint64_t const capacity) {
	swap_t* swap = (swap_t*) malloc (sizeof(swap_t));

	swap->identifiers = (uint64_t*) malloc ((1+capacity)*sizeof(uint64_t));

	swap->keys = (double*) malloc ((1+capacity)*sizeof(double));
	swap->pq = (uint64_t*) malloc ((1+capacity)*sizeof(uint64_t));
	swap->qp = (uint64_t*) malloc ((1+capacity)*sizeof(uint64_t));

	for (register uint64_t i=0; i<=capacity; ++i) {
		swap->identifiers[i] = ULONG_MAX;
		swap->qp[i] = ULONG_MAX;
	}

	swap->capacity = capacity;
	swap->size = 0;

	return swap;
}

void delete_swap (swap_t *const swap) {
	free (swap->identifiers);

	free (swap->keys);
	free (swap->pq);
	free (swap->qp);

	free (swap);
}

void clear_swap (swap_t *const swap) {
	for (register uint64_t i=0; i<=swap->capacity; ++i) {
		swap->identifiers[i] = ULONG_MAX;
		swap->qp[i] = ULONG_MAX;
	}
	swap->size = 0;
}

static boolean greater (swap_t const*const swap, uint64_t const i, uint64_t const j) {
	//uint64_t dbgi = swap->pq[i];
	//uint64_t dbgj = swap->pq[j];

	assert (swap->pq[i] <= swap->capacity);
	assert (swap->pq[j] <= swap->capacity);

	return swap->keys[swap->pq[i]] > swap->keys[swap->pq[j]];
}

static void exch (swap_t const*const swap, uint64_t const i, uint64_t const j) {
	//uint64_t dbgi = swap->pq[i];
	//uint64_t dbgj = swap->pq[j];

	assert (swap->pq[i] <= swap->capacity);
	assert (swap->pq[j] <= swap->capacity);

	uint64_t temp = swap->pq[i];

	swap->pq[i] = swap->pq[j];
	swap->pq[j] = temp;

	swap->qp[swap->pq[i]] = i;
	swap->qp[swap->pq[j]] = j;
}

static void swim (swap_t const*const swap, uint64_t k)  {
	for (;k>1 && greater(swap,(k>>1),k); k>>=1) {
		exch (swap,k,(k>>1));
	}
}

static void sink (swap_t const*const swap, uint64_t k) {
	for (uint64_t j=k<<1; j<=swap->size; ) {
		if (j < swap->size && greater (swap,j,j+1)) j++;
		if (!greater (swap,k,j)) break;
		exch (swap,k,j);

		k=j;
		j=(k<<1);
   }
}

static void insert (swap_t *const swap, uint64_t const i, double const key) {
	assert (swap->size <= swap->capacity);
	assert (i <= swap->capacity);

	swap->size++;

	swap->keys[i] = key;
	swap->qp[i] = swap->size;
	swap->pq[swap->size] = i;

	swim (swap,swap->size);
}

static void increase_key (swap_t const*const swap, uint64_t const i, double const key) {
	if (key > swap->keys[i]) {
		swap->keys[i] = key;
		sink (swap,swap->qp[i]);
	}else{
		LOG (warn,"Tried to increase key in swap with one of smaller value...\n");
	}
}

static void decrease_key (swap_t const*const swap, uint64_t const i, double const key) {
	if (key < swap->keys[i]) {
		swap->keys[i] = key;
		swim (swap,swap->qp[i]);
	}else{
		LOG (warn,"Tried to decrease key in swap with one of greater value...\n");
	}
}

static uint64_t del_min (swap_t *const swap) {
	if (swap->size==0) {
		LOG (error,"Swap underflow error...\n");
		return -ULONG_MAX;
	}

	uint64_t min = swap->pq[1];
	exch (swap,1,swap->size--);
	sink (swap,1);

	swap->qp[min] = ULONG_MAX;
	swap->pq[swap->size+1] = ULONG_MAX;

	return min;
}

static void delete (swap_t *const swap, uint64_t i) {
	uint64_t index = swap->qp[i];
	exch (swap,index,swap->size--);
	swim (swap,index);
	sink (swap,index);
	swap->keys[i] = ULONG_MAX;
	swap->qp[i] = ULONG_MAX;

	swap->identifiers[i-1] = ULONG_MAX;
}

static uint64_t get_available (swap_t const*const swap) {
	for (register uint64_t i=0; i<swap->capacity; ++i) {
		if (swap->identifiers[i] == ULONG_MAX) {
			return i;
		}
	}
	return ULONG_MAX;
}

static uint64_t get_index (swap_t const*const swap, uint64_t const id) {
	for (register uint64_t i=0; i<swap->capacity; ++i) {
		if (swap->identifiers[i] == id) {
			return i;
		}
	}
	return ULONG_MAX;
}

boolean is_active_identifier (swap_t const*const swap, uint64_t const id) {
	return get_index (swap,id) != ULONG_MAX;
}

void print_identifiers_priorities (swap_t const*const swap) {
	for (register uint64_t i=0; i<swap->capacity; ++i) {
		if (swap->qp[i] != ULONG_MAX) {
			LOG (info,"Identifier %lu has priority %lf. \n",swap->identifiers[i],swap->keys[i+1]);
		}
	}
}

boolean unset_priority (swap_t *const swap, uint64_t const id) {
	uint64_t alias = get_index (swap,id);
	if (alias != ULONG_MAX) {
		assert (alias <= swap->capacity);
		assert (swap->qp[alias+1] != ULONG_MAX);
		assert (swap->identifiers[alias] == id);
		swap->identifiers[alias] = ULONG_MAX;

		delete (swap,alias+1);

		return true;
	}else return false;
}


/**
 * Returns the identifier of the replaced page, if any ...
 */

uint64_t set_priority (swap_t *const swap, uint64_t const id, double const priority) {
	uint64_t alias = get_index (swap,id);

	if (alias != ULONG_MAX) {
		assert (alias <= swap->capacity);
		assert (swap->qp[alias+1] != ULONG_MAX);
		assert (swap->identifiers[alias] == id);

		increase_key (swap,alias+1,priority);
	}else if (swap->size < swap->capacity) {
		alias = get_available (swap);
		assert (alias != ULONG_MAX);

		swap->identifiers[alias] = id;

		insert (swap,alias+1,priority);
	}else{
		alias = del_min (swap);

		uint64_t previous = swap->identifiers[alias-1];
		assert (previous != ULONG_MAX);
		assert (previous != id);

		swap->identifiers[alias-1] = id;
		insert (swap,alias,priority);

		return previous;
	}
	return ULONG_MAX;
}

