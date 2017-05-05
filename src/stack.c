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

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include "buffer.h"
#include "queue.h"
#include "defs.h"

fifo_t* transform_into_queue (lifo_t *const stack) {
	fifo_t *const queue = new_queue ();
	free (queue->buffer);

	queue->buffer = stack->buffer;
	queue->capacity = stack->capacity;
	queue->tail = queue->size = stack->size;

	free (stack);
	return queue;
}

void* get_stack_element (lifo_t const*const stack, size_t const k) {
	return k > stack->size ? NULL : stack->buffer[k];
}

void delete_stack (lifo_t *const stack) {
	if (stack != NULL) {
		if (stack->buffer != NULL && stack->capacity)
			free (stack->buffer);
		free (stack);
	}
}

void clear_stack (lifo_t *const stack) {
	if (initial_capacity < stack->capacity) {
		free (stack->buffer);

		stack->capacity = initial_capacity;
		stack->buffer = (void**) calloc (initial_capacity,initial_capacity*sizeof(void*));
		if (stack->buffer == NULL) {
			LOG (error,"Stack cannot reserve memory for buffer...\n");
			exit (EXIT_FAILURE);
		}
	}
	stack->size = 0;
}

lifo_t* new_stack (void) {
	lifo_t *const stack = (lifo_t *const) malloc (sizeof(lifo_t));
	if (stack == NULL) {
		LOG (error,"Cannot allocate memory for new stack...\n");
		exit (EXIT_FAILURE);
	}
	stack->buffer = (void**) calloc (initial_capacity,initial_capacity*sizeof(void*));
	if (stack->buffer == NULL) {
		LOG (error,"Stack cannot reserve memory for buffer...\n");
		exit (EXIT_FAILURE);
	}
	stack->capacity = initial_capacity;
	stack->size = 0;
	return stack;
}

void* peek_at_stack (lifo_t const*const stack) {
	if (!stack->size) {
		LOG (error,"Stack underflow...\n");
		abort();
	}
	return stack->buffer [stack->size-1];
}

void* remove_from_stack (lifo_t *const stack) {
	if (!stack->size) {
		LOG (error,"Stack underflow...\n");
		abort();
	}
	void *const element = stack->buffer [--stack->size];
	if (stack->capacity>>2 >= initial_capacity && stack->size == stack->capacity>>2) {
		void** new_buffer = adjust_buffer (stack->buffer,stack->capacity,stack->capacity>>1);
		if (new_buffer != stack->buffer) {
			stack->buffer = new_buffer;
			stack->capacity >>= 1;
		}
	}
	return element;
}

void insert_into_stack (lifo_t *const stack, void *const element) {
	if (stack->size > stack->capacity) {
		LOG (error,"Stack buffer overrun detected... Program will terminate.\n");
		abort();
	}else if (stack->size == stack->capacity) {
		void** new_buffer = adjust_buffer (stack->buffer,stack->capacity,stack->capacity<<1);
		if (new_buffer != stack->buffer) {
			stack->buffer = new_buffer;
			stack->capacity <<= 1;
		}
	}
	stack->buffer [stack->size++] = element;
}


void* remove_from_position (lifo_t *const stack, size_t const k) {
	if (k < stack->size) {
		void *const element = stack->buffer [k];

		stack->size--;
		if (k < stack->size) {
			memmove (stack->buffer+k,stack->buffer+k+1,(stack->size-k)*sizeof(void*));
			stack->buffer [stack->size] = NULL;
		}

		if (stack->capacity>>2 >= initial_capacity && stack->size == stack->capacity>>2) {
			void** new_buffer = adjust_buffer (stack->buffer,stack->capacity,stack->capacity>>1);
			if (new_buffer != stack->buffer) {
				stack->buffer = new_buffer;
				stack->capacity >>= 1;
			}
		}

		return element;
	}else{
		LOG (error,"Cannot remove from position %u in a stack of %u elements...\n",k,stack->size);
		return NULL;
	}
}

void insert_into_position (lifo_t *const stack, size_t const pos, void *const element) {
	if (stack->size == stack->capacity) {
		void** new_buffer = adjust_buffer (stack->buffer,stack->capacity,stack->capacity<<1);
		if (new_buffer != stack->buffer) {
			stack->buffer = new_buffer;
			stack->capacity <<= 1;
		}
	}

	if (pos <= stack->size) {
		if (pos < stack->size) {
			memmove (stack->buffer+pos+1,stack->buffer+pos,(stack->size-pos)*sizeof(void*));
		}

		stack->buffer [pos] = element;
		stack->size++;
	}else{
		LOG (error,"Cannot add an element in a list of %u elements at position %u...\n",stack->size,pos);
		abort ();
	}
}

/**
 * Old-school binary search.
 */
unsigned find_position_in_sorted (lifo_t *const stack, void *const element, int (*cmp)(void const*const,void const*const)) {
	if (!stack->size) {
		LOG (info,"Unable to find element in an empty list. Returns 0 for initial import.\n");
		return 0;
	}

	unsigned m = 0;
	unsigned lo = 0;
	unsigned hi = stack->size;
	for (m=(stack->size>>1); m<hi; m=(lo+hi)>>1) {
		if (cmp (stack->buffer[lo],stack->buffer[hi-1]) > 0) {
			LOG (error,"List elements do not appear in order...\n");
			abort();
			break;
		}else{
			int balance = cmp (element,stack->buffer[m]);
			if (!balance) {
				while (m && !cmp (element,stack->buffer[m-1])) {
					--m;
				}
				return m;
			}else if (balance < 0) {
				hi = m;
			}else{
				lo = m+1;
			}
		}
	}
	return m;
}

