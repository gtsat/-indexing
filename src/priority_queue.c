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
#include "queue.h"
#include "priority_queue.h"
#include "buffer.h"
#include "defs.h"

fifo_t* get_priority_queue_entries (priority_queue_t *const priority_queue) {
	fifo_t *const queue = new_queue();
	if (priority_queue->size > queue->capacity) {
		queue->buffer = (void**) malloc ((priority_queue->size+1)*sizeof(void*));
		if (queue->buffer == NULL) {
			LOG (error,"Cannot allocate memory for new priority queue...\n");
			delete_priority_queue (priority_queue);
			delete_queue (queue);
			exit (EXIT_FAILURE);
		}
	}
	memcpy(queue->buffer,priority_queue->buffer+1,priority_queue->size);
	queue->head = 0;
	queue->tail = priority_queue->size;
	queue->size = priority_queue->size;
	queue->capacity = priority_queue->size + 1;
	return queue;
}

void delete_priority_queue (priority_queue_t *const priority_queue) {
	if (priority_queue != NULL) {
		if (priority_queue->buffer != NULL && priority_queue->capacity)
			free (priority_queue->buffer);
		free (priority_queue);
	}
}

void clear_priority_queue (priority_queue_t *const priority_queue) {
	if (initial_capacity < priority_queue->capacity) {
		priority_queue->buffer = adjust_buffer (priority_queue->buffer,
							1+priority_queue->capacity,
							1+initial_capacity);
		priority_queue->capacity = initial_capacity;
	}
	priority_queue->size = 0;
}

priority_queue_t* new_priority_queue (int (*cmp) (void const*const,void const*const)) {
	priority_queue_t *const priority_queue = (priority_queue_t *const) malloc (sizeof(priority_queue_t));

	if (priority_queue == NULL) {
		LOG (error,"Cannot allocate memory for new priority queue...\n");
		exit (EXIT_FAILURE);
	}

	priority_queue->buffer = (void**) malloc ((1+initial_capacity)*sizeof(void*));
	if (priority_queue->buffer == NULL) {
		LOG (error,"Priority queue cannot allocate memory to initialize priority queue...\n");
		exit (EXIT_FAILURE);
	}

	priority_queue->compare = cmp;
	priority_queue->capacity = initial_capacity;
	priority_queue->size = 0;

	return priority_queue;
}

static size_t sink (priority_queue_t *const priority_queue, size_t position) {
	while ((position<<1) <= priority_queue->size) {
		size_t other = position<<1;
		if (other+1 <= priority_queue->size) {
			if (priority_queue->compare(
					priority_queue->buffer[other+1],
					priority_queue->buffer[other])<0) {
				++other;
			}
		}

		if (priority_queue->compare(priority_queue->buffer[position],priority_queue->buffer[other])>=0) {
			void* swap = priority_queue->buffer[position];
			priority_queue->buffer[position] = priority_queue->buffer[other];
			priority_queue->buffer[other] = swap;
			position = other;
		}else return position;
	}
	return position;
}

static size_t swim (priority_queue_t *const priority_queue, size_t position) {
	for (size_t other=(position>>1); other; other>>=1) {
		if (priority_queue->compare(
				priority_queue->buffer[position],
				priority_queue->buffer[other])<0) {
			void* swap = priority_queue->buffer[position];
			priority_queue->buffer[position] = priority_queue->buffer[other];
			priority_queue->buffer[other] = swap;
			position = other;
		}else return position;
	}
	return position;
}

void insert_into_priority_queue (priority_queue_t *const priority_queue, void *const element) {
	if (priority_queue->size >= priority_queue->capacity) {
		LOG (error,"Heap buffer overrun detected... Program will terminate.\n");
		abort();
	}
	priority_queue->buffer[++priority_queue->size] = element;
	swim (priority_queue,priority_queue->size);

	if (priority_queue->size >= priority_queue->capacity) {
		void** new_buffer = (void**) malloc ((1+(priority_queue->capacity<<1))*sizeof(void**));
		if (new_buffer != NULL) {
			memcpy (new_buffer+1,priority_queue->buffer+1,priority_queue->capacity*sizeof(void*));
			free (priority_queue->buffer);
			priority_queue->capacity <<= 1;
			priority_queue->buffer = new_buffer;
		}else{
			LOG (error,"Priority queue unable to dynamically allocate additional memory...!\n");
		}
	}
}

void* peek_priority_queue (priority_queue_t const*const priority_queue) {
	if (!priority_queue->size) {
		LOG (error,"Priority queue underflow...\n");
		abort();
	}
	return priority_queue->buffer[1];
}

void* remove_from_priority_queue (priority_queue_t *const priority_queue) {
	if (!priority_queue->size) {
		LOG (error,"Priority queue underflow...\n");
		abort();
	}

	void *const min = priority_queue->buffer[1];

	priority_queue->buffer[1] = priority_queue->buffer[priority_queue->size--];
	sink (priority_queue,1);

	if (priority_queue->capacity>>2 >= initial_capacity && priority_queue->size == priority_queue->capacity>>2) {
		void** new_buffer = (void**) malloc ((1+(priority_queue->capacity>>1))*sizeof(void**));
		if (new_buffer != NULL) {
			memcpy (new_buffer+1,priority_queue->buffer+1,priority_queue->size*sizeof(void*));
			free (priority_queue->buffer);
			priority_queue->capacity >>= 1;
			priority_queue->buffer = new_buffer;
		}else{
			LOG (error,"Priority queue unable to unallocate redundant memory...!\n");
		}
	}

	return min;
}


void* remove_priority_queue_element (priority_queue_t *const priority_queue, size_t const pos) {
	if (pos && pos <= priority_queue->size) {
		void *const element = priority_queue->buffer[pos];

		priority_queue->buffer[pos] = priority_queue->buffer[priority_queue->size--];
		sink (priority_queue,pos);

		if (priority_queue->capacity>>2 >= initial_capacity && priority_queue->size == priority_queue->capacity>>2) {
			void** new_buffer = (void**) malloc ((1+(priority_queue->capacity>>1))*sizeof(void**));
			if (new_buffer != NULL) {
				memcpy (new_buffer+1,priority_queue->buffer+1,priority_queue->size*sizeof(void*));
				free (priority_queue->buffer);
				priority_queue->capacity >>= 1;
				priority_queue->buffer = new_buffer;
			}else{
				LOG (error,"Priority queue unable to unallocate redundant memory...!\n");
			}
		}

		return element;
	}else if (!pos) {
		LOG (error,"Invalid position...\n");
	}else{
		LOG (error,"Cannot remove from position %u in a priority-queue of %u elements...\n",pos,priority_queue->size);
	}
	return NULL;
}

