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

#include "defs.h"
#include "queue.h"
#include "buffer.h"

void* get_queue_element (fifo_t const*const queue, size_t const k) {
	if (k > queue->size){
		return NULL;
	}else if (queue->head+k < queue->capacity) {
		return queue->buffer[queue->head+k];
	}else{
		return queue->buffer[k-queue->capacity+queue->head];
	}
}

void delete_queue (fifo_t *const queue) {
	if (queue!=NULL) {
		if (queue->buffer!=NULL && queue->capacity)
			free (queue->buffer);
		free (queue);
	}
}

void clear_queue (fifo_t *const queue) {
	if (initial_capacity < queue->capacity) {
		free (queue->buffer);
		queue->capacity = initial_capacity;
		queue->buffer = malloc (initial_capacity*sizeof(void*));
		if (queue->buffer == NULL) {
			LOG (error,"Queue cannot reserve memory for buffer...\n");
			exit (EXIT_FAILURE);
		}
	}
	queue->size = 0;
	queue->head = 0;
	queue->tail = 0;
}

fifo_t* new_queue (void) {
	fifo_t *const queue = (fifo_t *const) malloc (sizeof(fifo_t));
	if (queue == NULL) {
		LOG (error,"Cannot allocate memory for new queue...\n");
		exit (EXIT_FAILURE);
	}
	queue->buffer = (void**) malloc (initial_capacity*sizeof(void*));
	if (queue->buffer == NULL) {
		LOG (error,"Queue cannot reserve memory for buffer...\n");
		exit (EXIT_FAILURE);
	}
	queue->capacity = initial_capacity;
	queue->size = 0;
	queue->head = 0;
	queue->tail = 0;
	return queue;
}

void* peek_tail_of_queue (fifo_t const*const queue) {
	if (!queue->size) {
		LOG (error,"Queue underflow...\n");
		abort();
	}
	return queue->buffer [queue->tail-1];
}

void* peek_head_of_queue (fifo_t const*const queue) {
	if (!queue->size) {
		LOG (error,"Queue underflow...\n");
		abort();
	}
	return queue->buffer [queue->head];
}

void* remove_head_of_queue (fifo_t *const queue) {
	if (!queue->size) {
		LOG (error,"Queue underflow...\n");
		abort();
	}

	void *const element = queue->buffer [queue->head];
	queue->head = (queue->head==queue->capacity-1?0:queue->head+1);
	--queue->size;

	if (queue->capacity>>2 >= initial_capacity && queue->size == queue->capacity>>2) {
		void** new_buffer = (void**) malloc ((queue->capacity>>1)*sizeof(void*));
		if (new_buffer == NULL) {
			LOG (error,"Queue cannot reserve additional memory...!\n");
			return element;
		}

		if (queue->tail >= queue->head) {
			memcpy (new_buffer,queue->buffer+queue->head,queue->size*sizeof(void*));
		}else{
			memcpy (new_buffer,queue->buffer+queue->head,(queue->capacity-queue->head)*sizeof(void*));
			memcpy (new_buffer+queue->capacity-queue->head,queue->buffer,queue->tail*sizeof(void*));
		}

		free (queue->buffer);
		queue->capacity >>= 1;
		queue->buffer = new_buffer;
		queue->tail = queue->size;
		queue->head = 0;
	}
	return element;
}

void* remove_tail_of_queue (fifo_t *const queue) {
	if (!queue->size) {
		LOG (error,"Queue underflow...\n");
		abort();
	}

	--queue->size;
	queue->tail = queue->tail ? queue->tail-1 : queue->capacity-1;

	void *const element = queue->buffer [queue->tail];

	if (queue->capacity>>2 >= initial_capacity && queue->size == (queue->capacity>>2)) {
		void** new_buffer = (void**) malloc ((queue->capacity>>1)*sizeof(void*));
		if (new_buffer == NULL) {
			LOG (error,"Queue cannot reserve additional memory...!\n");
			return element;
		}

		if (queue->tail >= queue->head)
			memcpy (new_buffer,queue->buffer+queue->head,queue->size*sizeof(void*));
		else{
			memcpy (new_buffer,queue->buffer+queue->head,(queue->capacity-queue->head)*sizeof(void*));
			memcpy (new_buffer+queue->capacity-queue->head,queue->buffer,queue->tail*sizeof(void*));
		}

		free (queue->buffer);
		queue->capacity >>= 1;
		queue->buffer = new_buffer;
		queue->tail = queue->size;
		queue->head = 0;
	}
	return element;
}

void expand_queue (fifo_t *const queue) {
        void** new_buffer = (void**) malloc ((queue->capacity<<1)*sizeof(void*));
        if (new_buffer == NULL) {
                LOG (error,"Unable to reserve additional memory for the queue...\n");
                return;
        }

        if (queue->tail > queue->head) {
                memcpy (new_buffer,queue->buffer+queue->head,queue->size*sizeof(void*));
        }else{
                memcpy (new_buffer, queue->buffer+queue->head, (queue->capacity-queue->head)*sizeof(void*));
                memcpy (new_buffer+queue->capacity-queue->head, queue->buffer, queue->tail*sizeof(void*));
        }

        free (queue->buffer);
        queue->tail = queue->size;
        queue->head = 0;
        queue->capacity <<= 1;
        queue->buffer = new_buffer;
}

void insert_at_tail_of_queue (fifo_t *const queue, void *const element) {
	if (queue->size > queue->capacity) {
		LOG (error,"Queue buffer overrun detected... Program will terminate.\n");
		abort();
	}

	if (queue->size == queue->capacity) {
		void** new_buffer = (void**) malloc ((queue->capacity<<1)*sizeof(void*));
		if (new_buffer == NULL) {
			LOG (error,"Unable to reserve additional memory for the queue...\n");
			return;
		}

		if (queue->tail > queue->head) {
			memcpy (new_buffer,queue->buffer+queue->head,queue->size*sizeof(void*));
		}else{
			memcpy (new_buffer, queue->buffer+queue->head, (queue->capacity-queue->head)*sizeof(void*));
			memcpy (new_buffer+queue->capacity-queue->head, queue->buffer, queue->tail*sizeof(void*));
		}

		free (queue->buffer);
		queue->tail = queue->size;
		queue->head = 0;
		queue->capacity <<= 1;
		queue->buffer = new_buffer;
	}

	queue->buffer [queue->tail] = element;
	queue->tail = queue->tail == queue->capacity-1 ? 0 : queue->tail+1;
	queue->size++;
}

void insert_at_head_of_queue (fifo_t *const queue, void *const element) {
	if (queue->size > queue->capacity) {
		LOG (error,"Queue buffer overrun detected... Program will terminate.\n");
		abort();
	}

	if (queue->size == queue->capacity) {
		void** new_buffer = (void**) malloc ((queue->capacity<<1)*sizeof(void*));
		if (new_buffer == NULL) {
			LOG (error,"Unable to reserve additional memory for the queue...\n");
			return;
		}

		if (queue->tail > queue->head) {
			memcpy (new_buffer+1, queue->buffer+queue->head, queue->size*sizeof(void*));
		}else{
			memcpy (new_buffer+1, queue->buffer+queue->head, (queue->capacity-queue->head)*sizeof(void*));
			memcpy (new_buffer+1+queue->capacity-queue->head, queue->buffer, queue->tail*sizeof(void*));
		}
		*new_buffer = element;

		free (queue->buffer);
		queue->size++;
		queue->tail = queue->size;
		queue->head = 0;
		queue->capacity <<= 1;
		queue->buffer = new_buffer;
	}else{
		queue->head = queue->head>0 ? queue->head-1 : queue->capacity-1;
		queue->buffer [queue->head] = element;
		queue->size++;
	}
}
