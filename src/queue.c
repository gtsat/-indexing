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

void* get_queue_element (fifo_t const*const queue, uint64_t const k) {
	if (k > queue->size){
		return NULL;
	}else if (queue->head+k < queue->capacity) {
		return queue->buffer[queue->head+k];
	}else{
		return queue->buffer[k+queue->head-queue->capacity];
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
			LOG (fatal,"Queue cannot reserve memory for buffer...\n");
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
		LOG (fatal,"Cannot allocate memory for new queue...\n");
		exit (EXIT_FAILURE);
	}
	queue->buffer = (void**) malloc (initial_capacity*sizeof(void*));
	if (queue->buffer == NULL) {
		LOG (fatal,"Queue cannot reserve memory for buffer...\n");
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
		return NULL;
	}
	return queue->buffer [queue->tail-1];
}

void* peek_head_of_queue (fifo_t const*const queue) {
	if (!queue->size) {
		LOG (error,"Queue underflow...\n");
		return NULL;
	}
	return queue->buffer [queue->head];
}

void* remove_head_of_queue (fifo_t *const queue) {
	if (!queue->size) {
		LOG (error,"Queue underflow...\n");
		return NULL;
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
		return NULL;
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
		LOG (fatal,"Queue buffer overrun detected... Program will terminate.\n");
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
		LOG (fatal,"Queue buffer overrun detected... Program will terminate.\n");
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

void insert_queue_element (fifo_t *const queue, uint64_t const pos, void *const element) {
	if (queue->size == queue->capacity) {
		insert_at_tail_of_queue (queue,0);
		queue->size--;
		if (queue->tail) {
			queue->tail--;
		}else{
			queue->tail = queue->capacity-1;
		}
	}

	if (pos <= queue->size) {
		uint64_t const new_pos = queue->head + pos >= queue->capacity ?
						pos + queue->head - queue->capacity
						: queue->head + pos;

		if (new_pos < queue->tail) {
			memmove (queue->buffer+queue->head+new_pos+1,
				queue->buffer+queue->head+new_pos,
				(queue->tail-new_pos)*sizeof(void*));
		}

		queue->buffer [queue->head+new_pos] = element;

		queue->size++;
		queue->tail++;
		if (queue->tail >= queue->capacity) {
			queue->tail = 0;
		}
	}else{
		LOG (error,"Cannot add an element in a list of %lu elements at position %lu...\n",queue->size,pos);
	}
}

void* remove_queue_element (fifo_t *const queue, uint64_t const pos) {
	void* rval = NULL;
	if (pos < queue->size) {
		uint64_t const new_pos = queue->head + pos >= queue->capacity ?
						pos + queue->head - queue->capacity
						: queue->head + pos;

		rval = queue->buffer [new_pos];

		if (new_pos < queue->tail) {
			memmove (queue->buffer+queue->head+new_pos,
				 queue->buffer+queue->head+new_pos+1,
				 (queue->tail-new_pos)*sizeof(void*));
		}

		queue->size--;
		if (queue->tail) {
			queue->tail--;
		}else{
			queue->tail = queue->capacity-1;
		}
	}else{
		LOG (error,"Cannot remove an element in a list of %lu elements from position %lu...\n",queue->size,pos);
	}
	return rval;
}


uint64_t find_position_in_sorted_queue (fifo_t *const queue, void *const element, int (*cmp)(void *const,void *const)) {
	if (queue->tail == queue->head) {
		assert (!queue->size);
		LOG (error,"Unable to find element in an empty list.\n");
		return 0;
	}

	uint64_t lo = 0;
	uint64_t hi = queue->size;
	uint64_t m = queue->size>>1;
	for (;m<hi;) {
		if (cmp (get_queue_element(queue,hi-1),get_queue_element(queue,lo)) < 0) {
			LOG (error,"Queue elements do not appear in order...\n");
			abort();
			break;
		}else{
			int balance = cmp (element,get_queue_element(queue,m));
			if (!balance) {
				return m;
			}else if (balance<0) {
				hi = m;
			}else{
				lo = m+1;
			}
			m = (lo+hi)>>1;
		}
	}
	return m;
}


