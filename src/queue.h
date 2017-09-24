#ifndef QUEUE_H_
#define QUEUE_H_

#include "defs.h"

fifo_t* new_queue (void);

void clear_queue (fifo_t *const queue);
void delete_queue (fifo_t *const queue);

void* remove_tail_of_queue (fifo_t *const queue);
void* remove_head_of_queue (fifo_t *const queue);

void* peek_tail_of_queue (fifo_t const*const queue);
void* peek_head_of_queue (fifo_t const*const queue);

void* get_queue_element (fifo_t const*const queue, uint64_t const position);

void insert_at_tail_of_queue (fifo_t *const queue, void *const element);
void insert_at_head_of_queue (fifo_t *const queue, void *const element);

void insert_queue_element (fifo_t *const queue, uint64_t const pos, void *const element);
void* remove_queue_element (fifo_t *const queue, uint64_t const pos);

uint64_t find_position_in_sorted_queue (fifo_t *const queue, void *const element, int (*cmp)(void *const,void *const));

void expand_queue (fifo_t *const queue);

#endif /* QUEUE_H_ */
