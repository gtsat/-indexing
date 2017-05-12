#ifndef PRIORITY_QUEUE_H_
#define PRIORITY_QUEUE_H_

#include "defs.h"

priority_queue_t* new_priority_queue (int (*)(void const*const,void const*const));

void clear_priority_queue (priority_queue_t *const);

void delete_priority_queue (priority_queue_t *const);

void* peek_priority_queue (priority_queue_t const*const);

void* remove_from_priority_queue (priority_queue_t *const);

void* remove_priority_queue_element (priority_queue_t *const, uint64_t const position);

void insert_into_priority_queue (priority_queue_t *const, void *const);

fifo_t* get_priority_queue_entries (priority_queue_t *const);

#endif /* PRIORITY_QUEUE_H_ */
