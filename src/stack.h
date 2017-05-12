#ifndef __STACK_H__
#define __STACK_H__

#include "defs.h"
#include "queue.h"

lifo_t* new_stack (void);

void clear_stack (lifo_t *const);
void delete_stack (lifo_t *const);

void* remove_from_stack (lifo_t *const);
void* peek_at_stack (lifo_t const*const);

void insert_into_stack (lifo_t *const, void *const);

void* remove_from_position (lifo_t *const stack, uint64_t const position);
void* get_at_position (lifo_t const*const stack, uint64_t const position);
void insert_into_position (lifo_t *const stack, uint64_t const pos, void *const element);

uint64_t find_position_in_sorted (lifo_t *const stack, void *const element, int (*cmp)(void *const,void *const));

fifo_t* transform_into_queue (lifo_t *const);

#endif /* __STACK_H__ */
