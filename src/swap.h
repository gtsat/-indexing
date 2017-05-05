#ifndef __SWAP_H__
#define __SWAP_H__

#include "defs.h"

/**
 * Input the id of the page used last along 
 * with its timestap for priority. If it is
 * a new and unseen page and the swap is full,
 * it then returns the id of the least recently 
 * used page, which is also deleted from memory. 
 * Otherwise, if it has already been seen, then
 * its priority is updated accordingly, or if 
 * there is still adequate space left, it is
 * then added accordingly. The purpose of this 
 * is to maintain only a fixed number of pages 
 * at any given time!
 */

size_t set_priority (swap_t *const, size_t const id, double const priority);
boolean unset_priority (swap_t *const, size_t const id);

swap_t* new_swap (size_t const capacity);
void delete_swap (swap_t *const);
void clear_swap (swap_t *const);

boolean is_active_identifier (swap_t const*const, size_t const);

#endif /* __SWAP_H__ */
