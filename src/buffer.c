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

void** adjust_buffer (void** buffer, uint64_t const old_size, uint64_t const new_size) {
	if (!new_size) {
		return buffer;
	}
	if (new_size > old_size) {
		void** new_buffer = (void**) calloc (new_size,new_size*sizeof(void*));
		if (new_buffer == NULL) {
			LOG (error,"Unable to dynamically allocate additional memory...!\n");
			abort ();
			return buffer;
		}
		if (old_size) {
			memcpy (new_buffer,buffer,old_size*sizeof(void*));
			free (buffer);
		}
		buffer = new_buffer;
	}else if (new_size < old_size) {
		if (!new_size) {
			free(buffer);
			buffer = NULL;
		}else{
			void** new_buffer = (void**) calloc (new_size,new_size*sizeof(void*));
			if (new_buffer == NULL) {
				LOG (error,"Unable to unallocate redundant memory...!\n");
				return buffer;
			}

			memcpy (new_buffer,buffer,new_size*sizeof(void*));
			free (buffer);
			buffer = new_buffer;
		}
	}
	assert (buffer != NULL);
	return buffer;
}
