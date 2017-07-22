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
#include <math.h>


boolean equal_keys (index_t const key1[],
			index_t const key2[],
			uint32_t const dimensions) {
	for (uint32_t j=0; j<dimensions; ++j) {
		double tmp = key1[j] - key2[j];
		if (tmp > DBL_MIN || tmp < -DBL_MIN) {
			return false;
		}
	}
	return true;
}

boolean key_enclosed_by_box (index_t const key[],
				 interval_t const box[],
				 uint32_t const dimensions) {
	for (uint32_t j=0; j<dimensions; ++j) {
		if (key[j] < box[j].start || key[j] > box[j].end) {
			return false;
		}
	}
	return true;
}

boolean box_enclosed_by_box (interval_t const small_box[],
				 interval_t const big_box[],
				 uint32_t const dimensions) {
	for (uint32_t j=0; j<dimensions; ++j) {
		if (small_box[j].start < big_box[j].start || small_box[j].end > big_box[j].end) {
			return false;
		}
	}
	return true;
}

boolean overlapping_boxes  (interval_t const box1[],
				interval_t const box2[],
				uint32_t const dimensions) {
	//index_t DBG1START = box1[1].start, DBG2START = box2[1].start;
	//index_t DBG1END = box1[1].end, DBG2END = box2[1].end;
	for (uint32_t j=0; j<dimensions; ++j) {
		if (box1[j].start > box2[j].end || box1[j].end < box2[j].start) {
			return false;
		}
	}
	return true;
}

int mincompare_symbol_table_entries (void const*const x, void const*const y) {
	symbol_table_entry_t* xentry = (symbol_table_entry_t*)x;
	symbol_table_entry_t* yentry = (symbol_table_entry_t*)y;

	if (xentry->key < yentry->key) return -1;
	else if (xentry->key > yentry->key) return 1;
	else return 0;
}

int mincompare_containers (void const*const x, void const*const y) {
	data_container_t* xcontainer = (data_container_t*)x;
	data_container_t* ycontainer = (data_container_t*)y;

	if (xcontainer->sort_key < ycontainer->sort_key) return -1;
	else if (xcontainer->sort_key > ycontainer->sort_key) return 1;
	else return 0;
}

int maxcompare_containers (void const*const x, void const*const y) {
	data_container_t* xcontainer = (data_container_t*)x;
	data_container_t* ycontainer = (data_container_t*)y;

	if (xcontainer->sort_key > ycontainer->sort_key) return -1;
	else if (xcontainer->sort_key < ycontainer->sort_key) return 1;
	else return 0;
}

double key_to_key_distance (index_t const key1[],
				index_t const key2[],
				uint32_t const dimensions) {
	double distance = 0;
	for (uint32_t j=0; j<dimensions; ++j)
		distance += pow(key1[j]-key2[j],2);
	return sqrt(distance);
}

double key_to_box_mindistance (index_t const key[],
				interval_t const box[],
				uint32_t const dimensions) {
	double distance = 0;
	for (uint32_t i=0; i<dimensions; ++i) {
		if (key[i] < box[i].start)
			distance += (box[i].start-key[i])*(box[i].start-key[i]);
		else if (key[i] > box[i].end)
			distance += pow(key[i]-box[i].end,2);
	}
	return sqrt(distance);
}

double key_to_box_maxdistance (index_t const key[],
				interval_t const box[],
				uint32_t const dimensions) {
	double distance = 0;
	for (uint32_t i=0; i<dimensions; ++i) {
		if (key[i] < box[i].start)
			distance += pow(box[i].end-key[i],2);
		else if (key[i] > box[i].end)
			distance += pow(key[i]-box[i].start,2);
		else{
			double temp1 = pow(key[i]-box[i].start,2);
			double temp2 = pow(box[i].end-key[i],2);
			distance += MAX (temp1,temp2);
		}
	}
	return sqrt(distance);
}

double box_to_box_mindistance (interval_t const box1[],
				interval_t const box2[],
				uint32_t const dimensions) {
	double distance = 0;
	for (uint32_t j=0; j<dimensions; ++j) {
		if (box1[j].end < box2[j].start)
			distance += pow(box2[j].start-box1[j].end,2);
		else if (box2[j].end < box1[j].start)
			distance += pow(box1[j].start-box2[j].end,2);
	}
	return sqrt(distance);
}

double box_to_box_maxdistance (interval_t const box1[],
				interval_t const box2[],
				uint32_t const dimensions) {
	double distance = 0;
	for (uint32_t j=0; j<dimensions; ++j) {
		if (box1[j].end < box2[j].start)
			distance += pow(box2[j].end-box1[j].start,2);
		else if (box2[j].end < box1[j].start)
			distance += pow(box1[j].end-box2[j].start,2);
		else{
			double temp1 = pow(box2[j].end-box1[j].start,2);
			double temp2 = pow(box1[j].end-box2[j].start,2);
			distance += MAX (temp1,temp2);
		}
	}
	return sqrt(distance);
}


/**
 * A note about processing skyline queries:
 * When corner[j]==false, it means that we are interested
 * in small values along dimension #j. The opposite holds
 * for corner[j]==true.
 */

boolean dominated_key  (index_t const key[], index_t const reference_point[],
						boolean const corner[], uint32_t const dimensions) {

	for (uint32_t j=0; j<dimensions; ++j) {
		if (corner[j]) {
			if (reference_point[j] < key[j]) {
				return false;
			}
		}else{
			if (reference_point[j] > key[j]) {
				return false;
			}
		}
	}

	return true;
}

boolean dominated_box (interval_t const box[],
						index_t const reference_point[],
						boolean const corner[],
						uint32_t const dimensions) {

	for (uint32_t j=0; j<dimensions; ++j) {
		if (corner[j]) {
			if (reference_point[j] < box[j].end) {
				return false;
			}
		}else{
			if (reference_point[j] > box[j].start) {
				return false;
			}
		}
	}
	return true;
}

double mindistance_ordered_multikey (multidata_container_t const*const multikey, uint32_t const offset) {
	double mindist = DBL_MAX;
	for (uint32_t i=offset+1; i<multikey->cardinality; ++i) {
		double tempdist = key_to_key_distance (multikey->keys+(i-1)*multikey->dimensions,
							multikey->keys+i*multikey->dimensions,
							multikey->dimensions);
		if (tempdist < mindist) mindist = tempdist;
	}
	return mindist;
}

double maxdistance_ordered_multikey (multidata_container_t const*const multikey, uint32_t const offset) {
	double maxdist = 0;
	for (uint32_t i=offset+1; i<multikey->cardinality; ++i) {
		double tempdist = key_to_key_distance (multikey->keys+(i-1)*multikey->dimensions,
							multikey->keys+i*multikey->dimensions,
							multikey->dimensions);
		if (tempdist > maxdist) maxdist = tempdist;
	}
	return maxdist;
}

double avgdistance_ordered_multikey (multidata_container_t const*const multikey, uint32_t const offset) {
	double sumdist = 0;
	for (uint32_t i=offset+1; i<multikey->cardinality; ++i)
		sumdist += key_to_key_distance (multikey->keys+(i-1)*multikey->dimensions,
						multikey->keys+i*multikey->dimensions,
						multikey->dimensions);
	return sumdist/(multikey->cardinality-offset-1);
}

double max_mindistance_ordered_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double maxdist = 0;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i) {
		double tempdist = box_to_box_mindistance (multibox->boxes+(i-1)*multibox->dimensions,
								multibox->boxes+i*multibox->dimensions,
								multibox->dimensions);
		if (tempdist > maxdist) maxdist = tempdist;
	}
	return maxdist;
}

double min_maxdistance_ordered_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double mindist = DBL_MAX;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i) {
		double tempdist = box_to_box_maxdistance (multibox->boxes+(i-1)*multibox->dimensions,
															multibox->boxes+i*multibox->dimensions,
															multibox->dimensions);
		if (tempdist < mindist) mindist = tempdist;
	}
	return mindist;
}

double avg_mindistance_ordered_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double sumdist = 0;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i)
		sumdist += box_to_box_mindistance (multibox->boxes+(i-1)*multibox->dimensions,
							multibox->boxes+i*multibox->dimensions,
							multibox->dimensions);
	return sumdist/(multibox->cardinality-1);
}

double avg_maxdistance_ordered_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double sumdist = 0;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i)
		sumdist += box_to_box_maxdistance (multibox->boxes+(i-1)*multibox->dimensions,
							multibox->boxes+i*multibox->dimensions,
							multibox->dimensions);
	return sumdist/(multibox->cardinality-1);
}

double mindistance_pairwise_multikey (multidata_container_t const*const multikey, uint32_t const offset) {
	double mindist = DBL_MAX;
	for (uint32_t i=offset+1; i<multikey->cardinality; ++i) {
		for (uint32_t j=offset; j<i; ++j) {
			double tempdist = key_to_key_distance (multikey->keys+j*multikey->dimensions,
								multikey->keys+i*multikey->dimensions,
								multikey->dimensions);
			if (tempdist < mindist) mindist = tempdist;
		}
	}
	return mindist;
}

double maxdistance_pairwise_multikey (multidata_container_t const*const multikey, uint32_t const offset) {
	double maxdist = 0;
	for (uint32_t i=offset+1; i<multikey->cardinality; ++i) {
		for (uint32_t j=offset; j<i; ++j) {
			double tempdist = key_to_key_distance (multikey->keys+j*multikey->dimensions,
								multikey->keys+i*multikey->dimensions,
								multikey->dimensions);
			if (tempdist > maxdist) maxdist = tempdist;
		}
	}
	return maxdist;
}

double avgdistance_pairwise_multikey (multidata_container_t const*const multikey, uint32_t const offset) {
	double sumdist = 0;
	for (uint32_t i=offset+1; i<multikey->cardinality; ++i) {
		for (uint32_t j=offset; j<i; ++j) {
			sumdist += key_to_key_distance (multikey->keys+j*multikey->dimensions,
							multikey->keys+i*multikey->dimensions,
							multikey->dimensions);
		}
	}
	return 2*sumdist/(multikey->cardinality-offset)/(multikey->cardinality-offset-1);
}

double max_mindistance_pairwise_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double maxdist = 0;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i) {
		for (uint32_t j=offset; j<i; ++j) {
			double tempdist = box_to_box_mindistance (multibox->boxes+j*multibox->dimensions,
									multibox->boxes+i*multibox->dimensions,
									multibox->dimensions);
			if (tempdist > maxdist) maxdist = tempdist;
		}
	}
	return maxdist;
}

double min_maxdistance_pairwise_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double mindist = DBL_MAX;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i) {
		for (uint32_t j=offset; j<i; ++j) {
			double tempdist = box_to_box_maxdistance (multibox->boxes+j*multibox->dimensions,
								multibox->boxes+i*multibox->dimensions,
								multibox->dimensions);
			if (tempdist < mindist) mindist = tempdist;

		}
	}
	return mindist;
}

double avg_mindistance_pairwise_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double sumdist = 0;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i) {
		for (uint32_t j=offset; j<i; ++j) {
			sumdist += box_to_box_mindistance (multibox->boxes+j*multibox->dimensions,
								multibox->boxes+i*multibox->dimensions,
								multibox->dimensions);
		}
	}
	return 2*sumdist/multibox->cardinality/(multibox->cardinality-1);
}

double avg_maxdistance_pairwise_multibox (multibox_container_t const*const multibox, uint32_t const offset) {
	double sumdist = 0;
	for (uint32_t i=offset+1; i<multibox->cardinality; ++i) {
		for (uint32_t j=offset; j<i; ++j) {
			sumdist += box_to_box_maxdistance (multibox->boxes+j*multibox->dimensions,
								multibox->boxes+i*multibox->dimensions,
								multibox->dimensions);
		}
	}
	return 2*sumdist/multibox->cardinality/(multibox->cardinality-1);
}

int mincompare_multicontainers (void const*const x, void const*const y) {
	if (((multidata_container_t*)x)->sort_key < ((multidata_container_t*)y)->sort_key) return -1;
	else if(((multidata_container_t*)x)->sort_key > ((multidata_container_t*)y)->sort_key) return 1;
	return 0;
}

int maxcompare_multicontainers (void const*const x, void const*const y) {
	if (((multidata_container_t*)x)->sort_key > ((multidata_container_t*)y)->sort_key) return -1;
	else if(((multidata_container_t*)x)->sort_key < ((multidata_container_t*)y)->sort_key) return 1;
	return 0;
}

arc_t* new_arc (object_t const from, object_t const to, arc_weight_t const weight) {
	arc_t* arc = (arc_t*) malloc (sizeof(arc_t));

	arc->weight = weight;
	arc->from = from;
	arc->to = to;

	return arc;
}

double pairwise_mindistance (fifo_t const*const queue, uint32_t dimensions) {
	double mindistance = DBL_MAX;
	for (register uint32_t i=1; i<queue->size; ++i) {
		for (register uint32_t j=0; j<i; ++j) {
			double temp_dist = key_to_key_distance (
								((data_pair_t *const)queue->buffer[i])->key,
								((data_pair_t *const)queue->buffer[j])->key,
								dimensions);
			if (temp_dist < mindistance) {
				mindistance = temp_dist;
			}
		}
	}
	return mindistance;
}

