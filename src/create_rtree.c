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

#include "common.h"
#include "rtree.h"
#include "unistd.h"
#include "getopt.h"

uint32_t DIMENSIONS;
uint32_t PAGESIZE;
char* HEAPFILE;
char* DATASET;

static
void print_notice (void) {
	puts ("\n Copyright (C) 2016 George Tsatsanifos <gtsatsanifos@gmail.com>\n");
	puts (" #indexing comes with ABSOLUTELY NO WARRANTY. This is free software, ");
	puts (" and you are welcome to redistribute it under certain conditions.\n");
}

static
void print_usage (char const*const program) {
	printf (" ** Usage:\t %s [option] [parameter]\n", program);
	puts ("\t\t-d --dims :\t The number of dimensions.");
	puts ("\t\t-b --block :\t The desired size of each block.");
	puts ("\t\t-a --dataset :\t The path to the datafile.");
	puts ("\t\t-t --tree :\t The path to the binary heap-file.");
}

static
void process_arguments (int argc,char *argv[]) {
	char const*const short_options = "ud:b:a:t:";
	const struct option long_options [] = {
		{"usage",0,NULL,'u'},
		{"dims",1,NULL,'d'},
		{"block",1,NULL,'b'},
		{"data",1,NULL,'a'},
		{"tree",1,NULL,'t'},
		{NULL,0,NULL,0}
	};

	int next_option;

	do{
		next_option = getopt_long (argc,argv,short_options,long_options,NULL);

		switch (next_option) {
		case 'u':
			print_usage (argv[0]);
			exit (EXIT_SUCCESS);
		case 'd':
			DIMENSIONS = atoi (optarg);
			break;
		case 'b':
			PAGESIZE = atoi (optarg);
			break;
		case 'a':
			DATASET = optarg;
			break;
		case 't':
			HEAPFILE = optarg;
			break;
		case -1:
			break;
		case '?':
			LOG (error,"Unknown option parameter: %s\n",optarg);
		default:
			print_usage (argv[0]);
			exit (EXIT_FAILURE);
		}
	}while(next_option!=-1);
}

int main (int argc, char* argv[]) {
	print_notice ();
	process_arguments (argc,argv);

	if (!DIMENSIONS) {
		LOG (error,"Please specify the dimensionality of the domain...\n");
	}
	if (!PAGESIZE) {
		LOG (error,"Please specify a block-size...\n");
	}
	if (!DATASET) {
		LOG (error,"Please specify a dataset to be indexed...\n");
	}
	if (!HEAPFILE) {
		LOG (error,"Please specify a filepath for the produced heapfile...\n");
	}

	if (DIMENSIONS && PAGESIZE && DATASET && HEAPFILE) {
		unlink (HEAPFILE);
		tree_t *const tree = new_rtree (HEAPFILE,PAGESIZE,DIMENSIONS);
		insert_records_from_textfile (tree,DATASET);
		//flush_tree (tree);
		//delete_records_from_textfile (tree,DATASET);
		delete_rtree (tree);
		return EXIT_SUCCESS;
	}else{
		print_usage (argv[0]);
		return EXIT_FAILURE;
	}
}

