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

#include "symbol_table.h"
#include "queue.h"


symbol_table_t* new_symbol_table_primitive (value_t const value) {
	symbol_table_t* rbtree = (symbol_table_t*) malloc (sizeof(symbol_table_t));
	if (rbtree == NULL)  {
		LOG (error,"Unable allocate memory to create symbol-table...\n");
		exit (EXIT_FAILURE);
	}else{
		rbtree->default_value = value;
		rbtree->compare = NULL;
		rbtree->root = NULL;
		rbtree->size = 0;
		return rbtree;
	}
}

symbol_table_t* new_symbol_table (value_t const value, int (*compare) (key__t const, key__t const)) {
	symbol_table_t* rbtree = (symbol_table_t*) malloc (sizeof(symbol_table_t));
	if (rbtree == NULL)  {
		LOG (error,"Unable allocate memory to create symbol-table...\n");
		exit (EXIT_FAILURE);
	}else{
		rbtree->default_value = value;
		rbtree->compare = compare;
		rbtree->root = NULL;
		rbtree->size = 0;
		return rbtree;
	}
}

key__t max_key (symbol_table_t const*const rbtree) {
	if (rbtree->root==NULL) {
		LOG (error,"Cannot return max-value key because the symbol-table is empty.\n");
		return -1;
	}
	tree_node_t* ptr = rbtree->root;
	for (;ptr->right!=NULL;ptr=ptr->right);
	return ptr->key;
}

value_t get (symbol_table_t const*const rbtree, key__t const key) {
	if (rbtree->compare!=NULL) {
		for (tree_node_t* ptr=rbtree->root; ptr != NULL;
			 ptr=rbtree->compare(key,ptr->key)<0?ptr->left:ptr->right)
			if (rbtree->compare(key,ptr->key)==0)
				return ptr->value;
	}else{
		for (tree_node_t* ptr=rbtree->root; ptr != NULL;
			ptr=key<ptr->key?ptr->left:ptr->right)
			if (key==ptr->key)
				return ptr->value;
	}
	LOG (info,"Requested key %u was not found in the symbol-table...\n",key);
	return rbtree->default_value;
}

static
void inorder_traversal_recursive (tree_node_t const*const tree_node,
					fifo_t *const queue,
					boolean const get_entries,
					boolean (*filter) (key__t const)) {

	if (tree_node->left != NULL)
		inorder_traversal_recursive (tree_node->left,queue,get_entries,filter);
	if (filter(tree_node->key)) {
		if (get_entries) {
			symbol_table_entry_t* entry = (symbol_table_entry_t*) malloc (sizeof(symbol_table_entry_t));
			if (entry == NULL) {
				LOG (error,"Unable to reserve enough additional memory to "
							"congregate the entries of the symbol-table.\n");
				abort();
			}
			entry->key = tree_node->key;
			entry->value = tree_node->value;
			insert_at_tail_of_queue (queue,entry);
		}else insert_at_tail_of_queue (queue,tree_node->key);
	}
	if (tree_node->right != NULL)
		inorder_traversal_recursive (tree_node->right,queue,get_entries,filter);
}

static
void preorder_traversal_recursive (tree_node_t const*const tree_node,
					fifo_t *const queue,
					boolean const get_entries,
					boolean (*filter) (key__t const)) {

	if (filter(tree_node->key)) {
		if (get_entries) {
			symbol_table_entry_t* entry = (symbol_table_entry_t*) malloc (sizeof(symbol_table_entry_t));
			entry->key = tree_node->key;
			entry->value = tree_node->value;
			insert_at_tail_of_queue (queue,entry);
		}else insert_at_tail_of_queue (queue,tree_node->value);
	}
	if (tree_node->left != NULL)
		preorder_traversal_recursive (tree_node->left,queue,get_entries,filter);
	if (tree_node->right != NULL)
		preorder_traversal_recursive (tree_node->right,queue,get_entries,filter);
}

static
void map_keys_recursive (symbol_table_t const*const rbtree,
				tree_node_t *const tree_node,
				key__t (*map)(key__t const,va_list),
				va_list args) {
	if (tree_node->left==NULL && tree_node->right==NULL) {
		va_list args_copy;
		va_copy (args_copy,args);
		tree_node->key = map (tree_node->key,args_copy);
		va_end (args_copy);
	}else if (tree_node->left==NULL) {
		va_list args_copy;
		va_copy (args_copy,args);
		tree_node->key = map (tree_node->key,args_copy);
		va_end (args_copy);

		map_keys_recursive (rbtree,tree_node->right,map,args);
		if ((rbtree->compare==NULL && tree_node->right->key < tree_node->key)
			|| (rbtree->compare!=NULL && rbtree->compare(tree_node->right->key,tree_node->key)<0)) {

			tree_node_t* tree_node_right = tree_node->right;

			value_t swap_value = tree_node->value;
			key__t swap_key = tree_node->key;

			tree_node->value = tree_node_right->value;
			tree_node->key = tree_node_right->key;

			tree_node_right->value = swap_value;
			tree_node_right->key = swap_key;
		}
	}else if (tree_node->right==NULL) {
		va_list args_copy;
		va_copy (args_copy,args);
		tree_node->key = map (tree_node->key,args_copy);
		va_end (args_copy);

		map_keys_recursive (rbtree,tree_node->left,map,args);
		if ((rbtree->compare==NULL && tree_node->left->key > tree_node->key)
			|| (rbtree->compare!=NULL && rbtree->compare(tree_node->left->key,tree_node->key))) {

			tree_node_t* tree_node_left= tree_node->left;

			value_t swap_value = tree_node->value;
			key__t swap_key = tree_node->key;

			tree_node->value = tree_node_left->value;
			tree_node->key = tree_node_left->key;

			tree_node_left->value = swap_value;
			tree_node_left->key = swap_key;
		}
	}else{
		va_list args_copy;
		va_copy (args_copy,args);
		tree_node->key = map (tree_node->key,args_copy);
		va_end (args_copy);

		map_keys_recursive (rbtree,tree_node->left,map,args);
		map_keys_recursive (rbtree,tree_node->right,map,args);
		tree_node_t* tree_node_left = tree_node->left;
		tree_node_t* tree_node_right = tree_node->right;

		if ((rbtree->compare==NULL
			&& tree_node_left->key > tree_node->key
			&& tree_node_right->key > tree_node->key)
			|| (rbtree->compare!=NULL
			&& rbtree->compare(tree_node_left->key,tree_node->key)>0
			&& rbtree->compare(tree_node_right->key,tree_node->key)>0)) {

			value_t swap_value = tree_node->value;
			key__t swap_key = tree_node->key;

			tree_node->value = tree_node_left->value;
			tree_node->key = tree_node_left->key;

			tree_node_left->value = swap_value;
			tree_node_left->key = swap_key;

			if ((rbtree->compare==NULL && tree_node_right->key < tree_node_left->key)
			|| (rbtree->compare!=NULL && rbtree->compare(tree_node_right->key,tree_node_left->key)<0)) {

				value_t swap_value = tree_node_right->value;
				key__t swap_key = tree_node_right->key;

				tree_node_right->value = tree_node_left->value;
				tree_node_right->key = tree_node_left->key;

				tree_node_left->value = swap_value;
				tree_node_left->key = swap_key;
			}
		}else if ((rbtree->compare==NULL
				&& tree_node_right->key < tree_node->key
				&& tree_node_right->key < tree_node_left->key)
				|| (rbtree->compare!=NULL
				&& rbtree->compare(tree_node_right->key,tree_node->key)<0
				&& rbtree->compare(tree_node_right->key,tree_node_left->key)<0)) {

			value_t swap_value = tree_node_right->value;
			key__t swap_key = tree_node_right->key;

			tree_node_right->value = tree_node_left->value;
			tree_node_right->key = tree_node_left->key;

			tree_node_left->value = swap_value;
			tree_node_left->key = swap_key;

			if ((rbtree->compare==NULL && tree_node_right->key < tree_node->key)
				|| (rbtree->compare!=NULL
				&& rbtree->compare(tree_node_right->key,tree_node->key)<0)) {

				value_t swap_value = tree_node_right->value;
				key__t swap_key = tree_node_right->key;

				tree_node_right->value = tree_node->value;
				tree_node_right->key = tree_node->key;

				tree_node->value = swap_value;
				tree_node->key = swap_key;
			}
		}else if ((rbtree->compare==NULL && tree_node_right->key < tree_node->key)
			|| (rbtree->compare!=NULL
			&& rbtree->compare(tree_node_right->key,tree_node->key)<0)) {

			value_t swap_value = tree_node_right->value;
			key__t swap_key = tree_node_right->key;

			tree_node_right->value = tree_node->value;
			tree_node_right->key = tree_node->key;

			tree_node->value = swap_value;
			tree_node->key = swap_key;
		}
	}
}

void map_keys (symbol_table_t const*const rbtree,
		key__t (*map)(key__t const,va_list),...) {
	va_list args;
	va_start (args,map);
	if (rbtree->root != NULL)
		map_keys_recursive (rbtree,rbtree->root,map,args);
	va_end (args);
}

static
boolean return_true (key__t const x) {return true;}

fifo_t* get_keys (symbol_table_t const*const rbtree) {
	fifo_t* queue = new_queue ();
	if (rbtree->root != NULL)
		inorder_traversal_recursive (rbtree->root,queue,false,&return_true);
	return queue;
}

fifo_t* get_values (symbol_table_t const*const rbtree) {
	fifo_t* queue = new_queue ();
	if (rbtree->root != NULL)
		preorder_traversal_recursive (rbtree->root,queue,false,&return_true);
	return queue;
}

fifo_t* get_entries (symbol_table_t const*const rbtree) {
	fifo_t* queue = new_queue ();
	if (rbtree->root != NULL)
		inorder_traversal_recursive (rbtree->root,queue,true,&return_true);
	return queue;
}

fifo_t* filter_keys (symbol_table_t const*const rbtree, boolean (*filter)(key__t const)) {
	fifo_t* queue = new_queue ();
	if (rbtree->root != NULL)
		inorder_traversal_recursive (rbtree->root,queue,false,filter);
	return queue;
}

static
void range_recursive (symbol_table_t const*const rbtree,
							tree_node_t const*const tree_node,
							key__t const from, key__t const to,
							fifo_t *const queue) {
	if (rbtree->compare!=NULL) {
		if (rbtree->compare(from,tree_node->key)<=0 && rbtree->compare(to,tree_node->key)>=0) insert_at_tail_of_queue (queue,tree_node->key);
		if (tree_node->left!=NULL && rbtree->compare(from,tree_node->key)<0) range_recursive (rbtree,tree_node->left,from,to,queue);
		if (tree_node->right!=NULL && rbtree->compare(to,tree_node->key)<0) range_recursive (rbtree,tree_node->right,from,to,queue);
	}else{
		if (from<=tree_node->key && to>=tree_node->key) insert_at_tail_of_queue (queue,tree_node->key);
		if (tree_node->left!=NULL && from<tree_node->key) range_recursive (rbtree,tree_node->left,from,to,queue);
		if (tree_node->right!=NULL && to<tree_node->key) range_recursive (rbtree,tree_node->right,from,to,queue);
	}
}

fifo_t* key_range (symbol_table_t const*const rbtree, key__t const from, key__t const to) {
	fifo_t* queue = new_queue ();
	if (rbtree->root != NULL) range_recursive (rbtree,rbtree->root,from,to,queue);
	return queue;
}

static
tree_node_t* delete_tree_node_recursive (tree_node_t *const tree_node) {
	if (tree_node != NULL) {
		if (tree_node->left != NULL)
			tree_node->left = delete_tree_node_recursive (tree_node->left);

		if (tree_node->right != NULL)
			tree_node->right = delete_tree_node_recursive (tree_node->right);

		free (tree_node);
	}

	return NULL;
}

void delete_symbol_table (symbol_table_t *const rbtree) {
	if (rbtree->root != NULL)
		rbtree->root = delete_tree_node_recursive (rbtree->root);
	free (rbtree);
}

void clear_symbol_table (symbol_table_t *const rbtree) {
	if (rbtree->root != NULL) {
		rbtree->root = delete_tree_node_recursive (rbtree->root);
	}
}

/* when a (new) right red node is traced */
static tree_node_t* rotate_left (tree_node_t *const tree_node) {
	tree_node_t* new_branch_root = tree_node->right;
	tree_node->right = new_branch_root->left;
	new_branch_root->left = tree_node;

	new_branch_root->color = tree_node->color;
	tree_node->color = red;


	tree_node->size = 1 + (tree_node->left!=NULL?tree_node->left->size:0)
							+ (tree_node->right!=NULL?tree_node->right->size:0);

	new_branch_root->size = 1 + tree_node->size +
						(new_branch_root->right!=NULL?new_branch_root->right->size:0);

	return new_branch_root;
}

/* when two consecutive left red nodes are encountered */
static tree_node_t* rotate_right (tree_node_t* tree_node) {
	tree_node_t* new_branch_root = tree_node->left;
	tree_node->left = new_branch_root->right;
	new_branch_root->right = tree_node;

	new_branch_root->right->color = red;

	tree_node->size = 1 + (tree_node->left!=NULL?tree_node->left->size:0)
							+ (tree_node->right!=NULL?tree_node->right->size:0);

	new_branch_root->size = 1 + tree_node->size +
						(new_branch_root->left!=NULL?new_branch_root->left->size:0);

	return new_branch_root;
}

/* both left and right nodes are red */
static color_t invert_colors (tree_node_t *const tree_node) {
	if (tree_node->left != NULL 
	&& tree_node->right != NULL 
	&& tree_node->left->color == red 
	&& tree_node->right->color == red) {
		tree_node->left->color = black;
		tree_node->right->color = black;
		return tree_node->color = !tree_node->color;
	}else return tree_node->color;
}

static
tree_node_t* insert_node_recursive (symbol_table_t const*const rbtree,
											tree_node_t *const tree_node,
											key__t const key, value_t const value) {
	if (tree_node == NULL) {
		tree_node_t* new_node = (tree_node_t*) malloc (sizeof(tree_node_t));
		if (new_node == NULL) {
			LOG (error,"Unable allocate additional memory to expand symbol-table...\n");
			exit (EXIT_FAILURE);
		}
		new_node->left = NULL;
		new_node->right = NULL;
		new_node->value = value;
		new_node->key = key;
		new_node->color = red;
		new_node->size = 1;
		return new_node;
	}else{
		if (key<tree_node->key) {
			tree_node->left = insert_node_recursive (rbtree,tree_node->left,key,value);
			if (tree_node->left->color == red
				&& tree_node->left->left != NULL
				&& tree_node->left->left->color == red)
					return rotate_right (tree_node);
		}else if (key>tree_node->key) {
			tree_node->right = insert_node_recursive (rbtree,tree_node->right,key,value);
			if (tree_node->right->color == red) {
				if (tree_node->left != NULL
				&& tree_node->left->color == red)
					invert_colors (tree_node);
				else return rotate_left (tree_node);
			}
		}else{
			tree_node->value = value;
		}

		tree_node->size = 1 + (tree_node->left!=NULL?tree_node->left->size:0)
								+ (tree_node->right!=NULL?tree_node->right->size:0);

		return tree_node;
	}
}

void set (symbol_table_t *const rbtree, key__t const key, value_t const value) {
	//LOG (info,"Inserting into the symbol-table value %u indexed by key %u.\n",value,key);
	rbtree->root = insert_node_recursive (rbtree,rbtree->root,key,value);
	rbtree->size = rbtree->root->size;
}

static
tree_node_t* del_min (tree_node_t* branch) {
	while (branch->left->left->left != NULL) {
		branch = branch->left;
		--branch->size;
	}

	tree_node_t* min = branch->left->left;
	branch->left->left = min->right;

	--branch->left->size;

	return min;
}

static
tree_node_t* remove_key_recursive (symbol_table_t *const rbtree,
											tree_node_t *const tree_node,
											key__t const key) {
	if (tree_node == NULL) return NULL;
	if ((rbtree->compare==NULL && rbtree->compare==NULL && key==tree_node->key)
			|| (rbtree->compare!=NULL && rbtree->compare(key,tree_node->key)==0)) {
		if (tree_node->left != NULL && tree_node->right != NULL) {
			tree_node_t* min_right;
			if (tree_node->right->left == NULL) {
				min_right = tree_node->right;
				min_right->left = tree_node->left;

				free (tree_node);
			}else if (tree_node->right->left->left == NULL) {
				min_right = tree_node->right->left;
				tree_node->right->left = min_right->right;
				min_right->right = tree_node->right;
				min_right->left = tree_node->left;

				tree_node->right->size = 1 + (tree_node->right->left!=NULL?tree_node->right->left->size:0)
										+ (tree_node->right->right!=NULL?tree_node->right->right->size:0);

				free (tree_node);
			}else{
				min_right = del_min (tree_node->right);
				min_right->right = tree_node->right;
				min_right->left = tree_node->left;

				free (tree_node);
			}

			min_right->size = 1 + (min_right->left!=NULL?min_right->left->size:0) + (min_right->right!=NULL?min_right->right->size:0);

			return min_right;
		}else if (tree_node->left != NULL) {
			tree_node_t* temp = tree_node->left;

			tree_node->size = tree_node->left->size + 1;

			free (tree_node);
			return temp;
		}else if (tree_node->right != NULL) {
			tree_node_t* temp = tree_node->right;

			tree_node->size = tree_node->right->size + 1;

			free (tree_node);
			return temp;
		}else{
			free (tree_node);
			return NULL;
		}
	}else if (tree_node->left!=NULL
			&& ((rbtree->compare==NULL && key<tree_node->key)
			|| (rbtree->compare!=NULL && rbtree->compare(key,tree_node->key)<0))) {
		tree_node->left = remove_key_recursive (rbtree,tree_node->left,key);
		if (tree_node->left !=NULL && tree_node->left->color == red) {
			if (tree_node->right != NULL && tree_node->right->color == red)
				invert_colors (tree_node);
			else if (tree_node->left->left != NULL && tree_node->left->left->color == red)
				return rotate_right (tree_node);
		}
	}else if (tree_node->right!=NULL) {
		tree_node->right = remove_key_recursive (rbtree,tree_node->right,key);
		if (tree_node->right!=NULL && tree_node->right->color == red) {
			if (tree_node->left != NULL && tree_node->left->color == red)
				invert_colors (tree_node);
			else return rotate_left (tree_node);
		}
	}

	tree_node->size = (tree_node->left!=NULL?tree_node->left->size:0)
							+ (tree_node->right!=NULL?tree_node->right->size:0);

	return tree_node;
}

/*
 *  Also, make this more elegant later on and get rid of the double cost...
 */
value_t unset (symbol_table_t *const rbtree, key__t const key) {
	value_t value = rbtree->default_value;
	if (rbtree->root != NULL) {
		value = get ((symbol_table_t const*const)rbtree,key);
		if (value != rbtree->default_value) {
			rbtree->root = remove_key_recursive (rbtree,rbtree->root,key);
			//LOG (info,"Successfully removed from the symbol-table value %u indexed by key %u.\n",value,key);
		}else{
			LOG (warn,"No entry indexed by key %u was found in the symbol-table to be removed...\n",key);
		}
	}
	rbtree->size = (rbtree->root!=NULL?rbtree->root->size:0);
	return value;
}
