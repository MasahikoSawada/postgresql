/*-------------------------------------------------------------------------
 *
 * radixtree.h
 *	  Interface for radix tree.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/lib/radixtree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RADIXTREE_H
#define RADIXTREE_H

#include "postgres.h"

#define RADIX_TREE_DEBUG 1

typedef struct radix_tree radix_tree;
typedef struct radix_tree_iter radix_tree_iter;

extern radix_tree *radix_tree_create(MemoryContext ctx);
extern Datum radix_tree_search(radix_tree *tree, uint64 key, bool *found);
extern void radix_tree_destroy(radix_tree *tree);
extern void radix_tree_insert(radix_tree *tree, uint64 key, Datum val, bool *found_p);
extern uint64 radix_tree_memory_usage(radix_tree *tree);
extern uint64 radix_tree_num_entries(radix_tree *tree);

extern radix_tree_iter *radix_tree_begin_iterate(radix_tree *tree);
extern bool radix_tree_iterate_next(radix_tree_iter *iter, uint64 *key_p, Datum *value_p);
extern void radix_tree_end_iterate(radix_tree_iter *iter);


#ifdef RADIX_TREE_DEBUG
extern void radix_tree_dump(radix_tree *tree);
extern void radix_tree_dump_search(radix_tree *tree, uint64 key);
extern void radix_tree_stats(radix_tree *tree);
#endif

#endif /* RADIXTREE_H */
