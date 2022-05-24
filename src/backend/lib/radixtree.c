/*-------------------------------------------------------------------------
 *
 * radixtree.c
 *		Implementation for adaptive radix tree.
 *
 * This module employs the idea from the paper "The Adaptive Radix Tree: ARTful
 * Indexing for Main-Memory Databases" by Viktor Leis, Alfons Kemper, and Thomas
 * Neumann, 2013.
 *
 * There are some differences from the proposed implementation.  For instance,
 * this radix tree module utilizes AVX2 instruction, enabling us to use 256-bit
 * width SIMD vector, whereas 128-bit width SIMD vector is used in the paper.
 * Also, there is no support for path compression and lazy path expansion. The
 * radix tree supports fixed length of the key so we don't expect the tree level
 * wouldn't be high.
 *
 * The key is a 64-bit unsigned integer and the value is a Datum. Both internal
 * nodes and leaf nodes have the identical structure. For internal tree nodes,
 * shift > 0, store the pointer to its child node as the value. The leaf nodes,
 * shift == 0, also have the Datum value that is specified by the user.
 *
 * XXX: the radix tree node never be shrunk.
 *
 * Interface
 * ---------
 *
 * radix_tree_create		- Create a new, empty radix tree
 * radix_tree_free			- Free the radix tree
 * radix_tree_insert		- Insert a key-value pair
 * radix_tree_delete		- Delete a key-value pair
 * radix_tree_begin_iterate	- Begin iterating through all key-value pairs
 * radix_tree_iterate_next	- Return next key-value pair, if any
 * radix_tree_end_iterate	- End iteration
 *
 * radix_tree_create() creates an empty radix tree in the given memory context
 * and memory contexts for all kinds of radix tree node under the memory context.
 *
 * radix_tree_iterate_next() ensures returning key-value pairs in the ascending
 * order of the key.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/radixtree.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "utils/memutils.h"
#include "lib/radixtree.h"
#include "lib/stringinfo.h"

#if defined(__AVX2__)
#include <immintrin.h>			/* AVX2 intrinsics */
#endif

/* The number of bits encoded in one tree level */
#define RADIX_TREE_NODE_FANOUT	8

/* The number of maximum slots in the node, used in node-256 */
#define RADIX_TREE_NODE_MAX_SLOTS (1 << RADIX_TREE_NODE_FANOUT)

/*
 * Return the number of bits required to represent nslots slots, used
 * in node-128 and node-256.
 */
#define RADIX_TREE_NODE_NSLOTS_BITS(nslots) ((nslots) / (sizeof(uint8) * BITS_PER_BYTE))

/* Mask for extracting a chunk from the key */
#define RADIX_TREE_CHUNK_MASK ((1 << RADIX_TREE_NODE_FANOUT) - 1)

/* Maximum shift the radix tree uses */
#define RADIX_TREE_MAX_SHIFT	key_get_shift(UINT64_MAX)

/* Tree level the radix tree uses */
#define RADIX_TREE_MAX_LEVEL	((sizeof(uint64) * BITS_PER_BYTE) / RADIX_TREE_NODE_FANOUT)

/* Get a chunk from the key */
#define GET_KEY_CHUNK(key, shift) \
	((uint8) (((key) >> (shift)) & RADIX_TREE_CHUNK_MASK))

/* Mapping from the value to the bit in is-set bitmap in the node-128 and node-256 */
#define NODE_BITMAP_BYTE(v) ((v) / RADIX_TREE_NODE_FANOUT)
#define NODE_BITMAP_BIT(v) (UINT64_C(1) << ((v) % RADIX_TREE_NODE_FANOUT))

/* Enum used radix_tree_node_search() */
typedef enum
{
	RADIX_TREE_FIND = 0,		/* find the key-value */
	RADIX_TREE_DELETE,			/* delete the key-value */
} radix_tree_action;

/*
 * supported radix tree nodes.
 *
 * XXX: should we add KIND_16 as we can utilize SSE2 SIMD instructions?
 */
typedef enum radix_tree_node_kind
{
	RADIX_TREE_NODE_KIND_4 = 0,
	RADIX_TREE_NODE_KIND_32,
	RADIX_TREE_NODE_KIND_128,
	RADIX_TREE_NODE_KIND_256
} radix_tree_node_kind;
#define RADIX_TREE_NODE_KIND_COUNT 4

/*
 * Base type for all nodes types.
 */
typedef struct radix_tree_node
{
	/*
	 * Number of children.  We use uint16 to be able to indicate 256 children
	 * at ta fanout of 8.
	 */
	uint16		count;

	/*
	 * Shift indicates which part of the key space is represented by this
	 * node. That is, the key is shifted by 'shift' and the lowest
	 * RADIX_TREE_NODE_FANOUT bits are then represented in chunk.
	 */
	uint8		shift;
	uint8		chunk;

	/* Size class of the node */
	radix_tree_node_kind kind;
} radix_tree_node;

/* Macros for radix tree nodes */
#define IS_LEAF_NODE(n) (((radix_tree_node *) (n))->shift == 0)
#define IS_EMPTY_NODE(n) (((radix_tree_node *) (n))->count == 0)
#define NODE_HAS_FREE_SLOT(n) \
	(((radix_tree_node *) (n))->count < \
	 radix_tree_node_info[((radix_tree_node *) (n))->kind].max_slots)

/*
 * To reduce memory usage compared to a simple radix tree with a fixed fanout
 * we use adaptive node sides, with different storage methods for different
 * numbers of elements.
 */
typedef struct radix_tree_node_4
{
	radix_tree_node n;

	/* 4 children, for key chunks */
	uint8		chunks[4];
	Datum		slots[4];
} radix_tree_node_4;

typedef struct radix_tree_node_32
{
	radix_tree_node n;

	/* 32 children, for key chunks */
	uint8		chunks[32];
	Datum		slots[32];
} radix_tree_node_32;

#define RADIX_TREE_NODE_128_BITS RADIX_TREE_NODE_NSLOTS_BITS(128)
typedef struct radix_tree_node_128
{
	radix_tree_node n;

	/*
	 * The index of slots for each fanout. 0 means unused whereas slots is
	 * 0-indexed. So we can get the slot of the chunk C by slots[C] - 1.
	 */
	uint8		slot_idxs[RADIX_TREE_NODE_MAX_SLOTS];

	/* A bitmap to track which slot is in use */
	uint8		isset[RADIX_TREE_NODE_128_BITS];

	Datum		slots[128];
} radix_tree_node_128;

#define RADIX_TREE_NODE_MAX_BITS RADIX_TREE_NODE_NSLOTS_BITS(RADIX_TREE_NODE_MAX_SLOTS)
typedef struct radix_tree_node_256
{
	radix_tree_node n;

	/* A bitmap to track which slot is in use */
	uint8		isset[RADIX_TREE_NODE_MAX_BITS];

	Datum		slots[RADIX_TREE_NODE_MAX_SLOTS];
} radix_tree_node_256;

/* Information of each size class */
typedef struct radix_tree_node_info_elem
{
	const char *name;
	int			max_slots;
	Size		size;
} radix_tree_node_info_elem;

static radix_tree_node_info_elem radix_tree_node_info[] =
{
	{"radix tree node 4", 4, sizeof(radix_tree_node_4)},
	{"radix tree node 32", 32, sizeof(radix_tree_node_32)},
	{"radix tree node 128", 128, sizeof(radix_tree_node_128)},
	{"radix tree node 256", 256, sizeof(radix_tree_node_256)},
};

/*
 * As we descend a radix tree, we push the node to the stack. The stack is used
 * at deletion.
 */
typedef struct radix_tree_stack_data
{
	radix_tree_node *node;
	struct radix_tree_stack_data *parent;
} radix_tree_stack_data;
typedef radix_tree_stack_data *radix_tree_stack;

/*
 * Iteration support.
 *
 * Iterating the radix tree returns each pair of key and value in the ascending order
 * of the key. To support this, the we iterate nodes of each level.
 * radix_tree_iter_node_data struct is used to track the iteration within a node.
 * radix_tree_iter has the array of this struct, stack, in order to track the iteration
 * of every level. During the iteration, we also construct the key to return. The key
 * is updated whenever we update the node iteration information, e.g., when advancing
 * the current index within the node or when moving to the next node at the same level.
 */
typedef struct radix_tree_iter_node_data
{
	radix_tree_node *node;		/* current node being iterated */
	int			current_idx;	/* current position. -1 for initial value */
} radix_tree_iter_node_data;

struct radix_tree_iter
{
	radix_tree *tree;

	/* Track the iteration on nodes of each level */
	radix_tree_iter_node_data stack[RADIX_TREE_MAX_LEVEL];
	int			stack_len;

	/* The key is being constructed during the iteration */
	uint64		key;
};

/* A radix tree with nodes */
struct radix_tree
{
	MemoryContext context;

	radix_tree_node *root;
	uint64		max_val;
	uint64		num_keys;
	MemoryContextData *slabs[RADIX_TREE_NODE_KIND_COUNT];

	/* statistics */
	uint64		mem_used;
	int32		cnt[RADIX_TREE_NODE_KIND_COUNT];
};

static radix_tree_node *radix_tree_node_grow(radix_tree *tree, radix_tree_node *parent,
											 radix_tree_node *node, uint64 key);
static bool radix_tree_node_search_child(radix_tree_node *node, radix_tree_node **child_p,
										 uint64 key);
static bool radix_tree_node_search(radix_tree_node *node, Datum **slot_p, uint64 key,
								   radix_tree_action action);
static void radix_tree_extend(radix_tree *tree, uint64 key);
static void radix_tree_new_root(radix_tree *tree, uint64 key, Datum val);
static radix_tree_node *radix_tree_node_insert_child(radix_tree *tree,
													 radix_tree_node *parent,
													 radix_tree_node *node,
													 uint64 key);
static void radix_tree_node_insert_val(radix_tree *tree, radix_tree_node *parent,
									   radix_tree_node *node, uint64 key, Datum val,
									   bool *replaced_p);
static inline void radix_tree_iter_update_key(radix_tree_iter *iter, uint8 chunk, uint8 shift);
static Datum radix_tree_node_iterate_next(radix_tree_iter *iter, radix_tree_iter_node_data *node_iter,
										  bool *found_p);
static void radix_tree_store_iter_node(radix_tree_iter *iter, radix_tree_iter_node_data *node_iter,
									   radix_tree_node *node);
static void radix_tree_update_iter_stack(radix_tree_iter *iter, int from);
static void radix_tree_verify_node(radix_tree_node *node);

/*
 * Helper functions for accessing each kind of nodes.
 */
static inline int
node_32_search_eq(radix_tree_node_32 *node, uint8 chunk)
{
#ifdef __AVX2__
	__m256i		_key = _mm256_set1_epi8(chunk);
	__m256i		_data = _mm256_loadu_si256((__m256i_u *) node->chunks);
	__m256i		_cmp = _mm256_cmpeq_epi8(_key, _data);
	uint32		bitfield = _mm256_movemask_epi8(_cmp);

	bitfield &= ((UINT64_C(1) << node->n.count) - 1);

	return (bitfield) ? __builtin_ctz(bitfield) : -1;

#else
	for (int i = 0; i < node->n.count; i++)
	{
		if (node->chunks[i] > chunk)
			return -1;

		if (node->chunks[i] == chunk)
			return i;
	}

	return -1;
#endif							/* __AVX2__ */
}

/*
 * This is a bit more complicated than search_chunk_array_16_eq(), because
 * until recently no unsigned uint8 comparison instruction existed on x86. So
 * we need to play some trickery using _mm_min_epu8() to effectively get
 * <=. There never will be any equal elements in the current uses, but that's
 * what we get here...
 */
static inline int
node_32_search_le(radix_tree_node_32 *node, uint8 chunk)
{
#ifdef __AVX2__
	__m256i		_key = _mm256_set1_epi8(chunk);
	__m256i		_data = _mm256_loadu_si256((__m256i_u *) node->chunks);
	__m256i		_min = _mm256_min_epu8(_key, _data);
	__m256i		cmp = _mm256_cmpeq_epi8(_key, _min);
	uint32_t	bitfield = _mm256_movemask_epi8(cmp);

	bitfield &= ((UINT64_C(1) << node->n.count) - 1);

	return (bitfield) ? __builtin_ctz(bitfield) : node->n.count;
#else
	int			index;

	for (index = 0; index < node->n.count; index++)
	{
		if (node->chunks[index] >= chunk)
			break;
	}

	return index;
#endif							/* __AVX2__ */
}

/* Does the given chunk in the node has the value? */
static inline bool
node_128_is_chunk_used(radix_tree_node_128 *node, uint8 chunk)
{
	return (node->slot_idxs[chunk] != 0);
}

/* Is the slot in the node used? */
static inline bool
node_128_is_slot_used(radix_tree_node_128 *node, uint8 slot)
{
	return ((node->isset[NODE_BITMAP_BYTE(slot)] & NODE_BITMAP_BIT(slot)) != 0);
}

/* Set the slot at the corresponding chunk */
static inline void
node_128_set(radix_tree_node_128 *node, uint8 chunk, Datum val)
{
	int			slotpos = 0;

	/* Search an unused slot */
	while (node_128_is_slot_used(node, slotpos))
		slotpos++;

	node->slot_idxs[chunk] = slotpos + 1;
	node->slots[slotpos] = val;
	node->isset[NODE_BITMAP_BYTE(slotpos)] |= NODE_BITMAP_BIT(slotpos);
}

/* Delete the slot at the corresponding chunk */
static inline void
node_128_unset(radix_tree_node_128 *node, uint8 chunk)
{
	int			slotpos = node->slot_idxs[chunk] - 1;

	if (!node_128_is_chunk_used(node, chunk))
		return;

	node->isset[NODE_BITMAP_BYTE(slotpos)] &= ~(NODE_BITMAP_BIT(slotpos));
	node->slot_idxs[chunk] = 0;
}

/* Return the slot data corresponding to the chunk */
static inline Datum
node_128_get_chunk_slot(radix_tree_node_128 *node, uint8 chunk)
{
	return node->slots[node->slot_idxs[chunk] - 1];
}

/* Return true if the slot corresponding to the given chunk is in use */
static inline bool
node_256_is_chunk_used(radix_tree_node_256 *node, uint8 chunk)
{
	return (node->isset[NODE_BITMAP_BYTE(chunk)] & NODE_BITMAP_BIT(chunk)) != 0;
}

/* Set the slot at the given chunk position */
static inline void
node_256_set(radix_tree_node_256 *node, uint8 chunk, Datum slot)
{
	node->slots[chunk] = slot;
	node->isset[NODE_BITMAP_BYTE(chunk)] |= NODE_BITMAP_BIT(chunk);
}

/* Set the slot at the given chunk position */
static inline void
node_256_unset(radix_tree_node_256 *node, uint8 chunk)
{
	node->isset[NODE_BITMAP_BYTE(chunk)] &= ~(NODE_BITMAP_BIT(chunk));
}

/*
 * Return the shift that is satisfied to store the given key.
 */
inline static int
key_get_shift(uint64 key)
{
	return (key == 0)
		? 0
		: (pg_leftmost_one_pos64(key) / RADIX_TREE_NODE_FANOUT) * RADIX_TREE_NODE_FANOUT;
}

/*
 * Return the max value stored in a node with the given shift.
 */
static uint64
shift_get_max_val(int shift)
{
	if (shift == RADIX_TREE_MAX_SHIFT)
		return UINT64_MAX;

	return (UINT64_C(1) << (shift + RADIX_TREE_NODE_FANOUT)) - 1;
}

/*
 * Allocate a new node with the given node kind.
 */
static radix_tree_node *
radix_tree_alloc_node(radix_tree *tree, radix_tree_node_kind kind)
{
	radix_tree_node *newnode;

	newnode = (radix_tree_node *) MemoryContextAllocZero(tree->slabs[kind],
														 radix_tree_node_info[kind].size);
	newnode->kind = kind;

	/* update the statistics */
	tree->mem_used += GetMemoryChunkSpace(newnode);
	tree->cnt[kind]++;

	return newnode;
}

/* Free the given node */
static void
radix_tree_free_node(radix_tree *tree, radix_tree_node *node)
{
	/*
	 * XXX: If we're deleting the root node, make the tree empty
	 */
	if (tree->root == node)
	{
		tree->root = NULL;
	}

	/* update the statistics */
	tree->mem_used -= GetMemoryChunkSpace(node);
	tree->cnt[node->kind]--;

	Assert(tree->mem_used >= 0);
	Assert(tree->cnt[node->kind] >= 0);

	pfree(node);
}

/* Free a stack made by radix_tree_delete */
static void
radix_tree_free_stack(radix_tree_stack stack)
{
	radix_tree_stack ostack;

	while (stack != NULL)
	{
		ostack = stack;
		stack = stack->parent;
		pfree(ostack);
	}
}

/* Copy the common fields without the kind */
static void
radix_tree_copy_node_common(radix_tree_node *src, radix_tree_node *dst)
{
	dst->shift = src->shift;
	dst->chunk = src->chunk;
	dst->count = src->count;
}

/*
 * The radix tree doesn't sufficient height. Extend the radix tree so it can
 * store the key.
 */
static void
radix_tree_extend(radix_tree *tree, uint64 key)
{
	int			max_shift;
	int			shift = tree->root->shift + RADIX_TREE_NODE_FANOUT;

	max_shift = key_get_shift(key);

	/* Grow tree from 'shift' to 'max_shift' */
	while (shift <= max_shift)
	{
		radix_tree_node_4 *node =
		(radix_tree_node_4 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);

		node->n.count = 1;
		node->n.shift = shift;
		node->chunks[0] = 0;
		node->slots[0] = PointerGetDatum(tree->root);

		tree->root->chunk = 0;
		tree->root = (radix_tree_node *) node;

		shift += RADIX_TREE_NODE_FANOUT;
	}

	tree->max_val = shift_get_max_val(max_shift);
}

/*
 * Wrapper for radix_tree_node_search to search the pointer to the child node in the
 * node.
 *
 * Return true if the corresponding child is found, otherwise return false.  On success,
 * it sets child_p.
 */
static bool
radix_tree_node_search_child(radix_tree_node *node, radix_tree_node **child_p, uint64 key)
{
	bool		found = false;
	Datum	   *slot_ptr;

	if (radix_tree_node_search(node, &slot_ptr, key, RADIX_TREE_FIND))
	{
		/* Found the pointer to the child node */
		found = true;
		*child_p = (radix_tree_node *) DatumGetPointer(*slot_ptr);
	}

	return found;
}

/*
 * Return true if the corresponding slot is used, otherwise return false.  On success,
 * sets the pointer to the slot to slot_p.
 */
static bool
radix_tree_node_search(radix_tree_node *node, Datum **slot_p, uint64 key,
					   radix_tree_action action)
{
	int			chunk = GET_KEY_CHUNK(key, node->shift);
	bool		found = false;

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
			{
				radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;

				/* Do linear search */
				for (int i = 0; i < n4->n.count; i++)
				{
					if (n4->chunks[i] > chunk)
						break;

					/*
					 * If we find the chunk in the node, do the specified
					 * action
					 */
					if (n4->chunks[i] == chunk)
					{
						if (action == RADIX_TREE_FIND)
							*slot_p = &(n4->slots[i]);
						else	/* RADIX_TREE_DELETE */
						{
							memmove(&(n4->chunks[i]), &(n4->chunks[i + 1]),
									sizeof(uint8) * (n4->n.count - i - 1));
							memmove(&(n4->slots[i]), &(n4->slots[i + 1]),
									sizeof(radix_tree_node *) * (n4->n.count - i - 1));
						}

						found = true;
						break;
					}
				}

				break;
			}
		case RADIX_TREE_NODE_KIND_32:
			{
				radix_tree_node_32 *n32 = (radix_tree_node_32 *) node;
				int			idx;

				/* Search by SIMD instructions */
				idx = node_32_search_eq(n32, chunk);

				/* If we find the chunk in the node, do the specified action */
				if (idx >= 0)
				{
					if (action == RADIX_TREE_FIND)
						*slot_p = &(n32->slots[idx]);
					else		/* RADIX_TREE_DELETE */
					{
						memmove(&(n32->chunks[idx]), &(n32->chunks[idx + 1]),
								sizeof(uint8) * (n32->n.count - idx - 1));
						memmove(&(n32->slots[idx]), &(n32->slots[idx + 1]),
								sizeof(radix_tree_node *) * (n32->n.count - idx - 1));
					}

					found = true;
				}

				break;
			}
		case RADIX_TREE_NODE_KIND_128:
			{
				radix_tree_node_128 *n128 = (radix_tree_node_128 *) node;

				/* If we find the chunk in the node, do the specified action */
				if (node_128_is_chunk_used(n128, chunk))
				{
					if (action == RADIX_TREE_FIND)
						*slot_p = &(n128->slots[n128->slot_idxs[chunk] - 1]);
					else		/* RADIX_TREE_DELETE */
						node_128_unset(n128, chunk);

					found = true;
				}

				break;
			}
		case RADIX_TREE_NODE_KIND_256:
			{
				radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

				/* If we find the chunk in the node, do the specified action */
				if (node_256_is_chunk_used(n256, chunk))
				{
					if (action == RADIX_TREE_FIND)
						*slot_p = &(n256->slots[chunk]);
					else		/* RADIX_TREE_DELETE */
						node_256_unset(n256, chunk);

					found = true;
				}

				break;
			}
	}

	/* Update the statistics */
	if (action == RADIX_TREE_DELETE && found)
		node->count--;

	return found;
}

/*
 * Create a new node as the root. Subordinate nodes will be created during
 * the insertion.
 */
static void
radix_tree_new_root(radix_tree *tree, uint64 key, Datum val)
{
	radix_tree_node_4 *n4 =
	(radix_tree_node_4 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);
	int			shift = key_get_shift(key);

	n4->n.shift = shift;
	tree->max_val = shift_get_max_val(shift);
	tree->root = (radix_tree_node *) n4;
}

/* Insert 'node' as a child node of 'parent' */
static radix_tree_node *
radix_tree_node_insert_child(radix_tree *tree, radix_tree_node *parent,
							 radix_tree_node *node, uint64 key)
{
	radix_tree_node *newchild =
	(radix_tree_node *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);

	Assert(!IS_LEAF_NODE(node));

	newchild->shift = node->shift - RADIX_TREE_NODE_FANOUT;
	newchild->chunk = GET_KEY_CHUNK(key, node->shift);

	radix_tree_node_insert_val(tree, parent, node, key, PointerGetDatum(newchild), NULL);

	return (radix_tree_node *) newchild;
}

/*
 * Insert the value to the node. The node grows if it's full.
 */
static void
radix_tree_node_insert_val(radix_tree *tree, radix_tree_node *parent,
						   radix_tree_node *node, uint64 key, Datum val,
						   bool *replaced_p)
{
	int			chunk = GET_KEY_CHUNK(key, node->shift);
	bool		replaced = false;

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
			{
				radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;
				int			idx;

				for (idx = 0; idx < n4->n.count; idx++)
				{
					if (n4->chunks[idx] >= chunk)
						break;
				}

				if (NODE_HAS_FREE_SLOT(n4))
				{
					if (n4->n.count == 0)
					{
						/* the first key for this node, add it */
					}
					else if (n4->chunks[idx] == chunk)
					{
						/* found the key, replace it */
						replaced = true;
					}
					else if (idx != n4->n.count)
					{
						/*
						 * the key needs to be inserted in the middle of the
						 * array, make space for the new key.
						 */
						memmove(&(n4->chunks[idx + 1]), &(n4->chunks[idx]),
								sizeof(uint8) * (n4->n.count - idx));
						memmove(&(n4->slots[idx + 1]), &(n4->slots[idx]),
								sizeof(radix_tree_node *) * (n4->n.count - idx));
					}

					n4->chunks[idx] = chunk;
					n4->slots[idx] = val;

					/* Done */
					break;
				}

				/* The node doesn't have free slot so needs to grow */
				node = radix_tree_node_grow(tree, parent, node, key);
				Assert(node->kind == RADIX_TREE_NODE_KIND_32);
			}
			/* FALLTHROUGH */
		case RADIX_TREE_NODE_KIND_32:
			{
				radix_tree_node_32 *n32 = (radix_tree_node_32 *) node;
				int			idx;

				idx = node_32_search_le(n32, chunk);

				if (NODE_HAS_FREE_SLOT(n32))
				{
					if (n32->n.count == 0)
					{
						/* first key for this node, add it */
					}
					else if (n32->chunks[idx] == chunk)
					{
						/* found the key, replace it */
						replaced = true;
					}
					else if (idx != n32->n.count)
					{
						/*
						 * the key needs to be inserted in the middle of the
						 * array, make space for the new key.
						 */
						memmove(&(n32->chunks[idx + 1]), &(n32->chunks[idx]),
								sizeof(uint8) * (n32->n.count - idx));
						memmove(&(n32->slots[idx + 1]), &(n32->slots[idx]),
								sizeof(radix_tree_node *) * (n32->n.count - idx));
					}

					n32->chunks[idx] = chunk;
					n32->slots[idx] = val;

					/* Done */
					break;
				}

				/* The node doesn't have free slot so needs to grow */
				node = radix_tree_node_grow(tree, parent, node, key);
				Assert(node->kind == RADIX_TREE_NODE_KIND_128);
			}
			/* FALLTHROUGH */
		case RADIX_TREE_NODE_KIND_128:
			{
				radix_tree_node_128 *n128 = (radix_tree_node_128 *) node;

				if (node_128_is_chunk_used(n128, chunk))
				{
					/* found the existing value */
					node_128_set(n128, chunk, val);
					replaced = true;
					break;
				}

				if (NODE_HAS_FREE_SLOT(n128))
				{
					node_128_set(n128, chunk, val);

					/* Done */
					break;
				}

				/* The node doesn't have free slot so needs to grow */
				node = radix_tree_node_grow(tree, parent, node, key);
				Assert(node->kind == RADIX_TREE_NODE_KIND_256);
			}
			/* FALLTHROUGH */
		case RADIX_TREE_NODE_KIND_256:
			{
				radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

				if (node_256_is_chunk_used(n256, chunk))
					replaced = true;

				node_256_set(n256, chunk, val);

				break;
			}
	}

	/* Update statistics */
	if (!replaced)
		node->count++;

	if (replaced_p)
		*replaced_p = replaced;

	/*
	 * Done. Finally, verify the chunk and value is inserted or replaced
	 * properly in the node.
	 */
	radix_tree_verify_node(node);
}

/* Change the node type to the next larger one */
static radix_tree_node *
radix_tree_node_grow(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
					 uint64 key)
{
	radix_tree_node *newnode = NULL;

	Assert(node->count == radix_tree_node_info[node->kind].max_slots);

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
			{
				radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;
				radix_tree_node_32 *new32 =
				(radix_tree_node_32 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_32);

				radix_tree_copy_node_common((radix_tree_node *) n4,
											(radix_tree_node *) new32);

				/* Copy both chunks and slots to the new node */
				memcpy(&(new32->chunks), &(n4->chunks), sizeof(uint8) * 4);
				memcpy(&(new32->slots), &(n4->slots), sizeof(Datum) * 4);

				newnode = (radix_tree_node *) new32;
				break;
			}
		case RADIX_TREE_NODE_KIND_32:
			{
				radix_tree_node_32 *n32 = (radix_tree_node_32 *) node;
				radix_tree_node_128 *new128 =
				(radix_tree_node_128 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_128);

				/* Copy both chunks and slots to the new node */
				radix_tree_copy_node_common((radix_tree_node *) n32,
											(radix_tree_node *) new128);

				for (int i = 0; i < n32->n.count; i++)
					node_128_set(new128, n32->chunks[i], n32->slots[i]);

				newnode = (radix_tree_node *) new128;
				break;
			}
		case RADIX_TREE_NODE_KIND_128:
			{
				radix_tree_node_128 *n128 = (radix_tree_node_128 *) node;
				radix_tree_node_256 *new256 =
				(radix_tree_node_256 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_256);
				int			cnt = 0;

				radix_tree_copy_node_common((radix_tree_node *) n128,
											(radix_tree_node *) new256);

				for (int i = 0; i < RADIX_TREE_NODE_MAX_SLOTS && cnt < n128->n.count; i++)
				{
					if (!node_128_is_chunk_used(n128, i))
						continue;

					node_256_set(new256, i, node_128_get_chunk_slot(n128, i));
					cnt++;
				}

				newnode = (radix_tree_node *) new256;
				break;
			}
		case RADIX_TREE_NODE_KIND_256:
			elog(ERROR, "radix tree node-256 cannot grow");
			break;
	}

	if (parent == node)
	{
		/* Replace the root node with the new large node */
		tree->root = newnode;
	}
	else
	{
		Datum	   *slot_ptr = NULL;

		/* Redirect from the parent to the node */
		radix_tree_node_search(parent, &slot_ptr, key, RADIX_TREE_FIND);
		Assert(*slot_ptr);
		*slot_ptr = PointerGetDatum(newnode);
	}

	/* Verify the node has grown properly */
	radix_tree_verify_node(newnode);

	/* Free the old node */
	radix_tree_free_node(tree, node);

	return newnode;
}

/*
 * Create the radix tree in the given memory context and return it.
 */
radix_tree *
radix_tree_create(MemoryContext ctx)
{
	radix_tree *tree;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(ctx);

	tree = palloc(sizeof(radix_tree));
	tree->max_val = 0;
	tree->root = NULL;
	tree->context = ctx;
	tree->num_keys = 0;
	tree->mem_used = 0;

	/* Create the slab allocator for each size class */
	for (int i = 0; i < RADIX_TREE_NODE_KIND_COUNT; i++)
	{
		tree->slabs[i] = SlabContextCreate(ctx,
										   radix_tree_node_info[i].name,
										   SLAB_DEFAULT_BLOCK_SIZE,
										   radix_tree_node_info[i].size);
		tree->cnt[i] = 0;
	}

	MemoryContextSwitchTo(old_ctx);

	return tree;
}

/*
 * Free the given radix tree.
 */
void
radix_tree_free(radix_tree *tree)
{
	for (int i = 0; i < RADIX_TREE_NODE_KIND_COUNT; i++)
		MemoryContextDelete(tree->slabs[i]);

	pfree(tree);
}

/*
 * Insert the key with the val.
 *
 * found_p is set to true if the key already present, otherwise false, if
 * it's not NULL.
 *
 * XXX: do we need to support update_if_exists behavior?
 */
void
radix_tree_insert(radix_tree *tree, uint64 key, Datum val, bool *found_p)
{
	int			shift;
	bool		replaced;
	radix_tree_node *node;
	radix_tree_node *parent = tree->root;

	/* Empty tree, create the root */
	if (!tree->root)
		radix_tree_new_root(tree, key, val);

	/* Extend the tree if necessary */
	if (key > tree->max_val)
		radix_tree_extend(tree, key);

	Assert(tree->root);

	shift = tree->root->shift;
	node = tree->root;
	while (shift > 0)
	{
		radix_tree_node *child;

		if (!radix_tree_node_search_child(node, &child, key))
			child = radix_tree_node_insert_child(tree, parent, node, key);

		Assert(child != NULL);

		parent = node;
		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
	}

	/* arrived at a leaf */
	Assert(IS_LEAF_NODE(node));

	radix_tree_node_insert_val(tree, parent, node, key, val, &replaced);

	/* Update the statistics */
	if (!replaced)
		tree->num_keys++;

	if (found_p)
		*found_p = replaced;
}

/*
 * Search the given key in the radix tree. Return true if the key is successfully
 * found, otherwise return false.  On success, we set the value to *val_p so
 * it must not be NULL.
 */
bool
radix_tree_search(radix_tree *tree, uint64 key, Datum *val_p)
{
	radix_tree_node *node;
	Datum	   *value_ptr;
	int			shift;

	Assert(val_p);

	if (!tree->root || key > tree->max_val)
		return false;

	node = tree->root;
	shift = tree->root->shift;
	while (shift > 0)
	{
		radix_tree_node *child;

		if (!radix_tree_node_search_child(node, &child, key))
			return false;

		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
	}

	/* We reached at a leaf node, search the corresponding slot */
	Assert(IS_LEAF_NODE(node));

	if (!radix_tree_node_search(node, &value_ptr, key, RADIX_TREE_FIND))
		return false;

	/* Found, set the value to return */
	*val_p = *value_ptr;
	return true;
}

/*
 * Delete the given key from the radix tree. Return true if the key is found (and
 * deleted), otherwise do nothing and return false.
 */
bool
radix_tree_delete(radix_tree *tree, uint64 key)
{
	radix_tree_node *node;
	int			shift;
	radix_tree_stack stack = NULL;
	bool		deleted;

	if (!tree->root || key > tree->max_val)
		return false;

	/*
	 * Descending the tree to search the key while building a stack of nodes
	 * we visited.
	 */
	node = tree->root;
	shift = tree->root->shift;
	while (shift >= 0)
	{
		radix_tree_node *child;
		radix_tree_stack new_stack;

		new_stack = (radix_tree_stack) palloc(sizeof(radix_tree_stack_data));
		new_stack->node = node;
		new_stack->parent = stack;
		stack = new_stack;

		if (IS_LEAF_NODE(node))
			break;

		if (!radix_tree_node_search_child(node, &child, key))
		{
			radix_tree_free_stack(stack);
			return false;
		}

		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
	}

	/*
	 * Delete the key from the leaf node and recursively delete internal nodes
	 * if necessary.
	 */
	Assert(IS_LEAF_NODE(stack->node));
	while (stack != NULL)
	{
		radix_tree_node *node;
		Datum	   *slot;

		/* pop the node from the stack */
		node = stack->node;
		stack = stack->parent;

		deleted = radix_tree_node_search(node, &slot, key, RADIX_TREE_DELETE);

		/* If the node didn't become empty, we stop deleting the key */
		if (!IS_EMPTY_NODE(node))
			break;

		Assert(deleted);

		/* The node became empty */
		radix_tree_free_node(tree, node);

		/*
		 * If we eventually deleted the root node while recursively deleting
		 * empty nodes, we make the tree empty.
		 */
		if (stack == NULL)
		{
			tree->root = NULL;
			tree->max_val = 0;
		}
	}

	if (deleted)
		tree->num_keys--;

	radix_tree_free_stack(stack);
	return deleted;
}

/* Create and return the iterator for the given radix tree */
radix_tree_iter *
radix_tree_begin_iterate(radix_tree *tree)
{
	MemoryContext old_ctx;
	radix_tree_iter *iter;
	int			top_level;

	old_ctx = MemoryContextSwitchTo(tree->context);

	iter = (radix_tree_iter *) palloc0(sizeof(radix_tree_iter));
	iter->tree = tree;

	/* empty tree */
	if (!iter->tree)
		return iter;

	top_level = iter->tree->root->shift / RADIX_TREE_NODE_FANOUT;

	iter->stack_len = top_level;
	iter->stack[top_level].node = iter->tree->root;
	iter->stack[top_level].current_idx = -1;

	/* Descend to the left most leaf node from the root */
	radix_tree_update_iter_stack(iter, top_level);

	MemoryContextSwitchTo(old_ctx);

	return iter;
}

/*
 * Return true with setting key_p and value_p if there is next key.  Otherwise,
 * return false.
 */
bool
radix_tree_iterate_next(radix_tree_iter *iter, uint64 *key_p, Datum *value_p)
{
	bool		found = false;
	Datum		slot = (Datum) 0;
	int			level;

	/* Empty tree */
	if (!iter->tree)
		return false;

	for (;;)
	{
		radix_tree_node *node;
		radix_tree_iter_node_data *node_iter;

		/*
		 * Iterate node at each level from the bottom of the tree until we
		 * search the next slot.
		 */
		for (level = 0; level <= iter->stack_len; level++)
		{
			slot = radix_tree_node_iterate_next(iter, &(iter->stack[level]), &found);

			if (found)
				break;
		}

		/* end of iteration */
		if (!found)
			return false;

		/* found the next slot at the leaf node, return it */
		if (level == 0)
		{
			*key_p = iter->key;
			*value_p = slot;
			return true;
		}

		/*
		 * We have advanced more than one nodes including internal nodes. So
		 * we need to update the stack by descending to the left most leaf
		 * node from this level.
		 */
		node = (radix_tree_node *) DatumGetPointer(slot);
		node_iter = &(iter->stack[level - 1]);
		radix_tree_store_iter_node(iter, node_iter, node);

		radix_tree_update_iter_stack(iter, level - 1);
	}
}

void
radix_tree_end_iterate(radix_tree_iter *iter)
{
	pfree(iter);
}

/*
 * Update the part of the key being constructed during the iteration with the
 * given chunk
 */
static inline void
radix_tree_iter_update_key(radix_tree_iter *iter, uint8 chunk, uint8 shift)
{
	iter->key &= ~(((uint64) RADIX_TREE_CHUNK_MASK) << shift);
	iter->key |= (((uint64) chunk) << shift);
}

/*
 * Iterate over the given radix tree node and returns the next slot of the given
 * node and set true to *found_p, if any.  Otherwise, set false to *found_p.
 */
static Datum
radix_tree_node_iterate_next(radix_tree_iter *iter, radix_tree_iter_node_data *node_iter,
							 bool *found_p)
{
	radix_tree_node *node = node_iter->node;
	Datum		slot = (Datum) 0;

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
			{
				radix_tree_node_4 *n4 = (radix_tree_node_4 *) node_iter->node;

				node_iter->current_idx++;

				if (node_iter->current_idx >= n4->n.count)
					goto not_found;

				slot = n4->slots[node_iter->current_idx];

				/* Update the part of the key with the current chunk */
				if (IS_LEAF_NODE(node))
					radix_tree_iter_update_key(iter, n4->chunks[node_iter->current_idx], 0);

				break;
			}
		case RADIX_TREE_NODE_KIND_32:
			{
				radix_tree_node_32 *n32 = (radix_tree_node_32 *) node;

				node_iter->current_idx++;

				if (node_iter->current_idx >= n32->n.count)
					goto not_found;

				slot = n32->slots[node_iter->current_idx];

				/* Update the part of the key with the current chunk */
				if (IS_LEAF_NODE(node))
					radix_tree_iter_update_key(iter, n32->chunks[node_iter->current_idx], 0);

				break;
			}
		case RADIX_TREE_NODE_KIND_128:
			{
				radix_tree_node_128 *n128 = (radix_tree_node_128 *) node;
				int			i;

				for (i = node_iter->current_idx + 1; i < 256; i++)
				{
					if (node_128_is_chunk_used(n128, i))
						break;
				}

				if (i >= 256)
					goto not_found;

				node_iter->current_idx = i;
				slot = node_128_get_chunk_slot(n128, i);

				/* Update the part of the key */
				if (IS_LEAF_NODE(node))
					radix_tree_iter_update_key(iter, node_iter->current_idx, 0);

				break;
			}
		case RADIX_TREE_NODE_KIND_256:
			{
				radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;
				int			i;

				for (i = node_iter->current_idx + 1; i < 256; i++)
				{
					if (node_256_is_chunk_used(n256, i))
						break;
				}

				if (i >= 256)
					goto not_found;

				node_iter->current_idx = i;
				slot = n256->slots[i];

				/* Update the part of the key */
				if (IS_LEAF_NODE(node))
					radix_tree_iter_update_key(iter, node_iter->current_idx, 0);

				break;
			}
	}

	*found_p = true;
	return slot;

not_found:
	*found_p = false;
	return (Datum) 0;
}

/*
 * Initialize and update the node iteration struct with the given radix tree node.
 * This function also updates the part of the key with the chunk of the given node.
 */
static void
radix_tree_store_iter_node(radix_tree_iter *iter, radix_tree_iter_node_data *node_iter,
						   radix_tree_node *node)
{
	node_iter->node = node;
	node_iter->current_idx = -1;

	radix_tree_iter_update_key(iter, node->chunk, node->shift + RADIX_TREE_NODE_FANOUT);
}

/*
 * Build the stack of the radix tree node while descending to the leaf from the 'from'
 * level.
 */
static void
radix_tree_update_iter_stack(radix_tree_iter *iter, int from)
{
	radix_tree_node *node = iter->stack[from].node;
	int			level = from;

	for (;;)
	{
		radix_tree_iter_node_data *node_iter = &(iter->stack[level--]);
		bool		found;

		/* Set the current node */
		radix_tree_store_iter_node(iter, node_iter, node);

		if (IS_LEAF_NODE(node))
			break;

		node = (radix_tree_node *)
			DatumGetPointer(radix_tree_node_iterate_next(iter, node_iter, &found));

		/*
		 * Since we always get the first slot in the node, we have to found
		 * the slot.
		 */
		Assert(found);
	}
}

/*
 * Return the number of keys in the radix tree.
 */
uint64
radix_tree_num_entries(radix_tree *tree)
{
	return tree->num_keys;
}

/*
 * Return the statistics of the amount of memory used by the radix tree.
 */
uint64
radix_tree_memory_usage(radix_tree *tree)
{
	return tree->mem_used;
}

/*
 * Verify the radix tree node.
 */
static void
radix_tree_verify_node(radix_tree_node *node)
{
#ifdef USE_ASSERT_CHECKING
	Assert(node->count >= 0);

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
			{
				radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;

				/* Check if the chunks in the node are sorted */
				for (int i = 1; i < n4->n.count; i++)
					Assert(n4->chunks[i - 1] < n4->chunks[i]);

				break;
			}
		case RADIX_TREE_NODE_KIND_32:
			{
				radix_tree_node_32 *n32 = (radix_tree_node_32 *) node;

				/* Check if the chunks in the node are sorted */
				for (int i = 1; i < n32->n.count; i++)
					Assert(n32->chunks[i - 1] < n32->chunks[i]);

				break;
			}
		case RADIX_TREE_NODE_KIND_128:
			{
				radix_tree_node_128 *n128 = (radix_tree_node_128 *) node;
				int			cnt = 0;

				for (int i = 0; i < RADIX_TREE_NODE_MAX_SLOTS; i++)
				{
					if (!node_128_is_chunk_used(n128, i))
						continue;

					/* Check if the corresponding slot is used */
					Assert(node_128_is_slot_used(n128, n128->slot_idxs[i] - 1));

					cnt++;
				}

				Assert(n128->n.count == cnt);
				break;
			}
		case RADIX_TREE_NODE_KIND_256:
			{
				radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;
				int			cnt = 0;

				for (int i = 0; i < RADIX_TREE_NODE_MAX_BITS; i++)
					cnt += pg_popcount32(n256->isset[i]);

				/* Check if the number of used chunk matches */
				Assert(n256->n.count == cnt);

				break;
			}
	}
#endif
}

/***************** DEBUG FUNCTIONS *****************/
#ifdef RADIX_TREE_DEBUG
void
radix_tree_stats(radix_tree *tree)
{
	fprintf(stderr, "num_keys = %lu, height = %u, n4 = %u(%lu), n32 = %u(%lu), n128 = %u(%lu), n256 = %u(%lu)",
			tree->num_keys,
			tree->root->shift / RADIX_TREE_NODE_FANOUT,
			tree->cnt[0], tree->cnt[0] * sizeof(radix_tree_node_4),
			tree->cnt[1], tree->cnt[1] * sizeof(radix_tree_node_32),
			tree->cnt[2], tree->cnt[2] * sizeof(radix_tree_node_128),
			tree->cnt[3], tree->cnt[3] * sizeof(radix_tree_node_256));
	/* radix_tree_dump(tree); */
}

static void
radix_tree_print_slot(StringInfo buf, uint8 chunk, Datum slot, int idx, bool is_leaf, int level)
{
	char		space[128] = {0};

	if (level > 0)
		sprintf(space, "%*c", level * 4, ' ');

	if (is_leaf)
		appendStringInfo(buf, "%s[%d] \"0x%X\" val(0x%lX) LEAF\n",
						 space,
						 idx,
						 chunk,
						 DatumGetInt64(slot));
	else
		appendStringInfo(buf, "%s[%d] \"0x%X\" -> ",
						 space,
						 idx,
						 chunk);
}

static void
radix_tree_dump_node(radix_tree_node *node, int level, StringInfo buf, bool recurse)
{
	bool		is_leaf = IS_LEAF_NODE(node);

	appendStringInfo(buf, "[\"%s\" type %d, cnt %u, shift %u, chunk \"0x%X\"] chunks:\n",
					 IS_LEAF_NODE(node) ? "LEAF" : "INNR",
					 (node->kind == RADIX_TREE_NODE_KIND_4) ? 4 :
					 (node->kind == RADIX_TREE_NODE_KIND_32) ? 32 :
					 (node->kind == RADIX_TREE_NODE_KIND_128) ? 128 : 256,
					 node->count, node->shift, node->chunk);

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
			{
				radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;

				for (int i = 0; i < n4->n.count; i++)
				{
					radix_tree_print_slot(buf, n4->chunks[i], n4->slots[i], i, is_leaf, level);

					if (!is_leaf)
					{
						if (recurse)
						{
							StringInfoData buf2;

							initStringInfo(&buf2);
							radix_tree_dump_node((radix_tree_node *) n4->slots[i], level + 1, &buf2, recurse);
							appendStringInfo(buf, "%s", buf2.data);
						}
						else
							appendStringInfo(buf, "\n");
					}
				}
				break;
			}
		case RADIX_TREE_NODE_KIND_32:
			{
				radix_tree_node_32 *n32 = (radix_tree_node_32 *) node;

				for (int i = 0; i < n32->n.count; i++)
				{
					radix_tree_print_slot(buf, n32->chunks[i], n32->slots[i], i, is_leaf, level);

					if (!is_leaf)
					{
						if (recurse)
						{
							StringInfoData buf2;

							initStringInfo(&buf2);
							radix_tree_dump_node((radix_tree_node *) n32->slots[i], level + 1, &buf2, recurse);
							appendStringInfo(buf, "%s", buf2.data);
						}
						else
							appendStringInfo(buf, "\n");
					}
				}
				break;
			}
		case RADIX_TREE_NODE_KIND_128:
			{
				radix_tree_node_128 *n128 = (radix_tree_node_128 *) node;

				for (int j = 0; j < 256; j++)
				{
					if (!node_128_is_chunk_used(n128, j))
						continue;

					appendStringInfo(buf, "slot_idxs[%d]=%d, ", j, n128->slot_idxs[j]);
				}
				appendStringInfo(buf, "\nisset-bitmap:");
				for (int j = 0; j < 16; j++)
				{
					appendStringInfo(buf, "%X ", (uint8) n128->isset[j]);
				}
				appendStringInfo(buf, "\n");

				for (int i = 0; i < 256; i++)
				{
					if (!node_128_is_chunk_used(n128, i))
						continue;

					radix_tree_print_slot(buf, i, node_128_get_chunk_slot(n128, i),
										  i, is_leaf, level);

					if (!is_leaf)
					{
						if (recurse)
						{
							StringInfoData buf2;

							initStringInfo(&buf2);
							radix_tree_dump_node((radix_tree_node *) node_128_get_chunk_slot(n128, i),
												 level + 1, &buf2, recurse);
							appendStringInfo(buf, "%s", buf2.data);
						}
						else
							appendStringInfo(buf, "\n");
					}
				}
				break;
			}
		case RADIX_TREE_NODE_KIND_256:
			{
				radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

				for (int i = 0; i < 256; i++)
				{
					if (!node_256_is_chunk_used(n256, i))
						continue;

					radix_tree_print_slot(buf, i, n256->slots[i], i, is_leaf, level);

					if (!is_leaf)
					{
						if (recurse)
						{
							StringInfoData buf2;

							initStringInfo(&buf2);
							radix_tree_dump_node((radix_tree_node *) n256->slots[i], level + 1, &buf2, recurse);
							appendStringInfo(buf, "%s", buf2.data);
						}
						else
							appendStringInfo(buf, "\n");
					}
				}
				break;
			}
	}
}

void
radix_tree_dump_search(radix_tree *tree, uint64 key)
{
	StringInfoData buf;
	radix_tree_node *node;
	int			shift;
	int			level = 0;

	elog(NOTICE, "-----------------------------------------------------------");
	elog(NOTICE, "max_val = %lu (0x%lX)", tree->max_val, tree->max_val);

	if (!tree->root)
	{
		elog(NOTICE, "tree is empty");
		return;
	}

	if (key > tree->max_val)
	{
		elog(NOTICE, "key %lu (0x%lX) is larger than max val",
			 key, key);
		return;
	}

	initStringInfo(&buf);
	node = tree->root;
	shift = tree->root->shift;
	while (shift >= 0)
	{
		radix_tree_node *child;

		radix_tree_dump_node(node, level, &buf, false);

		if (IS_LEAF_NODE(node))
		{
			Datum	   *dummy;

			/* We reached at a leaf node, find the corresponding slot */
			radix_tree_node_search(node, &dummy, key, RADIX_TREE_FIND);

			break;
		}

		if (!radix_tree_node_search_child(node, &child, key))
			break;

		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
		level++;
	}

	elog(NOTICE, "\n%s", buf.data);
}

void
radix_tree_dump(radix_tree *tree)
{
	StringInfoData buf;

	initStringInfo(&buf);

	elog(NOTICE, "-----------------------------------------------------------");
	elog(NOTICE, "max_val = %lu", tree->max_val);
	radix_tree_dump_node(tree->root, 0, &buf, true);
	elog(NOTICE, "\n%s", buf.data);
	elog(NOTICE, "-----------------------------------------------------------");
}
#endif
