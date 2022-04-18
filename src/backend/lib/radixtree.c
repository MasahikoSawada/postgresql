/*-------------------------------------------------------------------------
 *
 * radixtree.c
 *		Implementation for adaptive radix tree
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

#if defined(__SSE2__)
#include <emmintrin.h> // x86 SSE2 intrinsics
#endif

/* How many bits are encoded in one tree level */
#define RADIX_TREE_NODE_FANOUT	8

#define RADIX_TREE_NODE_MAX_SLOTS (1 << RADIX_TREE_NODE_FANOUT)
#define RADIX_TREE_CHUNK_MASK ((1 << RADIX_TREE_NODE_FANOUT) - 1)
#define RADIX_TREE_MAX_SHIFT	key_get_shift(UINT64_MAX)
#define RADIX_TREE_MAX_LEVEL	((sizeof(uint64) * BITS_PER_BYTE) / RADIX_TREE_NODE_FANOUT)

#define NodeIsLeaf(n) (((radix_tree_node *) (n))->shift == 0)
#define NodeHasFreeSlot(n) \
	(((radix_tree_node *) (n))->count < \
	 radix_tree_node_info[((radix_tree_node *) (n))->kind].max_slots)

#define GET_KEY_CHUNK(key, shift) \
	((uint8) (((key) >> (shift)) & RADIX_TREE_CHUNK_MASK))

typedef enum radix_tree_node_kind
{
	RADIX_TREE_NODE_KIND_4 = 0,
	RADIX_TREE_NODE_KIND_16,
	RADIX_TREE_NODE_KIND_48,
	RADIX_TREE_NODE_KIND_256
} radix_tree_node_kind;
#define RADIX_TREE_NODE_KIND_COUNT 4

typedef struct radix_tree_node
{
	uint8	count;
	uint8	shift;
	uint8	chunk;
	radix_tree_node_kind	kind;
} radix_tree_node;

/*
 * To reduce memory usage compared to a simple radix tree with a fixed fanout
 * we use adaptive node sides, with different storage methods for different
 * numbers of elements.
 */
typedef struct radix_tree_node_4
{
	radix_tree_node n;

	/* 4 children, for key chunks */
	uint8	chunks[4];
	Datum	slots[4];
} radix_tree_node_4;

typedef struct radix_tree_node_16
{
	radix_tree_node n;

	/* 16 children, for key chunks */
	uint8	chunks[16];
	Datum slots[16];
} radix_tree_node_16;

typedef struct radix_tree_node_48
{
	radix_tree_node n;

	uint8	slot_idxs[RADIX_TREE_NODE_MAX_SLOTS];
	Datum	slots[48];
} radix_tree_node_48;

typedef struct radix_tree_node_256
{
	radix_tree_node n;

	uint8	set[RADIX_TREE_NODE_MAX_SLOTS / (sizeof(uint8) * BITS_PER_BYTE)];
	Datum	slots[RADIX_TREE_NODE_MAX_SLOTS];
} radix_tree_node_256;

typedef struct radix_tree_node_info_elem
{
	const char *name;
	int		max_slots;
	Size	size;
} radix_tree_node_info_elem;

static radix_tree_node_info_elem radix_tree_node_info[] =
{
	{"radix tree node 4", 4, sizeof(radix_tree_node_4)},
	{"radix tree node 16", 16, sizeof(radix_tree_node_16)},
	{"radix tree node 48", 48, sizeof(radix_tree_node_48)},
	{"radix tree node 256", 256, sizeof(radix_tree_node_256)},
};

typedef struct radix_tree_iter_node_data
{
	radix_tree_node *node;
	int	current_idx;	/* -1 for initial value */
} radix_tree_iter_node_data;

struct radix_tree_iter
{
	radix_tree *tree;

	radix_tree_iter_node_data stack[RADIX_TREE_MAX_LEVEL];
	int	stack_len;
	uint64	key;
};

struct radix_tree
{
	uint64	max_val;
	MemoryContext context;
	uint64	mem_used;
	radix_tree_node	*root;
	MemoryContextData *slabs[RADIX_TREE_NODE_KIND_COUNT];
	uint64	num_keys;

	/* stats */
	int32	cnt[RADIX_TREE_NODE_KIND_COUNT];
};

static radix_tree_node *radix_tree_node_grow(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node);
static radix_tree_node *radix_tree_find_child(radix_tree_node *node, uint64 key);
static Datum *radix_tree_find_slot_ptr(radix_tree_node *node, uint8 chunk);
static void radix_tree_replace_slot(radix_tree_node *parent, radix_tree_node *node,
									uint8 chunk);
static void radix_tree_extend(radix_tree *tree, uint64 key);
static void radix_tree_new_root(radix_tree *tree, uint64 key, Datum val);
static radix_tree_node *radix_tree_insert_child(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
												uint64 key);
static void radix_tree_insert_val(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
								  uint64 key, Datum val, bool *replaced_p);

static inline void radix_tree_iter_update_key(radix_tree_iter *iter, uint8 chunk, uint8 shift);
static Datum radix_tree_node_iterate_next(radix_tree_iter *iter, radix_tree_iter_node_data *node_iter,
										  bool *found_p);
static void radix_tree_store_iter_node(radix_tree_iter *iter, radix_tree_iter_node_data *node_iter,
									   radix_tree_node *node);
static void radix_tree_update_iter_stack(radix_tree_iter *iter, int from);

#define RADIX_TREE_NODE_256_SET_BYTE(v) ((v) / RADIX_TREE_NODE_FANOUT)
#define RADIX_TREE_NODE_256_SET_BIT(v) (UINT64_C(1) << ((v) % RADIX_TREE_NODE_FANOUT))
static inline bool
node_256_isset(radix_tree_node_256 *node, uint8 chunk)
{
	return (node->set[RADIX_TREE_NODE_256_SET_BYTE(chunk)] &
			RADIX_TREE_NODE_256_SET_BIT(chunk)) != 0;

}

static inline void
node_256_set(radix_tree_node_256 *node, uint8 chunk, Datum slot)
{
	node->slots[chunk] = slot;
	node->set[RADIX_TREE_NODE_256_SET_BYTE(chunk)] |= RADIX_TREE_NODE_256_SET_BIT(chunk);
}

static inline int
node_48_get_slot_pos(radix_tree_node_48 *node, uint8 chunk)
{
	return node->slot_idxs[chunk] - 1;
}

static inline bool
node_48_is_slot_used(radix_tree_node_48 *node, uint8 chunk)
{
	return (node_48_get_slot_pos(node, chunk) >= 0);
}

static inline int
node_16_search_eq(radix_tree_node_16 *node, uint8 chunk)
{
#ifdef __SSE2__
	__m128i	_key = _mm_set1_epi8(chunk);
	__m128i _data = _mm_loadu_si128((__m128i_u *) node->chunks);
	__m128i _cmp = _mm_cmpeq_epi8(_key, _data);
	uint32	bitfield = _mm_movemask_epi8(_cmp);

	bitfield &= ((1 << node->n.count) - 1);

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
#endif	/* __SSE2__ */
}

/*
 * This is a bit more complicated than search_chunk_array_16_eq(), because
 * until recently no unsigned uint8 comparison instruction existed on x86. So
 * we need to play some trickery using _mm_min_epu8() to effectively get
 * <=. There never will be any equal elements in the current uses, but that's
 * what we get here...
 */
static inline int
node_16_search_le(radix_tree_node_16 *node, uint8 chunk)
{
#ifdef __SSE2__
	__m128i _key = _mm_set1_epi8(chunk);
	__m128i _data = _mm_loadu_si128((__m128i_u*) node->chunks);
	__m128i _min = _mm_min_epu8(_key, _data);
	__m128i cmp = _mm_cmpeq_epi8(_key, _min);
	uint32_t bitfield=_mm_movemask_epi8(cmp);

	bitfield &= ((1 << node->n.count) - 1);

	return (bitfield) ? __builtin_ctz(bitfield) : node->n.count;
#else
	int index;

	for (index = 0; index < node->n.count; index++)
	{
		if (node->chunks[index] >= chunk)
			break;
	}

	return index;
#endif	/* __SSE2__ */
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

	tree->mem_used += GetMemoryChunkSpace(newnode);

	newnode->kind = kind;

	/* stats */
	tree->cnt[kind]++;

	return newnode;
}

/* Free the given node */
static void
radix_tree_free_node(radix_tree *tree, radix_tree_node *node)
{
	tree->mem_used -= GetMemoryChunkSpace(node);
	tree->cnt[node->kind]--;

	pfree(node);
}

/* Copy the common fields */
static void
radix_tree_copy_node_common(radix_tree_node *src, radix_tree_node *dst)
{
	dst->shift = src->shift;
	dst->chunk = src->chunk;
	dst->count = src->count;
}

/*
 * The tree doesn't have not sufficient height, so grow it */
static void
radix_tree_extend(radix_tree *tree, uint64 key)
{
	int max_shift;
	int shift = tree->root->shift + RADIX_TREE_NODE_FANOUT;

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
 * Return the pointer to the child node corresponding with the key. Otherwise (if
 * not found) return NULL.
 */
static radix_tree_node *
radix_tree_find_child(radix_tree_node *node, uint64 key)
{
	Datum *slot_ptr;
	int chunk = GET_KEY_CHUNK(key, node->shift);

	slot_ptr = radix_tree_find_slot_ptr(node, chunk);

	return (slot_ptr == NULL) ? NULL : (radix_tree_node *) DatumGetPointer(*slot_ptr);
}

/*
 * Return the address of the slot corresponding to chunk in the node, if found.
 * Otherwise return NULL.
 */
static Datum *
radix_tree_find_slot_ptr(radix_tree_node *node, uint8 chunk)
{

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
		{
			radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;
			for (int i = 0; i < n4->n.count; i++)
			{
				if (n4->chunks[i] > chunk)
					break;

				if (n4->chunks[i] == chunk)
					return &(n4->slots[i]);
			}

			return NULL;

			break;
		}
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;
			int ret;

			ret = node_16_search_eq(n16, chunk);

			if (ret < 0)
				return NULL;

			return &(n16->slots[ret]);
			break;
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;

			if (!node_48_is_slot_used(n48, chunk))
				return NULL;

			return &(n48->slots[node_48_get_slot_pos(n48, chunk)]);
			break;
		}
		case RADIX_TREE_NODE_KIND_256:
		{
			radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

			if (!node_256_isset(n256, chunk))
				return NULL;

			return &(n256->slots[chunk]);
			break;
		}
	}

	pg_unreachable();
}

/* Redirect from the parent to the node */
static void
radix_tree_replace_slot(radix_tree_node *parent, radix_tree_node *node, uint8 chunk)
{
	Datum *slot_ptr;

	slot_ptr = radix_tree_find_slot_ptr(parent, chunk);
	*slot_ptr = PointerGetDatum(node);
}

/*
 * Create a new node as the root. Subordinate nodes will be created during
 * the insertion.
 */
static void
radix_tree_new_root(radix_tree *tree, uint64 key, Datum val)
{
	radix_tree_node_4 * n4 =
		(radix_tree_node_4 * ) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);
	int shift = key_get_shift(key);

	n4->n.shift = shift;
	tree->max_val = shift_get_max_val(shift);
	tree->root = (radix_tree_node *) n4;
}

static radix_tree_node *
radix_tree_insert_child(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
						uint64 key)
{
	radix_tree_node *newchild =
		(radix_tree_node *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);

	Assert(!NodeIsLeaf(node));

	newchild->shift = node->shift - RADIX_TREE_NODE_FANOUT;
	newchild->chunk = GET_KEY_CHUNK(key, node->shift);

	radix_tree_insert_val(tree, parent, node, key, PointerGetDatum(newchild),
						  NULL);

	return (radix_tree_node *) newchild;
}

/*
 * Insert the value to the node. The node grows if it's full.
 */
static void
radix_tree_insert_val(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
					  uint64 key, Datum val, bool *replaced_p)
{
	int chunk = GET_KEY_CHUNK(key, node->shift);
	bool replaced = false;

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
		{
			radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;
			int i;

			for (i = 0; i < n4->n.count; i++)
			{
				if (n4->chunks[i] >= chunk)
					break;
			}

			if (NodeHasFreeSlot(n4))
			{
				if (n4->n.count == 0)
				{
					/* first key for this node */
				}
				else if (n4->chunks[i] == chunk)
				{
					/* found the key */
					replaced = true;
				}
				else
				{
					/* make space for the new key */
					memmove(&(n4->chunks[i + 1]), &(n4->chunks[i]),
							sizeof(uint8) * (n4->n.count - i));
					memmove(&(n4->slots[i + 1]), &(n4->slots[i]),
							sizeof(radix_tree_node *) * (n4->n.count - i));
				}

				n4->chunks[i] = chunk;
				n4->slots[i] = val;
				break;
			}

			node = radix_tree_node_grow(tree, parent, node);
			Assert(node->kind == RADIX_TREE_NODE_KIND_16);
		}
		/* FALLTHROUGH */
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;
			int idx;

			idx = node_16_search_le(n16, chunk);

			if (NodeHasFreeSlot(n16))
			{
				if (n16->n.count == 0)
				{
					/* first key for this node */
				}
				else if (n16->chunks[idx] == chunk)
				{
					/* found the key */
					replaced = true;
				}
				else
				{
					/* make space for the new key */
					memmove(&(n16->chunks[idx + 1]), &(n16->chunks[idx]),
							sizeof(uint8) * (n16->n.count - idx));
					memmove(&(n16->slots[idx + 1]), &(n16->slots[idx]),
							sizeof(radix_tree_node *) * (n16->n.count - idx));
				}

				n16->chunks[idx] = chunk;
				n16->slots[idx] = val;
				break;
			}

			node = radix_tree_node_grow(tree, parent, node);
			Assert(node->kind == RADIX_TREE_NODE_KIND_48);
		}
		/* FALLTHROUGH */
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;

			if (node_48_is_slot_used(n48, chunk))
			{
				n48->slots[node_48_get_slot_pos(n48, chunk)] = val;
				replaced = true;
				break;
			}

			if (NodeHasFreeSlot(n48))
			{
				uint8 pos = n48->n.count + 1;

				n48->slot_idxs[chunk] = pos;
				n48->slots[pos - 1] = val;
				break;
			}

			node = radix_tree_node_grow(tree, parent, node);
			Assert(node->kind == RADIX_TREE_NODE_KIND_256);
		}
		/* FALLTHROUGH */
		case RADIX_TREE_NODE_KIND_256:
		{
			radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

			if (node_256_isset(n256, chunk))
				replaced = true;

			node_256_set(n256, chunk, val);
			break;
		}
	}

	if (!replaced)
		node->count++;

	if (replaced_p)
		*replaced_p = replaced;
}

/*
 * Change the node type to a larger one.
 */
static radix_tree_node *
radix_tree_node_grow(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node)
{
	radix_tree_node *newnode = NULL;

	Assert(node->count ==
		   radix_tree_node_info[node->kind].max_slots);

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
		{
			radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;
			radix_tree_node_16 *new16 =
				(radix_tree_node_16 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_16);

			radix_tree_copy_node_common((radix_tree_node *) n4,
										(radix_tree_node *) new16);

			memcpy(&(new16->chunks), &(n4->chunks), sizeof(uint8) * 4);
			memcpy(&(new16->slots), &(n4->slots), sizeof(Datum) * 4);

			/* Sort slots and chunks arrays */
			for (int i = 0; i < n4->n.count ; i++)
			{
				for (int j = i; j < n4->n.count; j++)
				{
					if (new16->chunks[i] > new16->chunks[j])
					{
						new16->chunks[i] = new16->chunks[j];
						new16->slots[i] = new16->slots[j];
					}
				}
			}

			newnode = (radix_tree_node *) new16;
			break;
		}
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;
			radix_tree_node_48 *new48 =
				(radix_tree_node_48 *) radix_tree_alloc_node(tree,RADIX_TREE_NODE_KIND_48);

			radix_tree_copy_node_common((radix_tree_node *) n16,
										(radix_tree_node *) new48);

			for (int i = 0; i < n16->n.count; i++)
			{
				new48->slot_idxs[n16->chunks[i]] = i + 1;
				new48->slots[i] = n16->slots[i];
			}

			/* TEST */
			/*
			{
				for (int i = 0; i < n16->n.count; i++)
				{
					uint8 chunk = n16->chunks[i];

					Assert(new48->slot_idxs[chunk] != 0);
				}
			}
			*/

			newnode = (radix_tree_node *) new48;
			break;
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;
			radix_tree_node_256 *new256 =
				(radix_tree_node_256 *) radix_tree_alloc_node(tree,RADIX_TREE_NODE_KIND_256);
			int cnt = 0;

			radix_tree_copy_node_common((radix_tree_node *) n48,
										(radix_tree_node *) new256);

			for (int i = 0; i < 256 && cnt < n48->n.count; i++)
			{
				if (!node_48_is_slot_used(n48, i))
					continue;

				node_256_set(new256, i, n48->slots[node_48_get_slot_pos(n48, i)]);
				cnt++;
			}

			/* test */
			/*
			{
				int n = 0;
				for (int i = 0; i < 32; i++)
					n += pg_popcount32((uint32) new256->set[i]);

				Assert(new256->n.count == n);
			}
			*/

			newnode = (radix_tree_node *) new256;
			break;
		}
		case RADIX_TREE_NODE_KIND_256:
			elog(ERROR, "radix tree node_256 cannot be grew");
			break;
	}

	if (parent == node)
		tree->root = newnode;
	else
		radix_tree_replace_slot(parent, newnode, node->chunk);

	radix_tree_free_node(tree, node);

	return newnode;
}

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

void
radix_tree_destroy(radix_tree *tree)
{
	for (int i = 0; i < RADIX_TREE_NODE_KIND_COUNT; i++)
		MemoryContextDelete(tree->slabs[i]);

	pfree(tree);
}

void
radix_tree_insert(radix_tree *tree, uint64 key, Datum val, bool *found_p)
{
	int shift;
	bool	replaced;
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

		child = radix_tree_find_child(node, key);

		if (child == NULL)
			child = radix_tree_insert_child(tree, parent, node, key);

		parent = node;
		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
	}

	/* arrived at a leaf */
	Assert(NodeIsLeaf(node));

	radix_tree_insert_val(tree, parent, node, key, val, &replaced);

	if (!replaced)
		tree->num_keys++;

	if (found_p)
		*found_p = replaced;
}

Datum
radix_tree_search(radix_tree *tree, uint64 key, bool *found)
{
	radix_tree_node *node;
	int shift;

	if (!tree->root || key > tree->max_val)
		goto not_found;

	node = tree->root;
	shift = tree->root->shift;
	while (shift >= 0)
	{
		radix_tree_node *child;

		if (NodeIsLeaf(node))
		{
			Datum *slot_ptr;
			int chunk = GET_KEY_CHUNK(key, node->shift);

			/* We reached at a leaf node, find the corresponding slot */
			slot_ptr = radix_tree_find_slot_ptr(node, chunk);

			if (slot_ptr == NULL)
				goto not_found;

			/* Found! */
			*found = true;
			return *slot_ptr;
		}

		child = radix_tree_find_child(node, key);

		if (child == NULL)
			goto not_found;

		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
	}

not_found:
	*found = false;
	return (Datum) 0;
}

radix_tree_iter *
radix_tree_begin_iterate(radix_tree *tree)
{
	MemoryContext old_ctx;
	radix_tree_iter *iter;
	int top_level;

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

bool
radix_tree_iterate_next(radix_tree_iter *iter, uint64 *key_p, Datum *value_p)
{
	bool found = false;
	Datum slot = (Datum) 0;
	int level;

	/* Empty tree */
	if (!iter->tree)
		return false;

	for (;;)
	{
		radix_tree_node *node;
		radix_tree_iter_node_data *node_iter;

		for (level = 0; level <= iter->stack_len; level++)
		{
			slot = radix_tree_node_iterate_next(iter, &(iter->stack[level]), &found);

			if (found)
				break;
		}

		/* end of iteration */
		if (!found)
			return false;

		/* FOUND at leaf !! */
		if (level == 0)
		{
			*key_p = iter->key;
			*value_p = slot;
			return true;
		}

		/*
		 * Descend to the left most leaf node from this level while refreshing
		 * the stack.
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
	Datum slot = (Datum) 0;

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
			if (NodeIsLeaf(node))
				radix_tree_iter_update_key(iter, n4->chunks[node_iter->current_idx], 0);

			break;
		}
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;

			node_iter->current_idx++;

			if (node_iter->current_idx >= n16->n.count)
				goto not_found;

			slot = n16->slots[node_iter->current_idx];

			/* Update the part of the key with the current chunk */
			if (NodeIsLeaf(node))
				radix_tree_iter_update_key(iter, n16->chunks[node_iter->current_idx], 0);

			break;
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;
			int i;

			for (i = node_iter->current_idx + 1; i < 256; i++)
			{
				if (node_48_is_slot_used(n48, i))
					break;
			}

			if (i >= 256)
				goto not_found;

			node_iter->current_idx = i;
			slot = n48->slots[node_48_get_slot_pos(n48, i)];

			/* Update the part of the key */
			if (NodeIsLeaf(node))
				radix_tree_iter_update_key(iter, node_iter->current_idx, 0);

			break;
		}
		case RADIX_TREE_NODE_KIND_256:
		{
			radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;
			int i;

			for (i = node_iter->current_idx + 1; i < 256; i++)
			{
				if (node_256_isset(n256, i))
					break;
			}

			if (i >= 256)
				goto not_found;

			node_iter->current_idx = i;
			slot = n256->slots[i];

			/* Update the part of the key */
			if (NodeIsLeaf(node))
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
	int level = from;

	for (;;)
	{
		radix_tree_iter_node_data *node_iter = &(iter->stack[level--]);
		bool found;

		/* Set the current node */
		radix_tree_store_iter_node(iter, node_iter, node);

		if (NodeIsLeaf(node))
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

uint64
radix_tree_num_entries(radix_tree *tree)
{
	return tree->num_keys;
}

uint64
radix_tree_memory_usage(radix_tree *tree)
{
	return tree->mem_used;
}

/***************** DEBUG FUNCTIONS *****************/
#ifdef RADIX_TREE_DEBUG
void
radix_tree_stats(radix_tree *tree)
{
	fprintf(stderr, "num_keys = %lu, height = %u, n4 = %u(%lu), n16 = %u(%lu), n48 = %u(%lu), n256 = %u(%lu)",
			tree->num_keys,
			tree->root->shift / RADIX_TREE_NODE_FANOUT,
			tree->cnt[0], tree->cnt[0] * sizeof(radix_tree_node_4),
			tree->cnt[1], tree->cnt[1] * sizeof(radix_tree_node_16),
			tree->cnt[2], tree->cnt[2] * sizeof(radix_tree_node_48),
			tree->cnt[3], tree->cnt[3] * sizeof(radix_tree_node_256));
	//radix_tree_dump(tree);
}

static void
radix_tree_print_slot(StringInfo buf, uint8 chunk, Datum slot, int idx, bool is_leaf, int level)
{
	char space[128] = {0};

	if (level > 0)
		sprintf(space, "%*c", level * 4, ' ');

	if (is_leaf)
		appendStringInfo(buf, "%s[%d] \"0x%X\" val(0x%lX) LEAF\n",
						 space,
						 idx,
						 chunk,
						 DatumGetInt64(slot));
	else
		appendStringInfo(buf , "%s[%d] \"0x%X\" -> ",
						 space,
						 idx,
						 chunk);
}

static void
radix_tree_dump_node(radix_tree_node *node, int level, StringInfo buf, bool recurse)
{
	bool is_leaf = NodeIsLeaf(node);

	appendStringInfo(buf, "[\"%s\" type %d, cnt %u, shift %u, chunk \"0x%X\"] chunks:\n",
					 NodeIsLeaf(node) ? "LEAF" : "INNR",
					 (node->kind == RADIX_TREE_NODE_KIND_4) ? 4 :
					 (node->kind == RADIX_TREE_NODE_KIND_16) ? 16 :
					 (node->kind == RADIX_TREE_NODE_KIND_48) ? 48 : 256,
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
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;

			for (int i = 0; i < n16->n.count; i++)
			{
				radix_tree_print_slot(buf, n16->chunks[i], n16->slots[i], i, is_leaf, level);

				if (!is_leaf)
				{
					if (recurse)
					{
						StringInfoData buf2;

						initStringInfo(&buf2);
						radix_tree_dump_node((radix_tree_node *) n16->slots[i], level + 1, &buf2, recurse);
						appendStringInfo(buf, "%s", buf2.data);
					}
					else
						appendStringInfo(buf, "\n");
				}
			}
			break;
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;

			for (int i = 0; i < 256; i++)
			{
				if (!node_48_is_slot_used(n48, i))
					continue;

				radix_tree_print_slot(buf, i, n48->slots[node_48_get_slot_pos(n48, i)],
									  i, is_leaf, level);

				if (!is_leaf)
				{
					if (recurse)
					{
						StringInfoData buf2;

						initStringInfo(&buf2);
						radix_tree_dump_node((radix_tree_node *) n48->slots[node_48_get_slot_pos(n48, i)],
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
				if (!node_256_isset(n256, i))
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
	int shift;
	int level = 0;

	elog(WARNING, "-----------------------------------------------------------");
	elog(WARNING, "max_val = %lu (0x%lX)", tree->max_val, tree->max_val);

	if (!tree->root)
	{
		elog(WARNING, "tree is empty");
		return;
	}

	if (key > tree->max_val)
	{
		elog(WARNING, "key %lu (0x%lX) is larger than max val",
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

		if (NodeIsLeaf(node))
		{
			int chunk = GET_KEY_CHUNK(key, node->shift);

			/* We reached at a leaf node, find the corresponding slot */
			radix_tree_find_slot_ptr(node, chunk);

			break;
		}

		child = radix_tree_find_child(node, key);

		if (child == NULL)
			break;

		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
		level++;
	}

	elog(WARNING, "\n%s", buf.data);
}

void
radix_tree_dump(radix_tree *tree)
{
	StringInfoData buf;

	initStringInfo(&buf);

	elog(WARNING, "-----------------------------------------------------------");
	elog(WARNING, "max_val = %lu", tree->max_val);
	radix_tree_dump_node(tree->root, 0, &buf, true);
	elog(WARNING, "\n%s", buf.data);
	elog(WARNING, "-----------------------------------------------------------");
}
#endif
