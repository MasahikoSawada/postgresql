/*-------------------------------------------------------------------------
 *
 * radixtree.c
 *		Implementation for adaptive radix tree.
 *
 * This module employs the idea from the paper "The Adaptive Radix Tree: ARTful
 * Indexing for Main-Memory Databases" by Viktor Leis, Alfons Kemper, and Thomas
 * Neumann, 2013. The radix tree uses adaptive node sizes, a small number of node
 * types, each with a different numbers of elements. Depending on the number of
 * children, the appropriate node type is used.
 *
 * There are some differences from the proposed implementation. For instance,
 * there is not support for path compression and lazy path expansion. The radix
 * tree supports fixed length of the key so we don't expect the tree level
 * wouldn't be high.
 *
 * Both the key and the value are 64-bit unsigned integer. The inner nodes and
 * the leaf nodes have slightly different structure: for inner tree nodes,
 * shift > 0, store the pointer to its child node as the value. The leaf nodes,
 * shift == 0, have the 64-bit unsigned integer that is specified by the user as
 * the value. The paper refers to this technique as "Multi-value leaves".  We
 * choose it to avoid an additional pointer traversal.  It is the reason this code
 * currently does not support variable-length keys.
 *
 * XXX: Most functions in this file have two variants for inner nodes and leaf
 * nodes, therefore there are duplication codes. While this sometimes makes the
 * code maintenance tricky, this reduces branch prediction misses when judging
 * whether the node is a inner node of a leaf node.
 *
 * XXX: the radix tree node never be shrunk.
 *
 *	  To generate a radix tree and associated functions for a use case several
 *	  macros have to be #define'ed before this file is included.  Including
 *	  the file #undef's all those, so a new radix tree can be generated
 *	  afterwards.
 *	  The relevant parameters are:
 *	  - RT_PREFIX - prefix for all symbol names generated. A prefix of 'foo'
 *		will result in radix tree type 'foo_radix_tree' and functions like
 *		'foo_create'/'foo_free' and so forth.
 *	  - RT_DECLARE - if defined function prototypes and type declarations are
 *		generated
 *	  - RT_DEFINE - if defined function definitions are generated
 *	  - RT_SCOPE - in which scope (e.g. extern, static inline) do function
 *		declarations reside
 *	  - RT_SHMEM - if defined, the radix tree is created in the DSA area
 *		so that multiple processes can access it simultaneously.
 *
 *	  Optional parameters:
 *	  - RT_DEBUG - if defined add stats tracking and debugging functions
 *
 * Interface
 * ---------
 *
 * RT_CREATE		- Create a new, empty radix tree
 * RT_FREE			- Free the radix tree
 * RT_ATTACH		- Attach to the radix tree
 * RT_DETACH		- Detach from the radix tree
 * RT_GET_HANDLE	- Return the handle of the radix tree
 * RT_SEARCH		- Search a key-value pair
 * RT_SET			- Set a key-value pair
 * RT_DELETE		- Delete a key-value pair
 * RT_BEGIN_ITERATE	- Begin iterating through all key-value pairs
 * RT_ITERATE_NEXT	- Return next key-value pair, if any
 * RT_END_ITER		- End iteration
 * RT_MEMORY_USAGE	- Get the memory usage
 * RT_NUM_ENTRIES	- Get the number of key-value pairs
 *
 * RT_CREATE() creates an empty radix tree in the given memory context
 * and memory contexts for all kinds of radix tree node under the memory context.
 *
 * RT_ITERATE_NEXT() ensures returning key-value pairs in the ascending
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

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "port/pg_bitutils.h"
#include "port/simd.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/* helpers */
#define RT_MAKE_PREFIX(a) CppConcat(a,_)
#define RT_MAKE_NAME(name) RT_MAKE_NAME_(RT_MAKE_PREFIX(RT_PREFIX),name)
#define RT_MAKE_NAME_(a,b) CppConcat(a,b)

/* function declarations */
#define RT_CREATE RT_MAKE_NAME(create)
#define RT_FREE RT_MAKE_NAME(free)
#define RT_SEARCH RT_MAKE_NAME(search)
#ifdef RT_SHMEM
#define RT_ATTACH RT_MAKE_NAME(attach)
#define RT_DETACH RT_MAKE_NAME(detach)
#define RT_GET_HANDLE RT_MAKE_NAME(get_handle)
#endif
#define RT_SET RT_MAKE_NAME(set)
#define RT_BEGIN_ITERATE RT_MAKE_NAME(begin_iterate)
#define RT_ITERATE_NEXT RT_MAKE_NAME(iterate_next)
#define RT_END_ITERATE RT_MAKE_NAME(end_iterate)
#define RT_DELETE RT_MAKE_NAME(delete)
#define RT_MEMORY_USAGE RT_MAKE_NAME(memory_usage)
#define RT_NUM_ENTRIES RT_MAKE_NAME(num_entries)
#define RT_DUMP RT_MAKE_NAME(dump)
#define RT_DUMP_SEARCH RT_MAKE_NAME(dump_search)
#define RT_STATS RT_MAKE_NAME(stats)

/* internal helper functions (no externally visible prototypes) */
#define RT_NEW_ROOT RT_MAKE_NAME(new_root)
#define RT_ALLOC_NODE RT_MAKE_NAME(alloc_node)
#define RT_INIT_NODE RT_MAKE_NAME(init_node)
#define RT_FREE_NODE RT_MAKE_NAME(free_node)
#define RT_EXTEND RT_MAKE_NAME(extend)
#define RT_SET_EXTEND RT_MAKE_NAME(set_extend)
//#define RT_GROW_NODE_KIND RT_MAKE_NAME(grow_node_kind)
#define RT_COPY_NODE RT_MAKE_NAME(copy_node)
#define RT_REPLACE_NODE RT_MAKE_NAME(replace_node)
#define RT_PTR_GET_LOCAL RT_MAKE_NAME(ptr_get_local)
#define RT_PTR_ALLOC_IS_VALID RT_MAKE_NAME(ptr_stored_is_valid)
#define RT_NODE_4_SEARCH_EQ RT_MAKE_NAME(node_4_search_eq)
#define RT_NODE_32_SEARCH_EQ RT_MAKE_NAME(node_32_search_eq)
#define RT_NODE_4_GET_INSERTPOS RT_MAKE_NAME(node_4_get_insertpos)
#define RT_NODE_32_GET_INSERTPOS RT_MAKE_NAME(node_32_get_insertpos)
#define RT_CHUNK_CHILDREN_ARRAY_SHIFT RT_MAKE_NAME(chunk_children_array_shift)
#define RT_CHUNK_VALUES_ARRAY_SHIFT RT_MAKE_NAME(chunk_values_array_shift)
#define RT_CHUNK_CHILDREN_ARRAY_DELETE RT_MAKE_NAME(chunk_children_array_delete)
#define RT_CHUNK_VALUES_ARRAY_DELETE RT_MAKE_NAME(chunk_values_array_delete)
#define RT_CHUNK_CHILDREN_ARRAY_COPY RT_MAKE_NAME(chunk_children_array_copy)
#define RT_CHUNK_VALUES_ARRAY_COPY RT_MAKE_NAME(chunk_values_array_copy)
#define RT_NODE_125_IS_CHUNK_USED RT_MAKE_NAME(node_125_is_chunk_used)
#define RT_NODE_INNER_125_GET_CHILD RT_MAKE_NAME(node_inner_125_get_child)
#define RT_NODE_LEAF_125_GET_VALUE RT_MAKE_NAME(node_leaf_125_get_value)
#define RT_NODE_INNER_256_IS_CHUNK_USED RT_MAKE_NAME(node_inner_256_is_chunk_used)
#define RT_NODE_LEAF_256_IS_CHUNK_USED RT_MAKE_NAME(node_leaf_256_is_chunk_used)
#define RT_NODE_INNER_256_GET_CHILD RT_MAKE_NAME(node_inner_256_get_child)
#define RT_NODE_LEAF_256_GET_VALUE RT_MAKE_NAME(node_leaf_256_get_value)
#define RT_NODE_INNER_256_SET RT_MAKE_NAME(node_inner_256_set)
#define RT_NODE_LEAF_256_SET RT_MAKE_NAME(node_leaf_256_set)
#define RT_NODE_INNER_256_DELETE RT_MAKE_NAME(node_inner_256_delete)
#define RT_NODE_LEAF_256_DELETE RT_MAKE_NAME(node_leaf_256_delete)
#define RT_KEY_GET_SHIFT RT_MAKE_NAME(key_get_shift)
#define RT_SHIFT_GET_MAX_VAL RT_MAKE_NAME(shift_get_max_val)
#define RT_NODE_SEARCH_INNER RT_MAKE_NAME(node_search_inner)
#define RT_NODE_SEARCH_LEAF RT_MAKE_NAME(node_search_leaf)
#define RT_NODE_UPDATE_INNER RT_MAKE_NAME(node_update_inner)
#define RT_NODE_DELETE_INNER RT_MAKE_NAME(node_delete_inner)
#define RT_NODE_DELETE_LEAF RT_MAKE_NAME(node_delete_leaf)
#define RT_NODE_INSERT_INNER RT_MAKE_NAME(node_insert_inner)
#define RT_NODE_INSERT_LEAF RT_MAKE_NAME(node_insert_leaf)
#define RT_NODE_INNER_ITERATE_NEXT RT_MAKE_NAME(node_inner_iterate_next)
#define RT_NODE_LEAF_ITERATE_NEXT RT_MAKE_NAME(node_leaf_iterate_next)
#define RT_UPDATE_ITER_STACK RT_MAKE_NAME(update_iter_stack)
#define RT_ITER_UPDATE_KEY RT_MAKE_NAME(iter_update_key)
#define RT_VERIFY_NODE RT_MAKE_NAME(verify_node)

/* type declarations */
#define RT_RADIX_TREE RT_MAKE_NAME(radix_tree)
#define RT_RADIX_TREE_CONTROL RT_MAKE_NAME(radix_tree_control)
#define RT_ITER RT_MAKE_NAME(iter)
#ifdef RT_SHMEM
#define RT_HANDLE RT_MAKE_NAME(handle)
#endif
#define RT_NODE RT_MAKE_NAME(node)
#define RT_NODE_ITER RT_MAKE_NAME(node_iter)
#define RT_NODE_BASE_4 RT_MAKE_NAME(node_base_4)
#define RT_NODE_BASE_32 RT_MAKE_NAME(node_base_32)
#define RT_NODE_BASE_125 RT_MAKE_NAME(node_base_125)
#define RT_NODE_BASE_256 RT_MAKE_NAME(node_base_256)
#define RT_NODE_INNER_4 RT_MAKE_NAME(node_inner_4)
#define RT_NODE_INNER_32 RT_MAKE_NAME(node_inner_32)
#define RT_NODE_INNER_125 RT_MAKE_NAME(node_inner_125)
#define RT_NODE_INNER_256 RT_MAKE_NAME(node_inner_256)
#define RT_NODE_LEAF_4 RT_MAKE_NAME(node_leaf_4)
#define RT_NODE_LEAF_32 RT_MAKE_NAME(node_leaf_32)
#define RT_NODE_LEAF_125 RT_MAKE_NAME(node_leaf_125)
#define RT_NODE_LEAF_256 RT_MAKE_NAME(node_leaf_256)
#define RT_SIZE_CLASS RT_MAKE_NAME(size_class)
#define RT_SIZE_CLASS_ELEM RT_MAKE_NAME(size_class_elem)
#define RT_SIZE_CLASS_INFO RT_MAKE_NAME(size_class_info)
#define RT_CLASS_4_FULL RT_MAKE_NAME(class_4_full)
#define RT_CLASS_32_PARTIAL RT_MAKE_NAME(class_32_partial)
#define RT_CLASS_32_FULL RT_MAKE_NAME(class_32_full)
#define RT_CLASS_125_FULL RT_MAKE_NAME(class_125_full)
#define RT_CLASS_256 RT_MAKE_NAME(class_256)
#define RT_KIND_MIN_SIZE_CLASS RT_MAKE_NAME(kind_min_size_class)

/* generate forward declarations necessary to use the radix tree */
#ifdef RT_DECLARE

typedef struct RT_RADIX_TREE RT_RADIX_TREE;
typedef struct RT_ITER RT_ITER;

#ifdef RT_SHMEM
typedef dsa_pointer RT_HANDLE;
#endif

#ifdef RT_SHMEM
RT_SCOPE RT_RADIX_TREE * RT_CREATE(MemoryContext ctx, dsa_area *dsa);
RT_SCOPE RT_RADIX_TREE * RT_ATTACH(dsa_area *dsa, dsa_pointer dp);
RT_SCOPE void RT_DETACH(RT_RADIX_TREE *tree);
RT_SCOPE RT_HANDLE RT_GET_HANDLE(RT_RADIX_TREE *tree);
#else
RT_SCOPE RT_RADIX_TREE * RT_CREATE(MemoryContext ctx);
#endif
RT_SCOPE void RT_FREE(RT_RADIX_TREE *tree);

RT_SCOPE bool RT_SEARCH(RT_RADIX_TREE *tree, uint64 key, uint64 *val_p);
RT_SCOPE bool RT_SET(RT_RADIX_TREE *tree, uint64 key, uint64 val);
RT_SCOPE bool RT_DELETE(RT_RADIX_TREE *tree, uint64 key);

RT_SCOPE RT_ITER * RT_BEGIN_ITERATE(RT_RADIX_TREE *tree);
RT_SCOPE bool RT_ITERATE_NEXT(RT_ITER *iter, uint64 *key_p, uint64 *value_p);
RT_SCOPE void RT_END_ITERATE(RT_ITER *iter);

RT_SCOPE uint64 RT_MEMORY_USAGE(RT_RADIX_TREE *tree);
RT_SCOPE uint64 RT_NUM_ENTRIES(RT_RADIX_TREE *tree);

#ifdef RT_DEBUG
RT_SCOPE void RT_DUMP(RT_RADIX_TREE *tree);
RT_SCOPE void RT_DUMP_SEARCH(RT_RADIX_TREE *tree, uint64 key);
RT_SCOPE void RT_STATS(RT_RADIX_TREE *tree);
#endif

#endif							/* RT_DECLARE */


/* generate implementation of the radix tree */
#ifdef RT_DEFINE

/* macros and types common to all implementations */
#ifndef RT_COMMON
#define RT_COMMON

#ifdef RT_DEBUG
#define UINT64_FORMAT_HEX "%" INT64_MODIFIER "X"
#endif

/* The number of bits encoded in one tree level */
#define RT_NODE_SPAN	BITS_PER_BYTE

/* The number of maximum slots in the node */
#define RT_NODE_MAX_SLOTS (1 << RT_NODE_SPAN)

/* Mask for extracting a chunk from the key */
#define RT_CHUNK_MASK ((1 << RT_NODE_SPAN) - 1)

/* Maximum shift the radix tree uses */
#define RT_MAX_SHIFT	RT_KEY_GET_SHIFT(UINT64_MAX)

/* Tree level the radix tree uses */
#define RT_MAX_LEVEL	((sizeof(uint64) * BITS_PER_BYTE) / RT_NODE_SPAN)

/* Invalid index used in node-125 */
#define RT_NODE_125_INVALID_IDX	0xFF

/* Get a chunk from the key */
#define RT_GET_KEY_CHUNK(key, shift) ((uint8) (((key) >> (shift)) & RT_CHUNK_MASK))

/* For accessing bitmaps */
#define BM_IDX(x)	((x) / BITS_PER_BITMAPWORD)
#define BM_BIT(x)	((x) % BITS_PER_BITMAPWORD)

/*
 * Supported radix tree node kinds and size classes.
 *
 * There are 4 node kinds and each node kind have one or two size classes,
 * partial and full. The size classes in the same node kind have the same
 * node structure but have the different number of fanout that is stored
 * in 'fanout' of RT_NODE. For example in size class 15, when a 16th element
 * is to be inserted, we allocate a larger area and memcpy the entire old
 * node to it.
 *
 * This technique allows us to limit the node kinds to 4, which limits the
 * number of cases in switch statements. It also allows a possible future
 * optimization to encode the node kind in a pointer tag.
 *
 * These size classes have been chose carefully so that it minimizes the
 * allocator padding in both the inner and leaf nodes on DSA.
 * node
 */
#define RT_NODE_KIND_4			0x00
#define RT_NODE_KIND_32			0x01
#define RT_NODE_KIND_125		0x02
#define RT_NODE_KIND_256		0x03
#define RT_NODE_KIND_COUNT		4

#endif							/* RT_COMMON */


typedef enum RT_SIZE_CLASS
{
	RT_CLASS_4_FULL = 0,
	RT_CLASS_32_PARTIAL,
	RT_CLASS_32_FULL,
	RT_CLASS_125_FULL,
	RT_CLASS_256
} RT_SIZE_CLASS;

/* Common type for all nodes types */
typedef struct RT_NODE
{
	/*
	 * Number of children.  We use uint16 to be able to indicate 256 children
	 * at the fanout of 8.
	 */
	uint16		count;

	/*
	 * Max capacity for the current size class. Storing this in the
	 * node enables multiple size classes per node kind.
	 * Technically, kinds with a single size class don't need this, so we could
	 * keep this in the individual base types, but the code is simpler this way.
	 * Note: node256 is unique in that it cannot possibly have more than a
	 * single size class, so for that kind we store zero, and uint8 is
	 * sufficient for other kinds.
	 */
	uint8		fanout;

	/*
	 * Shift indicates which part of the key space is represented by this
	 * node. That is, the key is shifted by 'shift' and the lowest
	 * RT_NODE_SPAN bits are then represented in chunk.
	 */
	uint8		shift;

	/* Node kind, one per search/set algorithm */
	uint8		kind;
} RT_NODE;


#define RT_PTR_LOCAL RT_NODE *

#ifdef RT_SHMEM
#define RT_PTR_ALLOC dsa_pointer
#else
#define RT_PTR_ALLOC RT_PTR_LOCAL
#endif


#ifdef RT_SHMEM
#define RT_INVALID_PTR_ALLOC InvalidDsaPointer
#else
#define RT_INVALID_PTR_ALLOC NULL
#endif

#define NODE_IS_LEAF(n)			(((RT_PTR_LOCAL) (n))->shift == 0)
#define NODE_IS_EMPTY(n)		(((RT_PTR_LOCAL) (n))->count == 0)
#define VAR_NODE_HAS_FREE_SLOT(node) \
	((node)->base.n.count < (node)->base.n.fanout)
#define FIXED_NODE_HAS_FREE_SLOT(node, class) \
	((node)->base.n.count < RT_SIZE_CLASS_INFO[class].fanout)

/* Base type of each node kinds for leaf and inner nodes */
/* The base types must be a be able to accommodate the largest size
class for variable-sized node kinds*/
typedef struct RT_NODE_BASE_4
{
	RT_NODE		n;

	/* 4 children, for key chunks */
	uint8		chunks[4];
} RT_NODE_BASE_4;

typedef struct RT_NODE_BASE_32
{
	RT_NODE		n;

	/* 32 children, for key chunks */
	uint8		chunks[32];
} RT_NODE_BASE_32;

/*
 * node-125 uses slot_idx array, an array of RT_NODE_MAX_SLOTS length, typically
 * 256, to store indexes into a second array that contains up to 125 values (or
 * child pointers in inner nodes).
 */
typedef struct RT_NODE_BASE_125
{
	RT_NODE		n;

	/* The index of slots for each fanout */
	uint8		slot_idxs[RT_NODE_MAX_SLOTS];

	/* isset is a bitmap to track which slot is in use */
	bitmapword		isset[BM_IDX(128)];
} RT_NODE_BASE_125;

typedef struct RT_NODE_BASE_256
{
	RT_NODE		n;
} RT_NODE_BASE_256;

/*
 * Inner and leaf nodes.
 *
 * Theres are separate for two main reasons:
 *
 * 1) the value type might be different than something fitting into a pointer
 *    width type
 * 2) Need to represent non-existing values in a key-type independent way.
 *
 * 1) is clearly worth being concerned about, but it's not clear 2) is as
 * good. It might be better to just indicate non-existing entries the same way
 * in inner nodes.
 */
typedef struct RT_NODE_INNER_4
{
	RT_NODE_BASE_4 base;

	/* number of children depends on size class */
	RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_INNER_4;

typedef struct RT_NODE_LEAF_4
{
	RT_NODE_BASE_4 base;

	/* number of values depends on size class */
	uint64		values[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_LEAF_4;

typedef struct RT_NODE_INNER_32
{
	RT_NODE_BASE_32 base;

	/* number of children depends on size class */
	RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_INNER_32;

typedef struct RT_NODE_LEAF_32
{
	RT_NODE_BASE_32 base;

	/* number of values depends on size class */
	uint64		values[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_LEAF_32;

typedef struct RT_NODE_INNER_125
{
	RT_NODE_BASE_125 base;

	/* number of children depends on size class */
	RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_INNER_125;

typedef struct RT_NODE_LEAF_125
{
	RT_NODE_BASE_125 base;

	/* number of values depends on size class */
	uint64		values[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_LEAF_125;

/*
 * node-256 is the largest node type. This node has RT_NODE_MAX_SLOTS length array
 * for directly storing values (or child pointers in inner nodes).
 */
typedef struct RT_NODE_INNER_256
{
	RT_NODE_BASE_256 base;

	/* Slots for 256 children */
	RT_PTR_ALLOC children[RT_NODE_MAX_SLOTS];
} RT_NODE_INNER_256;

typedef struct RT_NODE_LEAF_256
{
	RT_NODE_BASE_256 base;

	/* isset is a bitmap to track which slot is in use */
	bitmapword	isset[BM_IDX(RT_NODE_MAX_SLOTS)];

	/* Slots for 256 values */
	uint64		values[RT_NODE_MAX_SLOTS];
} RT_NODE_LEAF_256;

/* Information for each size class */
typedef struct RT_SIZE_CLASS_ELEM
{
	const char *name;
	int			fanout;

	/* slab chunk size */
	Size		inner_size;
	Size		leaf_size;

	/* slab block size */
	Size		inner_blocksize;
	Size		leaf_blocksize;
} RT_SIZE_CLASS_ELEM;

/*
 * Calculate the slab blocksize so that we can allocate at least 32 chunks
 * from the block.
 */
#define NODE_SLAB_BLOCK_SIZE(size)	\
	Max((SLAB_DEFAULT_BLOCK_SIZE / (size)) * (size), (size) * 32)

static const RT_SIZE_CLASS_ELEM RT_SIZE_CLASS_INFO[] = {
	[RT_CLASS_4_FULL] = {
		.name = "radix tree node 4",
		.fanout = 4,
		.inner_size = sizeof(RT_NODE_INNER_4) + 4 * sizeof(RT_PTR_ALLOC),
		.leaf_size = sizeof(RT_NODE_LEAF_4) + 4 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_INNER_4) + 4 * sizeof(RT_PTR_ALLOC)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_LEAF_4) + 4 * sizeof(uint64)),
	},
	[RT_CLASS_32_PARTIAL] = {
		.name = "radix tree node 15",
		.fanout = 15,
		.inner_size = sizeof(RT_NODE_INNER_32) + 15 * sizeof(RT_PTR_ALLOC),
		.leaf_size = sizeof(RT_NODE_LEAF_32) + 15 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_INNER_32) + 15 * sizeof(RT_PTR_ALLOC)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_LEAF_32) + 15 * sizeof(uint64)),
	},
	[RT_CLASS_32_FULL] = {
		.name = "radix tree node 32",
		.fanout = 32,
		.inner_size = sizeof(RT_NODE_INNER_32) + 32 * sizeof(RT_PTR_ALLOC),
		.leaf_size = sizeof(RT_NODE_LEAF_32) + 32 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_INNER_32) + 32 * sizeof(RT_PTR_ALLOC)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_LEAF_32) + 32 * sizeof(uint64)),
	},
	[RT_CLASS_125_FULL] = {
		.name = "radix tree node 125",
		.fanout = 125,
		.inner_size = sizeof(RT_NODE_INNER_125) + 125 * sizeof(RT_PTR_ALLOC),
		.leaf_size = sizeof(RT_NODE_LEAF_125) + 125 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_INNER_125) + 125 * sizeof(RT_PTR_ALLOC)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_LEAF_125) + 125 * sizeof(uint64)),
	},
	[RT_CLASS_256] = {
		.name = "radix tree node 256",
		.fanout = 256,
		.inner_size = sizeof(RT_NODE_INNER_256),
		.leaf_size = sizeof(RT_NODE_LEAF_256),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_INNER_256)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(RT_NODE_LEAF_256)),
	},
};

#define RT_SIZE_CLASS_COUNT lengthof(RT_SIZE_CLASS_INFO)

/* Map from the node kind to its minimum size class */
static const RT_SIZE_CLASS RT_KIND_MIN_SIZE_CLASS[RT_NODE_KIND_COUNT] = {
	[RT_NODE_KIND_4] = RT_CLASS_4_FULL,
	[RT_NODE_KIND_32] = RT_CLASS_32_PARTIAL,
	[RT_NODE_KIND_125] = RT_CLASS_125_FULL,
	[RT_NODE_KIND_256] = RT_CLASS_256,
};

#ifdef RT_SHMEM
/* A magic value used to identify our radix tree */
#define RT_RADIX_TREE_MAGIC 0x54A48167
#endif

/* A radix tree with nodes */
typedef struct RT_RADIX_TREE_CONTROL
{
#ifdef RT_SHMEM
	RT_HANDLE	handle;
	uint32		magic;
#endif

	RT_PTR_ALLOC root;
	uint64		max_val;
	uint64		num_keys;

	/* statistics */
#ifdef RT_DEBUG
	int32		cnt[RT_SIZE_CLASS_COUNT];
#endif
} RT_RADIX_TREE_CONTROL;

/* A radix tree with nodes */
typedef struct RT_RADIX_TREE
{
	MemoryContext context;

	/* pointing to either local memory or DSA */
	RT_RADIX_TREE_CONTROL *ctl;

#ifdef RT_SHMEM
	dsa_area   *dsa;
#else
	MemoryContextData *inner_slabs[RT_SIZE_CLASS_COUNT];
	MemoryContextData *leaf_slabs[RT_SIZE_CLASS_COUNT];
#endif
} RT_RADIX_TREE;

/*
 * Iteration support.
 *
 * Iterating the radix tree returns each pair of key and value in the ascending
 * order of the key. To support this, the we iterate nodes of each level.
 *
 * RT_NODE_ITER struct is used to track the iteration within a node.
 *
 * RT_ITER is the struct for iteration of the radix tree, and uses RT_NODE_ITER
 * in order to track the iteration of each level. During the iteration, we also
 * construct the key whenever updating the node iteration information, e.g., when
 * advancing the current index within the node or when moving to the next node
 * at the same level.
+ *
+ * XXX: Currently we allow only one process to do iteration. Therefore, rt_node_iter
+ * has the local pointers to nodes, rather than RT_PTR_ALLOC.
+ * We need either a safeguard to disallow other processes to begin the iteration
+ * while one process is doing or to allow multiple processes to do the iteration.
 */
typedef struct RT_NODE_ITER
{
	RT_PTR_LOCAL node;			/* current node being iterated */
	int			current_idx;	/* current position. -1 for initial value */
} RT_NODE_ITER;

typedef struct RT_ITER
{
	RT_RADIX_TREE *tree;

	/* Track the iteration on nodes of each level */
	RT_NODE_ITER stack[RT_MAX_LEVEL];
	int			stack_len;

	/* The key is being constructed during the iteration */
	uint64		key;
} RT_ITER;


static bool RT_NODE_INSERT_INNER(RT_RADIX_TREE *tree, RT_PTR_LOCAL parent, RT_PTR_ALLOC nodep, RT_PTR_LOCAL node,
								 uint64 key, RT_PTR_ALLOC child);
static bool RT_NODE_INSERT_LEAF(RT_RADIX_TREE *tree, RT_PTR_LOCAL parent, RT_PTR_ALLOC nodep, RT_PTR_LOCAL node,
								uint64 key, uint64 value);

/* verification (available only with assertion) */
static void RT_VERIFY_NODE(RT_PTR_LOCAL node);

/* Get the local address of an allocated node */
static inline RT_PTR_LOCAL
RT_PTR_GET_LOCAL(RT_RADIX_TREE *tree, RT_PTR_ALLOC node)
{
#ifdef RT_SHMEM
	return dsa_get_address(tree->dsa, (dsa_pointer) node);
#else
	return node;
#endif
}

static inline bool
RT_PTR_ALLOC_IS_VALID(RT_PTR_ALLOC ptr)
{
#ifdef RT_SHMEM
	return DsaPointerIsValid(ptr);
#else
	return PointerIsValid(ptr);
#endif
}

/*
 * Return index of the first element in 'base' that equals 'key'. Return -1
 * if there is no such element.
 */
static inline int
RT_NODE_4_SEARCH_EQ(RT_NODE_BASE_4 *node, uint8 chunk)
{
	int			idx = -1;

	for (int i = 0; i < node->n.count; i++)
	{
		if (node->chunks[i] == chunk)
		{
			idx = i;
			break;
		}
	}

	return idx;
}

/*
 * Return index of the chunk to insert into chunks in the given node.
 */
static inline int
RT_NODE_4_GET_INSERTPOS(RT_NODE_BASE_4 *node, uint8 chunk)
{
	int			idx;

	for (idx = 0; idx < node->n.count; idx++)
	{
		if (node->chunks[idx] >= chunk)
			break;
	}

	return idx;
}

/*
 * Return index of the first element in 'base' that equals 'key'. Return -1
 * if there is no such element.
 */
static inline int
RT_NODE_32_SEARCH_EQ(RT_NODE_BASE_32 *node, uint8 chunk)
{
	int			count = node->n.count;
#ifndef USE_NO_SIMD
	Vector8		spread_chunk;
	Vector8		haystack1;
	Vector8		haystack2;
	Vector8		cmp1;
	Vector8		cmp2;
	uint32		bitfield;
	int			index_simd = -1;
#endif

#if defined(USE_NO_SIMD) || defined(USE_ASSERT_CHECKING)
	int			index = -1;

	for (int i = 0; i < count; i++)
	{
		if (node->chunks[i] == chunk)
		{
			index = i;
			break;
		}
	}
#endif

#ifndef USE_NO_SIMD
	spread_chunk = vector8_broadcast(chunk);
	vector8_load(&haystack1, &node->chunks[0]);
	vector8_load(&haystack2, &node->chunks[sizeof(Vector8)]);
	cmp1 = vector8_eq(spread_chunk, haystack1);
	cmp2 = vector8_eq(spread_chunk, haystack2);
	bitfield = vector8_highbit_mask(cmp1) | (vector8_highbit_mask(cmp2) << sizeof(Vector8));
	bitfield &= ((UINT64CONST(1) << count) - 1);

	if (bitfield)
		index_simd = pg_rightmost_one_pos32(bitfield);

	Assert(index_simd == index);
	return index_simd;
#else
	return index;
#endif
}

/*
 * Return index of the chunk to insert into chunks in the given node.
 */
static inline int
RT_NODE_32_GET_INSERTPOS(RT_NODE_BASE_32 *node, uint8 chunk)
{
	int			count = node->n.count;
#ifndef USE_NO_SIMD
	Vector8		spread_chunk;
	Vector8		haystack1;
	Vector8		haystack2;
	Vector8		cmp1;
	Vector8		cmp2;
	Vector8		min1;
	Vector8		min2;
	uint32		bitfield;
	int			index_simd;
#endif

#if defined(USE_NO_SIMD) || defined(USE_ASSERT_CHECKING)
	int			index;

	for (index = 0; index < count; index++)
	{
		if (node->chunks[index] >= chunk)
			break;
	}
#endif

#ifndef USE_NO_SIMD
	spread_chunk = vector8_broadcast(chunk);
	vector8_load(&haystack1, &node->chunks[0]);
	vector8_load(&haystack2, &node->chunks[sizeof(Vector8)]);
	min1 = vector8_min(spread_chunk, haystack1);
	min2 = vector8_min(spread_chunk, haystack2);
	cmp1 = vector8_eq(spread_chunk, min1);
	cmp2 = vector8_eq(spread_chunk, min2);
	bitfield = vector8_highbit_mask(cmp1) | (vector8_highbit_mask(cmp2) << sizeof(Vector8));
	bitfield &= ((UINT64CONST(1) << count) - 1);

	if (bitfield)
		index_simd = pg_rightmost_one_pos32(bitfield);
	else
		index_simd = count;

	Assert(index_simd == index);
	return index_simd;
#else
	return index;
#endif
}

/*
 * Functions to manipulate both chunks array and children/values array.
 * These are used for node-4 and node-32.
 */

/* Shift the elements right at 'idx' by one */
static inline void
RT_CHUNK_CHILDREN_ARRAY_SHIFT(uint8 *chunks, RT_PTR_ALLOC *children, int count, int idx)
{
	memmove(&(chunks[idx + 1]), &(chunks[idx]), sizeof(uint8) * (count - idx));
	memmove(&(children[idx + 1]), &(children[idx]), sizeof(RT_PTR_ALLOC) * (count - idx));
}

static inline void
RT_CHUNK_VALUES_ARRAY_SHIFT(uint8 *chunks, uint64 *values, int count, int idx)
{
	memmove(&(chunks[idx + 1]), &(chunks[idx]), sizeof(uint8) * (count - idx));
	memmove(&(values[idx + 1]), &(values[idx]), sizeof(uint64 *) * (count - idx));
}

/* Delete the element at 'idx' */
static inline void
RT_CHUNK_CHILDREN_ARRAY_DELETE(uint8 *chunks, RT_PTR_ALLOC *children, int count, int idx)
{
	memmove(&(chunks[idx]), &(chunks[idx + 1]), sizeof(uint8) * (count - idx - 1));
	memmove(&(children[idx]), &(children[idx + 1]), sizeof(RT_PTR_ALLOC) * (count - idx - 1));
}

static inline void
RT_CHUNK_VALUES_ARRAY_DELETE(uint8 *chunks, uint64 *values, int count, int idx)
{
	memmove(&(chunks[idx]), &(chunks[idx + 1]), sizeof(uint8) * (count - idx - 1));
	memmove(&(values[idx]), &(values[idx + 1]), sizeof(uint64) * (count - idx - 1));
}

/* Copy both chunks and children/values arrays */
static inline void
RT_CHUNK_CHILDREN_ARRAY_COPY(uint8 *src_chunks, RT_PTR_ALLOC *src_children,
						  uint8 *dst_chunks, RT_PTR_ALLOC *dst_children)
{
	const int fanout = RT_SIZE_CLASS_INFO[RT_CLASS_4_FULL].fanout;
	const Size chunk_size = sizeof(uint8) * fanout;
	const Size children_size = sizeof(RT_PTR_ALLOC) * fanout;

	memcpy(dst_chunks, src_chunks, chunk_size);
	memcpy(dst_children, src_children, children_size);
}

static inline void
RT_CHUNK_VALUES_ARRAY_COPY(uint8 *src_chunks, uint64 *src_values,
						uint8 *dst_chunks, uint64 *dst_values)
{
	const int fanout = RT_SIZE_CLASS_INFO[RT_CLASS_4_FULL].fanout;
	const Size chunk_size = sizeof(uint8) * fanout;
	const Size values_size = sizeof(uint64) * fanout;

	memcpy(dst_chunks, src_chunks, chunk_size);
	memcpy(dst_values, src_values, values_size);
}

/* Functions to manipulate inner and leaf node-125 */

/* Does the given chunk in the node has the value? */
static inline bool
RT_NODE_125_IS_CHUNK_USED(RT_NODE_BASE_125 *node, uint8 chunk)
{
	return node->slot_idxs[chunk] != RT_NODE_125_INVALID_IDX;
}

static inline RT_PTR_ALLOC
RT_NODE_INNER_125_GET_CHILD(RT_NODE_INNER_125 *node, uint8 chunk)
{
	Assert(!NODE_IS_LEAF(node));
	return node->children[node->base.slot_idxs[chunk]];
}

static inline uint64
RT_NODE_LEAF_125_GET_VALUE(RT_NODE_LEAF_125 *node, uint8 chunk)
{
	Assert(NODE_IS_LEAF(node));
	Assert(((RT_NODE_BASE_125 *) node)->slot_idxs[chunk] != RT_NODE_125_INVALID_IDX);
	return node->values[node->base.slot_idxs[chunk]];
}

/* Functions to manipulate inner and leaf node-256 */

/* Return true if the slot corresponding to the given chunk is in use */
static inline bool
RT_NODE_INNER_256_IS_CHUNK_USED(RT_NODE_INNER_256 *node, uint8 chunk)
{
	Assert(!NODE_IS_LEAF(node));
	return node->children[chunk] != RT_INVALID_PTR_ALLOC;
}

static inline bool
RT_NODE_LEAF_256_IS_CHUNK_USED(RT_NODE_LEAF_256 *node, uint8 chunk)
{
	int			idx = BM_IDX(chunk);
	int			bitnum = BM_BIT(chunk);

	Assert(NODE_IS_LEAF(node));
	return (node->isset[idx] & ((bitmapword) 1 << bitnum)) != 0;
}

static inline RT_PTR_ALLOC
RT_NODE_INNER_256_GET_CHILD(RT_NODE_INNER_256 *node, uint8 chunk)
{
	Assert(!NODE_IS_LEAF(node));
	Assert(RT_NODE_INNER_256_IS_CHUNK_USED(node, chunk));
	return node->children[chunk];
}

static inline uint64
RT_NODE_LEAF_256_GET_VALUE(RT_NODE_LEAF_256 *node, uint8 chunk)
{
	Assert(NODE_IS_LEAF(node));
	Assert(RT_NODE_LEAF_256_IS_CHUNK_USED(node, chunk));
	return node->values[chunk];
}

/* Set the child in the node-256 */
static inline void
RT_NODE_INNER_256_SET(RT_NODE_INNER_256 *node, uint8 chunk, RT_PTR_ALLOC child)
{
	Assert(!NODE_IS_LEAF(node));
	node->children[chunk] = child;
}

/* Set the value in the node-256 */
static inline void
RT_NODE_LEAF_256_SET(RT_NODE_LEAF_256 *node, uint8 chunk, uint64 value)
{
	int			idx = BM_IDX(chunk);
	int			bitnum = BM_BIT(chunk);

	Assert(NODE_IS_LEAF(node));
	node->isset[idx] |= ((bitmapword) 1 << bitnum);
	node->values[chunk] = value;
}

/* Set the slot at the given chunk position */
static inline void
RT_NODE_INNER_256_DELETE(RT_NODE_INNER_256 *node, uint8 chunk)
{
	Assert(!NODE_IS_LEAF(node));
	node->children[chunk] = RT_INVALID_PTR_ALLOC;
}

static inline void
RT_NODE_LEAF_256_DELETE(RT_NODE_LEAF_256 *node, uint8 chunk)
{
	int			idx = BM_IDX(chunk);
	int			bitnum = BM_BIT(chunk);

	Assert(NODE_IS_LEAF(node));
	node->isset[idx] &= ~((bitmapword) 1 << bitnum);
}

/*
 * Return the shift that is satisfied to store the given key.
 */
static inline int
RT_KEY_GET_SHIFT(uint64 key)
{
	return (key == 0)
		? 0
		: (pg_leftmost_one_pos64(key) / RT_NODE_SPAN) * RT_NODE_SPAN;
}

/*
 * Return the max value stored in a node with the given shift.
 */
static uint64
RT_SHIFT_GET_MAX_VAL(int shift)
{
	if (shift == RT_MAX_SHIFT)
		return UINT64_MAX;

	return (UINT64CONST(1) << (shift + RT_NODE_SPAN)) - 1;
}

/*
 * Allocate a new node with the given node kind.
 */
static RT_PTR_ALLOC
RT_ALLOC_NODE(RT_RADIX_TREE *tree, RT_SIZE_CLASS size_class, bool inner)
{
	RT_PTR_ALLOC allocnode;
	size_t allocsize;

	if (inner)
		allocsize = RT_SIZE_CLASS_INFO[size_class].inner_size;
	else
		allocsize = RT_SIZE_CLASS_INFO[size_class].leaf_size;

#ifdef RT_SHMEM
	allocnode = dsa_allocate(tree->dsa, allocsize);
#else
	if (inner)
		allocnode = (RT_PTR_ALLOC) MemoryContextAlloc(tree->inner_slabs[size_class],
													  allocsize);
	else
		allocnode = (RT_PTR_ALLOC) MemoryContextAlloc(tree->leaf_slabs[size_class],
													  allocsize);
#endif

#ifdef RT_DEBUG
	/* update the statistics */
	tree->ctl->cnt[size_class]++;
#endif

	return allocnode;
}

/* Initialize the node contents */
static inline void
RT_INIT_NODE(RT_PTR_LOCAL node, uint8 kind, RT_SIZE_CLASS size_class, bool inner)
{
	if (inner)
		MemSet(node, 0, RT_SIZE_CLASS_INFO[size_class].inner_size);
	else
		MemSet(node, 0, RT_SIZE_CLASS_INFO[size_class].leaf_size);

	node->kind = kind;

	if (kind == RT_NODE_KIND_256)
		/* See comment for the RT_NODE type */
		Assert(node->fanout == 0);
	else
		node->fanout = RT_SIZE_CLASS_INFO[size_class].fanout;

	/* Initialize slot_idxs to invalid values */
	if (kind == RT_NODE_KIND_125)
	{
		RT_NODE_BASE_125 *n125 = (RT_NODE_BASE_125 *) node;

		memset(n125->slot_idxs, RT_NODE_125_INVALID_IDX, sizeof(n125->slot_idxs));
	}
}

/*
 * Create a new node as the root. Subordinate nodes will be created during
 * the insertion.
 */
static void
RT_NEW_ROOT(RT_RADIX_TREE *tree, uint64 key)
{
	int			shift = RT_KEY_GET_SHIFT(key);
	bool		inner = shift > 0;
	RT_PTR_ALLOC allocnode;
	RT_PTR_LOCAL newnode;

	allocnode = RT_ALLOC_NODE(tree, RT_CLASS_4_FULL, inner);
	newnode = RT_PTR_GET_LOCAL(tree, allocnode);
	RT_INIT_NODE(newnode, RT_NODE_KIND_4, RT_CLASS_4_FULL, inner);
	newnode->shift = shift;
	tree->ctl->max_val = RT_SHIFT_GET_MAX_VAL(shift);
	tree->ctl->root = allocnode;
}

static inline void
RT_COPY_NODE(RT_PTR_LOCAL newnode, RT_PTR_LOCAL oldnode)
{
	newnode->shift = oldnode->shift;
	newnode->count = oldnode->count;
}
#if 0
/*
 * Create a new node with 'new_kind' and the same shift, chunk, and
 * count of 'node'.
 */
static RT_NODE*
RT_GROW_NODE_KIND(RT_RADIX_TREE *tree, RT_PTR_LOCAL node, uint8 new_kind)
{
	RT_PTR_ALLOC allocnode;
	RT_PTR_LOCAL newnode;
	bool inner = !NODE_IS_LEAF(node);

	allocnode = RT_ALLOC_NODE(tree, RT_KIND_MIN_SIZE_CLASS[new_kind], inner);
	newnode = RT_PTR_GET_LOCAL(tree, allocnode);
	RT_INIT_NODE(newnode, new_kind, RT_KIND_MIN_SIZE_CLASS[new_kind], inner);
	RT_COPY_NODE(newnode, node);

	return newnode;
}
#endif
/* Free the given node */
static void
RT_FREE_NODE(RT_RADIX_TREE *tree, RT_PTR_ALLOC allocnode)
{
	/* If we're deleting the root node, make the tree empty */
	if (tree->ctl->root == allocnode)
	{
		tree->ctl->root = RT_INVALID_PTR_ALLOC;
		tree->ctl->max_val = 0;
	}

#ifdef RT_DEBUG
	{
		int i;
		RT_PTR_LOCAL node = RT_PTR_GET_LOCAL(tree, allocnode);

		/* update the statistics */
		for (i = 0; i < RT_SIZE_CLASS_COUNT; i++)
		{
			if (node->fanout == RT_SIZE_CLASS_INFO[i].fanout)
				break;
		}

		/* fanout of node256 is intentionally 0 */
		if (i == RT_SIZE_CLASS_COUNT)
			i = RT_CLASS_256;

		tree->ctl->cnt[i]--;
		Assert(tree->ctl->cnt[i] >= 0);
	}
#endif

#ifdef RT_SHMEM
	dsa_free(tree->dsa, allocnode);
#else
	pfree(allocnode);
#endif
}

static inline void
RT_NODE_UPDATE_INNER(RT_PTR_LOCAL node, uint64 key, RT_PTR_ALLOC new_child)
{
#define RT_ACTION_UPDATE
#define RT_NODE_LEVEL_INNER
#include "lib/radixtree_search_impl.h"
#undef RT_NODE_LEVEL_INNER
#undef RT_ACTION_UPDATE
}

/*
 * Replace old_child with new_child, and free the old one.
 */
static void
RT_REPLACE_NODE(RT_RADIX_TREE *tree, RT_PTR_LOCAL parent, RT_PTR_ALLOC old_child,
				RT_PTR_ALLOC new_child, uint64 key)
{
	RT_PTR_LOCAL old = RT_PTR_GET_LOCAL(tree, old_child);

#ifdef USE_ASSERT_CHECKING
	RT_PTR_LOCAL new = RT_PTR_GET_LOCAL(tree, new_child);

	Assert(old->shift == new->shift);
#endif

	if (parent == old)
	{
		/* Replace the root node with the new large node */
		tree->ctl->root = new_child;
	}
	else
		RT_NODE_UPDATE_INNER(parent, key, new_child);

	RT_FREE_NODE(tree, old_child);
}

/*
 * The radix tree doesn't sufficient height. Extend the radix tree so it can
 * store the key.
 */
static void
RT_EXTEND(RT_RADIX_TREE *tree, uint64 key)
{
	int			target_shift;
	RT_PTR_LOCAL root = RT_PTR_GET_LOCAL(tree, tree->ctl->root);
	int			shift = root->shift + RT_NODE_SPAN;

	target_shift = RT_KEY_GET_SHIFT(key);

	/* Grow tree from 'shift' to 'target_shift' */
	while (shift <= target_shift)
	{
		RT_PTR_ALLOC	allocnode;
		RT_PTR_LOCAL	node;
		RT_NODE_INNER_4 *n4;

		allocnode = RT_ALLOC_NODE(tree, RT_CLASS_4_FULL, true);
		node = RT_PTR_GET_LOCAL(tree, allocnode);
		RT_INIT_NODE(node, RT_NODE_KIND_4, RT_CLASS_4_FULL, true);
		node->shift = shift;
		node->count = 1;

		n4 = (RT_NODE_INNER_4 *) node;
		n4->base.chunks[0] = 0;
		n4->children[0] = tree->ctl->root;

		/* Update the root */
		tree->ctl->root = allocnode;

		shift += RT_NODE_SPAN;
	}

	tree->ctl->max_val = RT_SHIFT_GET_MAX_VAL(target_shift);
}

/*
 * The radix tree doesn't have inner and leaf nodes for given key-value pair.
 * Insert inner and leaf nodes from 'node' to bottom.
 */
static inline void
RT_SET_EXTEND(RT_RADIX_TREE *tree, uint64 key, uint64 value, RT_PTR_LOCAL parent,
			  RT_PTR_ALLOC nodep, RT_PTR_LOCAL node)
{
	int			shift = node->shift;

	Assert(RT_PTR_GET_LOCAL(tree, nodep) == node);

	while (shift >= RT_NODE_SPAN)
	{
		RT_PTR_ALLOC allocchild;
		RT_PTR_LOCAL newchild;
		int			newshift = shift - RT_NODE_SPAN;
		bool		inner = newshift > 0;

		allocchild = RT_ALLOC_NODE(tree, RT_CLASS_4_FULL, inner);
		newchild = RT_PTR_GET_LOCAL(tree, allocchild);
		RT_INIT_NODE(newchild, RT_NODE_KIND_4, RT_CLASS_4_FULL, inner);
		newchild->shift = newshift;
		RT_NODE_INSERT_INNER(tree, parent, nodep, node, key, allocchild);

		parent = node;
		node = newchild;
		nodep = allocchild;
		shift -= RT_NODE_SPAN;
	}

	RT_NODE_INSERT_LEAF(tree, parent, nodep, node, key, value);
	tree->ctl->num_keys++;
}

/*
 * Search for the child pointer corresponding to 'key' in the given node.
 *
 * Return true if the key is found, otherwise return false. On success, the child
 * pointer is set to child_p.
 */
static inline bool
RT_NODE_SEARCH_INNER(RT_PTR_LOCAL node, uint64 key, RT_PTR_ALLOC *child_p)
{
#define RT_NODE_LEVEL_INNER
#include "lib/radixtree_search_impl.h"
#undef RT_NODE_LEVEL_INNER
}

/*
 * Search for the value corresponding to 'key' in the given node.
 *
 * Return true if the key is found, otherwise return false. On success, the pointer
 * to the value is set to value_p.
 */
static inline bool
RT_NODE_SEARCH_LEAF(RT_PTR_LOCAL node, uint64 key, uint64 *value_p)
{
#define RT_NODE_LEVEL_LEAF
#include "lib/radixtree_search_impl.h"
#undef RT_NODE_LEVEL_LEAF
}

/*
 * Search for the child pointer corresponding to 'key' in the given node.
 *
 * Delete the node and return true if the key is found, otherwise return false.
 */
static inline bool
RT_NODE_DELETE_INNER(RT_PTR_LOCAL node, uint64 key)
{
#define RT_NODE_LEVEL_INNER
#include "lib/radixtree_delete_impl.h"
#undef RT_NODE_LEVEL_INNER
}

/*
 * Search for the value corresponding to 'key' in the given node.
 *
 * Delete the node and return true if the key is found, otherwise return false.
 */
static inline bool
RT_NODE_DELETE_LEAF(RT_PTR_LOCAL node, uint64 key)
{
#define RT_NODE_LEVEL_LEAF
#include "lib/radixtree_delete_impl.h"
#undef RT_NODE_LEVEL_LEAF
}

/* Insert the child to the inner node */
static bool
RT_NODE_INSERT_INNER(RT_RADIX_TREE *tree, RT_PTR_LOCAL parent, RT_PTR_ALLOC nodep, RT_PTR_LOCAL node,
					uint64 key, RT_PTR_ALLOC child)
{
#define RT_NODE_LEVEL_INNER
#include "lib/radixtree_insert_impl.h"
#undef RT_NODE_LEVEL_INNER
}

/* Insert the value to the leaf node */
static bool
RT_NODE_INSERT_LEAF(RT_RADIX_TREE *tree, RT_PTR_LOCAL parent, RT_PTR_ALLOC nodep, RT_PTR_LOCAL node,
					uint64 key, uint64 value)
{
#define RT_NODE_LEVEL_LEAF
#include "lib/radixtree_insert_impl.h"
#undef RT_NODE_LEVEL_LEAF
}

/*
 * Create the radix tree in the given memory context and return it.
 */
RT_SCOPE RT_RADIX_TREE *
#ifdef RT_SHMEM
RT_CREATE(MemoryContext ctx, dsa_area *dsa)
#else
RT_CREATE(MemoryContext ctx)
#endif
{
	RT_RADIX_TREE *tree;
	MemoryContext old_ctx;
#ifdef RT_SHMEM
	dsa_pointer dp;
#endif

	old_ctx = MemoryContextSwitchTo(ctx);

	tree = (RT_RADIX_TREE *) palloc0(sizeof(RT_RADIX_TREE));
	tree->context = ctx;

#ifdef RT_SHMEM
	tree->dsa = dsa;
	dp = dsa_allocate0(dsa, sizeof(RT_RADIX_TREE_CONTROL));
	tree->ctl = (RT_RADIX_TREE_CONTROL *) dsa_get_address(dsa, dp);
	tree->ctl->handle = dp;
	tree->ctl->magic = RT_RADIX_TREE_MAGIC;
#else
	tree->ctl = (RT_RADIX_TREE_CONTROL *) palloc0(sizeof(RT_RADIX_TREE_CONTROL));

	/* Create the slab allocator for each size class */
	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
	{
		tree->inner_slabs[i] = SlabContextCreate(ctx,
												 RT_SIZE_CLASS_INFO[i].name,
												 RT_SIZE_CLASS_INFO[i].inner_blocksize,
												 RT_SIZE_CLASS_INFO[i].inner_size);
		tree->leaf_slabs[i] = SlabContextCreate(ctx,
												RT_SIZE_CLASS_INFO[i].name,
												RT_SIZE_CLASS_INFO[i].leaf_blocksize,
												RT_SIZE_CLASS_INFO[i].leaf_size);
	}
#endif

	tree->ctl->root = RT_INVALID_PTR_ALLOC;

	MemoryContextSwitchTo(old_ctx);

	return tree;
}

#ifdef RT_SHMEM
RT_SCOPE RT_RADIX_TREE *
RT_ATTACH(dsa_area *dsa, RT_HANDLE handle)
{
	RT_RADIX_TREE *tree;
	dsa_pointer	control;

	/* XXX: memory context support */
	tree = (RT_RADIX_TREE *) palloc0(sizeof(RT_RADIX_TREE));

	/* Find the control object in shard memory */
	control = handle;

	tree->dsa = dsa;
	tree->ctl = (RT_RADIX_TREE_CONTROL *) dsa_get_address(dsa, control);
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);

	/* XXX: do we need to set a callback on exit to detach dsa? */

	return tree;
}

RT_SCOPE void
RT_DETACH(RT_RADIX_TREE *tree)
{
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
	pfree(tree);
}

RT_SCOPE RT_HANDLE
RT_GET_HANDLE(RT_RADIX_TREE *tree)
{
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
	return tree->ctl->handle;
}
#endif

/*
 * Free the given radix tree.
 */
RT_SCOPE void
RT_FREE(RT_RADIX_TREE *tree)
{
#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);

	/*
	 * Vandalize the control block to help catch programming error where
	 * other backends access the memory formerly occupied by this radix tree.
	 */
	tree->ctl->magic = 0;
	dsa_free(tree->dsa, tree->ctl->handle); // XXX
	//dsa_detach(tree->dsa);
#else
	pfree(tree->ctl);

	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
	{
		MemoryContextDelete(tree->inner_slabs[i]);
		MemoryContextDelete(tree->leaf_slabs[i]);
	}
#endif

	pfree(tree);
}

/*
 * Set key to value. If the entry already exists, we update its value to 'value'
 * and return true. Returns false if entry doesn't yet exist.
 */
RT_SCOPE bool
RT_SET(RT_RADIX_TREE *tree, uint64 key, uint64 value)
{
	int			shift;
	bool		updated;
	RT_PTR_LOCAL parent;
	RT_PTR_ALLOC nodep;
	RT_PTR_LOCAL  node;

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	/* Empty tree, create the root */
	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root))
		RT_NEW_ROOT(tree, key);

	/* Extend the tree if necessary */
	if (key > tree->ctl->max_val)
		RT_EXTEND(tree, key);

	//Assert(tree->ctl->root);

	nodep = tree->ctl->root;
	parent = RT_PTR_GET_LOCAL(tree, nodep);
	shift = parent->shift;

	/* Descend the tree until a leaf node */
	while (shift >= 0)
	{
		RT_PTR_ALLOC child;

		node = RT_PTR_GET_LOCAL(tree, nodep);

		if (NODE_IS_LEAF(node))
			break;

		if (!RT_NODE_SEARCH_INNER(node, key, &child))
		{
			RT_SET_EXTEND(tree, key, value, parent, nodep, node);
			return false;
		}

		parent = node;
		nodep = child;
		shift -= RT_NODE_SPAN;
	}

	updated = RT_NODE_INSERT_LEAF(tree, parent, nodep, node, key, value);

	/* Update the statistics */
	if (!updated)
		tree->ctl->num_keys++;

	return updated;
}

/*
 * Search the given key in the radix tree. Return true if there is the key,
 * otherwise return false.  On success, we set the value to *val_p so it must
 * not be NULL.
 */
RT_SCOPE bool
RT_SEARCH(RT_RADIX_TREE *tree, uint64 key, uint64 *value_p)
{
	RT_PTR_LOCAL node;
	int			shift;

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif
	Assert(value_p != NULL);

	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root) || key > tree->ctl->max_val)
		return false;

	node = RT_PTR_GET_LOCAL(tree, tree->ctl->root);
	shift = node->shift;

	/* Descend the tree until a leaf node */
	while (shift >= 0)
	{
		RT_PTR_ALLOC child;

		if (NODE_IS_LEAF(node))
			break;

		if (!RT_NODE_SEARCH_INNER(node, key, &child))
			return false;

		node = RT_PTR_GET_LOCAL(tree, child);
		shift -= RT_NODE_SPAN;
	}

	return RT_NODE_SEARCH_LEAF(node, key, value_p);
}

/*
 * Delete the given key from the radix tree. Return true if the key is found (and
 * deleted), otherwise do nothing and return false.
 */
RT_SCOPE bool
RT_DELETE(RT_RADIX_TREE *tree, uint64 key)
{
	RT_PTR_LOCAL node;
	RT_PTR_ALLOC allocnode;
	RT_PTR_ALLOC stack[RT_MAX_LEVEL] = {0};
	int			shift;
	int			level;
	bool		deleted;

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root) || key > tree->ctl->max_val)
		return false;

	/*
	 * Descend the tree to search the key while building a stack of nodes we
	 * visited.
	 */
	allocnode = tree->ctl->root;
	node = RT_PTR_GET_LOCAL(tree, allocnode);
	shift = node->shift;
	level = -1;
	while (shift > 0)
	{
		RT_PTR_ALLOC child;

		/* Push the current node to the stack */
		stack[++level] = allocnode;
		node = RT_PTR_GET_LOCAL(tree, allocnode);

		if (!RT_NODE_SEARCH_INNER(node, key, &child))
			return false;

		allocnode = child;
		shift -= RT_NODE_SPAN;
	}

	/* Delete the key from the leaf node if exists */
	node = RT_PTR_GET_LOCAL(tree, allocnode);
	deleted = RT_NODE_DELETE_LEAF(node, key);

	if (!deleted)
	{
		/* no key is found in the leaf node */
		return false;
	}

	/* Found the key to delete. Update the statistics */
	tree->ctl->num_keys--;

	/*
	 * Return if the leaf node still has keys and we don't need to delete the
	 * node.
	 */
	if (!NODE_IS_EMPTY(node))
		return true;

	/* Free the empty leaf node */
	RT_FREE_NODE(tree, allocnode);

	/* Delete the key in inner nodes recursively */
	while (level >= 0)
	{
		allocnode = stack[level--];

		node = RT_PTR_GET_LOCAL(tree, allocnode);
		deleted = RT_NODE_DELETE_INNER(node, key);
		Assert(deleted);

		/* If the node didn't become empty, we stop deleting the key */
		if (!NODE_IS_EMPTY(node))
			break;

		/* The node became empty */
		RT_FREE_NODE(tree, allocnode);
	}

	return true;
}

static inline void
RT_ITER_UPDATE_KEY(RT_ITER *iter, uint8 chunk, uint8 shift)
{
	iter->key &= ~(((uint64) RT_CHUNK_MASK) << shift);
	iter->key |= (((uint64) chunk) << shift);
}

/*
 * Advance the slot in the inner node. Return the child if exists, otherwise
 * null.
 */
static inline RT_PTR_LOCAL
RT_NODE_INNER_ITERATE_NEXT(RT_ITER *iter, RT_NODE_ITER *node_iter)
{
#define RT_NODE_LEVEL_INNER
#include "lib/radixtree_iter_impl.h"
#undef RT_NODE_LEVEL_INNER
}

/*
 * Advance the slot in the leaf node. On success, return true and the value
 * is set to value_p, otherwise return false.
 */
static inline bool
RT_NODE_LEAF_ITERATE_NEXT(RT_ITER *iter, RT_NODE_ITER *node_iter,
						  uint64 *value_p)
{
#define RT_NODE_LEVEL_LEAF
#include "lib/radixtree_iter_impl.h"
#undef RT_NODE_LEVEL_LEAF
}

/*
 * Update each node_iter for inner nodes in the iterator node stack.
 */
static void
RT_UPDATE_ITER_STACK(RT_ITER *iter, RT_PTR_LOCAL from_node, int from)
{
	int			level = from;
	RT_PTR_LOCAL node = from_node;

	for (;;)
	{
		RT_NODE_ITER *node_iter = &(iter->stack[level--]);

		node_iter->node = node;
		node_iter->current_idx = -1;

		/* We don't advance the leaf node iterator here */
		if (NODE_IS_LEAF(node))
			return;

		/* Advance to the next slot in the inner node */
		node = RT_NODE_INNER_ITERATE_NEXT(iter, node_iter);

		/* We must find the first children in the node */
		Assert(node);
	}
}

/* Create and return the iterator for the given radix tree */
RT_SCOPE RT_ITER *
RT_BEGIN_ITERATE(RT_RADIX_TREE *tree)
{
	MemoryContext old_ctx;
	RT_ITER    *iter;
	RT_PTR_LOCAL root;
	int			top_level;

	old_ctx = MemoryContextSwitchTo(tree->context);

	iter = (RT_ITER *) palloc0(sizeof(RT_ITER));
	iter->tree = tree;

	/* empty tree */
	if (!iter->tree->ctl->root)
		return iter;

	root = RT_PTR_GET_LOCAL(tree, iter->tree->ctl->root);
	top_level = root->shift / RT_NODE_SPAN;
	iter->stack_len = top_level;

	/*
	 * Descend to the left most leaf node from the root. The key is being
	 * constructed while descending to the leaf.
	 */
	RT_UPDATE_ITER_STACK(iter, root, top_level);

	MemoryContextSwitchTo(old_ctx);

	return iter;
}

/*
 * Return true with setting key_p and value_p if there is next key.  Otherwise,
 * return false.
 */
RT_SCOPE bool
RT_ITERATE_NEXT(RT_ITER *iter, uint64 *key_p, uint64 *value_p)
{
	/* Empty tree */
	if (!iter->tree->ctl->root)
		return false;

	for (;;)
	{
		RT_PTR_LOCAL child = NULL;
		uint64		value;
		int			level;
		bool		found;

		/* Advance the leaf node iterator to get next key-value pair */
		found = RT_NODE_LEAF_ITERATE_NEXT(iter, &(iter->stack[0]), &value);

		if (found)
		{
			*key_p = iter->key;
			*value_p = value;
			return true;
		}

		/*
		 * We've visited all values in the leaf node, so advance inner node
		 * iterators from the level=1 until we find the next child node.
		 */
		for (level = 1; level <= iter->stack_len; level++)
		{
			child = RT_NODE_INNER_ITERATE_NEXT(iter, &(iter->stack[level]));

			if (child)
				break;
		}

		/* the iteration finished */
		if (!child)
			return false;

		/*
		 * Set the node to the node iterator and update the iterator stack
		 * from this node.
		 */
		RT_UPDATE_ITER_STACK(iter, child, level - 1);

		/* Node iterators are updated, so try again from the leaf */
	}

	return false;
}

RT_SCOPE void
RT_END_ITERATE(RT_ITER *iter)
{
	pfree(iter);
}

/*
 * Return the number of keys in the radix tree.
 */
RT_SCOPE uint64
RT_NUM_ENTRIES(RT_RADIX_TREE *tree)
{
	return tree->ctl->num_keys;
}

/*
 * Return the statistics of the amount of memory used by the radix tree.
 */
RT_SCOPE uint64
RT_MEMORY_USAGE(RT_RADIX_TREE *tree)
{
	// XXX is this necessary?
	Size		total = sizeof(RT_RADIX_TREE);

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
	total = dsa_get_total_size(tree->dsa);
#else
	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
	{
		total += MemoryContextMemAllocated(tree->inner_slabs[i], true);
		total += MemoryContextMemAllocated(tree->leaf_slabs[i], true);
	}
#endif

	return total;
}

/*
 * Verify the radix tree node.
 */
static void
RT_VERIFY_NODE(RT_PTR_LOCAL node)
{
#ifdef USE_ASSERT_CHECKING
	Assert(node->count >= 0);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				RT_NODE_BASE_4 *n4 = (RT_NODE_BASE_4 *) node;

				for (int i = 1; i < n4->n.count; i++)
					Assert(n4->chunks[i - 1] < n4->chunks[i]);

				break;
			}
		case RT_NODE_KIND_32:
			{
				RT_NODE_BASE_32 *n32 = (RT_NODE_BASE_32 *) node;

				for (int i = 1; i < n32->n.count; i++)
					Assert(n32->chunks[i - 1] < n32->chunks[i]);

				break;
			}
		case RT_NODE_KIND_125:
			{
				RT_NODE_BASE_125 *n125 = (RT_NODE_BASE_125 *) node;
				int			cnt = 0;

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					uint8		slot = n125->slot_idxs[i];
					int			idx = BM_IDX(slot);
					int			bitnum = BM_BIT(slot);

					if (!RT_NODE_125_IS_CHUNK_USED(n125, i))
						continue;

					/* Check if the corresponding slot is used */
					Assert(slot < node->fanout);
					Assert((n125->isset[idx] & ((bitmapword) 1 << bitnum)) != 0);

					cnt++;
				}

				Assert(n125->n.count == cnt);
				break;
			}
		case RT_NODE_KIND_256:
			{
				if (NODE_IS_LEAF(node))
				{
					RT_NODE_LEAF_256 *n256 = (RT_NODE_LEAF_256 *) node;
					int			cnt = 0;

					for (int i = 0; i < BM_IDX(RT_NODE_MAX_SLOTS); i++)
						cnt += bmw_popcount(n256->isset[i]);

					/* Check if the number of used chunk matches */
					Assert(n256->base.n.count == cnt);

					break;
				}
			}
	}
#endif
}

/***************** DEBUG FUNCTIONS *****************/
#ifdef RT_DEBUG
void
rt_stats(RT_RADIX_TREE *tree)
{
	ereport(NOTICE, (errmsg("num_keys = " UINT64_FORMAT ", height = %d, n4 = %u, n15 = %u, n32 = %u, n125 = %u, n256 = %u",
						 tree->ctl->num_keys,
						 tree->ctl->root->shift / RT_NODE_SPAN,
						 tree->ctl->cnt[RT_CLASS_4_FULL],
						 tree->ctl->cnt[RT_CLASS_32_PARTIAL],
						 tree->ctl->cnt[RT_CLASS_32_FULL],
						 tree->ctl->cnt[RT_CLASS_125_FULL],
						 tree->ctl->cnt[RT_CLASS_256])));
}

static void
rt_dump_node(RT_PTR_LOCAL node, int level, bool recurse)
{
	char		space[125] = {0};

	fprintf(stderr, "[%s] kind %d, fanout %d, count %u, shift %u:\n",
			NODE_IS_LEAF(node) ? "LEAF" : "INNR",
			(node->kind == RT_NODE_KIND_4) ? 4 :
			(node->kind == RT_NODE_KIND_32) ? 32 :
			(node->kind == RT_NODE_KIND_125) ? 125 : 256,
			node->fanout == 0 ? 256 : node->fanout,
			node->count, node->shift);

	if (level > 0)
		sprintf(space, "%*c", level * 4, ' ');

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				for (int i = 0; i < node->count; i++)
				{
					if (NODE_IS_LEAF(node))
					{
						RT_NODE_LEAF_4 *n4 = (RT_NODE_LEAF_4 *) node;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, n4->base.chunks[i], n4->values[i]);
					}
					else
					{
						RT_NODE_INNER_4 *n4 = (RT_NODE_INNER_4 *) node;

						fprintf(stderr, "%schunk 0x%X ->",
								space, n4->base.chunks[i]);

						if (recurse)
							rt_dump_node(n4->children[i], level + 1, recurse);
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
		case RT_NODE_KIND_32:
			{
				for (int i = 0; i < node->count; i++)
				{
					if (NODE_IS_LEAF(node))
					{
						RT_NODE_LEAF_32 *n32 = (RT_NODE_LEAF_32 *) node;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, n32->base.chunks[i], n32->values[i]);
					}
					else
					{
						RT_NODE_INNER_32 *n32 = (RT_NODE_INNER_32 *) node;

						fprintf(stderr, "%schunk 0x%X ->",
								space, n32->base.chunks[i]);

						if (recurse)
						{
							rt_dump_node(n32->children[i], level + 1, recurse);
						}
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
		case RT_NODE_KIND_125:
			{
				RT_NODE_BASE_125 *b125 = (RT_NODE_BASE_125 *) node;

				fprintf(stderr, "slot_idxs ");
				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!RT_NODE_125_IS_CHUNK_USED(b125, i))
						continue;

					fprintf(stderr, " [%d]=%d, ", i, b125->slot_idxs[i]);
				}
				if (NODE_IS_LEAF(node))
				{
					RT_NODE_LEAF_125 *n = (RT_NODE_LEAF_125 *) node;

					fprintf(stderr, ", isset-bitmap:");
					for (int i = 0; i < BM_IDX(128); i++)
					{
						fprintf(stderr, UINT64_FORMAT_HEX " ", (uint64) n->base.isset[i]);
					}
					fprintf(stderr, "\n");
				}

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!RT_NODE_125_IS_CHUNK_USED(b125, i))
						continue;

					if (NODE_IS_LEAF(node))
					{
						RT_NODE_LEAF_125 *n125 = (RT_NODE_LEAF_125 *) b125;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, i, RT_NODE_LEAF_125_GET_VALUE(n125, i));
					}
					else
					{
						RT_NODE_INNER_125 *n125 = (RT_NODE_INNER_125 *) b125;

						fprintf(stderr, "%schunk 0x%X ->",
								space, i);

						if (recurse)
							rt_dump_node(RT_NODE_INNER_125_GET_CHILD(n125, i),
										 level + 1, recurse);
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
		case RT_NODE_KIND_256:
			{
				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (NODE_IS_LEAF(node))
					{
						RT_NODE_LEAF_256 *n256 = (RT_NODE_LEAF_256 *) node;

						if (!RT_NODE_LEAF_256_IS_CHUNK_USED(n256, i))
							continue;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, i, RT_NODE_LEAF_256_GET_VALUE(n256, i));
					}
					else
					{
						RT_NODE_INNER_256 *n256 = (RT_NODE_INNER_256 *) node;

						if (!RT_NODE_INNER_256_IS_CHUNK_USED(n256, i))
							continue;

						fprintf(stderr, "%schunk 0x%X ->",
								space, i);

						if (recurse)
							rt_dump_node(RT_NODE_INNER_256_GET_CHILD(n256, i), level + 1,
										 recurse);
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
	}
}

void
rt_dump_search(RT_RADIX_TREE *tree, uint64 key)
{
	RT_PTR_LOCAL node;
	int			shift;
	int			level = 0;

	elog(NOTICE, "-----------------------------------------------------------");
	elog(NOTICE, "max_val = " UINT64_FORMAT "(0x" UINT64_FORMAT_HEX ")",
		 tree->ctl->max_val, tree->ctl->max_val);

	if (!tree->ctl->root)
	{
		elog(NOTICE, "tree is empty");
		return;
	}

	if (key > tree->ctl->max_val)
	{
		elog(NOTICE, "key " UINT64_FORMAT "(0x" UINT64_FORMAT_HEX ") is larger than max val",
			 key, key);
		return;
	}

	node = tree->ctl->root;
	shift = tree->ctl->root->shift;
	while (shift >= 0)
	{
		RT_PTR_LOCAL child;

		rt_dump_node(node, level, false);

		if (NODE_IS_LEAF(node))
		{
			uint64		dummy;

			/* We reached at a leaf node, find the corresponding slot */
			RT_NODE_SEARCH_LEAF(node, key, &dummy);

			break;
		}

		if (!RT_NODE_SEARCH_INNER(node, key, &child))
			break;

		node = child;
		shift -= RT_NODE_SPAN;
		level++;
	}
}

void
rt_dump(RT_RADIX_TREE *tree)
{

	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
		fprintf(stderr, "%s\tinner_size %zu\tinner_blocksize %zu\tleaf_size %zu\tleaf_blocksize %zu\n",
				RT_SIZE_CLASS_INFO[i].name,
				RT_SIZE_CLASS_INFO[i].inner_size,
				RT_SIZE_CLASS_INFO[i].inner_blocksize,
				RT_SIZE_CLASS_INFO[i].leaf_size,
				RT_SIZE_CLASS_INFO[i].leaf_blocksize);
	fprintf(stderr, "max_val = " UINT64_FORMAT "\n", tree->ctl->max_val);

	if (!tree->ctl->root)
	{
		fprintf(stderr, "empty tree\n");
		return;
	}

	rt_dump_node(tree->ctl->root, 0, true);
}
#endif

#endif							/* RT_DEFINE */


/* undefine external parameters, so next radix tree can be defined */
#undef RT_PREFIX
#undef RT_SCOPE
#undef RT_DECLARE
#undef RT_DEFINE

/* locally declared macros */
#undef NODE_IS_LEAF
#undef NODE_IS_EMPTY
#undef VAR_NODE_HAS_FREE_SLOT
#undef FIXED_NODE_HAS_FREE_SLOT
#undef RT_SIZE_CLASS_COUNT
#undef RT_RADIX_TREE_MAGIC

/* type declarations */
#undef RT_RADIX_TREE
#undef RT_RADIX_TREE_CONTROL
#undef RT_PTR_ALLOC
#undef RT_INVALID_PTR_ALLOC
#undef RT_HANDLE
#undef RT_ITER
#undef RT_NODE
#undef RT_NODE_ITER
#undef RT_NODE_BASE_4
#undef RT_NODE_BASE_32
#undef RT_NODE_BASE_125
#undef RT_NODE_BASE_256
#undef RT_NODE_INNER_4
#undef RT_NODE_INNER_32
#undef RT_NODE_INNER_125
#undef RT_NODE_INNER_256
#undef RT_NODE_LEAF_4
#undef RT_NODE_LEAF_32
#undef RT_NODE_LEAF_125
#undef RT_NODE_LEAF_256
#undef RT_SIZE_CLASS
#undef RT_SIZE_CLASS_ELEM
#undef RT_SIZE_CLASS_INFO
#undef RT_CLASS_4_FULL
#undef RT_CLASS_32_PARTIAL
#undef RT_CLASS_32_FULL
#undef RT_CLASS_125_FULL
#undef RT_CLASS_256
#undef RT_KIND_MIN_SIZE_CLASS

/* function declarations */
#undef RT_CREATE
#undef RT_FREE
#undef RT_ATTACH
#undef RT_DETACH
#undef RT_GET_HANDLE
#undef RT_SET
#undef RT_BEGIN_ITERATE
#undef RT_ITERATE_NEXT
#undef RT_END_ITERATE
#undef RT_DELETE
#undef RT_MEMORY_USAGE
#undef RT_NUM_ENTRIES
#undef RT_DUMP
#undef RT_DUMP_SEARCH
#undef RT_STATS

/* internal helper functions */
#undef RT_NEW_ROOT
#undef RT_ALLOC_NODE
#undef RT_INIT_NODE
#undef RT_FREE_NODE
#undef RT_EXTEND
#undef RT_SET_EXTEND
#undef RT_GROW_NODE_KIND
#undef RT_COPY_NODE
#undef RT_REPLACE_NODE
#undef RT_PTR_GET_LOCAL
#undef RT_PTR_ALLOC_IS_VALID
#undef RT_NODE_4_SEARCH_EQ
#undef RT_NODE_32_SEARCH_EQ
#undef RT_NODE_4_GET_INSERTPOS
#undef RT_NODE_32_GET_INSERTPOS
#undef RT_CHUNK_CHILDREN_ARRAY_SHIFT
#undef RT_CHUNK_VALUES_ARRAY_SHIFT
#undef RT_CHUNK_CHILDREN_ARRAY_DELETE
#undef RT_CHUNK_VALUES_ARRAY_DELETE
#undef RT_CHUNK_CHILDREN_ARRAY_COPY
#undef RT_CHUNK_VALUES_ARRAY_COPY
#undef RT_NODE_125_IS_CHUNK_USED
#undef RT_NODE_INNER_125_GET_CHILD
#undef RT_NODE_LEAF_125_GET_VALUE
#undef RT_NODE_INNER_256_IS_CHUNK_USED
#undef RT_NODE_LEAF_256_IS_CHUNK_USED
#undef RT_NODE_INNER_256_GET_CHILD
#undef RT_NODE_LEAF_256_GET_VALUE
#undef RT_NODE_INNER_256_SET
#undef RT_NODE_LEAF_256_SET
#undef RT_NODE_INNER_256_DELETE
#undef RT_NODE_LEAF_256_DELETE
#undef RT_KEY_GET_SHIFT
#undef RT_SHIFT_GET_MAX_VAL
#undef RT_NODE_SEARCH_INNER
#undef RT_NODE_SEARCH_LEAF
#undef RT_NODE_UPDATE_INNER
#undef RT_NODE_DELETE_INNER
#undef RT_NODE_DELETE_LEAF
#undef RT_NODE_INSERT_INNER
#undef RT_NODE_INSERT_LEAF
#undef RT_NODE_INNER_ITERATE_NEXT
#undef RT_NODE_LEAF_ITERATE_NEXT
#undef RT_UPDATE_ITER_STACK
#undef RT_ITER_UPDATE_KEY
#undef RT_VERIFY_NODE

#undef RT_DEBUG
