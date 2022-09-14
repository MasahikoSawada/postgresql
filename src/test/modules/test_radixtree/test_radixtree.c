/*--------------------------------------------------------------------------
 *
 * test_radixtree.c
 *		Test radixtree set data structure.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_radixtree/test_radixtree.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* uncomment to use shared memory for the tree */
/* #define TEST_SHARED_RT */

#define UINT64_HEX_FORMAT "%" INT64_MODIFIER "X"

/* Convenient macros to test results */
#define EXPECT_TRUE(expr)	\
	do { \
		if (!(expr)) \
			elog(ERROR, \
				 "%s was unexpectedly false in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_FALSE(expr)	\
	do { \
		if (expr) \
			elog(ERROR, \
				 "%s was unexpectedly true in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_EQ_U64(result_expr, expected_expr)	\
	do { \
		uint64		_result = (result_expr); \
		uint64		_expected = (expected_expr); \
		if (_result != _expected) \
			elog(ERROR, \
				 "%s yielded " UINT64_HEX_FORMAT ", expected " UINT64_HEX_FORMAT " (%s) in file \"%s\" line %u", \
				 #result_expr, _result, _expected, #expected_expr, __FILE__, __LINE__); \
	} while (0)

/*
 * With uint64, 64-bit platforms store the value in the last-level child
 * pointer, and 32-bit platforms store this in a single-value leaf.
 * This gives us buildfarm coverage for both paths in this module.
 */
typedef uint64 TestValueType;

/*
 * The node class name and the number of keys big enough to grow nodes
 * into each size class.
 */
typedef struct rt_node_class_test_elem
{
	char	*class_name;
	int		nkeys;
} rt_node_class_test_elem;
static rt_node_class_test_elem rt_node_class_tests[] = {
	{
		.class_name = "node-4",	/* RT_CLASS_4 */
		.nkeys = 2,
	},
	{
		.class_name = "node-16-lo", /* RT_CLASS_16_LO */
		.nkeys = 15,
	},
	{
		.class_name = "node-16-hi", /* RT_CLASS_16_HI */
		.nkeys = 30,
	},
	{
		.class_name = "node-48", /* RT_CLASS_48 */
		.nkeys = 60,
	},
	{
		.class_name = "node-256", /* RT_CLASS_256 */
		.nkeys = 256,
	},
};

/*
 * A struct to define a pattern of integers, for use with the test_pattern()
 * function.
 */
typedef struct
{
	char	   *test_name;		/* short name of the test, for humans */
	char	   *pattern_str;	/* a bit pattern */
	uint64		spacing;		/* pattern repeats at this interval */
	uint64		num_values;		/* number of integers to set in total */
}			test_spec;

/* Test patterns borrowed from test_integerset.c */
static const test_spec test_specs[] = {
	{
		"all ones", "1111111111",
		10, 1000000
	},
	{
		"alternating bits", "0101010101",
		10, 1000000
	},
	{
		"clusters of ten", "1111111111",
		10000, 1000000
	},
	{
		"clusters of hundred",
		"1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
		10000, 1000000
	},
	{
		"one-every-64k", "1",
		65536, 1000000
	},
	{
		"sparse", "100000000000000000000000000000001",
		10000000, 1000000
	},
	{
		"single values, distance > 2^32", "1",
		UINT64CONST(10000000000), 100000
	},
	{
		"clusters, distance > 2^32", "10101010",
		UINT64CONST(10000000000), 1000000
	},
	{
		"clusters, distance > 2^60", "10101010",
		UINT64CONST(2000000000000000000),
		23						/* can't be much higher than this, or we
								 * overflow uint64 */
	}
};

/* define the radix tree implementation to test */
#define RT_PREFIX rt
#define RT_SCOPE
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_VALUE_TYPE TestValueType
#ifdef TEST_SHARED_RT
#define RT_SHMEM
#endif
#define RT_DEBUG
#include "lib/radixtree.h"


/*
 * Return the number of keys in the radix tree.
 */
static uint64
rt_num_entries(rt_radix_tree *tree)
{
	return tree->ctl->num_keys;
}

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_radixtree);

static void
test_empty(void)
{
	rt_radix_tree *radixtree;
	rt_iter		*iter;
	uint64		key;

#ifdef TEST_SHARED_RT
	int			tranche_id = LWLockNewTrancheId();
	dsa_area   *dsa;

	LWLockRegisterTranche(tranche_id, "test_radix_tree");
	dsa = dsa_create(tranche_id);

	radixtree = rt_create(CurrentMemoryContext, work_mem, dsa, tranche_id);
#else
	radixtree = rt_create(CurrentMemoryContext, work_mem);
#endif

	/* Should not find anything in an empty tree */
	EXPECT_TRUE(rt_find(radixtree, 0) == NULL);
	EXPECT_TRUE(rt_find(radixtree, 1) == NULL);
	EXPECT_TRUE(rt_find(radixtree, PG_UINT64_MAX) == NULL);
	EXPECT_FALSE(rt_delete(radixtree, 0));
	EXPECT_TRUE(rt_num_entries(radixtree) == 0);

	/* Iterating on an empty tree should not return anything */
	iter = rt_begin_iterate(radixtree);
	EXPECT_TRUE(rt_iterate_next(iter, &key) == NULL);
	rt_end_iterate(iter);

	rt_free(radixtree);

#ifdef TEST_SHARED_RT
	dsa_detach(dsa);
#endif
}

/* Basic set, find, and delete tests */
static void
do_test_basic(int children, int shift, bool reverse)
{
	rt_radix_tree	*radixtree;
	rt_iter    *iter;
	uint64 *keys;

#ifdef TEST_SHARED_RT
	int			tranche_id = LWLockNewTrancheId();
	dsa_area   *dsa;

	LWLockRegisterTranche(tranche_id, "test_radix_tree");
	dsa = dsa_create(tranche_id);
#endif

#ifdef TEST_SHARED_RT
	radixtree = rt_create(CurrentMemoryContext, work_mem, dsa, tranche_id);
#else
	radixtree = rt_create(CurrentMemoryContext, work_mem);
#endif

	keys = palloc(sizeof(uint64) * children);
	for (int i = 0; i < children; i++)
	{
		if (reverse)
			keys[i] = (uint64) (children - 1 - i) << shift;
		else
			keys[i] = (uint64) i << shift;
	}

	/*
	 * Insert keys. Since the tree was just created, rt_set should return
	 * false.
	 */
	for (int i = 0; i < children; i++)
		EXPECT_FALSE(rt_set(radixtree, keys[i], (TestValueType*) &keys[i]));

	rt_stats(radixtree);

	/* look up keys */
	for (int i = 0; i < children; i++)
	{
		TestValueType *value;

		value = rt_find(radixtree, keys[i]);

		/* Test rt_find returns the expected value */
		EXPECT_TRUE(value != NULL);
		EXPECT_EQ_U64(*value, (TestValueType) keys[i]);
	}

	/* update keys */
	for (int i = 0; i < children; i++)
	{
		TestValueType update = keys[i] + 1;

		/* rt_set should report the key found */
		EXPECT_TRUE(rt_set(radixtree, keys[i], (TestValueType*) &update));
	}

	/* delete and re-insert keys */
	for (int i = 0; i < children; i++)
	{
		EXPECT_TRUE(rt_delete(radixtree, keys[i]));
		EXPECT_FALSE(rt_set(radixtree, keys[i], (TestValueType*) &keys[i]));
	}

	/* look up keys after deleting and re-inserting */
	for (int i = 0; i < children; i++)
	{
		TestValueType *value;

		value = rt_find(radixtree, keys[i]);

		/* Test that rt_find returns the expected value */
		EXPECT_TRUE(value != NULL);
		EXPECT_EQ_U64(*value, (TestValueType) keys[i]);
	}

	/* test that iteration returns the expected keys and values */
	iter = rt_begin_iterate(radixtree);

	for (int i = 0; i < children; i++)
	{
		uint64		expected;
		uint64		iterkey;
		TestValueType		*iterval;

		/* iteration is ordered by key, so adjust expected value accordingly */
		if (reverse)
			expected = keys[children - 1 - i];
		else
			expected = keys[i];

		iterval = rt_iterate_next(iter, &iterkey);

		EXPECT_TRUE(iterval != NULL);
		EXPECT_EQ_U64(iterkey, expected);
		EXPECT_EQ_U64(*iterval, expected);
	}

	rt_end_iterate(iter);

	/* delete all keys again */
	for (int i = 0; i < children; i++)
		EXPECT_TRUE(rt_delete(radixtree, keys[i]));

	/* test that all keys were deleted */
	for (int i = 0; i < children; i++)
		EXPECT_TRUE(rt_find(radixtree, keys[i]) == NULL);

	rt_stats(radixtree);

	pfree(keys);
	rt_free(radixtree);
#ifdef TEST_SHARED_RT
	dsa_detach(dsa);
#endif
}

static void
test_basic(rt_node_class_test_elem *test_info, int shift)
{
	elog(NOTICE, "testing node %s with shift %d", test_info->class_name, shift);

	/* Test nodes while changing the key insertion order */
	do_test_basic(test_info->nkeys, shift, false);
	do_test_basic(test_info->nkeys, shift, true);
}

/*
 * Test with a repeating pattern, defined by the 'spec'.
 */
static void
test_pattern(const test_spec * spec)
{
	rt_radix_tree *radixtree;
	rt_iter    *iter;
	MemoryContext radixtree_ctx;
	uint64		n;
	uint64		last_int;
	uint64		ndeleted;
	uint64		nbefore;
	uint64		nafter;
	int			patternlen;
	uint64	   *pattern_values;
	uint64		pattern_num_values;
#ifdef TEST_SHARED_RT
	int			tranche_id = LWLockNewTrancheId();
	dsa_area   *dsa;

	LWLockRegisterTranche(tranche_id, "test_radix_tree");
	dsa = dsa_create(tranche_id);
#endif

	elog(NOTICE, "testing radix tree with pattern \"%s\"", spec->test_name);

	/* Pre-process the pattern, creating an array of integers from it. */
	patternlen = strlen(spec->pattern_str);
	pattern_values = palloc(patternlen * sizeof(uint64));
	pattern_num_values = 0;
	for (int i = 0; i < patternlen; i++)
	{
		if (spec->pattern_str[i] == '1')
			pattern_values[pattern_num_values++] = i;
	}

	/*
	 * Allocate the radix tree.
	 *
	 * Allocate it in a separate memory context, so that we can print its
	 * memory usage easily.
	 */
	radixtree_ctx = AllocSetContextCreate(CurrentMemoryContext,
										  "radixtree test",
										  ALLOCSET_SMALL_SIZES);
	MemoryContextSetIdentifier(radixtree_ctx, spec->test_name);

#ifdef TEST_SHARED_RT
	radixtree = rt_create(radixtree_ctx, work_mem, dsa, tranche_id);
#else
	radixtree = rt_create(radixtree_ctx, work_mem);
#endif

	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		uint64		x = 0;

		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			x = last_int + pattern_values[i];

			EXPECT_FALSE(rt_set(radixtree, x, (TestValueType*) &x));
			n++;
		}
		last_int += spec->spacing;
	}

	/* Check that rt_num_entries works */
	EXPECT_EQ_U64(rt_num_entries(radixtree), spec->num_values);

	for (n = 0; n < 100000; n++)
	{
		bool		found;
		bool		expected;
		uint64		x;
		TestValueType		*v;

		/*
		 * Pick next value to probe at random.  We limit the probes to the
		 * last integer that we added to the set, plus an arbitrary constant
		 * (1000).  There's no point in probing the whole 0 - 2^64 range, if
		 * only a small part of the integer space is used.  We would very
		 * rarely hit values that are actually in the set.
		 */
		x = pg_prng_uint64_range(&pg_global_prng_state, 0, last_int + 1000);

		/* Do we expect this value to be present in the set? */
		if (x >= last_int)
			expected = false;
		else
		{
			uint64		idx = x % spec->spacing;

			if (idx >= patternlen)
				expected = false;
			else if (spec->pattern_str[idx] == '1')
				expected = true;
			else
				expected = false;
		}

		/* Is it present according to rt_search() ? */
		v = rt_find(radixtree, x);
		found = (v != NULL);

		EXPECT_TRUE(found == expected);
		if (found)
			EXPECT_EQ_U64(*v, (TestValueType) x);
	}

	iter = rt_begin_iterate(radixtree);
	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			uint64		expected = last_int + pattern_values[i];
			uint64		x;
			TestValueType		*val;

			val = rt_iterate_next(iter, &x);
			if (val == NULL)
				break;

			EXPECT_EQ_U64(x, expected);
			EXPECT_EQ_U64(*val, (TestValueType) expected);

			n++;
		}
		last_int += spec->spacing;
	}

	rt_end_iterate(iter);

	/* iterator returned the expected number of entries */
	EXPECT_EQ_U64(n, spec->num_values);

	nbefore = rt_num_entries(radixtree);
	ndeleted = 0;
	for (n = 0; n < 1; n++)
	{
		uint64		x;
		TestValueType		*v;

		/*
		 * Pick next value to probe at random.  We limit the probes to the
		 * last integer that we added to the set, plus an arbitrary constant
		 * (1000).  There's no point in probing the whole 0 - 2^64 range, if
		 * only a small part of the integer space is used.  We would very
		 * rarely hit values that are actually in the set.
		 */
		x = pg_prng_uint64_range(&pg_global_prng_state, 0, last_int + 1000);

		/* Is it present according to rt_find() ? */
		v = rt_find(radixtree, x);

		if (!v)
			continue;

		/* If the key is found, delete it and check again */
		EXPECT_TRUE(rt_delete(radixtree, x));
		EXPECT_TRUE(rt_find(radixtree, x) == NULL);
		EXPECT_FALSE(rt_delete(radixtree, x));

		ndeleted++;
	}

	nafter = rt_num_entries(radixtree);

	/* Check that rt_num_entries works */
	EXPECT_EQ_U64(nbefore - ndeleted, nafter);

	rt_free(radixtree);
	MemoryContextDelete(radixtree_ctx);
#ifdef TEST_SHARED_RT
	dsa_detach(dsa);
#endif
}

Datum
test_radixtree(PG_FUNCTION_ARGS)
{
	/* borrowed from RT_MAX_SHIFT */
	const int max_shift = (pg_leftmost_one_pos64(UINT64_MAX) / BITS_PER_BYTE) * BITS_PER_BYTE;

	test_empty();

	for (int i = 0; i < lengthof(rt_node_class_tests); i++)
	{
		rt_node_class_test_elem *test_info = &(rt_node_class_tests[i]);

		/* leaf nodes */
		test_basic(test_info, 0);

		/* internal nodes */
		test_basic(test_info, 8);

		/* max-level nodes */
		test_basic(test_info, max_shift);
	}

	/* Test different test patterns, with lots of entries */
	for (int i = 0; i < lengthof(test_specs); i++)
		test_pattern(&test_specs[i]);

	PG_RETURN_VOID();
}
