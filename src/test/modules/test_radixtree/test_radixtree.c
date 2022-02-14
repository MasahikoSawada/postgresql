/*--------------------------------------------------------------------------
 *
 * test_radixtree.c
 *		Test radixtree set data structure.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_radixtree/test_radixtree.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "lib/radixtree.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define UINT64_HEX_FORMAT "%" INT64_MODIFIER "X"

/*
 * If you enable this, the "pattern" tests will print information about
 * how long populating, probing, and iterating the test set takes, and
 * how much memory the test set consumed.  That can be used as
 * micro-benchmark of various operations and input patterns (you might
 * want to increase the number of values used in each of the test, if
 * you do that, to reduce noise).
 *
 * The information is printed to the server's stderr, mostly because
 * that's where MemoryContextStats() output goes.
 */
static const bool intset_test_stats = true;

static int radix_tree_node_max_entries[] = {4, 16, 48, 256};

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
} test_spec;

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
		10000, 10000000
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

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_radixtree);

static void test_empty(void);

static void
test_empty(void)
{
	radix_tree *radixtree;
	bool found;

	radixtree = radix_tree_create(CurrentMemoryContext);

	radix_tree_search(radixtree, 0, &found);
	if (found)
		elog(ERROR, "radix_tree_search on empty tree returned true");

	radix_tree_search(radixtree, 1, &found);
	if (found)
		elog(ERROR, "radix_tree_search on empty tree returned true");

	radix_tree_search(radixtree, PG_UINT64_MAX, &found);
	if (found)
		elog(ERROR, "radix_tree_search on empty tree returned true");

	if (radix_tree_num_entries(radixtree) != 0)
		elog(ERROR, "radix_tree_num_entries on empty tree return non-zero");

	radix_tree_destroy(radixtree);
}

static void
check_search_on_node(radix_tree *radixtree, uint8 shift, int start, int end)
{
	for (int i = start; i < end; i++)
	{
		uint64 key = ((uint64) i << shift);
		bool found;
		Datum val;

		val = radix_tree_search(radixtree, key, &found);
		if (!found)
			elog(ERROR, "key 0x" UINT64_HEX_FORMAT " is not found on node-%d",
				 key, end);
		if (DatumGetUInt64(val) != key)
			elog(ERROR, "radix_tree_search with key 0x" UINT64_HEX_FORMAT " returns 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT,
				 key, DatumGetUInt64(val), key);
	}
}

static void
test_node_types(uint8 shift)
{
	radix_tree *radixtree;
	uint64 num_entries;

	radixtree = radix_tree_create(CurrentMemoryContext);

	for (int i = 0; i < 256; i++)
	{
		uint64 key = ((uint64) i << shift);
		bool found;

		radix_tree_insert(radixtree, key, Int64GetDatum(key), &found);

		if (found)
			elog(ERROR, "newly inserted key 0x" UINT64_HEX_FORMAT " found", key);

		for (int j = 0; j < lengthof(radix_tree_node_max_entries); j++)
		{
			if (i == (radix_tree_node_max_entries[j] - 1))
			{
				check_search_on_node(radixtree, shift,
									 (j == 0) ? 0 : radix_tree_node_max_entries[j - 1],
									 radix_tree_node_max_entries[j]);
				break;
			}
		}
	}

	num_entries = radix_tree_num_entries(radixtree);

	if (num_entries != 256)
		elog(ERROR,
			 "radix_tree_num_entries returned" UINT64_FORMAT ", expected " UINT64_FORMAT,
			 num_entries, UINT64CONST(256));
}

/*
 * Test with a repeating pattern, defined by the 'spec'.
 */
static void
test_pattern(const test_spec *spec)
{
	radix_tree *radixtree;
	radix_tree_iter *iter;
	MemoryContext radixtree_ctx;
	TimestampTz starttime;
	TimestampTz endtime;
	uint64		n;
	uint64		last_int;
	int			patternlen;
	uint64	   *pattern_values;
	uint64		pattern_num_values;

	elog(NOTICE, "testing radix tree with pattern \"%s\"", spec->test_name);
	if (intset_test_stats)
		fprintf(stderr, "-----\ntesting radix tree with pattern \"%s\"\n", spec->test_name);

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
	 * Allocate the integer set.
	 *
	 * Allocate it in a separate memory context, so that we can print its
	 * memory usage easily.  (intset_create() creates a memory context of its
	 * own, too, but we don't have direct access to it, so we cannot call
	 * MemoryContextStats() on it directly).
	 */
	radixtree_ctx = AllocSetContextCreate(CurrentMemoryContext,
										  "radixtree test",
										  ALLOCSET_SMALL_SIZES);
	MemoryContextSetIdentifier(radixtree_ctx, spec->test_name);
	radixtree = radix_tree_create(radixtree_ctx);

	/*
	 * Add values to the set.
	 */
	starttime = GetCurrentTimestamp();

	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		uint64		x = 0;

		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			bool found;

			x = last_int + pattern_values[i];

			radix_tree_insert(radixtree, x, Int64GetDatum(x), &found);

			if (found)
				elog(ERROR, "newly inserted key 0x" UINT64_HEX_FORMAT " found", x);

			n++;
		}
		last_int += spec->spacing;
	}

	endtime = GetCurrentTimestamp();

	if (intset_test_stats)
		fprintf(stderr, "added " UINT64_FORMAT " values in %d ms\n",
				spec->num_values, (int) (endtime - starttime) / 1000);

	/*
	 * Print stats on the amount of memory used.
	 *
	 * We print the usage reported by intset_memory_usage(), as well as the
	 * stats from the memory context.  They should be in the same ballpark,
	 * but it's hard to automate testing that, so if you're making changes to
	 * the implementation, just observe that manually.
	 */
	if (intset_test_stats)
	{
		uint64		mem_usage;

		/*
		 * Also print memory usage as reported by intset_memory_usage().  It
		 * should be in the same ballpark as the usage reported by
		 * MemoryContextStats().
		 */
		mem_usage = radix_tree_memory_usage(radixtree);
		fprintf(stderr, "radix_tree_memory_usage() reported " UINT64_FORMAT " (%0.2f bytes / integer)\n",
				mem_usage, (double) mem_usage / spec->num_values);

		MemoryContextStats(radixtree_ctx);
	}

	/* Check that intset_get_num_entries works */
	n = radix_tree_num_entries(radixtree);
	if (n != spec->num_values)
		elog(ERROR, "radix_tree_num_entries returned " UINT64_FORMAT ", expected " UINT64_FORMAT, n, spec->num_values);

	/*
	 * Test random-access probes with intset_is_member()
	 */
	starttime = GetCurrentTimestamp();

	for (n = 0; n < 100000; n++)
	{
		bool		found;
		bool		expected;
		uint64		x;
		Datum		v;

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

		/* Is it present according to intset_is_member() ? */
		v = radix_tree_search(radixtree, x, &found);

		if (found != expected)
			elog(ERROR, "mismatch at 0x" UINT64_HEX_FORMAT ": %d vs %d", x, found, expected);
		if (found && (DatumGetUInt64(v) != x))
			elog(ERROR, "found 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT,
				 DatumGetUInt64(v), x);
	}
	endtime = GetCurrentTimestamp();
	if (intset_test_stats)
		fprintf(stderr, "probed " UINT64_FORMAT " values in %d ms\n",
				n, (int) (endtime - starttime) / 1000);

	/*
	 * Test iterator
	 */
	starttime = GetCurrentTimestamp();

	iter = radix_tree_begin_iterate(radixtree);
	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			uint64		expected = last_int + pattern_values[i];
			uint64		x;
			uint64		val;

			if (!radix_tree_iterate_next(iter, &x, &val))
				break;

			if (x != expected)
				elog(ERROR,
					 "iterate returned wrong key; got 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT " at %d", x, expected, i);
			if (DatumGetUInt64(val) != expected)
				elog(ERROR,
					 "iterate returned wrong value; got 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT " at %d", x, expected, i);
			n++;
		}
		last_int += spec->spacing;
	}
	endtime = GetCurrentTimestamp();
	if (intset_test_stats)
		fprintf(stderr, "iterated " UINT64_FORMAT " values in %d ms\n",
				n, (int) (endtime - starttime) / 1000);

	if (n < spec->num_values)
		elog(ERROR, "iterator stopped short after " UINT64_FORMAT " entries, expected " UINT64_FORMAT, n, spec->num_values);
	if (n > spec->num_values)
		elog(ERROR, "iterator returned " UINT64_FORMAT " entries, " UINT64_FORMAT " was expected", n, spec->num_values);

	MemoryContextDelete(radixtree_ctx);
}

Datum
test_radixtree(PG_FUNCTION_ARGS)
{
	test_empty();

	for (int shift = 0; shift <= (64 - 8); shift += 8)
		test_node_types(shift);

	/* Test different test patterns, with lots of entries */
	for (int i = 0; i < lengthof(test_specs); i++)
	{
		test_pattern(&test_specs[i]);
	}

	PG_RETURN_VOID();
}

#ifdef UNUSED
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
} test_spec;

static const test_spec test_specs[] = {
	{
		"all ones", "1111111111",
		10, 10000000
	},
	{
		"alternating bits", "0101010101",
		10, 10000000
	},
	{
		"clusters of ten", "1111111111",
		10000, 10000000
	},
	{
		"clusters of hundred",
		"1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
		10000, 100000000
	},
	{
		"one-every-64k", "1",
		65536, 10000000
	},
	{
		"sparse", "100000000000000000000000000000001",
		10000000, 10000000
	},
	{
		"single values, distance > 2^32", "1",
		UINT64CONST(10000000000), 1000000
	},
	{
		"clusters, distance > 2^32", "10101010",
		UINT64CONST(10000000000), 10000000
	},
	{
		"clusters, distance > 2^60", "10101010",
		UINT64CONST(2000000000000000000),
		23						/* can't be much higher than this, or we
								 * overflow uint64 */
	}
};

static void test_pattern(const test_spec *spec);
static void test_empty(void);
static void test_single_value(uint64 value);
static void check_with_filler(IntegerSet *intset, uint64 x, uint64 value, uint64 filler_min, uint64 filler_max);
static void test_single_value_and_filler(uint64 value, uint64 filler_min, uint64 filler_max);
static void test_huge_distances(void);

/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_radixtree(PG_FUNCTION_ARGS)
{
	/* Tests for various corner cases */
	test_empty();
	test_huge_distances();
	test_single_value(0);
	test_single_value(1);
	test_single_value(PG_UINT64_MAX - 1);
	test_single_value(PG_UINT64_MAX);
	test_single_value_and_filler(0, 1000, 2000);
	test_single_value_and_filler(1, 1000, 2000);
	test_single_value_and_filler(1, 1000, 2000000);
	test_single_value_and_filler(PG_UINT64_MAX - 1, 1000, 2000);
	test_single_value_and_filler(PG_UINT64_MAX, 1000, 2000);

	/* Test different test patterns, with lots of entries */
	for (int i = 0; i < lengthof(test_specs); i++)
	{
		test_pattern(&test_specs[i]);
	}

	PG_RETURN_VOID();
}

/*
 * Test with a repeating pattern, defined by the 'spec'.
 */
static void
test_pattern(const test_spec *spec)
{
	radix_tree *tree;
	MemoryContext radixtree_ctx;
	MemoryContext old_ctx;
	TimestampTz starttime;
	TimestampTz endtime;
	uint64		n;
	uint64		last_int;
	int			patternlen;
	uint64	   *pattern_values;
	uint64		pattern_num_values;

	elog(NOTICE, "testing intset with pattern \"%s\"", spec->test_name);
	if (intset_test_stats)
		fprintf(stderr, "-----\ntesting intset with pattern \"%s\"\n", spec->test_name);

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
	 * Allocate the integer set.
	 *
	 * Allocate it in a separate memory context, so that we can print its
	 * memory usage easily.  (intset_create() creates a memory context of its
	 * own, too, but we don't have direct access to it, so we cannot call
	 * MemoryContextStats() on it directly).
	 */
	radixtree_ctx = AllocSetContextCreate(CurrentMemoryContext,
									   "radixtree test",
									   ALLOCSET_SMALL_SIZES);
	MemoryContextSetIdentifier(radixtree_ctx, spec->test_name);
	radix_tree = radix_tree_create(radixtree_ctx);

	/*
	 * Add values to the set.
	 */
	starttime = GetCurrentTimestamp();

	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		uint64		x = 0;

		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			x = last_int + pattern_values[i];

			radix_tree_insert(intset, x, Int64GetDatum(x));
			n++;
		}
		last_int += spec->spacing;
	}

	endtime = GetCurrentTimestamp();

	if (intset_test_stats)
		fprintf(stderr, "added " UINT64_FORMAT " values in %d ms\n",
				spec->num_values, (int) (endtime - starttime) / 1000);

	/*
	 * Print stats on the amount of memory used.
	 *
	 * We print the usage reported by intset_memory_usage(), as well as the
	 * stats from the memory context.  They should be in the same ballpark,
	 * but it's hard to automate testing that, so if you're making changes to
	 * the implementation, just observe that manually.
	 */
	if (intset_test_stats)
	{
		uint64		mem_usage;

		/*
		 * Also print memory usage as reported by intset_memory_usage().  It
		 * should be in the same ballpark as the usage reported by
		 * MemoryContextStats().
		 */
		/*
		mem_usage = intset_memory_usage(intset);
		fprintf(stderr, "intset_memory_usage() reported " UINT64_FORMAT " (%0.2f bytes / integer)\n",
				mem_usage, (double) mem_usage / spec->num_values);

		MemoryContextStats(intset_ctx);
		*/
	}

	/* Check that intset_get_num_entries works */
	n = intset_num_entries(intset);
	if (n != spec->num_values)
		elog(ERROR, "intset_num_entries returned " UINT64_FORMAT ", expected " UINT64_FORMAT, n, spec->num_values);

	/*
	 * Test random-access probes with intset_is_member()
	 */
	starttime = GetCurrentTimestamp();

	for (n = 0; n < 100000; n++)
	{
		bool		b;
		bool		expected;
		uint64		x;

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

		/* Is it present according to intset_is_member() ? */
		b = intset_is_member(intset, x);

		if (b != expected)
			elog(ERROR, "mismatch at " UINT64_FORMAT ": %d vs %d", x, b, expected);
	}
	endtime = GetCurrentTimestamp();
	if (intset_test_stats)
		fprintf(stderr, "probed " UINT64_FORMAT " values in %d ms\n",
				n, (int) (endtime - starttime) / 1000);

	/*
	 * Test iterator
	 */
	starttime = GetCurrentTimestamp();

	intset_begin_iterate(intset);
	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			uint64		expected = last_int + pattern_values[i];
			uint64		x;

			if (!intset_iterate_next(intset, &x))
				break;

			if (x != expected)
				elog(ERROR, "iterate returned wrong value; got " UINT64_FORMAT ", expected " UINT64_FORMAT, x, expected);
			n++;
		}
		last_int += spec->spacing;
	}
	endtime = GetCurrentTimestamp();
	if (intset_test_stats)
p		fprintf(stderr, "iterated " UINT64_FORMAT " values in %d ms\n",
				n, (int) (endtime - starttime) / 1000);

	if (n < spec->num_values)
		elog(ERROR, "iterator stopped short after " UINT64_FORMAT " entries, expected " UINT64_FORMAT, n, spec->num_values);
	if (n > spec->num_values)
		elog(ERROR, "iterator returned " UINT64_FORMAT " entries, " UINT64_FORMAT " was expected", n, spec->num_values);

	MemoryContextDelete(intset_ctx);
}

/*
 * Test with a set containing a single integer.
 */
static void
test_single_value(uint64 value)
{
	IntegerSet *intset;
	uint64		x;
	uint64		num_entries;
	bool		found;

	elog(NOTICE, "testing intset with single value " UINT64_FORMAT, value);

	/* Create the set. */
	intset = intset_create();
	intset_add_member(intset, value);

	/* Test intset_get_num_entries() */
	num_entries = intset_num_entries(intset);
	if (num_entries != 1)
		elog(ERROR, "intset_num_entries returned " UINT64_FORMAT ", expected 1", num_entries);

	/*
	 * Test intset_is_member() at various special values, like 0 and maximum
	 * possible 64-bit integer, as well as the value itself.
	 */
	if (intset_is_member(intset, 0) != (value == 0))
		elog(ERROR, "intset_is_member failed for 0");
	if (intset_is_member(intset, 1) != (value == 1))
		elog(ERROR, "intset_is_member failed for 1");
	if (intset_is_member(intset, PG_UINT64_MAX) != (value == PG_UINT64_MAX))
		elog(ERROR, "intset_is_member failed for PG_UINT64_MAX");
	if (intset_is_member(intset, value) != true)
		elog(ERROR, "intset_is_member failed for the tested value");

	/*
	 * Test iterator
	 */
	intset_begin_iterate(intset);
	found = intset_iterate_next(intset, &x);
	if (!found || x != value)
		elog(ERROR, "intset_iterate_next failed for " UINT64_FORMAT, x);

	found = intset_iterate_next(intset, &x);
	if (found)
		elog(ERROR, "intset_iterate_next failed " UINT64_FORMAT, x);
}

/*
 * Test with an integer set that contains:
 *
 * - a given single 'value', and
 * - all integers between 'filler_min' and 'filler_max'.
 *
 * This exercises different codepaths than testing just with a single value,
 * because the implementation buffers newly-added values.  If we add just a
 * single value to the set, we won't test the internal B-tree code at all,
 * just the code that deals with the buffer.
 */
static void
test_single_value_and_filler(uint64 value, uint64 filler_min, uint64 filler_max)
{
	IntegerSet *intset;
	uint64		x;
	bool		found;
	uint64	   *iter_expected;
	uint64		n = 0;
	uint64		num_entries = 0;
	uint64		mem_usage;

	elog(NOTICE, "testing intset with value " UINT64_FORMAT ", and all between " UINT64_FORMAT " and " UINT64_FORMAT,
		 value, filler_min, filler_max);

	intset = intset_create();

	iter_expected = palloc(sizeof(uint64) * (filler_max - filler_min + 1));
	if (value < filler_min)
	{
		intset_add_member(intset, value);
		iter_expected[n++] = value;
	}

	for (x = filler_min; x < filler_max; x++)
	{
		intset_add_member(intset, x);
		iter_expected[n++] = x;
	}

	if (value >= filler_max)
	{
		intset_add_member(intset, value);
		iter_expected[n++] = value;
	}

	/* Test intset_get_num_entries() */
	num_entries = intset_num_entries(intset);
	if (num_entries != n)
		elog(ERROR, "intset_num_entries returned " UINT64_FORMAT ", expected " UINT64_FORMAT, num_entries, n);

	/*
	 * Test intset_is_member() at various spots, at and around the values that
	 * we expect to be set, as well as 0 and the maximum possible value.
	 */
	check_with_filler(intset, 0,
					  value, filler_min, filler_max);
	check_with_filler(intset, 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, filler_min - 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, filler_min,
					  value, filler_min, filler_max);
	check_with_filler(intset, filler_min + 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, value - 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, value,
					  value, filler_min, filler_max);
	check_with_filler(intset, value + 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, filler_max - 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, filler_max,
					  value, filler_min, filler_max);
	check_with_filler(intset, filler_max + 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, PG_UINT64_MAX - 1,
					  value, filler_min, filler_max);
	check_with_filler(intset, PG_UINT64_MAX,
					  value, filler_min, filler_max);

	intset_begin_iterate(intset);
	for (uint64 i = 0; i < n; i++)
	{
		found = intset_iterate_next(intset, &x);
		if (!found || x != iter_expected[i])
			elog(ERROR, "intset_iterate_next failed for " UINT64_FORMAT, x);
	}
	found = intset_iterate_next(intset, &x);
	if (found)
		elog(ERROR, "intset_iterate_next failed " UINT64_FORMAT, x);

	mem_usage = intset_memory_usage(intset);
	if (mem_usage < 5000 || mem_usage > 500000000)
		elog(ERROR, "intset_memory_usage() reported suspicious value: " UINT64_FORMAT, mem_usage);
}

/*
 * Helper function for test_single_value_and_filler.
 *
 * Calls intset_is_member() for value 'x', and checks that the result is what
 * we expect.
 */
static void
check_with_filler(IntegerSet *intset, uint64 x,
				  uint64 value, uint64 filler_min, uint64 filler_max)
{
	bool		expected;
	bool		actual;

	expected = (x == value || (filler_min <= x && x < filler_max));

	actual = intset_is_member(intset, x);

	if (actual != expected)
		elog(ERROR, "intset_is_member failed for " UINT64_FORMAT, x);
}

/*
 * Test empty set
 */
static void
test_empty(void)
{
	IntegerSet *intset;
	uint64		x;

	elog(NOTICE, "testing intset with empty set");

	intset = intset_create();

	/* Test intset_is_member() */
	if (intset_is_member(intset, 0) != false)
		elog(ERROR, "intset_is_member on empty set returned true");
	if (intset_is_member(intset, 1) != false)
		elog(ERROR, "intset_is_member on empty set returned true");
	if (intset_is_member(intset, PG_UINT64_MAX) != false)
		elog(ERROR, "intset_is_member on empty set returned true");

	/* Test iterator */
	intset_begin_iterate(intset);
	if (intset_iterate_next(intset, &x))
		elog(ERROR, "intset_iterate_next on empty set returned a value (" UINT64_FORMAT ")", x);
}

/*
 * Test with integers that are more than 2^60 apart.
 *
 * The Simple-8b encoding used by the set implementation can only encode
 * values up to 2^60.  That makes large differences like this interesting
 * to test.
 */
static void
test_huge_distances(void)
{
	IntegerSet *intset;
	uint64		values[1000];
	int			num_values = 0;
	uint64		val = 0;
	bool		found;
	uint64		x;

	elog(NOTICE, "testing intset with distances > 2^60 between values");

	val = 0;
	values[num_values++] = val;

	/* Test differences on both sides of the 2^60 boundary. */
	val += UINT64CONST(1152921504606846976) - 1;	/* 2^60 - 1 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976) - 1;	/* 2^60 - 1 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976);	/* 2^60 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976);	/* 2^60 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976);	/* 2^60 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976) + 1;	/* 2^60 + 1 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976) + 1;	/* 2^60 + 1 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976) + 1;	/* 2^60 + 1 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976) + 2;	/* 2^60 + 2 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976) + 2;	/* 2^60 + 2 */
	values[num_values++] = val;

	val += UINT64CONST(1152921504606846976);	/* 2^60 */
	values[num_values++] = val;

	/*
	 * We're now very close to 2^64, so can't add large values anymore.  But
	 * add more smaller values to the end, to make sure that all the above
	 * values get flushed and packed into the tree structure.
	 */
	while (num_values < 1000)
	{
		val += pg_prng_uint32(&pg_global_prng_state);
		values[num_values++] = val;
	}

	/* Create an IntegerSet using these values */
	intset = intset_create();
	for (int i = 0; i < num_values; i++)
		intset_add_member(intset, values[i]);

	/*
	 * Test intset_is_member() around each of these values
	 */
	for (int i = 0; i < num_values; i++)
	{
		uint64		x = values[i];
		bool		expected;
		bool		result;

		if (x > 0)
		{
			expected = (values[i - 1] == x - 1);
			result = intset_is_member(intset, x - 1);
			if (result != expected)
				elog(ERROR, "intset_is_member failed for " UINT64_FORMAT, x - 1);
		}

		result = intset_is_member(intset, x);
		if (result != true)
			elog(ERROR, "intset_is_member failed for " UINT64_FORMAT, x);

		expected = (i != num_values - 1) ? (values[i + 1] == x + 1) : false;
		result = intset_is_member(intset, x + 1);
		if (result != expected)
			elog(ERROR, "intset_is_member failed for " UINT64_FORMAT, x + 1);
	}

	/*
	 * Test iterator
	 */
	intset_begin_iterate(intset);
	for (int i = 0; i < num_values; i++)
	{
		found = intset_iterate_next(intset, &x);
		if (!found || x != values[i])
			elog(ERROR, "intset_iterate_next failed for " UINT64_FORMAT, x);
	}
	found = intset_iterate_next(intset, &x);
	if (found)
		elog(ERROR, "intset_iterate_next failed " UINT64_FORMAT, x);
}
#endif
