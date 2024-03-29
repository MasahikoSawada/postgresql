/*--------------------------------------------------------------------------
 *
 * test_tidstore.c
 *		Test TidStore data structure.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_tidstore/test_tidstore.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tidstore.h"
#include "fmgr.h"
#include "funcapi.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tidstore_create);
PG_FUNCTION_INFO_V1(tidstore_set_block_offsets);
PG_FUNCTION_INFO_V1(tidstore_dump_tids);
PG_FUNCTION_INFO_V1(tidstore_lookup_tids);
PG_FUNCTION_INFO_V1(tidstore_is_full);
PG_FUNCTION_INFO_V1(tidstore_destroy);

static TidStore *tidstore = NULL;
static int64	num_tids = 0;
static size_t	max_bytes =  (2 * 1024 * 1024L); /* 2MB */

/*
 * Create a TidStore. If shared is false, the tidstore is created
 * on TopMemoryContext, otherwise on DSA. Although the tidstore
 * is created on DSA, only the same process can subsequently use
 * the tidstore. The tidstore handle is not shared anywhere.
*/
Datum
tidstore_create(PG_FUNCTION_ARGS)
{
	bool	shared = PG_GETARG_BOOL(0);
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	if (shared)
	{
		int tranche_id;
		dsa_area *dsa;

		tranche_id = LWLockNewTrancheId();
		LWLockRegisterTranche(tranche_id, "test_tidstore");
		dsa = dsa_create(tranche_id);
		dsa_pin_mapping(dsa);

		tidstore = TidStoreCreate(max_bytes, dsa);
	}
	else
		tidstore = TidStoreCreate(max_bytes, NULL);

	num_tids = 0;

	MemoryContextSwitchTo(old_ctx);

	PG_RETURN_VOID();
}

static void
sanity_check_array(ArrayType *ta)
{
	if (ARR_HASNULL(ta) && array_contains_nulls(ta))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("array must not contain nulls")));

	if (ARR_NDIM(ta) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("argument must be empty or one-dimensional array")));
}

/* Sanity check if we've called tidstore_create() */
static void
check_tidstore_available(void)
{
	if (tidstore == NULL)
		elog(ERROR, "tidstore is not initialized");
}

/* Set the given block and offsets pairs */
Datum
tidstore_set_block_offsets(PG_FUNCTION_ARGS)
{
	BlockNumber blkno = PG_GETARG_INT64(0);
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P_COPY(1);
	OffsetNumber *offs;
	int	noffs;

	check_tidstore_available();
	sanity_check_array(ta);

	noffs = ArrayGetNItems(ARR_NDIM(ta), ARR_DIMS(ta));
	offs = ((OffsetNumber *) ARR_DATA_PTR(ta));

	/* Set TIDs */
	TidStoreLockExclusive(tidstore);
	TidStoreSetBlockOffsets(tidstore, blkno, offs, noffs);
	TidStoreUnlock(tidstore);

	/* Update statistics */
	num_tids += noffs;

	PG_RETURN_VOID();
}

/*
 * Dump and return TIDs in the tidstore. The output TIDs are ordered.
 */
Datum
tidstore_dump_tids(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ItemPointerData	*tids;

	check_tidstore_available();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TidStoreIter	*iter;
		TidStoreIterResult	*iter_result;
		int64			ntids = 0;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		tids = (ItemPointerData *)
			palloc0(sizeof(ItemPointerData) * num_tids);

		/* Collect TIDs stored in the tidstore */
		TidStoreLockShare(tidstore);
		iter = TidStoreBeginIterate(tidstore);
		while ((iter_result = TidStoreIterateNext(iter)) != NULL)
		{
			for (int i = 0; i < iter_result->num_offsets; i++)
				ItemPointerSet(&(tids[ntids++]), iter_result->blkno,
							   iter_result->offsets[i]);
		}
		TidStoreUnlock(tidstore);

		Assert(ntids == num_tids);

		funcctx->user_fctx = tids;
		funcctx->max_calls = num_tids;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	tids = (ItemPointerData *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		int i = funcctx->call_cntr;
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(&(tids[i])));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Test if the given TIDs exist on the tidstore.
 */
Datum
tidstore_lookup_tids(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P_COPY(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	ItemPointer	tids;
	int		ntids;
	Datum	values[2];
	bool	nulls[2] = {false};

	check_tidstore_available();
	sanity_check_array(ta);

	InitMaterializedSRF(fcinfo, 0);

	ntids = ArrayGetNItems(ARR_NDIM(ta), ARR_DIMS(ta));
	tids = ((ItemPointer) ARR_DATA_PTR(ta));

	for (int i = 0; i < ntids; i++)
	{
		bool found;
		ItemPointerData tid = tids[i];

		TidStoreLockShare(tidstore);
		found = TidStoreIsMember(tidstore, &tid);
		TidStoreUnlock(tidstore);

		values[0] = ItemPointerGetDatum(&tid);
		values[1] = BoolGetDatum(found);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/*
 * Return true if the size of tidstore reached the maximum memory
 * limit.
 */
Datum
tidstore_is_full(PG_FUNCTION_ARGS)
{
	bool		is_full;

	check_tidstore_available();

	is_full = (TidStoreMemoryUsage(tidstore) > max_bytes);

	PG_RETURN_BOOL(is_full);
}

/* Free the tidstore */
Datum
tidstore_destroy(PG_FUNCTION_ARGS)
{
	check_tidstore_available();

	TidStoreDestroy(tidstore);
	tidstore = NULL;
	num_tids = 0;

	/* DSA for tidstore will be detached at the end of session */

	PG_RETURN_VOID();
}
