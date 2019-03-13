#include "postgres.h"

#include "access/hash.h"
#include "access/relscan.h"
#include "access/tsmapi.h"
#include "executor/executor.h"
#include "executor/nodeMatchRecognize.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/rel.h"

static void begin_partition(MatchRecognizeState *mrstate);
static void spool_tuples(MatchRecognizeState *mrstate, int64 pos);
static void release_partition(MatchRecognizeState *mrstate);

static void
spool_tuples(MatchRecognizeState *mrstate, int64 pos)
{
	PlanState *outerPlan;
	TupleTableSlot *outerslot;

	if (!mrstate->buffer)
		return;
	if (mrstate->partition_spooled)
		return;

	if (!tuplestore_in_memory(mrstate->buffer))
		pos = -1;

	outerPlan = outerPlanState(mrstate);

	while (mrstate->spooled_rows <= pos || pos == -1)
	{
		outerslot = ExecProcNode(outerPlan);
		if (TupIsNull(outerslot))
		{
			mrstate->partition_spooled = true;
			mrstate->more_partitions = false;
			break;
		}

		tuplestore_puttupleslot(mrstate->buffer, outerslot);
		mrstate->spooled_rows++;
	}
}

static void
release_partition(MatchRecognizeState *mrstate)
{

	if (mrstate->buffer)
		tuplestore_end(mrstate->buffer);
	mrstate->buffer = NULL;
	mrstate->partition_spooled = false;
}

static void
begin_partition(MatchRecognizeState *mrstate)
{
	PlanState *outerPlan = outerPlanState(mrstate);

	mrstate->partition_spooled = false;
	mrstate->framehead_valid = false;
	mrstate->frametail_valid = false;
	mrstate->spooled_rows = 0;
	mrstate->currentpos = 0;
	mrstate->frameheadpos = 0;
	mrstate->frametailpos = 0;
	if (mrstate->framehead_slot)
		ExecClearTuple(mrstate->framehead_slot);
	if (mrstate->frametail_slot)
		ExecClearTuple(mrstate->frametail_slot);


	if (TupIsNull(mrstate->first_part_slot))
	{
		TupleTableSlot *outerslot = ExecProcNode(outerPlan);

		if (!TupIsNull(outerslot))
			ExecCopySlot(mrstate->first_part_slot, outerslot);
		else
		{
			mrstate->partition_spooled = true;
			mrstate->more_partitions = false;
			return;
		}
	}

	mrstate->buffer = tuplestore_begin_heap(false, false, work_mem);

	mrstate->current_ptr = 0;

	tuplestore_set_eflags(mrstate->buffer, 0);

	mrstate->framehead_ptr = mrstate->frametail_ptr = -1;

	tuplestore_puttupleslot(mrstate->buffer, mrstate->first_part_slot);
	mrstate->spooled_rows++;
}

static TupleTableSlot *
ExecMatchRecognize(PlanState *pstate)
{
	MatchRecognizeState *mrstate = castNode(MatchRecognizeState, pstate);
	ExprContext *econtext;
	TupleTableSlot *slot;
	PlanState *outerNode;

	CHECK_FOR_INTERRUPTS();

	if (mrstate->all_done)
		return NULL;

	if (mrstate->buffer == NULL)
	{
		begin_partition(mrstate);
	}
	else
	{
		mrstate->currentpos++;
		mrstate->framehead_valid = false;
		mrstate->frametail_valid = false;
	}

	spool_tuples(mrstate, mrstate->currentpos);

	if (mrstate->partition_spooled &&
		mrstate->currentpos >= mrstate->spooled_rows)
	{
		release_partition(mrstate);

		if (mrstate->more_partitions)
		{
			begin_partition(mrstate);
		}
		else
		{
			mrstate->all_done = true;
			return NULL;
		}
	}

	econtext = mrstate->ss.ps.ps_ExprContext;
	ResetExprContext(econtext);

	tuplestore_select_read_pointer(mrstate->buffer, mrstate->current_ptr);

	/* Read the current row */
	tuplestore_gettupleslot(mrstate->buffer, true, true,
							mrstate->ss.ss_ScanTupleSlot);

	/* eval */

	tuplestore_trim(mrstate->buffer);
	econtext->ecxt_scantuple = mrstate->ss.ss_ScanTupleSlot;

	return ExecProject(mrstate->ss.ps.ps_ProjInfo);
}

MatchRecognizeState *
ExecInitMatchRecognize(MatchRecognize *node, EState *estate, int eflags)
{
	MatchRecognizeState *mrstate;
	Plan	*outerPlan;
	TupleDesc	scanDesc;

	mrstate = makeNode(MatchRecognizeState);
	mrstate->ss.ps.plan = (Plan *) node;
	mrstate->ss.ps.state = estate;
	mrstate->ss.ps.ExecProcNode = ExecMatchRecognize;

	ExecAssignExprContext(estate, &mrstate->ss.ps);

	/* initialize child node */
	outerPlan = outerPlan(node);
	outerPlanState(mrstate) = ExecInitNode(outerPlan, estate, eflags);
	//mrstate->ss.ps.lefttree = ExecInitNode(node->plan.lefttree, estate, eflags);

	ExecCreateScanSlotFromOuterPlan(estate, &mrstate->ss, &TTSOpsMinimalTuple);
	scanDesc = mrstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	mrstate->ss.ps.outeropsset = true;
	mrstate->ss.ps.outerops = &TTSOpsMinimalTuple;
	mrstate->ss.ps.outeropsfixed = true;

	mrstate->first_part_slot = ExecInitExtraTupleSlot(estate, scanDesc,
													  &TTSOpsMinimalTuple);
	mrstate->temp_slot_1 = ExecInitExtraTupleSlot(estate, scanDesc,
												  &TTSOpsMinimalTuple);
	mrstate->temp_slot_2 = ExecInitExtraTupleSlot(estate, scanDesc,
												  &TTSOpsMinimalTuple);

	mrstate->framehead_slot = mrstate->frametail_slot = NULL;

	ExecInitResultTupleSlotTL(&mrstate->ss.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&mrstate->ss.ps, NULL);

	mrstate->partEqfunction =
		execTuplesMatchPrepare(scanDesc,
							   node->partNumCols,
							   node->partColIdx,
							   node->partOperators,
							   &mrstate->ss.ps);

	mrstate->more_partitions = false;

	return mrstate;
}

void
ExecEndMatchRecognize(MatchRecognizeState *node)
{
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecEndNode(node->ss.ps.lefttree);
}
