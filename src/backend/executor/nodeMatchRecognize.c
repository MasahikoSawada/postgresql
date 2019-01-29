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
#include "utils/tqual.h"

static TupleTableSlot *
ExecMatchRecognize(PlanState *pstate)
{
	MatchRecognizeState *node = castNode(MatchRecognizeState, pstate);
	TupleTableSlot *slot;
	PlanState *outerNode;

	outerNode = node->ss.ps.lefttree;
	slot = ExecProcNode(outerNode);

	return slot;
}

MatchRecognizeState *
ExecInitMatchRecognize(MatchRecognize *node, EState *estate, int eflags)
{
	MatchRecognizeState *mrstate;

	mrstate = makeNode(MatchRecognizeState);
	mrstate->ss.ps.plan = (Plan *) node;
	mrstate->ss.ps.state = estate;
	mrstate->ss.ps.ExecProcNode = ExecMatchRecognize;

	/* initialize child node */
	mrstate->ss.ps.lefttree = ExecInitNode(node->plan.lefttree, estate, eflags);

	ExecInitResultTupleSlotTL(estate, &mrstate->ss.ps);

	return mrstate;
}

void
ExecEndMatchRecognize(MatchRecognizeState *node)
{
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecEndNode(node->ss.ps.lefttree);
}
