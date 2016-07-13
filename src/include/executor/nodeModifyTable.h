/*-------------------------------------------------------------------------
 *
 * nodeModifyTable.h
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeModifyTable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMODIFYTABLE_H
#define NODEMODIFYTABLE_H

#include "nodes/execnodes.h"

extern ModifyTableState *ExecInitModifyTable(ModifyTable *node, EState *estate, int eflags);
extern TupleTableSlot *ExecModifyTable(ModifyTableState *node);
extern void ExecEndModifyTable(ModifyTableState *node);
extern void ExecReScanModifyTable(ModifyTableState *node);
extern TupleTableSlot *ExecInsert(ModifyTableState *mtstate,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   List *arbiterIndexes,
		   OnConflictAction onconflict,
		   EState *estate,
		   bool canSetTag);
extern TupleTableSlot *ExecDelete(ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool canSetTag);
extern TupleTableSlot *ExecUpdate(ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool canSetTag);

#endif   /* NODEMODIFYTABLE_H */
