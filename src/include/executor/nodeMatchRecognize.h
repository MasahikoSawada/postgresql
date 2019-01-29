#ifndef NODEMATCHRECOGNIZE_H
#define NODEMATCHRECOGNIZE_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern MatchRecognizeState *ExecInitMatchRecognize(MatchRecognize *node,
												   EState *estate,
												   int eflags);
extern void ExecEndMatchRecognize(MatchRecognizeState *node);

#endif
