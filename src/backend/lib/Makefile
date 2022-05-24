#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for lib (miscellaneous stuff)
#
# IDENTIFICATION
#    src/backend/lib/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/lib
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	binaryheap.o \
	bipartite_match.o \
	bloomfilter.o \
	dshash.o \
	hyperloglog.o \
	ilist.o \
	integerset.o \
	knapsack.o \
	pairingheap.o \
	radixtree.o \
	rbtree.o \

# radixtree.o need CFLAGS_AVX2
radixtree.o: CFLAGS+=$(CFLAGS_AVX2)

include $(top_srcdir)/src/backend/common.mk
