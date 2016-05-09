#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for btree2
#
#-------------------------------------------------------------------------

subdir = src/backend/access/nbtree2
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = nbt2compare.o nbt2insert.o nbt2page.o nbtree2.o nbt2search.o \
       nbt2utils.o nbt2sort.o nbt2validate.o nbt2xlog.o

include $(top_srcdir)/src/backend/common.mk
