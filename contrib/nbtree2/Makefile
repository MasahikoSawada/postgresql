#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for btree2
#
#-------------------------------------------------------------------------

MODULE_big = nbtree2

OBJS = nbt2compare.o nbt2insert.o nbt2page.o nbtree2.o nbt2search.o \
       nbt2utils.o nbt2sort.o nbt2validate.o nbt2xlog.o
EXTENSION = nbtree2
DATA = nbtree2--1.0.sql
PGFILEDESC = "Btree with abbreviated key"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
