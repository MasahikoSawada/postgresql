# contrib/pg_visibilitymap/Makefile

MODULE_big = pg_visibilitymap
OBJS = pg_visibilitymap.o $(WIN32RES)

EXTENSION = pg_visibilitymap
DATA = pg_visibilitymap--1.0.sql
PGFILEDESC = "pg_visibilitymap - monitoring of visibility map"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_visibilitymap
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
