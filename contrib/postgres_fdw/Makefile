# contrib/postgres_fdw/Makefile

MODULE_big = postgres_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	deparse.o \
	option.o \
	postgres_fdw.o \
	shippable.o
PGFILEDESC = "postgres_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = postgres_fdw
DATA = postgres_fdw--1.0.sql postgres_fdw--1.0--1.1.sql postgres_fdw--1.1--1.2.sql

REGRESS = postgres_fdw query_cancel
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/postgres_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

# We have the sole isolation test case that requires injection points,
# so invoke the isolation test if injection points are enabled.
ifeq ($(enable_injection_points),yes)
EXTRA_INSTALL=src/test/modules/injection_points
ISOLATION = foreign_recheck
endif

include $(top_srcdir)/contrib/contrib-global.mk
endif
