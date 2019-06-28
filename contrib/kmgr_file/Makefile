# contrib/kmgr_file/Makefile

MODULES = kmgr_file
PGFILEDESC = "kmgr_file - example of a kmgr plugin"

OBJS = kmgr_file.o

PREGRESS = kmgr_test
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/kmgr_file
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
