# contrib/test_kmgr/Makefile

MODULES = test_kmgr
PGFILEDESC = "test_kmgr - example of a key management provider plugin"

OBJS = test_kgmr.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_kmgr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
