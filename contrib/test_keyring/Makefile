# contrib/test_keyring/Makefile

MODULES = test_keyring
PGFILEDESC = "test_keyring - example of a key ring plugin"

OBJS = test_kgmr.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_keyring
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
