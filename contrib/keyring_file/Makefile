# contrib/test_keyring/Makefile

MODULES = keyring_file
PGFILEDESC = "keyring_file - example of a key ring plugin"

OBJS = kerying_file.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/keyring_file
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
