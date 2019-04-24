# contrib/test_keyring/Makefile

MODULE_big = test_keyring
DATA = test_keyring--1.0.sql
OBJS = kr.o

EXTENSION = test_keyring

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
