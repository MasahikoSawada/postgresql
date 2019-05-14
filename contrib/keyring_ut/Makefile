# contrib/keyring_ut/Makefile

MODULE_big = keyring_ut
DATA = keyring_ut--1.0.sql
OBJS = kr.o

EXTENSION = keyring_ut

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/keyring_ut
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
