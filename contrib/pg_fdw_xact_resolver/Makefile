# contrib/pg_fdw_xact_resolver/Makefile

MODULES = pg_fdw_xact_resolver
PGFILEDESC = "pg_fdw_xact_resolver - foreign transaction resolver demon"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_fdw_xact_resolver
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
