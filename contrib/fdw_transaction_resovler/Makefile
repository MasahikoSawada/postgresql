# contrib/fdw_transaction_resolver/Makefile

MODULES = fdw_transaction_resolver
PGFILEDESC = "fdw_transaction_resolver - foreign transaction resolver demon"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/fdw_transaction_resolver
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
