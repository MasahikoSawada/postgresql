#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_page_recover
#
# Portions Copyright (c) 2019, PostgreSQL Global Development Group
#
# src/bin/pg_page_recover/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_page_recover - page recovery"
PGAPPICON = win32

subdir = src/bin/pg_page_recover
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

OBJS	= pg_page_recover.o xlogreader.o heappageredo.o pageutils.o \
	xlogutils.o $(WIN32RES)

EXTRA_CLEAN = xlogreader.c

all: pg_page_recover

pg_page_recover: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_page_recover$(X) '$(DESTDIR)$(bindir)/pg_page_recover$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_page_recover$(X)'

clean distclean maintainer-clean:
	rm -f pg_page_recover$(X) $(OBJS) xlogreader.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
