# src/bin/pg_keysetup/Makefile

PGFILEDESC = "pg_keysetup - generate encryption key from a password"
PGAPPICON=win32

subdir = src/bin/pg_keysetup
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = pg_keysetup.o $(RMGRDESCOBJS) encryption.o $(WIN32RES)

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

all: pg_keysetup

pg_keysetup: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

encryption.c: % : $(top_srcdir)/src/backend/storage/smgr/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_keysetup$(X) '$(DESTDIR)$(bindir)/pg_keysetup$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_keysetup$(X)'

clean distclean maintainer-clean:
	rm -f pg_keysetup$(X) $(OBJS) encryption.c
