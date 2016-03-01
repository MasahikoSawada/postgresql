#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/replication
#
# IDENTIFICATION
#    src/backend/replication/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/replication
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = walsender.o walreceiverfuncs.o walreceiver.o basebackup.o \
	syncgroup_gram.o repl_gram.o slot.o \
	slotfuncs.o syncrep.o

SUBDIRS = logical

include $(top_srcdir)/src/backend/common.mk

# repl_scanner is compiled as part of repl_gram
repl_gram.o: repl_scanner.c
# syncgroup_scanner is complied as part of syncgroup_gram
syncgroup_gram.o: syncgroup_scanner.c syncgroup_gram.c
syncgroup_gram.c: BISONFLAGS += -d

syncgroup_scanner.c: FLEXFLAGS = -CF -p
syncgroup_scanner.c: FLEX_NO_BACKUP=yes

syncgroup_gram.o : syncgroup_gram.h

# repl_gram.c and repl_scanner.c are in the distribution tarball, so
# they are not cleaned here.
# syncgroup_gram.c and syncgroup_scanner.c are in the distribution tarball, so
# they are not cleaned here.
