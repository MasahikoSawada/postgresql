# Copyright (c) 2022, PostgreSQL Global Development Group

# Test for wraparound emergency situation

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 8;
use IPC::Run qw(pump finish timer);

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');

$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', qq/
autovacuum = off # run autovacuum only when to anti wraparound
max_prepared_transactions=10
autovacuum_naptime = 1s
# So it's easier to verify the order of operations
autovacuum_max_workers=1
autovacuum_vacuum_cost_delay=0
log_autovacuum_min_duration=0
/);
$node_primary->start;

#
# Create tables for a few different test scenarios
#

$node_primary->safe_psql('postgres', qq/
CREATE TABLE large(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO large(data) SELECT generate_series(1,30000);

CREATE TABLE large_trunc(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO large_trunc(data) SELECT generate_series(1,30000);

CREATE TABLE small(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO small(data) SELECT generate_series(1,15000);

CREATE TABLE small_trunc(id serial primary key, data text, filler text default repeat(random()::text, 10));
INSERT INTO small_trunc(data) SELECT generate_series(1,15000);

CREATE TABLE autovacuum_disabled(id serial primary key, data text) WITH (autovacuum_enabled=false);
INSERT INTO autovacuum_disabled(data) SELECT generate_series(1,1000);
/);

# Delete a few rows to ensure that vacuum has work to do.
$node_primary->safe_psql('postgres', qq/
DELETE FROM large WHERE id % 2 = 0;
DELETE FROM large_trunc WHERE id > 10000;
DELETE FROM small WHERE id % 2 = 0;
DELETE FROM small_trunc WHERE id > 1000;
DELETE FROM autovacuum_disabled WHERE id % 2 = 0;
/);


# Stop the server and temporarily disable log_statement while running in single-user mode
$node_primary->stop;
$node_primary->append_conf('postgresql.conf', qq/
log_statement = 'none'
/);

# Need to reset to a clog page boundary, otherwise we'll get errors
# about the file not existing. With default compilation settings
# CLOG_XACTS_PER_PAGE is 32768. The value below is 32768 *
# (2000000000/32768 + 1), with 2000000000 being the max value for
# autovacuum_freeze_max_age.

command_like([ 'pg_resetwal', '-x2000027648', $node_primary->data_dir ],
	     qr/Write-ahead log reset/, 'pg_resetwal -x to');

my $in  = '';
my $out = '';
my $timer = timer(5);

# Start the server in single-user mode.  That allows us to test interactions
# without autovacuums.
my $h = $node_primary->start_single_user_mode('postgres', \$in, \$out, $timer);

$out = "";
# Must be a single line with a new line at the end.
$in .=
    "SELECT datname, " .
    "age(datfrozenxid) > current_setting('autovacuum_freeze_max_age')::int as old ".
    "FROM pg_database ORDER BY 1;\n";

# Pump until we got the result.
pump $h until ($out != "" || $timer->is_expired);

# Check all database are old enough.
like($out, qr/1: datname = "postgres"[^\r\n]+\r\n\t 2: old = "t"/,
     "postgres database is old enough");
like($out, qr/1: datname = "template0"[^\r\n]+\r\n\t 2: old = "t"/,
     "template0 database is old enough");
like($out, qr/1: datname = "template1"[^\r\n]+\r\n\t 2: old = "t"/,
     "template1 database is old enough");

# Terminate single user mode.
$in .= "\cD";
finish $h or die "postgres --single returned $?";

# Revert back the logging setting.
$node_primary->append_conf('postgresql.conf', qq/
log_statement = 'all'
/);

# Now test autovacuum behaviour.
$node_primary->start;

ok($node_primary->poll_query_until('postgres', qq/
    SELECT NOT EXISTS (
        SELECT *
        FROM pg_database
        WHERE age(datfrozenxid) > current_setting('autovacuum_freeze_max_age')::int)
/),
   "xid horizon increased");

my $ret = $node_primary->safe_psql('postgres', qq/
SELECT relname, age(relfrozenxid) > current_setting('autovacuum_freeze_max_age')::int
FROM pg_class
WHERE oid = ANY(ARRAY['large'::regclass, 'large_trunc', 'small', 'small_trunc', 'autovacuum_disabled'])
ORDER BY 1
/);
is($ret, "autovacuum_disabled|f
large|f
large_trunc|f
small|f
small_trunc|f", "all tables are vacuumed");

$node_primary->stop;
