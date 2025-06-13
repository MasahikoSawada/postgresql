
# Copyright (c) 2025, PostgreSQL Global Development Group

# Tests for parallel heap vacuum.

use strict;
use warnings FATAL => 'all';
use locale;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test persistency of statistics generated for injection points.
if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init;
$node->start;
$node->safe_psql('postgres', qq[create extension injection_points;]);

$node->safe_psql('postgres', qq[
create table t (i int) with (autovacuum_enabled = off);
create index on t (i);
		 ]);
my $nrows = 1_000_000;
my $first = int($nrows * rand());
my $second = $nrows - $first;

my $psql = $node->background_psql('postgres', on_error_stop => 0);

# Begin the transaciton that holds xmin.
$psql->query_safe('begin; select pg_current_xact_id();');

# consume some xids
$node->safe_psql('postgres', qq[
select pg_current_xact_id();
select pg_current_xact_id();
select pg_current_xact_id();
select pg_current_xact_id();
select pg_current_xact_id();
		 ]);

# While inserting $nrows tuples into the table with an older XID,
# we inject some tuples with a newer XID filling one page somewhere
# in the table.

# Insert the first part of rows.
$psql->query_safe(qq[insert into t select generate_series(1, $first);]);

# Insert some rows with a newer XID, which needs to fill at least
# one page to prevent the page from begin frozen in the following
# vacuum.
my $xid = $node->safe_psql('postgres', qq[
begin;
insert into t select 0 from generate_series(1, 300);
select pg_current_xact_id()::xid;
commit;
]);

# Insert remaining rows and commit.
$psql->query_safe(qq[insert into t select generate_series($first, $nrows);]);
$psql->query_safe(qq[commit;]);

# Delete some rows.
$node->safe_psql('postgres', qq[delete from t where i between 1 and 20000;]);

# Execute parallel vacuum that freezes all rows except for the
# tuple inserted by $psql. We should update the relfrozenxid up to
# that XID. Setting a lower value to maintenance_work_mem invokes
# multiple rounds of heap scanning and the number of parallel workers
# will ramp-down thanks to the injection points.
$node->safe_psql('postgres', qq[
set vacuum_freeze_min_age to 5;
set max_parallel_maintenance_workers TO 5;
set maintenance_work_mem TO 256;
select injection_points_set_local();
select injection_points_attach('parallel-vacuum-ramp-down-workers', 'notice');
select injection_points_attach('parallel-heap-vacuum-disable-leader-participation', 'notice');
vacuum (parallel 5, verbose) t;
		 ]);

is( $node->safe_psql('postgres', qq[select relfrozenxid from pg_class where relname = 't';]),
    "$xid", "relfrozenxid is updated as expected");

# Check if we have successfully frozen the table in the previous
# vacuum by scanning all tuples.
$node->safe_psql('postgres', qq[vacuum (freeze, parallel 0, verbose, disable_page_skipping) t;]);
is( $node->safe_psql('postgres', qq[select $xid < relfrozenxid::text::int from pg_class where relname = 't';]),
    "t", "all rows are frozen");

$node->stop;
done_testing();

