
# Copyright (c) 2024-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(
    allows_streaming => 1
    );
$primary->start();

# Check both wal_level and effective_wal_level values.
is( $primary->safe_psql('postgres', qq[
select current_setting('wal_level'), current_setting('effective_wal_level');
			]),
    "replica|replica",
    "wal_level and effective_wal_level starts with the same value 'replica'");

$primary->safe_psql('postgres',
		    qq[select pg_create_physical_replication_slot('test_phy_slot', false, false)]);
is( $primary->safe_psql('postgres', qq[show effective_wal_level]),
    "replica",
    "effective_wal_level doesn't change with a new physical slot");

# Create a new logical slot
$primary->safe_psql('postgres',
		    qq[select pg_create_logical_replication_slot('test_slot', 'pgoutput')]);

# effective_wal_level must be bumped to 'logical'
is( $primary->safe_psql('postgres', qq[
select current_setting('wal_level'), current_setting('effective_wal_level');
			]),
    "replica|logical",
    "effective_wal_level bumped to logical upon logical slot creation");

# restart the server and check again.
$primary->restart();
is( $primary->safe_psql('postgres', qq[
select current_setting('wal_level'), current_setting('effective_wal_level');
			]),
    "replica|logical",
    "effective_wal_level becomes logical during startup");

# Take backup during the effective_wal_level being 'logical'.
$primary->backup('my_backup');

# Initialize standby1 node from the backup 'my_backup'. Note that the
# backup was taken during the logical decoding begin enabled on the
# primary because of one logical slot, but replication slots are not
# included in the basebackup.
my $standby1 = PostgreSQL::Test::Cluster->new('standby1');
$standby1->init_from_backup($primary, 'my_backup',
			    has_streaming => 1);
$standby1->set_standby_mode();
$standby1->start;
is( $standby1->safe_psql('postgres', qq[
select current_setting('wal_level'), current_setting('effective_wal_level');
			]),
    "replica|logical",
    "effective_wal_level='logical' on standby");

# Promote the standby1 node that doesn't have any logical slot. So
# the logical decoding must be disabled at promotion.
$standby1->promote;
is( $standby1->safe_psql('postgres', qq[
select current_setting('wal_level'), current_setting('effective_wal_level');
			]),
    "replica|replica",
    "effective_wal_level got decrased to 'replica' during promotion");
$standby1->stop;

my $standby2 = PostgreSQL::Test::Cluster->new('standby2');
$standby2->init_from_backup($primary, 'my_backup',
			   has_streaming => 1);
$standby2->set_standby_mode();
$standby2->start;

# Create a logical slot on the standby.
$standby2->create_logical_slot_on_standby($primary, 'standby2_slot', 'postgres');

# Promote the standby2 node that has one logical slot. So the logical decoding
# keeps enabled even after the promotion.
$standby2->promote;
is( $standby2->safe_psql('postgres', qq[
select current_setting('wal_level'), current_setting('effective_wal_level');
			]),
    "replica|logical",
    "effective_wal_level keeps 'logical' even after the promotion");
$standby2->safe_psql('postgres',
		     qq[select pg_create_logical_replication_slot('standby2_slot2', 'pgoutput')]);
$standby2->stop;

$primary->stop;
done_testing();
