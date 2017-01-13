# Tests for transaction involving foreign servers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

sub test_fdw_xact
{
	my ($self, $standby, $sql, $gxid, $expected, $msg) = @_;
	my $result;

	$self->safe_psql('postgres', $sql . "PREPARE TRANSACTION '$gxid'");

	# test on master node
	$result = $self->safe_psql('postgres', "SELECT count(*) FROM pg_fdw_xacts WHERE identifier = '$gxid'");
	is($result, $expected, $msg);

	# test on standby node
	$result = $standby->safe_psql('postgres', "SELECT count(*) FROM pg_fdw_xacts");
	is($result, $expected, $msg);
}

# Setup master node
my $node_master = get_new_node("maseter");
my $node_standby = get_new_node("standby");

my $port_coord = $node_master->port();

$node_master->init(allows_streaming => 1);
$node_master->append_conf('postgresql.conf', qq(
	max_prepared_foreign_transactions = 10
	max_prepared_transactions = 10
));
$node_master->start;

# Take backup
my $backup_name = 'master_backup';
$node_master->backup($backup_name);

# Set up standby node
$node_standby->init_from_backup($node_master, $backup_name);
$node_standby->start();

# Create foreign server
$node_master->safe_psql('postgres',
					   qq[CREATE EXTENSION postgres_fdw]);
$node_master->safe_psql('postgres', "
CREATE SERVER loopback1 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'postgres', port '$port_coord', two_phase_commit 'off');
CREATE SERVER loopback2 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'postgres', port '$port_coord', two_phase_commit 'on');
CREATE SERVER loopback3 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'postgres', port '$port_coord', two_phase_commit 'on');
");

# Create user mapping
$node_master->safe_psql('postgres', "
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback1;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback2;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback3;
");

# Ceate table
$node_master->safe_psql('postgres', "
CREATE SCHEMA sc1;
CREATE TABLE sc1.t1 (c int);
INSERT INTO sc1.t1 SELECT generate_series(1,10);
");

# Create foreign table
$node_master->safe_psql('postgres', "
CREATE FOREIGN TABLE ft1 (c int) SERVER loopback1 OPTIONS (schema_name 'sc1', table_name 't1');
CREATE FOREIGN TABLE ft2 (c int) SERVER loopback2 OPTIONS (schema_name 'sc1', table_name 't1');
CREATE FOREIGN TABLE ft3 (c int) SERVER loopback3 OPTIONS (schema_name 'sc1', table_name 't1');
");

my $result;

#
# Prepare two transaction involving foreign servers.
# Check if we can commit and rollback  transaction involving foreign servers after recovery.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE ft2 SET c = 1 WHERE c = 1;
UPDATE ft3 SET c = 2 WHERE c = 2;
PREPARE TRANSACTION 'gxid1';
BEGIN;
UPDATE ft2 SET c = 3 WHERE c = 3;
UPDATE ft3 SET c = 4 WHERE c = 4;
PREPARE TRANSACTION 'gxid2';
");

$node_master->stop;
$node_master->start;

$result = $node_master->psql('postgres', "COMMIT PREPARED 'gxid1'");
is($result, 0, 'Commit foreigin transaction after recovery');
$result = $node_master->psql('postgres', "ROLLBACK PREPARED 'gxid2'");
is($result, 0, 'Rollback foreigin transaction after recovery');

#
# Prepare two transaction involving foreign servers and immediately shutdown.
# Check if we can commit and rollback transaction involving foreign servers after crash recovery.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE ft2 SET c = 5 WHERE c = 5;
UPDATE ft3 SET c = 6 WHERE c = 6;
PREPARE TRANSACTION 'gxid1';
BEGIN;
UPDATE ft2 SET c = 7 WHERE c = 7;
UPDATE ft3 SET c = 8 WHERE c = 8;
PREPARE TRANSACTION 'gxid2';
");

$node_master->teardown_node;
$node_master->start;

$result = $node_master->psql('postgres', "COMMIT PREPARED 'gxid1'");
is($result, 0, 'Commit foreigin transaction after crash recovery');
$result = $node_master->psql('postgres', "ROLLBACK PREPARED 'gxid2'");
is($result, 0, 'Rollback foreigin transaction after crash recovery');

#
# Commit and rollback transactions involving foreign servers and immediately shutdown. In this
# case, information about insertion and deletion of fdw_xact exists only WAL.
# Check if the recovery can reply and resolve foreign transaciton.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE ft2 SET c = 1 WHERE c = 1;
UPDATE ft3 SET c = 2 WHERE c = 2;
COMMIT;
BEGIN;
UPDATE ft2 SET c = 3 WHERE c = 3;
UPDATE ft3 SET c = 4 WHERE c = 4;
ROLLBACK;
");

$node_master->teardown_node;
$node_master->start;

$result = $node_master->psql('postgres', 'SELECT count(*) FROM pg_fdw_xacts');
is($result, 0, "Commit and rollback during recovery");
