# Tests for transaction involving foreign servers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

# Setup master node
my $node_master = get_new_node("master");
my $node_standby = get_new_node("standby");

$node_master->init(allows_streaming => 1);
$node_master->append_conf('postgresql.conf', qq(
max_prepared_foreign_transactions = 10
max_prepared_transactions = 10
));
$node_master->start;

# Take backup from master node
my $backup_name = 'master_backup';
$node_master->backup($backup_name);

# Set up standby node
$node_standby->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby->start;

# Set up foreign nodes
my $node_fs1 = get_new_node("fs1");
my $node_fs2 = get_new_node("fs2");
my $fs1_port = $node_fs1->port;
my $fs2_port = $node_fs2->port;
$node_fs1->init;
$node_fs2->init;
$node_fs1->append_conf('postgresql.conf', "max_prepared_transactions = 10");
$node_fs2->append_conf('postgresql.conf', "max_prepared_transactions = 10");
$node_fs1->start;
$node_fs2->start;

# Create foreign server
$node_master->safe_psql('postgres', "CREATE EXTENSION postgres_fdw");
$node_master->safe_psql('postgres', "
CREATE SERVER fs1 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname 'postgres', port '$fs1_port', two_phase_commit 'on');
");
$node_master->safe_psql('postgres', "
CREATE SERVER fs2 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname 'postgres', port '$fs2_port', two_phase_commit 'on');
");

# Create user mapping
$node_master->safe_psql('postgres', "
CREATE USER MAPPING FOR CURRENT_USER SERVER fs1;
CREATE USER MAPPING FOR CURRENT_USER SERVER fs2;
");

# Ceate table on foreign server and import them.
$node_fs1->safe_psql('postgres', "
CREATE SCHEMA fs;
CREATE TABLE fs.t1 AS SELECT generate_series(1,10) AS c;
");
$node_fs2->safe_psql('postgres', "
CREATE SCHEMA fs;
CREATE TABLE fs.t2 AS SELECT generate_series(1,10) AS c;
");
$node_master->safe_psql('postgres', "
IMPORT FOREIGN SCHEMA fs FROM SERVER fs1 INTO public;
IMPORT FOREIGN SCHEMA fs FROM SERVER fs2 INTO public;
CREATE TABLE local_table (c int);
INSERT INTO local_table SELECT generate_series(1,10);
");

# Switch to synchronous replication
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '*'");
$node_master->reload;

my $result;

#
# Prepare two transaction involving foreign servers.
# Check if we can commit and rollback transaction involving foreign servers after recovery.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE t1 SET c = 1 WHERE c = 1;
UPDATE t2 SET c = 2 WHERE c = 2;
PREPARE TRANSACTION 'gxid1';
BEGIN;
UPDATE t1 SET c = 3 WHERE c = 3;
UPDATE t2 SET c = 4 WHERE c = 4;
PREPARE TRANSACTION 'gxid2';
");

$node_master->stop;
$node_master->start;

$result = $node_master->psql('postgres', "COMMIT PREPARED 'gxid1'");
is($result, 0, 'Commit foreigin transaction after recovery');
$result = $node_master->psql('postgres', "ROLLBACK PREPARED 'gxid2'");
is($result, 0, 'Rollback foreigin transaction after recovery');

#
# Prepare two transaction involving foreign servers and shutdown master node immediately.
# Check if we can commit and rollback transaction involving foreign servers after crash recovery.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE t1 SET c = 5 WHERE c = 5;
UPDATE t2 SET c = 6 WHERE c = 6;
PREPARE TRANSACTION 'gxid1';
BEGIN;
UPDATE t1 SET c = 7 WHERE c = 7;
UPDATE t2 SET c = 8 WHERE c = 8;
PREPARE TRANSACTION 'gxid2';
");

$node_master->teardown_node;
$node_master->start;

$result = $node_master->psql('postgres', "COMMIT PREPARED 'gxid1'");
is($result, 0, 'Commit foreigin transaction after crash recovery');
$result = $node_master->psql('postgres', "ROLLBACK PREPARED 'gxid2'");
is($result, 0, 'Rollback foreigin transaction after crash recovery');

#
# Commit transactions involving foreign servers and shutdown master node immediately.
# In this case, information about insertion and deletion of fdw_xact exists at only WAL.
# Check if fdw_xact entry can be processed properly during recovery.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE t1 SET c = 1 WHERE c = 1;
UPDATE t2 SET c = 2 WHERE c = 2;
COMMIT;
");

$node_master->teardown_node;
$node_master->start;

$result = $node_master->safe_psql('postgres', 'SELECT count(*) FROM pg_fdw_xacts');
is($result, 0, "Remove fdw_xact entry during recovery");

#
# A foreign server down after prepared foregin transaction but before commit it.
# Check dangling transaction can be processed propelry by pg_fdw_xact() function.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE t1 SET c = 1 WHERE c = 1;
UPDATE t2 SET c = 2 WHERE c = 2;
PREPARE TRANSACTION 'gxid1';
");

$node_fs1->stop;

# Since node_fs1 down COMMIT PREPARED will fail on node_fs1.
$node_master->psql('postgres', "COMMIT PREPARED 'gxid1'");

$node_fs1->start;
$result = $node_master->safe_psql('postgres', "SELECT count(*) FROM pg_fdw_xact_resolve() WHERE status = 'resolved'");
is($result, 1, "pg_fdw_xact_resolve function");

#
# Check if the standby node can process prepared foreign transaction after
# promotion of the standby server.
#
$node_master->safe_psql('postgres', "
BEGIN;
UPDATE t1 SET c = 5 WHERE c = 5;
UPDATE t2 SET c = 6 WHERE c = 6;
PREPARE TRANSACTION 'gxid1';
BEGIN;
UPDATE t1 SET c = 7 WHERE c = 7;
UPDATE t2 SET c = 8 WHERE c = 8;
PREPARE TRANSACTION 'gxid2';
");

$node_master->teardown_node;
$node_standby->promote;

$result = $node_standby->psql('postgres', "COMMIT PREPARED 'gxid1'");
is($result, 0, 'Commit foreigin transaction after promotion');
$result = $node_standby->psql('postgres', "ROLLBACK PREPARED 'gxid2'");
is($result, 0, 'Rollback foreigin transaction after promotion');
$result = $node_standby->safe_psql('postgres', "SELECT count(*) FROM pg_fdw_xacts");
is($result, 0, "Check fdw_xact entry on new master node");
