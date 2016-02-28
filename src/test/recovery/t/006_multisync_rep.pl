use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 8;


# Initialize master node with synchronous_standby_names = 'standby1,standby2'
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1,allows_sync_rep => 1);
$node_master->start;
my $backup_name = 'my_backup';

# Take backup
$node_master->backup($backup_name);

# Create standby1 linking to master
my $node_standby_1 = get_new_node('standby1');
$node_standby_1->init_from_backup($node_master, $backup_name,
								  has_streaming => 1);
$node_standby_1->start;


# Create standby2 linking to master
my $node_standby_2 = get_new_node('standby2');
$node_standby_2->init_from_backup($node_master, $backup_name,
								  has_streaming => 1);
$node_standby_2->start;

# Create standby3 linking to master
my $node_standby_3 = get_new_node('standby3');
$node_standby_3->init_from_backup($node_master, $backup_name,
								  has_streaming => 1);
$node_standby_3->start;

# Create standby4 linking to master
my $node_standby_4 = get_new_node('standby4');
$node_standby_4->init_from_backup($node_master, $backup_name,
								  has_streaming => 1);

# Check application sync_state on master initially
my $result = $node_master->psql('postgres', "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication;");
print "$result \n";
is($result, "standby1|1|sync\nstandby2|2|potential\nstandby3|0|async", 'checked for standbys state for backward compatibility');


# Change the s_s_names = '*' and check sync state
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '*';");
$node_master->psql('postgres', "SELECT pg_reload_conf();");

$result = $node_master->psql('postgres', "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication;");
print "$result \n";
is($result, "standby1|1|sync\nstandby2|1|potential\nstandby3|1|potential", 'checked for standbys state for backward compatibility with asterisk');

# Stop all standbys
$node_standby_1->stop;
$node_standby_2->stop;
$node_standby_3->stop;

# Change the s_s_names = '2[standby1,standby2,standby3]' and check sync state
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '2[standby1,standby2,standby3]';");
$node_master->psql('postgres', "SELECT pg_reload_conf();");

# Standby2 and standby3 should be 'sync'
$node_standby_2->start;
$node_standby_3->start;
$result = $node_master->psql('postgres', "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication;");
print "$result \n";
is($result, "standby2|2|sync\nstandby3|3|sync", 'checked for sync standbys state transition 1');

# Standby1 should be 'sync' instead of standby3, and standby3 should turn to 'potential'
$node_standby_1->start;
$node_standby_4->start;
$result = $node_master->psql('postgres', "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication;");
print "$result \n";
is($result, "standby2|2|sync\nstandby3|3|potential\nstandby1|1|sync\nstandby4|0|async", 'checked for sync standbys state transition 2');

# Change the s_s_names = '2[standby1,*,standby2]' and check sync state
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '2[standby1,*,standby2]';");
$node_master->psql('postgres', "SELECT pg_reload_conf();");

$result = $node_master->psql('postgres', "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication;");
print "$result \n";
is($result, "standby2|2|sync\nstandby3|2|potential\nstandby1|1|sync\nstandby4|2|potential", 'checked for sync standbys state with asterisk 1');

$node_standby_4->stop;

# Change the s_s_names = '2[*]' and check sync state
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '2[*]';");
$node_master->psql('postgres', "SELECT pg_reload_conf();");

$result = $node_master->psql('postgres', "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication;");
print "$result \n";
is($result, "standby2|1|sync\nstandby3|1|sync\nstandby1|1|potential", 'checked for sync standbys state with asterisk 2');

# Create some content on master and check its presence on standby 1 and standby 2
$node_master->psql('postgres', "CREATE TABLE tab_int AS SELECT generate_series(1,1002) AS a");

$result =  $node_standby_1->psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 1: $result\n";
is($result, qq(1002), 'check synced content on standby 1');

$result =  $node_standby_1->psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 2: $result\n";
is($result, qq(1002), 'check synced content on standby 2');
