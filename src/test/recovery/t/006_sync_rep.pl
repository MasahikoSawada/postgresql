# Minimal test testing synchronous replication sync_state transition
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 7;


# Initialize master node with synchronous_standby_names = 'standby1,standby2'
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1,allows_sync_rep => 1);
$node_master->start;
my $backup_name = 'my_backup';

my $check_sql = "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication ORDER BY application_name;";

# Take backup
$node_master->backup($backup_name);

# Create standby1 linking to master
my $node_standby_1 = get_new_node('standby1');
$node_standby_1->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby_1->start;


# Create standby2 linking to master
my $node_standby_2 = get_new_node('standby2');
$node_standby_2->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby_2->start;

# Create standby3 linking to master
my $node_standby_3 = get_new_node('standby3');
$node_standby_3->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby_3->start;

# Create standby4
my $node_standby_4 = get_new_node('standby4');
$node_standby_4->init_from_backup($node_master, $backup_name, has_streaming => 1);

# Check application sync_state on master initially
my $result = $node_master->safe_psql('postgres', $check_sql);
print "$result \n";
is($result, "standby1|1|sync\nstandby2|2|potential\nstandby3|0|async", 'checked for synchornous standbys state for backward compatibility');

# Change the synchronou_standby_names = '*' and check sync_state.
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '*';");
$node_master->reload;

# Only Standby1 should be 'sync'.
$result = $node_master->safe_psql('postgres', $check_sql);
print "$result \n";
is($result, "standby1|1|sync\nstandby2|1|potential\nstandby3|1|potential", 'checked for synchronous standbys state for backward compatibility with asterisk');

# Stop all standbys
$node_standby_1->stop;
$node_standby_2->stop;
$node_standby_3->stop;

# Change the synchronous_standby_names = '2[standby1,standby2,standby3]' and check sync_state.
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '2[standby1,standby2,standby3]';");
$node_master->reload;

$node_standby_2->start;
$node_standby_3->start;

# Standby2 and standby3 should be 'sync'.
$result = $node_master->safe_psql('postgres', $check_sql);
print "$result \n";
is($result, "standby2|2|sync\nstandby3|3|sync", 'checked for synchronous standbys state transition 1');

$node_standby_1->start;
$node_standby_4->start;

# Standby1 should be 'sync' instead of standby3, and standby3 should turn to 'potential'.
# Standby4 should be added as 'async'.
$result = $node_master->safe_psql('postgres', $check_sql);
print "$result \n";
is($result, "standby1|1|sync\nstandby2|2|sync\nstandby3|3|potential\nstandby4|0|async", 'checked for synchronous standbys state transition 2');

# Change the synchronous_standby_names = '2[standby1,*,standby2]' and check sync_state
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '2[standby1,*,standby2]';");
$node_master->reload;

# Standby1 and standby2 should be 'sync', and sync_priority of standby2 should be 2, not 3.
$result = $node_master->safe_psql('postgres', $check_sql);
print "$result \n";
is($result, "standby1|1|sync\nstandby2|2|sync\nstandby3|2|potential\nstandby4|2|potential", 'checked for synchronous standbys state with asterisk 1');

# Change the synchronous_standby_names = '2[*]' and check sync state
$node_master->psql('postgres', "ALTER SYSTEM SET synchronous_standby_names = '2[*]';");
$node_master->reload;

# Since standby2 and standby3 have more higher index number of WalSnd array, these standbys should be 'sync' instead of standby1.
$result = $node_master->safe_psql('postgres', $check_sql);
print "$result \n";
is($result, "standby1|1|potential\nstandby2|1|sync\nstandby3|1|sync\nstandby4|1|potential", 'checked for synchronous standbys state with asterisk 2');

# Stop Standby3 which is considered as 'sync.
$node_standby_3->stop;

# Standby1 become 'sync'
$result = $node_master->safe_psql('postgres', $check_sql);
print "$result \n";
is($result, "standby1|1|sync\nstandby2|1|sync\nstandby4|1|potential", 'checked for synchronous standbys state with asterisk 3');
