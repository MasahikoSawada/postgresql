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

# Take backup
$node_master->backup($backup_name);

#Create standby1 linking to master
my $node_standby_1 = get_new_node('standby1');
$node_standby_1->init_from_backup($node_master, $backup_name,
                                                                  has_streaming => 1);
$node_standby_1->start;


#Create standby2 linking to master
my $node_standby_2 = get_new_node('standby2');
$node_standby_2->init_from_backup($node_master, $backup_name,
                                                                  has_streaming => 1);
$node_standby_2->start;


# check application sync_state on master initially
my $result = $node_master->psql('postgres', "select application_name, sync_state from pg_stat_replication;");
print "$result \n";
is($result, "standby1|sync\nstandby2|potential", 'checked for standbys state for backward compatibility');


#change the s_s_names = '*' and check sync state
$node_master->psql('postgres', "alter system set synchronous_standby_names = '*';");
$node_master->psql('postgres', "select pg_reload_conf();");

$result = $node_master->psql('postgres', "select application_name, sync_state from pg_stat_replication;");
print "$result \n";
is($result, "standby1|sync\nstandby2|potential", 'checked for standbys state for backward compatibility');


#change the s_s_names = '2[standby1,standby2]' and check sync state
$node_master->psql('postgres', "alter system set synchronous_standby_names = '2[standby1,standby2]';");
$node_master->psql('postgres', "select pg_reload_conf();");

$result = $node_master->psql('postgres', "select application_name, sync_state from pg_stat_replication;");
print "$result \n";
is($result, "standby1|sync\nstandby2|sync", 'checked for sync standbys state');

# change the s_s_names = '2[standby1,standby2,*]' and check sync state
$node_master->psql('postgres', "alter system set synchronous_standby_names = '2[standby1,standby2,*]';");
$node_master->psql('postgres', "select pg_reload_conf();");

$result = $node_master->psql('postgres', "select application_name, sync_state from pg_stat_replication;");
print "$result \n";
is($result, "standby1|sync\nstandby2|sync", 'checked for sync standbys state');


# change the s_s_names = '2[*]' and check sync state

$node_master->psql('postgres', "alter system set synchronous_standby_names = '2[*]';");
$node_master->psql('postgres', "select pg_reload_conf();");

$result = $node_master->psql('postgres', "select application_name, sync_state from pg_stat_replication;");
print "$result \n";
is($result, "standby1|sync\nstandby2|sync", 'checked for sync standbys state');


#Create some content on master and check its presence on standby 1 and standby 2
$node_master->psql('postgres', "CREATE TABLE tab_int AS SELECT generate_series(1,1002) AS a");


$result =  $node_standby_1->psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 1: $result\n";
is($result, qq(1002), 'check synced content on standby 1');

$result =  $node_standby_1->psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 2: $result\n";
is($result, qq(1002), 'check synced content on standby 2');
