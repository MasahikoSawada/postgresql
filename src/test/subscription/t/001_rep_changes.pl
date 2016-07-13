# Basic logical replication test
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

# Initialize provider node
my $node_provider = get_new_node('provider');
$node_provider->init(allows_streaming => 'logical');
$node_provider->start;

# Create subscriber node
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Create some preexisting content on provider
$node_provider->safe_psql('postgres',
	"CREATE TABLE tab_notrep AS SELECT generate_series(1,10) AS a");
$node_provider->safe_psql('postgres',
	"CREATE TABLE tab_ins (a int)");
$node_provider->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)");

# Setup structure on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_notrep (a int)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_ins (a int)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_rep (a int primary key)");

# Setup logical replication
my $provider_connstr = $node_provider->connstr . ' dbname=postgres';
$node_provider->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub");
$node_provider->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_ins_only WITH noreplicate_delete noreplicate_update");
$node_provider->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub ADD TABLE tab_rep");
$node_provider->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_ins");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub WITH CONNECTION '$provider_connstr' PUBLICATION tap_pub, tap_pub_ins_only");

# Wait for subscriber to finish table sync
my $appname = 'tap_sub';
my $caughtup_query =
"SELECT pg_current_xlog_location() <= write_location FROM pg_stat_replication WHERE application_name = '$appname';";
$node_provider->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for subscriber to catch up";

my $result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM tab_notrep");
print "node_subscriber: $result\n";
is($result, qq(0), 'check non-replicated table is empty on subscriber');


$node_provider->safe_psql('postgres',
	"INSERT INTO tab_ins SELECT generate_series(1,50)");
$node_provider->safe_psql('postgres',
	"DELETE FROM tab_ins WHERE a > 20");
$node_provider->safe_psql('postgres',
	"UPDATE tab_ins SET a = -a");

$node_provider->safe_psql('postgres',
	"INSERT INTO tab_rep SELECT generate_series(1,50)");
$node_provider->safe_psql('postgres',
	"DELETE FROM tab_rep WHERE a > 20");
$node_provider->safe_psql('postgres',
	"UPDATE tab_rep SET a = -a");

$node_provider->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for subscriber to catch up";

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*), min(a), max(a) FROM tab_ins");
print "node_subscriber: $result\n";
is($result, qq(50|1|50), 'check replicated inserts on subscriber');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*), min(a), max(a) FROM tab_rep");
print "node_subscriber: $result\n";
is($result, qq(20|-20|-1), 'check replicated changes on subscriber');

# check that change of connection string and/or publication list causes
# restart of subscription workers. Not all of these are registered as tests
# as we need to poll for a change but the test suite will fail none the less
# when something goes wrong.
my $oldpid = $node_provider->safe_psql('postgres',
	"SELECT pid FROM pg_stat_replication WHERE application_name = '$appname';");
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub WITH CONNECTION '$provider_connstr '");
$node_provider->poll_query_until('postgres',
	"SELECT pid != $oldpid FROM pg_stat_replication WHERE application_name = '$appname';")
  or die "Timed out while waiting for apply to restart";

$oldpid = $node_provider->safe_psql('postgres',
	"SELECT pid FROM pg_stat_replication WHERE application_name = '$appname';");
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub WITH PUBLICATION tap_pub_ins_only");
$node_provider->poll_query_until('postgres',
	"SELECT pid != $oldpid FROM pg_stat_replication WHERE application_name = '$appname';")
  or die "Timed out while waiting for apply to restart";

$node_provider->safe_psql('postgres',
	"INSERT INTO tab_ins SELECT generate_series(1001,1100)");
$node_provider->safe_psql('postgres',
	"DELETE FROM tab_rep");

$node_provider->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for subscriber to catch up";

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*), min(a), max(a) FROM tab_ins");
print "node_subscriber: $result\n";
is($result, qq(1152|1|1100), 'check replicated inserts after subscription publication change');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*), min(a), max(a) FROM tab_rep");
print "node_subscriber: $result\n";
is($result, qq(20|-20|-1), 'check changes skipped after subscription publication change');

$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub");

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM pg_subscription");
print "node_subscriber $result\n";
is($result, qq(0), 'check subscription was dropped on subscriber');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM pg_subscription_rel");
print "node_subscriber $result\n";
is($result, qq(0), 'check subscription relation status was dropped on subscriber');

$result =
  $node_provider->safe_psql('postgres', "SELECT count(*) FROM pg_replication_slots");
print "node_provider $result\n";
is($result, qq(0), 'check replication slot was dropped on provider');

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM pg_replication_origin");
print "node_subscriber $result\n";
is($result, qq(0), 'check replication origin was dropped on subscriber');

$node_subscriber->stop('fast');
$node_provider->stop('fast');
