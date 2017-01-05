# Tests for transaction involving foreign servers
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More test => 10;

# Setup coordinator node
my $node_coord = get_new_node("corrdinator");
$node_coord->init();
$node_coord->append_conf('postgresql.conf', qq(
	max_prepard_foreign_transactions = 10
));

$node_coord->start;

# Setup shard servers
my $node_shard1 = get_new_node("shard1");
$node_shard1->append_conf('postgrseql.conf', qq(
	max_prepard_transactions = 10
));

my $node_shard2 = get_new_node("shard2");
$node_shard2->append_conf('postgrseql.conf', qq(
	max_prepard_transactions = 10
));

my $node_shard3 = get_new_node("shard3");
$node_shard3->append_conf('postgrseql.conf', qq(
	max_prepard_transactions = 10
));

$node_shard1->start;
$node_shard2->start;
$node_shard3->start;
