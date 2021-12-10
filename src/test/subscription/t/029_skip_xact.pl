
# Copyright (c) 2022, PostgreSQL Global Development Group

# Tests for skipping logical replication transactions
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 4;
use Time::HiRes qw(usleep);

my $offset = 0;

# Test skipping the transaction. This function must be called after the caller
# has inserted data that conflicts with the subscriber.  The commit-LSN of the
# error transaction that is used to specify to ALTER SUBSCRIPTION ... SKIP is
# fetched from the server logs. After executing ALTER SUBSCRITPION ... SKIP, we
# check if logical replication can continue working by inserting $nonconflict_data
# on the publisher.
sub test_skip_xact
{
	my ($node_publisher, $node_subscriber, $subname, $relname,
		$nonconflict_data, $expected, $msg)
	  = @_;

	# Wait until a conflict occurs on the subscriber.
	$node_subscriber->wait_for_log(
		qr/CONTEXT:  processing remote data during "INSERT" for replication target relation/,
		$offset);

	# Get the commit-LSN of the error transaction.
	my $contents = slurp_file($node_subscriber->logfile, $offset);
	$contents =~
	  qr/processing remote data during "INSERT" for replication target relation "public.$relname" in transaction \d+ committed at LSN ([[:xdigit:]]+\/[[:xdigit:]]+)/
	  or die "could not get error-LSN";
	my $lsn = $1;

	# Set skip lsn
	$node_subscriber->safe_psql('postgres',
		"ALTER SUBSCRIPTION $subname SKIP (lsn = '$lsn')");

	# Restart the subscriber node to restart logical replication with no interval
	$node_subscriber->restart;

	# Wait for the failed transaction to be skipped
	$node_subscriber->poll_query_until('postgres',
		"SELECT subskiplsn = '0/0' FROM pg_subscription WHERE subname = '$subname'"
	);

	# Wait for the log indicating that successfully skipped the transaction, and
	# advance the offset of the log file for the next test.
	$offset = $node_subscriber->wait_for_log(
		qr/LOG:  done skipping logical replication transaction which committed at $lsn/,
		$offset);

	# Insert non-conflict data
	$node_publisher->safe_psql('postgres',
		"INSERT INTO $relname VALUES $nonconflict_data");

	$node_publisher->wait_for_catchup($subname);

	# Check replicated data
	my $res = $node_subscriber->safe_psql('postgres',
		"SELECT count(*) FROM $relname");
	is($res, $expected, $msg);
}

# Create publisher node. Set a low value to logical_decoding_work_mem
# so we can test streaming cases easily.
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf(
	'postgresql.conf',
	qq[
logical_decoding_work_mem = 64kB
max_prepared_transactions = 10
]);
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf(
	'postgresql.conf',
	qq[
max_prepared_transactions = 10
]);

# The subscriber will enter an infinite error loop, so we don't want
# to overflow the server log with error messages.
$node_subscriber->append_conf(
	'postgresql.conf',
	qq[
wal_retrieve_retry_interval = 2s
]);
$node_subscriber->start;

# Initial table setup on both publisher and subscriber. On the subscriber, we
# create the same tables but with primary keys. Also, insert some data that
# will conflict with the data replicated from publisher later.
$node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
CREATE TABLE test_tab (a int);
CREATE TABLE test_tab_streaming (a int, b text);
COMMIT;
]);
$node_subscriber->safe_psql(
	'postgres',
	qq[
BEGIN;
CREATE TABLE test_tab (a int primary key);
CREATE TABLE test_tab_streaming (a int primary key, b text);
INSERT INTO test_tab VALUES (1);
INSERT INTO test_tab_streaming VALUES (1, md5(1::text));
COMMIT;
]);

# Setup publications
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql(
	'postgres',
	qq[
CREATE PUBLICATION tap_pub FOR TABLE test_tab;
CREATE PUBLICATION tap_pub_streaming FOR TABLE test_tab_streaming;
]);

# Create subscriptions
$node_subscriber->safe_psql(
	'postgres',
	qq[
CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (two_phase = on);
CREATE SUBSCRIPTION tap_sub_streaming CONNECTION '$publisher_connstr' PUBLICATION tap_pub_streaming WITH (streaming = on);
]);

$node_publisher->wait_for_catchup('tap_sub');
$node_publisher->wait_for_catchup('tap_sub_streaming');

# Insert data to test_tab1, raising an error on the subscriber due to violation
# of the unique constraint on test_tab. Then skip the transaction.
$node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab VALUES (1);
COMMIT;
]);
test_skip_xact($node_publisher, $node_subscriber, "tap_sub", "test_tab",
	"(2)", "2", "test skipping transaction");

# Test for PREPARE and COMMIT PREPARED. Insert the same data to test_tab1 and
# PREPARE the transaction, raising an error. Then skip the transaction.
$node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab VALUES (1);
PREPARE TRANSACTION 'gtx';
COMMIT PREPARED 'gtx';
]);
test_skip_xact($node_publisher, $node_subscriber, "tap_sub", "test_tab",
	"(3)", "3", "test skipping prepare and commit prepared ");

# Test for STREAM COMMIT. Insert enough rows to test_tab_streaming to exceed the 64kB
# limit, also raising an error on the subscriber during applying spooled changes for the
# same reason. Then skip the transaction.
$node_publisher->safe_psql(
	'postgres',
	qq[
BEGIN;
INSERT INTO test_tab_streaming SELECT i, md5(i::text) FROM generate_series(1, 10000) s(i);
COMMIT;
]);
test_skip_xact($node_publisher, $node_subscriber, "tap_sub_streaming",
	"test_tab_streaming", "(2, md5(2::text))",
	"2", "test skipping stream-commit");

my $res = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM pg_prepared_xacts");
is($res, "0",
	"check all prepared transactions are resolved on the subscriber");
