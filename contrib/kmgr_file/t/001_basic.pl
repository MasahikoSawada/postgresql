use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

my $keyword = "secret keyword";
my $node = get_new_node('test');
$node->init();
$node->append_conf('postgresql.conf',
				   qq(kmgr_plugin_library = 'kmgr_file'));
$node->start;

# Check is the given relation file is encrypted
sub is_encrypted
{
	my $node = shift;
	my $filepath = shift;
	my $expected = shift;
	my $testname = shift;
	my $pgdata = $node->data_dir;

	open my $file, '<' , "$pgdata/$filepath";
	sysread $file, my $buffer, 8192;

	my $ret = $buffer !~ /$keyword/ ? 1 : 0;

	is($ret, $expected, $testname);

	close $file;
}

# Prepare both encrypted and non-encrypted tablespaces
my $enctblspc = $node->data_dir() . '_enctblspc';
my $plntblspc = $node->data_dir() . '_plntblspc';
mkdir $enctblspc or die;
mkdir $plntblspc or die;
$node->safe_psql('postgres',
				 qq(
				 CREATE TABLESPACE enctblspc LOCATION '$enctblspc' WITH (encryption = on);
				 CREATE TABLESPACE plntblspc LOCATION '$plntblspc' WITH (encryption = off);
				 ));

# populate tables
$node->safe_psql('postgres',
				 qq(
				 CREATE TABLE enc (a text) TABLESPACE enctblspc;
				 CREATE TABLE pln (a text) TABLESPACE plntblspc;
				 ));
$node->safe_psql('postgres',
				 qq(
				 INSERT INTO enc VALUES ('$keyword');
				 INSERT INTO pln SELECT ('$keyword');
				 ));

my $enc_filepath = $node->safe_psql('postgres', "SELECT pg_relation_filepath('enc')");
my $pln_filepath = $node->safe_psql('postgres', "SELECT pg_relation_filepath('pln')");

# Read encrypted table
my $ret = $node->safe_psql('postgres', 'SELECT a FROM enc');
is($ret, "$keyword", 'Read encrypted table');

# Sync to disk
$node->safe_psql('postgres', 'CHECKPOINT');

# Encrypted table must be encrypted
is_encrypted($node, $enc_filepath, 1, 'encrypted table is encrypted');

# Plain table must not be encrypted
is_encrypted($node, $pln_filepath, 0, 'non-encrypted table is not encrypted');

# Read encrypted table from disk while decryption
$node->restart;
$ret = $node->safe_psql('postgres', 'SELECT a FROM enc');
is($ret, "$keyword", 'Read encrypted table');

# Rotate enryption keys, and read encrypted table
$node->psql('postgres',
			'SELECT pg_rotate_encryption_key()');
$ret = $node->safe_psql('postgres', 'SELECT a FROM enc');
is($ret, "$keyword", 'Read encrypted table');


# Create an encrypted database, and create table
$node->psql('postgres',
			'CREATE DATABASE encdb TABLESPACE enctblspc');

# System catalog on the encrypted database must be encrypted.
my $pgclass_filepath = $node->safe_psql('encdb',
									   "SELECT pg_relation_filepath('pg_class')");
is_encrypted($node, $pgclass_filepath, 1, 'system catalog is encrypted');

# Change encryption properly by moving tablespace
$node->psql('postgres',
			qq(
			ALTER TABLE enc SET TABLESPACE plntblspc;
			ALTER TABLE pln SET TABLESPACE enctblspc;
			));

# Now, the result of is_encryption is reverse
$enc_filepath = $node->safe_psql('postgres', "SELECT pg_relation_filepath('enc')");
$pln_filepath = $node->safe_psql('postgres', "SELECT pg_relation_filepath('pln')");
$node->psql('postgres', 'CHECKPOINT');
is_encrypted($node, $enc_filepath, 0, 'encrypted table gets plain');
is_encrypted($node, $pln_filepath, 1, 'plain tble gets encrypted');

# Create another encrypted tablespace and table
my $enctblspc2 = $node->data_dir() . '_enctblspc2';
mkdir $enctblspc2 or die;
$node->safe_psql('postgres',
				 qq(CREATE TABLESPACE enctblspc2 LOCATION '$enctblspc2' WITH (encryption = on);));
$node->safe_psql('postgres',
				 qq(
				 CREATE TABLE enc2 (a text) TABLESPACE enctblspc2;
				 INSERT INTO enc2 VALUES ('$keyword')));

# We create the encrypted tablespace and table during recovery
$node->teardown_node;
$node->start;

# Check the table is recovered successfully
$ret = $node->safe_psql('postgres', 'SELECT a FROM enc2');
is($ret, "$keyword", 'Read encrypted table');
