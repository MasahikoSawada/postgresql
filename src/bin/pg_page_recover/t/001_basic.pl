use strict;
use warnings;
use PostgresNode;
use TestLib;
use File::Copy;
use Test::More tests => 7;

my $node_target = get_new_node('target');
$node_target->init(has_archiving => 1, allows_streaming => 1);

my $datadir_target = $node_target->data_dir();

# enable checksums
command_ok(
	[ 'pg_checksums', '--enable', '-D', $datadir_target ]);

$node_target->start;

# Populate table
$node_target->psql(
  'postgres', "
    BEGIN;
	CREATE TABLE test (a int);
	INSERT INTO test SELECT generate_series(1,10);
    COMMIT;
	");

# Get file node and file path
my $targetrelpath = $node_target->safe_psql(
	'postgres',	"SELECT pg_relation_filepath('test'::regclass);");
my $targetrel = $node_target->safe_psql(
	'postgres', "SELECT relfilenode FROM pg_class WHERE relname = 'test'");

# Take backup
my $backup_name = 'my_backup';
$node_target->backup($backup_name);

# Generate update WAL
$node_target->safe_psql(
	'postgres', "
    BEGIN;
	INSERT INTO test SELECT generate_series(1,10);
    COMMIT;
	");

# Get base node
my $node_base = get_new_node('base');
$node_base->init_from_backup($node_target, $backup_name);

my $datadir_base = $node_base->data_dir();
my $archivedir = $node_target->archive_dir();

# create work dir
my $workdir = $node_target->basedir() . "/work";
mkdir $workdir;

# Set page header and block size
my $pageheader_size = 24;
my $block_size = $node_target->safe_psql('postgres', 'SHOW block_size;');
$node_target->stop;

# Time to create some corruption
copy("$datadir_target/$targetrelpath", "/tmp/");
open my $file, '+<', "$datadir_target/$targetrelpath";
seek($file, $pageheader_size, 0);
syswrite($file, "\0\0\0\0\0\0\0\0\0");
close $file;

# pg_checksums detects data currption.
$node_target->command_checks_all(
	[
	 'pg_checksums', '--check',
	 '-D',           $datadir_target,
	 '--filenode',   $targetrel
	],
	1,
	[qr/Bad checksums:.*1/],
	[qr/checksum verification failed/],
	"fails with corrupted data for single relfilenode on tablespace"
  );

# Recover one currupt page
command_ok(
	[
	 'pg_page_recover',
	 "-D", $datadir_target,
	 "-B", $datadir_base,
	 "-r", $targetrelpath . ":0",
	 "-R", "cp $archivedir/%f %p",
	 "-w", $workdir
	],
	'pg_page_recover test');

# Checksums check must pass
command_ok(
	[ 'pg_checksums', '--check',
	  '-D', $datadir_target,
	 '--filenode',   $targetrel
	],
	"database cluster is successfully recovered");

# Check if recovered table retuns correct result
$node_target->start;
my $cnt;
$node_target->psql('postgres', 'SELECT count(*) FROM test', stdout => \$cnt);
is($cnt, '20', 'Check recovered table');
