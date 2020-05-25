use strict;
use warnings;
use TestLib;
use PostgresNode;
use Test::More tests => 8;

my $node = get_new_node('node');
$node->init(enable_kms => 1);
$node->start;

sub test_cipher
{
	my ($node, $inlen, $test_name) = @_;

	my $expected = $node->safe_psql(
		'postgres',
		qq(SELECT repeat('1', $inlen);));

	my $res = $node->safe_psql(
		'postgres',
		qq(
		SELECT pg_decrypt(pg_encrypt(repeat('1', $inlen)));
		));
	is($res, $expected, $test_name);
}

# Control file should know that checksums are disabled.
command_like(
	[ 'pg_controldata', $node->data_dir ],
	qr/Key management version:.*1/,
	'key manager is enabled in control file');

test_cipher($node, 6, 'less block size');
test_cipher($node, 16, 'one block size');
test_cipher($node, 20, 'more than one block size');

# Get the token encrypted
my $token = 'test_token';
my $token_enc = $node->safe_psql('postgres',
								 qq(SELECT pg_encrypt('$token')));
# Change the cluster passphrase command
$node->safe_psql('postgres',
				 qq(ALTER SYSTEM SET cluster_passphrase_command =
				 'echo 1234123456789012345678901234567890123456789012345678901234567890';));
$node->reload;

my $ret = $node->safe_psql('postgres', 'SELECT pg_rotate_cluster_passphrase()');
is($ret, 't', 'cluster passphrase rotation');

$node->restart;

# Decrypt the token after passphrase rotation.
my $ret_token = $node->safe_psql('postgres',
								 qq(SELECT pg_decrypt('$token_enc')));
is($ret_token, $token, 'decrypt after passphrase rotation');
