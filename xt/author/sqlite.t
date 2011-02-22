use strict;
use warnings;
use Test::More 0.96;

# test an actual use-case

eval 'require DBD::SQLite'
	or plan skip_all => 'DBD::SQLite required for this author test';

use DBIx::TableLoader;
my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:');
my $records;

my $data = [
	[qw(color smell size)],
	[qw(black skunk medium)],
	[qw(red   apple small)],
	[qw(green Christmas  large)],
	[qw(green frog  small)],
];

DBIx::TableLoader->new(name => 'silly ness', dbh => $dbh, data => $data)->load();

my $table_info = $dbh->table_info('main', '%', '%', 'TABLE')->fetchall_arrayref({})->[0];
is($table_info->{TABLE_NAME}, 'silly ness', 'table name');

$records = $dbh->selectall_arrayref(
	q[SELECT * FROM "silly ness" WHERE color = 'green' ORDER BY size DESC],
	{Slice => {}}
);

is_deeply($records, [
	{color => 'green', smell => 'frog', size => 'small'},
	{color => 'green', smell => 'Christmas', size => 'large'},
	], 'got expected records'
);

$records = $dbh->selectall_hashref(
	q[SELECT * FROM "silly ness" WHERE color = 'green'],
	'smell'
);

is_deeply($records, {
		frog => {color => 'green', smell => 'frog', size => 'small'},
		Christmas => {color => 'green', smell => 'Christmas', size => 'large'},
	}, 'got expected records'
);

$records = $dbh->selectall_arrayref(q[SELECT smell, color FROM "silly ness" WHERE size = 'small' ORDER BY size]);

is_deeply($records, [[qw(apple red)], [qw(frog green)]], 'got expected records');

done_testing;
