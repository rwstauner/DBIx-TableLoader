use strict;
use warnings;
use Test::More 0.96;
use Test::MockObject 1.09 ();

my $driver_type;
my $dbh = Test::MockObject->new()
	->mock(type_info => sub { return {TYPE_NAME => $driver_type}; })
	->mock(quote_identifier => sub { shift; join('.', map { qq["$_"] } grep { $_ } @_); });

$dbh->fake_module('DBI', SQL_LONGVARCHAR => sub { ':-P' });

my $mod = 'DBIx::TableLoader';
eval "require $mod" or die $@;

my $loader;

foreach my $args (
	[],
	[{columns => []}],
){
	is(eval { $loader = $mod->new(@$args) }, undef, 'useless without columns');
	like($@, qr/columns/, 'Died without columns');
}

my %def_args = (
	default_column_type => 'foo',
	dbh => $dbh,
);

# NOTE: determine_column_types is not specifically tested
# but it sets the values returned from columns() and column_names()

foreach my $args ( 
	[{columns =>  [qw(d b i)] , %def_args}],
	[{data    => [[qw(d b i)]], %def_args}],
){
	$loader = new_ok($mod, $args);
	is_deeply($loader->columns, [[qw(d foo)], [qw(b foo)], [qw(i foo)]], 'string columns');
	is_deeply($loader->column_names, [qw(d b i)], 'string columns (names)');
	is_deeply($loader->quoted_column_names, [qw("d" "b" "i")], 'string columns (names) (quoted)');
}

	$loader = new_ok($mod, [{columns => [[a => 'bar'], ['b'], 'c'], %def_args}]);
	is_deeply($loader->columns, [[qw(a bar)], [qw(b foo)], [qw(c foo)]], 'mixed columns');
	is_deeply($loader->column_names, [qw(a b c)], 'mixed columns (names)');
	is_deeply($loader->quoted_column_names, [qw("a" "b" "c")], 'mixed columns (names) (quoted)');

	$loader = new_ok($mod, [{columns => [[a => 'bar foo'], ['b', 'gri zz ly'], 'c'], %def_args}]);
	is_deeply($loader->columns, [['a', 'bar foo'], ['b', 'gri zz ly'], [qw(c foo)]], 'multi-word data types');
	is_deeply($loader->column_names, [qw(a b c)], 'multi-word data types (names)');
	is_deeply($loader->quoted_column_names, [qw("a" "b" "c")], 'multi-word data types (names) (quoted)');

{

	# column type

	my $args = [dbh => $dbh, columns => ['foo']];

	# create new instance for each test to avoid internal caching
	$driver_type = 'boo';
	is(new_ok($mod, $args)->default_column_type, 'boo', 'column type from dbh');
	$driver_type = '';
	is(new_ok($mod, $args)->default_column_type, 'text', 'default column type');
	$driver_type = 'no matter';
	is(new_ok($mod, [@$args, default_column_type => 'bear'])->default_column_type, 'bear', 'default column type');

	# sql data type

	is(new_ok($mod, $args)->default_sql_data_type, ':-P', 'default sql data type');

}

# get_row
{
	my $loader = new_ok($mod, [dbh => $dbh, data => [ [qw(a b c)],
		[1, 2, 3],
		[qw(a b c)],
		[0, 0, 0],
	]]);
	is_deeply($loader->get_row, [1,2,3], 'got row');
	is_deeply($loader->get_row, [qw(a b c)], 'got row');
	is_deeply($loader->get_row, [0, 0, 0], 'got row');
}

# name
foreach my $test (
	[ [], 'data' ],
	[ [name_prefix => 'pre_'], 'pre_data' ],
	[ [name_prefix => 'pre', name_suffix => 'post'], 'predatapost' ],
	[ [name => 'tab', name_suffix => ' grr'], 'tab grr' ],
){
	my ($attr, $exp) = @$test;
	my $loader = new_ok($mod, [columns => ['goo'], dbh => $dbh, @$attr]);
	is($loader->name, $exp, 'expected name');
	is($loader->quoted_name, qq{"$exp"}, 'expected quoted name');
}

done_testing;
