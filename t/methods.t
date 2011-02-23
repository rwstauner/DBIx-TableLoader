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
my $get_row_override_data = {cat => [qw(meow string)], dog => [qw(bark squirrel)], bear => [qw(grr picnicbasket)]};
foreach my $test (
	# normal behavior
	[ simple => [], [
		[1, 2, 3],
		[qw(a b c)],
		[0, 0, 0],
	]],
	# modify each row
	[ map_rows => [map_rows => sub { [map { $_ . $_ } @{ $_[0] }] }], [
		[qw(11 22 33)],
		[qw(aa bb cc)],
		[qw(00 00 00)],
	]],
	# stupid example of alternate get_row... not useful, but it works
	# (map_rows would more appropriately do the same thing)
	[ get_row =>  [get_row  => sub { [reverse @{ shift @{ $_[0]->{data} } || return undef }] }], [
		[3, 2, 1],
		[qw(c b a)],
		[0, 0, 0],
	]],
	# example of both
	[ get_row_map_rows =>  [
			get_row  => sub { [reverse @{ shift @{ $_[0]->{data} } || return undef }] },
			map_rows => sub { [map { join('', ($_) x 3) } @{ $_[0] }] }], [
		[qw(333 222 111)],
		[qw(ccc bbb aaa)],
		[qw(000 000 000)],
	]],
	# more useful get_row... using an alternate input data format
	[ alt_get_row => [
			data => undef,
			columns => [qw(animal says chases)],
			get_row => sub { my ($an, $ar) = each %$get_row_override_data; $ar && [$an x 2, @$ar] }], [
		# map keys() so that the data comes out in the same order
		map { [$_ x 2, @{$$get_row_override_data{$_}}] } keys %$get_row_override_data,
	]],
){
	my ($title, $over, $exp) = @$test;
	my $args = [dbh => $dbh, data => [ [qw(a b c)],
		[1, 2, 3],
		[qw(a b c)],
		[0, 0, 0],
	]];

	my $loader = new_ok($mod, [@$args, @$over]);

	is_deeply($loader->get_row, $_, "$title: get_row")
		foreach @$exp;

	is($loader->get_row, undef, "$title: no more rows");
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
