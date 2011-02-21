use strict;
use warnings;
use Test::More 0.96;

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
	# without a DBI default_column_type won't work (so set it)
	default_column_type => 'foo',
);

foreach my $args ( 
	[{columns =>  [qw(d b i)] , %def_args}],
	[{data    => [[qw(d b i)]], %def_args}],
){
	$loader = new_ok($mod, $args);
	is_deeply($loader->columns, [[qw(d foo)], [qw(b foo)], [qw(i foo)]], 'string columns');
	is_deeply($loader->column_names, [qw(d b i)], 'string columns (names)');
}

	$loader = new_ok($mod, [{columns => [[a => 'bar'], ['b'], 'c'], %def_args}]);
	is_deeply($loader->columns, [[qw(a bar)], [qw(b foo)], [qw(c foo)]], 'mixed columns');
	is_deeply($loader->column_names, [qw(a b c)], 'mixed columns (names)');

	$loader = new_ok($mod, [{columns => [[a => 'bar foo'], ['b', 'gri zz ly'], 'c'], %def_args}]);
	is_deeply($loader->columns, [['a', 'bar foo'], ['b', 'gri zz ly'], [qw(c foo)]], 'multi-word data types');
	is_deeply($loader->column_names, [qw(a b c)], 'multi-word data types (names)');

done_testing;
