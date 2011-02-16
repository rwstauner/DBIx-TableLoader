use strict;
use warnings;
use Test::More 0.96;
use Test::MockObject 1.09 ();
#use lib 't/lib';
#use TLHelper;

my $mod = 'DBIx::TableLoader';
eval "require $mod";

my $loader;

my $dbh = Test::MockObject->new()
	->mock(quote_identifier => sub { shift; join('.', map { qq{"$_"} } grep { $_ } @_) })
;

my %def_args = (
	columns => ['a'],
	dbh     => $dbh,
	# without a DBI default_column_type won't work (so set it)
	default_column_type => 'foo',
);

$loader = new_ok($mod, [{%def_args}]);
like($loader->create_prefix, qr/CREATE\s+TABLE\s+"data"\s+\(/, 'default create prefix');
like($loader->create_sql,    qr/CREATE\s+TABLE\s+"data"\s*\(\s*"a"\s+foo\s*\)/, 'default create sql');
like($loader->drop_sql,      qr/DROP\s+TABLE\s+"data"/, 'drop sql');

$loader = new_ok($mod, [{%def_args, create_suffix => 'CONSTRAINT primary key a)'}]);
is($loader->create_suffix, 'CONSTRAINT primary key a)', 'create suffix');
like($loader->create_sql,    qr/CREATE\s+TABLE\s+"data"\s*\(\s*"a"\s+foo\s*\CONSTRAINT primary key a\)/, 'create sql with suffix');

$loader = new_ok($mod, [{%def_args, create_suffix => ') NOT!'}]);
is($loader->create_suffix, ') NOT!', 'create suffix');
like($loader->create_sql,    qr/CREATE\s+TABLE\s+"data"\s*\(\s*"a"\s+foo\s*\) NOT!/, 'create sql with suffix');

$loader = new_ok($mod, [{%def_args, columns => [[a => 'bar'], ['b'], 'c']}]);
like($loader->create_sql,    qr/CREATE\s+TABLE\s+"data"\s*\(\s*"a"\s+bar,\s+"b"\s+foo,\s+"c"\s+foo\s*\)/, 'multiple columns');

$loader = new_ok($mod, [{%def_args, columns => [[a => 'bar foo'], ['b', 'gri zz ly'], 'c']}]);
like($loader->create_sql,    qr/CREATE\s+TABLE\s+"data"\s*\(\s*"a"\s+bar foo,\s+"b"\s+gri zz ly,\s+"c"\s+foo\s*\)/, 'multi-word data types');

$loader = new_ok($mod, [{%def_args, table_type => 'TEMP'}]);
like($loader->create_prefix, qr/CREATE\s+TEMP\s+TABLE\s+"data"\s+\(/, 'default create prefix');
like($loader->create_sql,    qr/CREATE\s+TEMP\s+TABLE\s+"data"\s*\(\s*"a"\s+foo\s*\)/, 'default create sql');
like($loader->drop_sql,      qr/DROP\s+TEMP\s+TABLE\s+"data"/, 'drop sql');

# TODO: inserts

done_testing;
