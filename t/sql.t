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

sub test_create {
	my ($title, $loader, $prefix, $columns, $suffix) = @_;
	like($loader->create_prefix, qr/${prefix}/, "$title create prefix");
	like($loader->create_suffix, qr/${suffix}/, "$title create suffix");
	like($loader->create_sql,    qr/${prefix}${columns}${suffix}/, "$title create sql");
}

$loader = new_ok($mod, [{%def_args}]),
test_create(default => $loader,
	qr/CREATE\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+foo\s*/,
	qr/\)/,
);
like($loader->drop_sql,      qr/DROP\s+TABLE\s+"data"/, 'drop sql');

test_create(constraint_suffix =>
	new_ok($mod, [{%def_args, create_suffix => 'CONSTRAINT primary key a)'}]),
	qr/CREATE\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+foo\s*/,
	qr/CONSTRAINT primary key a\)/,
);

test_create(suffix =>
	new_ok($mod, [{%def_args, create_suffix => ') NOT!'}]),
	qr/CREATE\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+foo\s*/,
	qr/\) NOT!/,
);

test_create(multiple_columns =>
	new_ok($mod, [{%def_args, columns => [[a => 'bar'], ['b'], 'c']}]),
	qr/CREATE\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+bar,\s+"b"\s+foo,\s+"c"\s+foo\s*/,
	qr/\)/,
);

test_create(multi_word_data_types =>
	new_ok($mod, [{%def_args, columns => [[a => 'bar foo'], ['b', 'gri zz ly'], 'c']}]),
	qr/CREATE\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+bar foo,\s+"b"\s+gri zz ly,\s+"c"\s+foo\s*/,
	qr/\)/,
);

$loader = new_ok($mod, [{%def_args, table_type => 'TEMP'}]),
test_create(table_type => $loader,
	qr/CREATE\s+TEMP\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+foo\s*/,
	qr/\)/,
);
like($loader->drop_sql,      qr/DROP\s+TABLE\s+"data"/, 'drop sql');

# TODO: inserts

done_testing;
