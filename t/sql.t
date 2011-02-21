use strict;
use warnings;
use Test::More 0.96;
use Test::MockObject 1.09 ();
#use lib 't/lib';
#use TLHelper;

my $mod = 'DBIx::TableLoader';
eval "require $mod";

my $loader;

my $dbh_done;
my $dbh = Test::MockObject->new()
	->mock(quote_identifier => sub { shift; join('.', map { qq{"$_"} } grep { $_ } @_) })
	->mock(do => sub { $dbh_done = $_[1]; })
;

my %def_args = (
	columns => ['a'],
	dbh     => $dbh,
	# without a DBI default_column_type won't work (so set it)
	default_column_type => 'foo',
);

sub test_statement {
	my ($method, $title, $loader, $prefix, $middle, $suffix) = @_;
	like($loader->${\"${method}_prefix"}, qr/^${prefix}$/, "$title $method prefix");
	like($loader->${\"${method}_suffix"}, qr/^${suffix}$/, "$title $method suffix");
	like($loader->${\"${method}_sql"}, qr/^${prefix}\s*${middle}\s*${suffix}$/, "$title $method sql");
	# call the method which sends the sql to dbh->do (which is mocked)
	     $loader->${\"${method}"};
	like($dbh_done, qr/^${prefix}\s*${middle}\s*${suffix}$/, "$title $method sql passed to dbh");
}
sub test_create { test_statement('create', @_); }
sub test_drop   { test_statement('drop',   @_); }

$loader = new_ok($mod, [{%def_args}]),
test_create(default => $loader,
	qr/CREATE\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+foo\s*/,
	qr/\)/,
);
test_drop(default => $loader,
	qr/DROP\s+TABLE/,
	qr/"data"/,
	qr/\s*/,
);

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

$loader = new_ok($mod, [{%def_args, table_type => 'TEMP'}]);
test_create(table_type => $loader,
	qr/CREATE\s+TEMP\s+TABLE\s+"data"\s+\(/,
	qr/\s*"a"\s+foo\s*/,
	qr/\)/,
);
test_drop(default => $loader,
	qr/DROP\s+TABLE/,
	qr/"data"/,
	qr/\s*/,
);

test_drop(cascade =>
	new_ok($mod, [{%def_args, drop_suffix => 'CASCADE'}]),
	qr/DROP\s+TABLE/,
	qr/"data"/,
	qr/CASCADE/,
);

test_drop(prefix_suffix_drop =>
	new_ok($mod, [{%def_args, drop_prefix => 'DROP TABLE IF EXISTS', drop_suffix => 'CASCADE'}]),
	qr/DROP TABLE IF EXISTS/,
	qr/"data"/,
	qr/CASCADE/,
);

# TODO: inserts

done_testing;
