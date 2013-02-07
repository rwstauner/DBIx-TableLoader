# vim: set ts=2 sts=2 sw=2 expandtab smarttab:
use strict;
use warnings;
use Test::More 0.96;

# test an actual use-case

eval 'require Test::mysqld'
  or plan skip_all => 'Test::mysqld required for these tests';

use DBIx::TableLoader;
my $mysqld = Test::mysqld->new(    my_cnf => {
  'skip-networking' => '', # no TCP socket
  'skip-innodb' => '', #faster start
});
unless ($mysqld) {
  no warnings 'once';
  plan skip_all => "Unable to start mysql $Test::mysqld::errstr.";
}
my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'));
my $records;

my $data = [
  [qw(color smell size)],
  [qw(black skunk med i u m)], # invalid: end columns get concatenated
  [qw(bad bad bad bad)],       # invalid: ignored
  [qw(red   apple small)],
  [qw(green Christmas  large)],
  [qw(green frog  small)],
];

my @invalid;
DBIx::TableLoader->new(
  name => 'silly ness',
  dbh  => $dbh,
  data => $data,
  handle_invalid_row => sub {
    my ($loader, $err, $row) = @_;
    if( $row->[0] eq 'bad' ){
      push(@invalid, $_[2]);
      return 0;
    }
    else {
      return [ @$row[0,1], join '', @$row[2 .. $#$row] ];
    }
  },
)->load();

is_deeply
  \@invalid,
  [ [qw(bad bad bad bad)] ],
  'invalid row ignored';

my $table_info = $dbh->table_info()->fetchall_arrayref({})->[0];
is($table_info->{TABLE_NAME}, 'silly ness', 'table name');

$records = $dbh->selectall_arrayref(
  q[SELECT * FROM ].$dbh->quote_identifier('silly ness').q[ WHERE color = 'green' ORDER BY size DESC],
  {Slice => {}}
);

is_deeply($records, [
  {color => 'green', smell => 'frog', size => 'small'},
  {color => 'green', smell => 'Christmas', size => 'large'},
  ], 'got expected records'
);

$records = $dbh->selectall_hashref(
  q[SELECT * FROM ].$dbh->quote_identifier('silly ness').q[ WHERE color = 'green'],
  'smell'
);

is_deeply($records, {
    frog => {color => 'green', smell => 'frog', size => 'small'},
    Christmas => {color => 'green', smell => 'Christmas', size => 'large'},
  }, 'got expected records'
);

$records = $dbh->selectall_arrayref(q[SELECT smell, color FROM ].$dbh->quote_identifier('silly ness').q[ WHERE size = 'small' ORDER BY smell]);

is_deeply($records, [[qw(apple red)], [qw(frog green)]], 'got expected records');

done_testing;
