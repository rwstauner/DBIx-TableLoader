# vim: set ts=2 sts=2 sw=2 expandtab smarttab:
use strict;
use warnings;

package DBIx::TableLoader;
# ABSTRACT: Easily load a database table from a data set

use Carp qw(croak);
#use DBI 1.13 (); # oldest DBI on CPAN as of 2011-02-15; Has SQL_LONGVARCHAR
use Try::Tiny 0.09;

=method new

Create a new instance.  Accepts a hash or hashref of options.

This module is very configurable but tries to use good defaults
in the hopes that you won't need to configure too much in most cases.

Most likely needed options:

=for :list
* C<dbh> - A B<d>ataB<b>ase B<h>andle as returned by C<< DBI->connect() >>
* C<data> - An arrayref of arrayrefs of data (which will be the input records)

See L</OPTIONS> for the full list.

=cut

sub new {
  my $class = shift;
  my $self = bless {}, $class;

  my %opts = @_ == 1 ? %{$_[0]} : @_;

  my %defaults = (%{ $self->base_defaults }, %{ $self->defaults });
  while( my ($key, $value) = each %defaults ){
    $self->{$key} = exists($opts{$key})
      ? delete $opts{$key}
      : $value;
  }

  # be loud about typos
  croak("Unknown options: ${\join(', ', keys %opts)}")
    if %opts;

  # custom routine to handle type of input data (hook for subclasses)
  $self->prepare_data();

  # normalize 'columns' attribute
  $self->determine_column_types();

  return $self;
}

=method base_defaults

Returns a hashref of the options defined by the base class
and their default values.

=cut

sub base_defaults {
  return {
    catalog              => undef,
    columns              => undef,
    create               => 1,
    create_prefix        => '',
    create_sql           => '',
    create_suffix        => '',
    # 'data' attribute may not be useful in subclasses
    data                 => undef,
    dbh                  => undef,
    default_column_type  => '',
    default_sql_data_type => '',
    drop                 => 0,
    drop_prefix          => '',
    drop_sql             => '',
    drop_suffix          => '',
    get_row              => undef,
    grep_rows            => undef,
    handle_invalid_row   => undef,
    map_rows             => undef,
    # default_name() method will default to 'data' if 'name' is blank
    # this way subclasses don't have to override this value in defaults()
    name                 => '',
    name_prefix          => '',
    name_suffix          => '',
    quoted_name          => undef,
    schema               => undef,
    table_type           => '', # TEMP, TEMPORARY, VIRTUAL?
    transaction          => 1,
  };
}

=method defaults

Returns a hashref of additional options defined by a subclass.

=cut

sub defaults {
  return {};
}

=method columns

  my $columns = $loader->columns;
  # [ ['column1', 'data type'], ['column two', 'data type'] ]

Returns an arrayref of the columns.
Each element is an arrayref of column name and column data type.

=cut

sub columns {
  my ($self) = @_;
  # by default the column names are found in the first row of the data
  # (but circumvent get_row() to avoid any grep or map subs)
  return $self->{columns} ||= $self->_get_custom_or_raw_row();
}

=method column_names

  my $column_names = $loader->column_names;
  # ['column1', 'column two']

Returns an arrayref of the column names.

=cut

sub column_names {
  my ($self) = @_;
  # return the first element of each arrayref
  return [ map { $$_[0] } @{ $self->columns } ];
}

=method create

Executes a C<CREATE TABLE> SQL statement on the database handle.

=cut

sub create {
  my ($self) = @_;
  $self->{dbh}->do($self->create_sql);
}

=method create_prefix

Generates the opening of the C<CREATE TABLE> statement
(everything before the column specifications).

Defaults to C<< "CREATE $table_type TABLE $quoted_name (" >>.

=cut

sub create_prefix {
  my ($self) = @_;
  return $self->{create_prefix} ||=
    "CREATE $self->{table_type} TABLE " .
      $self->quoted_name . " (";
}

=method create_sql

Generates the SQL for the C<CREATE TABLE> statement
by concatenating L</create_prefix>,
the column definitions,
and L</create_suffix>.

Can be overridden in the constructor.

=cut

sub create_sql {
  my ($self) = @_;
  $self->{create_sql} ||=
    join(' ',
      $self->create_prefix,

      # column definitions (each element is: [name, data_type])
      join(', ', map {
        $self->{dbh}->quote_identifier($_->[0]) . ' ' . $_->[1]
      } @{ $self->columns }),

      $self->create_suffix
    );
}

=method create_suffix

Generates the closing of the C<CREATE TABLE> statement
(everything after the column specifications).

Defaults to C<< ")" >>.

=cut

sub create_suffix {
  my ($self) = @_;
  return $self->{create_suffix} ||=
    ')';
}

# ask the driver what data type it uses for the desired SQL standard type

sub _data_type_from_driver {
  my ($self, $data_type) = @_;
  if( my $type = $self->{dbh}->type_info($data_type) ){
    return $type->{TYPE_NAME};
  }
  return;
}

=method default_name

Returns the default (base) name for the table.

This is mostly for subclasses where a useful table name
can be determined from the input (like a filename).
In this module it defaults to C<'data'>.

This gets concatenated together with
L</name_prefix> and L</name_suffix> in L</name>.

=cut

sub default_name {
  return 'data';
}

=method default_column_type

Columns that have not been given an explicit data type
will be defined using the C<default_column_type>.

You can pass a value explicitly to the constructor,
or it will try to determine an appropriate (string) type
based on the database driver (using L</default_sql_data_type>).

If all else fails it will default to C<text>
(which works for SQLite, PostgreSQL, MySQL, and some others).

=cut

sub default_column_type {
  my ($self) = @_;
  return $self->{default_column_type} ||= try {
    $self->_data_type_from_driver($self->default_sql_data_type);
  }
    # outside the eval in case there was an error
    || 'text';
}

=method default_sql_data_type

Passed to L<DBI/type_info> to query the database driver
for an appropriate default column type.

Defaults to C<DBI::SQL_LONGVARCHAR>.

=cut

sub default_sql_data_type {
  my ($self) = @_;
  $self->{default_sql_data_type} ||= try {
    # if this doesn't work default_column_type will just use 'text'
    require DBI;
    DBI::SQL_LONGVARCHAR();
  };
}

=method determine_column_types

This method goes through the C<columns> and converts any scalar
column name to an arrayref of column name and C<default_column_type>.
It modifies itself and returns nothing.
It is called automatically from the constructor.

=cut

sub determine_column_types {
  my ($self) = @_;
  my ($columns, $type) = ($self->columns, $self->default_column_type);

  croak("Unable to determine columns!")
    unless $columns && @$columns;

  # break reference
  $columns = [@$columns];

  # reset each element to an arrayref if it isn't already
  foreach my $column ( @$columns ){
    # upgrade lone string to arrayref otherwise break reference
    $column = ref $column ? [@$column] : [$column];
    # append column type if missing
    push(@$column, $type)
      unless @$column > 1;
  }

  # restore changes
  $self->{columns} = $columns;
  return;
}

=method drop

Execute the C<DROP TABLE> statement on the database handle.

=cut

sub drop {
  my ($self) = @_;
  $self->{dbh}->do($self->drop_sql);
}

=method drop_prefix

Returns the portion of the SQL statement before the table name.

Defaults to C<DROP TABLE>.

=cut

sub drop_prefix {
  my ($self) = @_;
  # default to "DROP TABLE" since SQLite, PostgreSQL, and MySQL
  # all accept it (rather than "DROP $table_type TABLE")
  $self->{drop_prefix} ||= 'DROP TABLE';
}

=method drop_sql

Generates the SQL for the C<DROP TABLE> statement
by concatenating L</drop_prefix>, L</quoted_name>, and L</drop_suffix>.

Alternatively C<drop_sql> can be set in the constructor
if you need something more complex.

=cut

sub drop_sql {
  my ($self) = @_;
  return $self->{drop_sql} ||= join(' ',
    $self->drop_prefix,
    $self->quoted_name,
    $self->drop_suffix,
  );
}

=method drop_suffix

Returns the portion of the SQL statement after the table name.

Nothing by default.

=cut

sub drop_suffix {
  my ($self) = @_;
  # default is blank
  return $self->{drop_suffix};
}

# call get_raw_row unless a custom 'get_row' is defined
# (this is the essence of get_row() but without the grep/map subs)

sub _get_custom_or_raw_row {
  my ($self) = @_;
  # considered { $self->{get_row} ||= $self->can('get_raw_row'); } in new()
  # but it just seemed a little strange... this is more normal/clear
  return $self->{get_row}
       ? $self->{get_row}->($self)
       : $self->get_raw_row();
}

=method get_raw_row

Subclasses will override this method according to the input data format.

This is called from L</get_row> to retrieve the next row of raw data.

It should return C<undef> when there are no more rows.

=cut

sub get_raw_row {
  my ($self) = @_;
  # It would be simpler to shift the data but I don't think it actually
  # gains us anything.  This way we're not modifying anything unexpectedly.
  # Besides subclasses will likely be more useful than this one.
  return $self->{data}->[ $self->{row_index}++ ];
}

=method get_row

  my $row = $loader->get_row();

Returns a single row of data at a time (as an arrayref).
This method will be called repeatedly until it returns C<undef>.
The returned arrayref will be flattened and passed to L<DBI/execute>.

=cut

sub get_row {
  my ($self) = @_;
  my $row;

  GETROW: {
    $row = $self->_get_custom_or_raw_row()
      or last GETROW;

    # call grep_rows with the same semantics as map_rows (below)
    if( $self->{grep_rows} ){
      local $_ = $row;
      # if grep returns false try the block again
      redo GETROW
        unless $self->{grep_rows}->($row, $self);
    }

    # Send the row first since it's the important part.
    # This isn't a method call, and $self will likely be seldom used.
    if( $self->{map_rows} ){
      # localize $_ to the $row for consistency with the built in map()
      local $_ = $row;
      # also pass row as the first argument to simulate a normal function call
      $row = $self->{map_rows}->($row, $self);
    }

    # validate the row before passing a bad value to the DBI
    $row = try {
      $self->validate_row($row);
    }
    catch {
      # file/line position is unhelpful, but so is the newline
      chomp(my $e = $_[0]);
      # if there was an error, pass it through the handler
      # the handler should die, return a row, or return false to skip
      $self->handle_invalid_row($e, $row);
    }
      or redo GETROW;
  }

  return $row;
}

=method handle_invalid_row

This is called from L</get_row> when a row is determined to be invalid
(when L</validate_row> throws an error).

If C<handle_invalid_row> was not specified in the constructor
this method is a no-op:
the original row will be returned (and eventually passed to L<DBI/execute>).

Possible values for the C<handle_invalid_row> option:

=begin :list

* C<die>  - Calls C<die()>  with the error message
* C<warn> - Calls C<warn()> with the error message and returns the row unmodified

=item * code ref

If it's a subroutine reference it is called as a method,
receiving the loader object, the error message, and the row:

  $handler->($loader, $error, $row);

The handler should either C<die> to cease processing,
return false to skip this row and get the next one,
or return a (possibly modified) row that will be passed to L<DBI/execute>.

This allows you to, for example, write to a log when a bad row
is found without aborting your transaction:

  handle_invalid_row => sub {
    my ($self, $error, $row) = @_;
    $logger->log(['Bad row: %s: %s', $error, $row]);
    return; # return false to skip this row and move to the next one
  }

=end :list

=cut

sub handle_invalid_row {
  my ($self, $error, $row) = @_;

  if( my $handler = $self->{handle_invalid_row} ){
    # should this be croak/carp?
    if( $handler eq 'die' ){
      die $error . "\n";
    }
    elsif( $handler eq 'warn' ){
      warn $error . "\n";
      return $row;
    }
    # otherwise it should be a coderef (or a method name (for a subclass maybe))
    else {
      return $self->$handler($error, $row);
    }
  }

  # pass through if no handler was defined
  return $row;
}

=method insert_sql

Generate the C<INSERT> SQL statement that will be passed to L<DBI/prepare>.

=cut

sub insert_sql {
  my ($self) = @_;
  join(' ',
    'INSERT INTO',
    $self->quoted_name,
    '(',
      join(', ', @{ $self->quoted_column_names } ),
    ')',
    'VALUES(',
      join(', ', ('?') x @{ $self->columns }),
    ')'
  );
}

=method insert_all

Execute an C<INSERT> statement on the database handle for each row of data.
It will call L<DBI/prepare> using L</insert_sql>
and then call L<DBI/execute> once for each row returned by L</get_row>.

=cut

sub insert_all {
  my ($self) = @_;

  my $rows = 0;
  my $sth = $self->{dbh}->prepare($self->insert_sql);
  while( my $row = $self->get_row() ){
    $sth->execute(@$row);
    ++$rows;
  }

  return $rows;
}

=method load

  my $number_of_rows = $loader->load();

Load data into database table.
This is a wrapper that does the most commonly needed things
in a single method call.
If the C<transaction> setting is true (default)
the actions will be wrapped in a transaction.

=for :list
* L</drop> (if configured)
* L</create> (if configured)
* L</insert_all>

Returns the number of rows inserted.

=cut

sub load {
  my ($self) = @_;
  my $rows;

  # is it appropriate/sufficient to call prepare_data() from new()?

  try {

  $self->{dbh}->begin_work()
    if $self->{transaction};

  $self->drop()
    if $self->{drop};

  $self->create()
    if $self->{create};

    $rows = $self->insert_all();

  $self->{dbh}->commit()
    if $self->{transaction};

  }
  catch {
    # explicitly end the transaction that we started
    # in case this isn't the last thing being done with the dbh
    $self->{dbh}->rollback()
      if $self->{transaction};

    # propagate the exception
    die $_[0];
  };

  return $rows;
}

=method name

Returns the full table name
(concatenation of C<name_prefix>, C<name>, and C<name_suffix>).

=cut

sub name {
  my ($self) = @_;
  return $self->{_name} ||=
    $self->{name_prefix} .
    ($self->{name} || $self->default_name) .
    $self->{name_suffix};
}

=method prepare_data

This method is called from L</new> after the object is blessed (obviously).
Any preparation work specific to the type of data should be done here.

This is mostly a hook for subclasses and does very little in this module.

=cut

sub prepare_data {
  my ($self) = @_;
  $self->{row_index} = 0;
}

=method quoted_name

Returns the full, quoted table name.
Passes C<catalog>, C<schema>, and C<name> attributes to L<DBI/quote_identifier>.

=cut

sub quoted_name {
  my ($self) = @_;
  # allow quoted name to be passed in to handle edge cases
  return $self->{quoted_name} ||=
    $self->{dbh}->quote_identifier(
      $self->{catalog}, $self->{schema}, $self->name);
}

=method quoted_column_names

  my $quoted_names = $loader->quoted_column_names();
  # ['"column1"', '"column two"']

Returns an arrayref of column names quoted by the database driver.

=cut

sub quoted_column_names {
  my ($self) = @_;
  return $self->{quoted_column_names} ||= [
    map { $self->{dbh}->quote_identifier($_) }
      @{ $self->column_names }
  ];
}

=method validate_row

Called from L</get_row> to check that the provided row is valid.

It may C<die> for any error
which will be caught in L</get_row>
and the error will be passed to L</handle_invalid_row>.

The return value works like that of L</handle_invalid_row>:
On success, the valid row (possibly modified) should be returned.
If a false value is returned L</get_row> will attempt to
get another row.

Currently this only checks that the number of fields in the row
matches the number of columns expected,
however other checks may be added in the future.
Subclasses can overwrite this to define their own validations
(though calling the original (superclass method) is recommended).

=cut

sub validate_row {
  my ($self, $row) = @_;

  # DBI will croak if exec'd with different numbers
  my $num_columns = @{ $self->columns };

  die 'Row has ' . @$row . ' fields when ' .  $num_columns . " are expected\n"
    if @$row != $num_columns;

  # are there other validation checks we can do?

  return $row;
}

1;

=for stopwords CSV SQLite PostgreSQL MySQL TODO arrayrefs

=for test_synopsis
my (@connection_args, $dbh, $data);

=head1 SYNOPSIS

  my $dbh = DBI->connect(@connection_args);

  DBIx::TableLoader->new(dbh => $dbh, data => $data)->load();

  # interact with new database table full of data in $dbh

=head1 DESCRIPTION

This module tries to provide a fast and simple (but very configurable)
interface for taking a set of data and loading it into a database table.

Common uses would be to take data from a file (like a CSV)
and load it into a SQLite table.
(For that specific case see L<DBIx::TableLoader::CSV>.)

In most cases simply calling C<load()> is sufficient,
but all methods are documented below for completeness.

=head1 OPTIONS

This module is very [excessively] configurable.
In most cases the default values will be sufficient,
but you should be able to customize the object to fit your needs.

Frequently Used Options:

=begin :list

* C<columns> - Arrayref of column definitions
Each element can be an arrayref of column name and data type
or just a string for the column name and L</default_column_type> will be used.
If not passed in the first row of C<data> will be assumed to be column names.

  columns => ['first_name', 'last_name', ['last_seen', 'date']]

* C<dbh> - A database handle as returned by C<< DBI->connect() >>
This module probably isn't useful without one.

* C<data> - An arrayref of arrayrefs of data to populate the table;
Subclasses may define more appropriate options and ignore this parameter.
If you're using this base class, you'll probably need this
(unless you provide your own C<get_row> coderef).

  data => [ ['polar', 'bear', '2010-08-15'], ['blue', 'duck', '2009-07-30'] ]

=end :list

Less common options that are available when you desire extra tweaking power:

=begin :list

* C<create> - Boolean; Whether or not to perform the C<CREATE TABLE> statement
Defaults to true.

* C<default_column_type> - Default data type for each column
This will be used for each column that does not explicitly define a data type.
The default will be determined from the database driver
using C<default_sql_data_type>.  See L</default_column_type>.

  default_column_type => 'CHAR(50)'

* C<drop> - Boolean to execute a C<DROP TABLE> statement before C<CREATE TABLE>
Defaults to false.  Set it to true if the named table already exists and you
want to recreate it.

* C<get_row> - A sub (coderef) that will override L</get_raw_row>
You can use this if your input data is in a different format
than the module expects (to split a string into an arrayref, for instance).
This is called like a method (the object will be C<$_[0]>).
The return value will be passed to C<map_rows> if both are present.

  # each record is a line from a log file;
  # use the m// operator in list context to capture desired fields
  get_row => sub { my $s = <$io>; [ $s =~ m/^(\d+)\s+"([^"]+)"\s+(\S+)$/ ] }

C<NOTE>: If you use C<get_row> and don't pass C<data>
you will probably want to pass C<columns>
(otherwise columns will be taken from the first call to C<get_row>).

* C<grep_rows> - A sub (coderef) to determine if a row should be used or skipped
Named after the built in C<grep> function.
It will receive the row as an arrayref in C<$_[0]>.
(The row will also be available in C<$_>
for consistency with the built in C<grep>.)
The object will be passed as C<$_[1]> in case you want it.
If it returns a true value the row will be used.
If it returns false the next row will be fetched and the process will repeat
(until all rows have been exhausted).

  grep_rows => sub { $_->[1] =~ /something/ } # accept the row if it matches

  grep_rows => sub { my ($row, $obj) = @_; do_something(); } # 2 variables

* C<handle_invalid_row> - How to handle invalid rows.
Can be C<die>, C<warn>, or a sub (coderef).
See L</handle_invalid_row> for more details.
Default is to ignore (in which case DBI will likely error).

* C<map_rows> - A sub (coderef) to filter/mangle a row before use
Named after the built in C<map> function.
It will receive the row as an arrayref in C<$_[0]>.
(The row will also be available in C<$_>
for consistency with the built in C<map>.)
The object will be passed as C<$_[1]> in case you want it.
It should return an arrayref (which will be used as the row).

  map_rows => sub { [ map { uc $_ } @$_ ] } # uppercase all the fields

  map_rows => sub { my ($row, $obj) = @_; do_something(); } # 2 variables

* C<name> - Table name
Defaults to C<'data'>.  Subclasses may provide a more useful default.

* C<table_type> - String that will go before C<TABLE> in L</create_prefix>
A useful value might be C<TEMPORARY> or C<TEMP>.
This is probably database driver dependent, so use an appropriate value.

* C<transaction> - Boolean
All the operations in L</load> will be wrapped in a transaction by default.
Set this option to false to disable this.

=end :list

Options that will seldom be necessary
but are available for completeness and/or consistency:

=begin :list

* C<catalog> - Table catalog
Passed to L<DBI/quote_identifier> to get the full, quoted table name.
None by default.

* C<create_prefix> - The opening of the SQL statement
See L</create_prefix>.  Overwrite if you need something more complex.

* C<create_sql> - The C<CREATE TABLE> statement
See L</create_sql>.  Overwrite if you need something more complex.

* C<create_suffix> - The closing of the SQL statement
See L</create_suffix>.  Overwrite if you need something more complex.

* C<default_sql_data_type> - Default SQL standard data type
If C<default_column_type> is not supplied it will be determined by
asking the database driver for a type corresponding to C<DBI::SQL_LONGVARCHAR>.
Alternate values can be passed (C<DBI::SQL_VARCHAR()> for instance).
See L</default_sql_data_type>.

* C<drop_prefix> - The opening of the SQL statement
See L</drop_prefix>.  Overwrite if you need something more complex.

* C<drop_sql> - The C<DROP TABLE> statement
Will be constructed if not provided.  See L</drop_sql>.

* C<drop_suffix> - The closing of the SQL statement
See L</drop_suffix>.  Overwrite if you need something more complex.

* C<name_prefix> - String prepended to table name
Probably mostly useful in subclasses where C<name> is determined automatically.

* C<name_suffix> - String appended to table name
Probably mostly useful in subclasses where C<name> is determined automatically.

* C<quoted_name> - Full table name, properly quoted
Only necessary if you need something more complicated than
C<< $dbh->quote_identifier($catalog, $schema, $table) >>
(see L<DBI/quote_identifier>).

* C<schema> - Table schema
Passed to L<DBI/quote_identifier> to get the full, quoted table name.
None by default.

=end :list

=head1 SUBCLASSING

This module was designed to be subclassed
for use with specific data input formats.

L<DBIx::TableLoader::CSV> is a prime example.
It is the entire reason this base module was designed.

Subclasses will likely want to override the following methods:

=for :list
* L</defaults> - a hashref of additional acceptable options (and default values)
* L</default_name> - if you can determine a good default name from the input
* L</get_raw_row> - to return the next row of data
* L</prepare_data> - to initialize your object/data (open the file, etc.)

Be sure to check out the code for L<DBIx::TableLoader::CSV>.
Also see a very simple example in F<t/subclass.t>.

=head1 RATIONALE

It seemed frequent that I would find a data set that was difficult to
view/analyze (CSV, log file, etc.) and would prefer to load it into a database
for its powerful, familiar processing abilities.

I once chose to use MySQL because its built in C<LOAD DATA> command
read the malformed CSV I was given and the C<.import> command in SQLite did not.

I wrote this module so that I'd never have to make such a choice again.
I wanted to be able to use the power of L<Text::CSV> to make sure I could
take any CSV I ever got and load it into SQLite easily.

I tried to make this module a base class to be able to handle various formats.

=head1 TODO

This is more of a list of ideas than features that are planned.

=for :list
* Allow a custom column name transformation sub to be passed in
* Use L<String::CamelCase/decamelize> by default?
* Allow extra columns (like C<id>) to be added and/or generated
* Option to scan the data to guess appropriate data types for each column
* Make a SQLite function so that you could call this from a C<dbish> command line?
* Allow C<UPDATE> statements and specify the key columns (for the C<WHERE> clause)

=head1 SEE ALSO

=for :list
* L<DBIx::TableLoader::CSV>

=cut
