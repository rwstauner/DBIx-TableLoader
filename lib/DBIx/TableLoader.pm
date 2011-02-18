package DBIx::TableLoader;
# ABSTRACT: Easily load a database table from a dataset

=head1 SYNOPSIS

	my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:');
	my $loader = DBIx::TableLoader->new(dbh => $dbh, data => $data);
	$loader->load();
	# interact with new database table full of data

=cut

use strict;
use warnings;
use Carp qw(croak);
#use DBI 1.13 (); # oldest DBI on CPAN as of 2011-02-15; Has SQL_LONGVARCHAR

=method new

Create a new instance.

The module is very configurable but tries to guess good defaults
in the hopes that you won't need to configure too much in most cases.

Options:

=for :list
* C<create> - Boolean; Whether or not to perform the C<CREATE TABLE> statement.
Defaults to true.
* C<catalog> - Table catalog;
Passed to L<DBI/quote_identifier> to get the full, quoted table name.
None by default.
* C<columns> - Arrayref of column definitions;
Each element can be an arrayref of column name and data type
or just a string for the column name and L</default_column_type> will be used.
* C<create_prefix> - The opening of the SQL statement;
See L</create_prefix>.  Overwrite if you need something more complex.
* C<create_sql> - The C<CREATE TABLE> statement;
See L</create_sql>.  Overwrite if you need something more complex.
* C<create_suffix> - The closing of the SQL statement;
See L</create_suffix>.  Overwrite if you need something more complex.
* C<data> - An arrayref of arrayrefs of data to populate the table;
Subclasses may define more appopriate options and ignore this parameter.
* C<dbh> - A database handle as returned by C<< DBI->connect() >>
* C<default_column_type> - The default data type that will be used for each
column that does not define an explicit data type;
The default will be guessed from the database driver
using C<default_sql_data_type>.  See L</default_column_type>.
* C<default_sql_data_type> - Default SQL standard data type;
If C<default_column_type> is not supplied it will be determined by
asking the database driver for a type corresponding to C<DBI::SQL_LONGVARCHAR>.
Alternate values can be passed (C<DBI::SQL_VARCHAR()> for instance).
See L</default_sql_data_type>.
* C<drop> - Boolean;
Whether or not to execute a C<DROP> statement before C<CREATE TABLE>;
Defaults to false.  Set it to true if the named table already exists and you
want to recreate it.
* C<name> - Table name; Defaults to 'data'.
Subclasses may provide a more useful default.
* C<name_prefix> - String prepended to table name;
Probably mostly useful in subclasses where C<name> is determined automatically.
* C<name_suffix> - String appended to table name.
Probably mostly useful in subclasses where C<name> is determined automatically.
* C<quoted_name> - Full table name, properly quoted;  Only necessary if you need
something more complicated than
C<< $dbh->quote_identifier($catalog, $schema, $table) >>
(see L<DBI/quote_identifier>).
* C<schema> - Table schema;
Passed to L<DBI/quote_identifier> to get the full, quoted table name.
None by default.
* C<table_type> - String that will go before C<TABLE> in C<create_prefix>.
C<TEMPORARY> would be an example of a useful value.
This is probably database driver dependent, so use an appropriate value.

=cut

sub new {
	my $class = shift;
	my $self = bless {}, $class;

	my %opts = @_ == 1 ? %{$_[0]} : @_;

	my $defaults = $self->defaults;
	while( my ($key, $value) = each %$defaults ){
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

sub defaults {
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
		# name() method will default to 'data' if 'name' is blank
		# this way subclasses don't have to override this value in defaults() hash
		name                 => '',
		name_prefix          => '',
		name_suffix          => '',
		quoted_name          => undef,
		schema               => undef,
		table_type           => '', # TEMP, TEMPORARY, VIRTUAL?
	};
}

=method columns

Returns an arrayref of the columns.
Each element is an arrayref of column name and column data type.

=cut

sub columns {
	my ($self) = @_;
	# by default the column names are found in the first row of the data
	return $self->{columns} ||= $self->fetchrow();
}

=method column_names

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

=method default_column_type

Columns that have not been given an explicit data type
will be defined using the C<default_column_type>.

You can pass a value explicitly to the constructor,
or it will try to guess an appropriate (string) type
based on the database driver (using L</default_sql_data_type>).

If all else fails it will default to C<text>
(which works for C<SQLite>, C<PostgreSQL>, C<MySQL>, and some others).

=cut

sub default_column_type {
	my ($self) = @_;
	return $self->{default_column_type} ||= eval {
		if( my $type = $self->{dbh}->type_info($self->default_sql_data_type) ){
			return $type->{TYPE_NAME};
		}
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
	$self->{default_sql_data_type} ||= eval {
		require DBI;
		DBI::SQL_LONGVARCHAR();
	};
}

=method determine_column_types

This method goes through the C<columns> and converts any scalar
column name to an arrayref of column name and C<default_column_type>.
It modifies itself and returns nothing.

=cut

sub determine_column_types {
	my ($self) = @_;
	my ($columns, $type) = ($self->columns, $self->default_column_type);

	croak("Unable to determine columns!")
		unless $columns && @$columns;

	# reset each element to an arrayref if it isn't already
	foreach my $column ( @$columns ){
		# upgrade lone string to arrayref
		$column = [$column]
			if ! ref $column;
		# append column type if missing
		push(@$column, $type)
			unless @$column > 1;
	}

	return;
}

=method drop

Execute the C<DROP TABLE> statement on the database handle.

=cut

sub drop {
	my ($self) = @_;
	$self->{dbh}->do($self->drop_sql);
}

=method drop_sql

Generate the SQL for the C<DROP TABLE> statement.

=cut

sub drop_sql {
	my ($self) = @_;
	# TODO: look up SQL docs to determine if including type is appropriate
	return "DROP $self->{table_type} TABLE " .
		$self->quoted_name;
}

=method fetchrow

	my $row = $loader->fetchrow();

Returns a single row of data at a time (as an arrayref).
This method will be called repeatedly until it returns undef.

This was designed for subclasses to be able to override
and was influenced (obviously) by L<DBI/fetchrow>.

=cut

sub fetchrow {
	my ($self) = @_;
	return $self->{data}->[++$self->{row_index}];
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
			join(', ', @{ $self->column_names } ),
		')',
		'VALUES(',
			join(', ', ('?') x @{ $self->columns }),
		')'
	);
}

=method insert_all

Execute an C<INSERT> statement on the database handle for each row of data.
It will call L<DBI/prepare> using L</insert_sql>
and then call L<DBI/execure> once for each row returned by L</fetchrow>.

=cut

sub insert_all {
	my ($self) = @_;

	my $rows = 0;
	my $sth = $self->{dbh}->prepare($self->insert_sql);
	while( my $row = $self->fetchrow() ){
		++$rows;
		$sth->execute(@$row);
	}

	return $rows;
}

=method load

	my $count = $loader->load();

Load data into database table.
This is a wrapper that does the most commonly needed things
in a single method call.

=for :list
* L</drop> (if configured)
* L</create> (if configured)
* L</insert_all>

Returns the number of rows inserted.

=cut

sub load {
	my ($self) = @_;

	# is it appropriate/sufficient to call prepare_data() from new()?

	$self->drop()
		if $self->{drop};

	$self->create()
		if $self->{create};

	return $self->insert_all();
}

=method name

Returns the full table name
(concatenation of C<name_prefix>, C<name>, and C<name_suffix>).

=cut

sub name {
	my ($self) = @_;
	return $self->{_name} ||=
		$self->{name_prefix} . ($self->{name} || 'data') . $self->{name_suffix};
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

1;

=for stopwords CSV SQLite TODO dataset fetchrow

=for Pod::Coverage defaults

=head1 DESCRIPTION

This module tries to provide a fast and simple (but also configurable)
interface for taking a set of data and loading it into a database table.

Common uses would be to take data from a file (like a CSV)
and load it into a SQLite table.

=head1 RATIONALE

It seemed frequent that I would find a dataset that was difficult to
view/analyze (CSV, log file, etc) and would prefer to load it into a database
for its powerful, familiar processing abilities.

I once chose to use C<MySQL> because its built-in C<LOAD DATA> command
read the malformed CSV I was given and C<SQLite>'s C<.import> command did not.

I wrote this module so that I'd never have to make such a terrible choice again.
I wanted to be able to use the power of L<Text::CSV> to make sure I could
take any CSV I ever got and load it into C<SQLite> easily.

I tried to make this module a base class to be able to handle various formats.

=head1 TODO

=for :list
* Allow a custom column name transformation sub to be passed in
* Use L<String::CamelCase/decamelize> by default?
* Allow extra columns (like C<id>) to be added and/or generated
* Consider using something like L<Text::CSV::SQLhelper>
to guess appropriate data types for each column

=head1 SEE ALSO

* L<DBIx::TableLoader::CSV>

=cut
