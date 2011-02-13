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

=method new

Options:

=for :list
* C<create> - Boolean; Whether or not to perform the CREATE statement.
Defaults to true
* TODO: list the rest

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
	croak("Unknown options: ${\join(', ', keys %opts)}");

	# custom routine to handle type of input data (hook for subclasses)
	$self->prepare_data();

	# normalize 'columns' attribute
	$self->determine_column_types();

	return $self;
}

sub defaults {
	return {
		columns              => undef,
		create               => 1,
		create_prefix        => '',
		create_suffix        => '',
		# 'data' attribute may not be useful in subclasses
		data                 => undef,
		dbh                  => undef,
		# data type that will work most commonly across various database vendors
		default_column_type  => 'varchar',
		drop                 => 0,
		identifier_quote     => '"', # '`'
		name                 => 'data',
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
	return [ map { $$_[0] } $self->columns ];
}

=method create

Executes a C<CREATE TABLE> SQL statement on the database handle.

=cut

sub create {
	my ($self) = @_;
	$self->{dbh}->do($self->create_sql);
}

=method create_prefix

Generates the opening of the C<CREATE> statement
(everything before the column specifications).

=cut

sub create_prefix {
	my ($self) = @_;
	return $self->{create_prefix} ||=
		"CREATE $self->{table_type} TABLE " .
			$self->quote_identifier($self->name) . " (";
}

=method create_sql

Generates the SQL for the C<CREATE> statement.

=cut

sub create_sql {
	my ($self) = @_;
	$self->{create_sql} ||=
		join(' ',
			$self->create_prefix,

			# column definitions
			join(', ', map {
				$self->quote_identifier($_->[0]) . ' ' . $_->[1]
			} $self->columns),

			$self->create_suffix
		);
}

=method create_suffix

Generates the closing of the C<CREATE> statement
(everything after the column specifications).

=cut

sub create_suffix {
	my ($self) = @_;
	return $self->{create_suffix} ||=
		')';
}

=method determine_column_types

This method goes through the C<columns> and converts any scalar
column name to an arrayref of column name and C<default_column_type>.
It modifies itself and returns nothing.

=cut

sub determine_column_types {
	my ($self) = @_;
	my ($columns, $type) = ($self->columns, $self->{default_column_type});

	# reset each element to an arrayref if it isn't already
	foreach my $column ( @$columns ){
		ref $column
			or $column = [$column, $type];
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
	return "DROP $self->{table_type} TABLE " .
		$self->quote_identifier($self->name);
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
		$self->quote_identifier($self->name),
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
(C<name_prefix> concatenated together with C<name>).

=cut

sub name {
	my ($self) = @_;
	return $self->{name_prefix} . $self->{name};
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

=method quote_identifier

	my $quoted = $loader->quote_identifier($name);

Wrap the supplied string with C<identifier_quote>.

=cut

sub quote_identifier {
	my ($self, $identifier) = @_;
	return
		$self->{identifier_quote} .
		$identifier .
		$self->{identifier_quote};
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
* Use String::CamelCase::decamelize by default?
* Allow extra columns (like C<id>) to be added and/or generated
* Consider using something like L<Text::CSV::SQLhelper>
to guess appropriate data types for each column

=head1 SEE ALSO

* L<DBIx::TableLoader::CSV>

=cut
