package Test::DataLoader::MySQL;
use strict;
use warnings;
use DBI;
use DBD::mysql;
use Carp;
use base qw(Exporter);
our $VERSION = '0.0.3';
use 5.008;

=head1 NAME

Test::DataLoader::MySQL - Load testdata into MySQL database

=head1 SYNOPSIS

  my $data = Test::DataLoader::MySQL->new($dbh);
  $data->add('foo', #table name
             1,     # data id
             {# data_href: column => value
                 id => 1, 
                 name => 'aaa',
             },
             ['id']); # primary keys
  $data->add('foo', 2,
             {
                 id => 2,
                 name => 'bbb',
             },
             ['id']);
  $data->load('foo', 1); #load data into database
  # ... tests using database
  $data->clear;# when finished

if table has auto_increment

  data->add('foo', 1,
           {
               name => 'aaa',
           },
           ['id']);
  my $keys = $data->load('foo', 1);#load data and get auto_increment
  is( $keys->{id}, 2); # get key value(generated by auto_increment)
  # ... tests using database
  $data->clear;# when finished

read from external file

  # data.pm
  my $data = Test::DataLoader::MySQL->init(); # use init(not new)
  $data->add('foo', 1,
             {
                id => 1,
                name => 'aaa',
             },
             ['id']);
  # in your testcode
  my $data = Test::DataLoader::MySQL->new($dbh);
  $data->load('foo', 1);
  # ... tests using database
  $data->clear;# when finished

=head1 DESCRIPTION

Load testdata into MySQL database.

=cut

=head1 methods

=cut

my $singleton; #instance object is shared for reading data from external file.

=head2 new

create new instance
parameter $dbh(provided by DBI) is required;
If Keep option is NOT specified(default), loaded data is deleted when instance is destroyed, otherwise(specified Keep option) loaded data is remain.

  #$dbh = DBI->connect(...);

  my $data = Test::DataLoader::MySQL->new($dbh); # loaded data is deleted when $data is DESTROYed
  # or
  my $data = Test::DataLoader::MySQL->new($dbh, Keep => 1); # loaded data is remain

if you want to use external file and in external file, use init() instead of new().

=cut

sub new {
    my $class = shift;
    my ($dbh, %options) = @_;
    my $self = defined $singleton ? $singleton : {};

    $self = {
        dbh => $dbh,
        loaded => [],
        Keep => exists $options{Keep} ? $options{Keep} :  0,
    };

    bless $self, $class;
    $singleton = $self;
    return $self;
}

=head2 add

add testdata into this modules (not loading testdata)

  $data->add('foo',  # table_name
              1,     # data_id, 
              {      # data which you want to load into database. specified by hash_ref
                 id => 1,
                 name => 'aaa',
              },
              ['id'] #key(s), specified by array_ref, this is important.
              );

table_name and data_id is like a database's key. For example, table_name is 'foo' and data_id is 1 and 'foo' and 2 is dealt with defferent data even if contained data is equal( ex id=>1, name=>'aaa').

Key is important, because when $data is DESTROYed, this module delete all data which had been loaded and deleted data is found by specified key(s) in this method.

=cut

sub add {
    my $self = shift;
    my ($table_name, $data_id, $data_href, $key_aref) = @_;

    carp "already exists $table_name : $data_id" if ( exists $self->{data} &&
                                                      exists $self->{data}->{$table_name}->{$data_id} );
    $self->{data}->{$table_name}->{$data_id} = { data => $data_href, key => $key_aref };
}

=head2 load

load testdata from this module into database.

 $data->load('foo', 1);

first parameter is table_name, second parameter is data_id. meaning of them are same as specified in add-method.
third parameter is option href, if you want to alter data with add method. for example,

 $data->add('foo', 1, { id=>1, name=>'aaa' }); #registered name is 'aaa'
 $data->load('foo', 1, { name=>'bbb' });       #but loaded name is 'bbb' because option href is specified.

return hash_ref. it contains database key and value. this is useful for AUTO_INCREMENT key.

 my $key = $data->load('foo', 1);
 my $id = $key->{id};

=cut

sub load {
    my $self = shift;
    my ($table_name, $data_id, $option_href) = @_;
    my $dbh = $self->{dbh};
    my %data = %{$self->_data($table_name, $data_id)};
    croak("data not found $table_name : $data_id") if ( !%data );
    if ( defined $option_href ) {
        for my $key ( keys %{$option_href} ) {
            $data{$key} = $option_href->{$key};
        }
    }

    my $sth = $dbh->prepare($self->_insert_sql($table_name, $data_id));
    my $i=1;
    for my $column ( sort keys %data ) {
        $sth->bind_param($i++, $data{$column});
    }
    $sth->execute();
    $sth->finish;

    my $keys = $self->_set_loaded_keys($table_name, $data_id, \%data);

    push @{$self->{loaded}}, [$table_name, \%data, $self->_key($table_name, $data_id)];

    $dbh->do('commit');
    return $keys;
}

=head2 load_file

add data from external file

 $data->load_file('data.pm');

parameter is filename.

=cut

sub load_file {
    my $self = shift;
    my ( $filename ) = @_;
    require $filename;
    croak("can't read $filename") if ( $@ );
}

=head2 init

create new instance for external file

 my $data = Test::DataLoader::MySQL->init();
 #$data->add(...

=cut

sub init {
    my $class = shift;
    my $self = defined $singleton ? $singleton : { };
    bless $self, $class;
    $singleton = $self;
    return $self;
}

sub _set_loaded_keys {
    my $self = shift;
    my($table_name, $data_id, $data_href) = @_;
    my $dbh = $self->{dbh};

    my $result;
    for my $key ( @{$self->_key($table_name, $data_id)} ) {
        if ( !defined $data_href->{$key} ) { #for auto_increment
            my $sth = $dbh->prepare("select LAST_INSERT_ID() from dual");
            $sth->execute();
            my @id = $sth->fetchrow_array;
            $data_href->{$key} = $id[0];
        }
        $result->{$key} = $data_href->{$key};
    }
    return $result;
}


=head2 do_select

do select statement

  $data->do_select('foo', "id=1");

first parameter is table_name which you want to select. second parameter is where closure. Omitting second parameter is not allowed, if you want to use all data,  use condition which is aloways true such as "1=1".

=cut

sub do_select {
    my $self = shift;
    my ($table, $condition) = @_;
    my $dbh = $self->{dbh};
    croak( "Error: condition undefined" ) if !defined $condition;

    my $sth = $dbh->prepare("select * from $table where $condition");
    $sth->execute();

    my @result;
    while( my $item = $sth->fetchrow_hashref ) {
        push @result, $item;
    }
    $sth->finish();

    return @result if wantarray;
    return $result[0];
}

sub _insert_sql {
    my $self = shift;
    my ($table_name, $data_id) = @_;
    my $sql = sprintf("insert into %s set ", $table_name);
    $sql .= join(',', map { "$_=?" } sort keys %{$self->_data($table_name, $data_id)});
    return $sql;
}

sub _data {
    my $self = shift;
    my ($table_name, $data_id) = @_;

    return $self->{data}->{$table_name}->{$data_id}->{data};
}

sub _key {
    my $self = shift;
    my ($table_name, $data_id) = @_;
    return $self->{data}->{$table_name}->{$data_id}->{key};
}

sub _loaded {
    my $self = shift;
    return $self->{loaded};
}

sub DESTROY {
    my $self = shift;
    if ( @{$self->_loaded} ) {
        carp "clear was not called in $0";
        $self->clear if !$self->{Keep};
        $self->{loaded} = [];
    }

}

=head2 clear

clear all loaded data from database;

=cut

sub clear {
    my $self = shift;
    my $dbh = $self->{dbh};

    return if !defined $dbh;

    for my $loaded ( reverse @{$self->_loaded} ) {
        my $table = $loaded->[0];
        my %data = %{$loaded->[1]};
        my @keys = @{$loaded->[2]};
        my $condition = join(',', map { "$_=?" } @keys);

        my $sth = $dbh->prepare("delete from $table where $condition");
        my $i=1;
        for my $key ( @keys ) {
            $sth->bind_param($i++, $data{$key});
        }
        $sth->execute();
        $sth->finish;
    }
    $dbh->do('commit') if defined $dbh;
}

1;
__END__

=head1 AUTHOR

Takuya Tsuchida E<lt>tsucchi@cpan.orgE<gt>


=head1 REPOSITORY

L<http://github.com/tsucchi/Test-DataLoader-MySQL>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
