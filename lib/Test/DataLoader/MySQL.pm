package Test::DataLoader::MySQL;
use strict;
use warnings;
use DBI;

=head1 NAME

Test::DataLoader::MySQL - Load testdata into MySQL database

=head1 SYNOPSIS

write synopsis here ->atode

=head1 DESCRIPTION

write description here
=cut

=head1 methods

=cut

=head2 new

create new instance
parameter $dbh is needed;

=cut

sub new {
    my $class = shift;
    my ($dbh, %options) = @_;
    my $self = {
        dbh => $dbh,
        loaded => [],
        Keep => exists $options{Keep} ? $options{Keep} :  0,
    };
    bless $self, $class;
}

=head2 add

add testdata into this modules (not loading testdata)

=cut
sub add {
    my $self = shift;
    my ($table_name, $data_id, $data_href, $key_aref) = @_;
    $self->{data}->{$table_name}->{$data_id} = { data => $data_href, key => $key_aref };
}

=head2 load

load testdata from this module into database

=cut
sub load {
    my $self = shift;
    my ($table_name, $data_id) = @_;
    my $dbh = $self->{dbh};
    my %data = %{$self->_data($table_name, $data_id)};

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

    $self->_delete_loaded_data if ( !$self->{Keep} );
}

sub _delete_loaded_data {
    my $self = shift;
    my $dbh = $self->{dbh};
    for my $loaded ( @{$self->_loaded} ) {
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
    $dbh->do('commit');
}

1;
__END__

=head1 AUTHOR

Takuya Tsuchida E<lt>takuya.tsuchida@gmail.comE<gt>

=head1 SEE ALSO

write here if related module exists

=head1 REPOSITORY

http://github.com/tsucchi/Test-DataLoader-MySQL

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
