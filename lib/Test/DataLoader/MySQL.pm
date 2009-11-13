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
    my ($dbh) = @_;
    my $self = {
        dbh => $dbh,
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

    my $sql = sprintf( $self->_insert_sql($table_name, $data_id) );
    my $sth = $dbh->prepare($sql);

    my $i=1;
    my %data = %{$self->_data($table_name, $data_id)};
    for my $column ( sort keys %data ) {
        $sth->bind_param($i++, $data{$column});
    }
    $sth->execute();
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

1;
__END__

=head1 AUTHOR

Takuya Tsuchida E<lt>takuya.tsuchida@gmail.comE<gt>

=head1 SEE ALSO

write here if related module exists

=head1 REPOSITORY

write source code repository

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
