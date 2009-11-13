package t::util;
use strict;
use warnings;
use Carp;
use base qw(Exporter);
use Test::mysqld;

our @EXPORT = qw(dbh do_select);

our $mysqld;

BEGIN {
    $mysqld = Test::mysqld->new(
        my_cnf => {
            'skip-networking' => '',
        }
    );
}

sub dbh {
    return if !$mysqld;

    my $dbh = DBI->connect(
        $mysqld->dsn(),
    ) or die $DBI::errstr;
    return $dbh;
}

sub do_select {
    my ($dbh, $table, $condition) = @_;
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


1;
