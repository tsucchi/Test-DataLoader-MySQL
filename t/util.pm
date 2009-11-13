package t::util;
use strict;
use warnings;
use Carp;
use base qw(Exporter);
use Test::mysqld;

our @EXPORT = qw(dbh do_select);

our $mysqld;

sub dbh {
    $mysqld = Test::mysqld->new(
        my_cnf => {
            'skip-networking' => '',
        }
    );
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
    my $ref = $sth->fetchrow_hashref;
    return $ref;
}

1;
