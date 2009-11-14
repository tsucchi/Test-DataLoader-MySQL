#!/usr/bin/perl -w
use strict;
use warnings;
use Test::More;

eval "use Test::mysqld";
plan skip_all => "Test::mysqld is need for test" if ( $@ );

use Test::DataLoader::MySQL;

my $mysqld = Test::mysqld->new( my_cnf => {
                                  'skip-networking' => '',
                                }
                              );
my $dbh = DBI->connect($mysqld->dsn()) or die $DBI::errstr;

$dbh->do("CREATE TABLE foo (id INTEGER, name VARCHAR(20))");


my $data = Test::DataLoader::MySQL->new($dbh);
$data->load_file('t/testdata.pm');

$data->load('foo', 1);#load data #1
$data->load('foo', 2);#load data #2

is_deeply($data->do_select('foo', "id=1"), { id=>1, name=>'aaa'});
is_deeply([$data->do_select('foo', "id IN(1,2)")], [ { id=>1, name=>'aaa'},
                                                     { id=>2, name=>'bbb'},]);



# if $data::DESTOROY is called, data is deleted
$data = undef;#DESTOROY

$data = Test::DataLoader::MySQL->new($dbh);
is($data->do_select('foo', "1=1"), undef);

done_testing();
