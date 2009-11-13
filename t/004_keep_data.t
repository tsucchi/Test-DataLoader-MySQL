#!/usr/bin/perl -w
use strict;
use warnings;
use Test::More;

eval { use Test::mysqld };
plan skip_all => "Test::mysqld is need for test" if ( $@ );

use t::util;
use Test::DataLoader::MySQL;

my $dbh = dbh() || die $Test::mysqld::errstr;;

$dbh->do("CREATE TABLE foo (id INTEGER, name VARCHAR(20))");
$dbh->do("insert into foo set id=0,name='xxx'");

my $data = Test::DataLoader::MySQL->new($dbh, Keep => 1);#Keep option specified
$data->add('foo', 1,
           {
               id => 1,
               name => 'aaa',
           },
           ['id']);
$data->add('foo', 2,
           {
               id => 2,
               name => 'bbb',
           },
           ['id']);

$data->load('foo', 1);#load data #1
$data->load('foo', 2);#load data #2

# if $data::DESTOROY is called, data is deleted
$data = undef;#DESTOROY
my $expected = [
    { id=>0, name=>'xxx'},
    { id=>1, name=>'aaa'},
    { id=>2, name=>'bbb'},
];
is_deeply([do_select($dbh, 'foo', "1=1")], $expected);#remain all data because Keep option specified

done_testing();
