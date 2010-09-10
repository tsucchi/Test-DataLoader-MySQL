#!/usr/bin/perl -w
use strict;
use warnings;
use Test::More;
use DBI;

eval "use Test::mysqld 0.11";
plan skip_all => "Test::mysqld 0.11(or grator version) is need for test" if ( $@ );

plan tests => 1;
use Test::DataLoader::MySQL;

my $mysqld = Test::mysqld->new( my_cnf => {
                                  'skip-networking' => '',
                                }
                              );
my $dbh = DBI->connect($mysqld->dsn()) or die $DBI::errstr;

$dbh->do("CREATE TABLE foo (id INTEGER, name VARCHAR(20))");


my $data = Test::DataLoader::MySQL->new($dbh);
$data->add('foo', 1,
           {
               id => 1,
               name => 'aaa',
           },
           ['id']);

$data->load('foo', 1, { name=>'bbb' });#load data #1 but name is altered to 'aaa'->'bbb'
is_deeply($data->do_select('foo', "id=1"), { id=>1, name=>'bbb'});

$data->clear;

$mysqld->stop;
