#!/usr/bin/perl -w
use strict;
use warnings;
use Test::More;
use DBI;

eval "use Test::mysqld 0.11";
plan skip_all => "Test::mysqld 0.11(or grator version) is need for test" if ( $@ );

my $mysqld = Test::mysqld->new( my_cnf => {
                                  'skip-networking' => '',
                                }
                              );
plan skip_all => "MySQL may not be installed" if ( !defined $mysqld );

plan tests => 4;

use Test::DataLoader::MySQL;

my $dbh = DBI->connect($mysqld->dsn()) or die $DBI::errstr;

$dbh->do("CREATE TABLE foo (id INTEGER, name VARCHAR(20))");


my $data = Test::DataLoader::MySQL->new($dbh);
$data->load_direct('foo',
           {
               id => 1,
               name => 'aaa',
           },
           ['id']);
$data->load_direct('foo',
           {
               id => 2,
               name => 'bbb',
           },
           ['id']);



is_deeply($data->do_select('foo', "id=1"), { id=>1, name=>'aaa'});
is_deeply([$data->do_select('foo', "id IN(1,2)")], [ { id=>1, name=>'aaa'},
                                                     { id=>2, name=>'bbb'},]);

# Test auto_increment
 $dbh->do("CREATE TABLE bar (id INTEGER AUTO_INCREMENT, name VARCHAR(20), PRIMARY KEY(id))") || die $dbh->errstr;
 my $key = $data->load_direct('bar',
            {
                name => 'ccc',
            },
            ['id']);
 is( $key->{id}, 1);
 is_deeply($data->do_select('bar', "id=1"), { id=>1, name=>'ccc'});

$data->clear;

$mysqld->stop;

