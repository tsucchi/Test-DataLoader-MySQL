#!/usr/bin/perl -w
use strict;
use warnings;
use Test::More;

eval "use Test::mysqld 0.11";
plan skip_all => "Test::mysqld 0.11(or grator version) is need for test" if ( $@ );

plan tests => 7;
use Test::DataLoader::MySQL;

my $mysqld = Test::mysqld->new( my_cnf => {
                                  'skip-networking' => '',
                                }
                              );
my $dbh = DBI->connect($mysqld->dsn()) or die $DBI::errstr;

$dbh->do("CREATE TABLE foo (id INTEGER AUTO_INCREMENT, name VARCHAR(20), PRIMARY KEY(id))") || die $dbh->errstr;
$dbh->do("insert into foo set name='xxx'");

my $data = Test::DataLoader::MySQL->new($dbh);
$data->add('foo', 1,
           {
               name => 'aaa',
           },
           ['id']);
$data->add('foo', 2,
           {
               name => 'bbb',
           },
           ['id']);

my $keys;
$keys = $data->load('foo', 1);#load data #1
is( $keys->{id}, 2);
is_deeply( $data->_loaded, [['foo', {id=>2, name=>'aaa'}, ['id']]]);

$keys = $data->load('foo', 2);#load data #2
is( $keys->{id}, 3);
is_deeply( $data->_loaded, [ ['foo', {id=>2, name=>'aaa'}, ['id']],
                             ['foo', {id=>3, name=>'bbb'}, ['id']], ]);


is_deeply($data->do_select('foo', "id=2"), { id=>2, name=>'aaa'});
is_deeply([$data->do_select('foo', "id IN(2,3)")], [ { id=>2, name=>'aaa'},
                                                     { id=>3, name=>'bbb'},]);


$data->clear;

$data = Test::DataLoader::MySQL->new($dbh);
is_deeply($data->do_select('foo', "1=1"), { id=>1, name=>'xxx'});#remain only not loaded by Test::DataLoader::MySQL
$data->clear;

$mysqld->stop;
