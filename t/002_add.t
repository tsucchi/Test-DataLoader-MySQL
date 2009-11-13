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

my $data = Test::DataLoader::MySQL->new($dbh);
$data->add('foo', 1,
           {
               id => 1,
               name => 'aaa',
           },
           ['id']);
is($data->_insert_sql('foo', 1), "insert into foo set id=?,name=?");
$data->load('foo', 1);



my $ref = do_select($dbh, 'foo', "id=1");
is_deeply($ref, { id=>1, name=>'aaa'});
done_testing();
