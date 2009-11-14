#!/usr/bin/perl
use strict;
use warnings;
use ExtUtils::MakeMaker;
use Test::More;
use Test::Dependencies exclude => [qw/Test::Dependencies Test::DataLoader::MySQL Test::mysqld/],
                       style   => 'light' ;
ok_dependencies();
