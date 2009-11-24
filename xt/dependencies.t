#!/usr/bin/perl
use strict;
use warnings;
use ExtUtils::MakeMaker;
use Test::Module::Used;

my $used = Test::Module::Used->new(
    exclude_in_testdir => ['Test::DataLoader::MySQL'],
);
$used->ok;
