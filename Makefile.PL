use inc::Module::Install;
name           'Test-DataLoader-MySQL';
author         'Takuya Tsuchida takuya.tsuchida@gmail.com';
version        '0.0.1_01';
license        'perl';
#perl_version   '5.008';
all_from       'lib/Test/DataLoader/MySQL.pm';
requires       'DBI';
test_requires  'Test::More'  => '0.88';   #want to use done_testing()
WriteAll;
