use inc::Module::Install;

name     'Daemonise';
all_from 'lib/Daemonise.pm';
author   q{Lenz Gschwendtner <norbu09@cpan.org>};
license  'perl';

requires 'Mouse';
requires 'Mouse::Role';
requires 'Config::Any';
requires 'Unix::Syslog';
requires 'Store::CouchDB';
requires 'Net::RabbitMQ';
requires 'JSON';
requires 'LWP::UserAgent';
requires 'Data::Dumper';
requires 'DateTime';
requires 'Digest::MD5';
requires 'Redis';
requires 'Riemann::Client';
requires 'Scalar::Util';

recommends 'Tie::Syslog';

build_requires 'Test::More';

WriteAll;

