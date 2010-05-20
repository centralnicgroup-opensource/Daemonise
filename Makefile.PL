use inc::Module::Install;

name     'Daemonise';
all_from 'lib/Daemonise.pm';
author   q{Lenz Gschwendtner <norbu09@cpan.org>};
license  'perl';

requires 'Mouse';
requires 'MouseX::Object::Pluggable';
requires 'Config::Any';
requires 'Unix::Syslog';


build_requires 'Test::More';

auto_install;

WriteAll;

