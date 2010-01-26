#!/usr/bin/perl -Ilib

use strict;
use warnings;
use Daemonise;
use Data::Dumper;

my $d = Daemonise->new();
$d->pid_file('/var/run/test-daemon.pid');
#$d->user('lenz');
#$d->group('staff');
$d->config_file('t/test.conf');
$d->configure;
$d->name('test daemon');
print Dumper($d->config);

print "User: ".$d->user."\n";

$d->load_plugin("CouchDB");
$d->couch_port(5985);

print Dumper $d->lookup('iwmn/defaults/url');

$d->daemonise;

while(1){
    sleep 10;
    print STDERR "blubb\n";
}
