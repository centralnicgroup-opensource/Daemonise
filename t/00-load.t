#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Daemonise' );
}

diag( "Testing Daemonise $Daemonise::VERSION, Perl $], $^X" );
