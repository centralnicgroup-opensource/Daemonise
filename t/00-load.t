#!perl -T

use Test::More;

BEGIN {
    use_ok( 'Daemonise' );
}

diag( "Testing Daemonise $Daemonise::VERSION, Perl $], $^X" );

ok (Daemonise->new(), 'Init Daemonise');

done_testing;
