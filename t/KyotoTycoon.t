use Test::More;
use Test::Deep;

use Daemonise;

my $d = Daemonise->new(no_daemon => 1);
$d->load_plugin('KyotoTycoon');
$d->configure;

plan qw/no_plan/;

{
    my $ret = $d->cache_set('key', 'scalar value');
    ok($ret, 'storing non ref data');

    my $time = time + $d->cache_default_expire;
    ($ret, my $exp) = $d->cache_get('key');
    is($exp, $time, 'default expire time was set');
    is($ret, 'scalar value', 'retrieving non ref data');

    $ret = $d->cache_set('key', { complex => 'structure' });
    ok($ret, 'storing complex data');

    $ret = $d->cache_get('key');
    cmp_deeply($ret, { complex => 'structure' }, 'retrieving complex data');

    $ret = $d->cache_del('key');
    ok($ret, 'deleting cache key');
}
