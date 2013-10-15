package Daemonise::Plugin::Redis;

use Mouse::Role;

# ABSTRACT: Daemonise Redis plugin

use Redis;

=head1 SYNOPSIS

This plugin conflicts with other plugins that provide caching, like the KyotoTycoon plugin.

    use Daemonise;
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('Redis');
    
    $d->configure;
    
    # get a redis key
    my $value = $d->redis->get("some_key");
    
    # set a key and expire (see Redis module for more)
    $d->redis->set(key => "value");
    $d->redis->expire(key, 600);
    
    # allow only one instance of this deamon to run at a time
    $d->lock;
    
    # when you are done with mission critical single task stuff
    $d->unlock;


=head1 ATTRIBUTES

=head2 redis_host

=cut

has 'redis_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

=head2 redis_port

=cut

has 'redis_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 6379 },
);

=head2 redis_connect_timeout

=cut

has 'redis_connect_timeout' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 2 },
);

=head2 redis_connect_rate

=cut

has 'redis_connect_rate' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 500 },
);

=head2 redis_default_expire

=cut

has 'redis_default_expire' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 600 },
);

=head2 redis

=cut

has 'redis' => (
    is       => 'rw',
    isa      => 'Redis',
    required => 1,
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring Redis plugin") if $self->debug;

    if (ref($self->config->{redis}) eq 'HASH') {
        foreach my $conf_key (
            'host',            'port',
            'connect_timeout', 'connect_rate',
            'default_expire'
            )
        {
            my $attr = "redis_" . $conf_key;
            $self->$attr($self->config->{redis}->{$conf_key})
                if defined $self->config->{redis}->{$conf_key};
        }
    }

    $self->redis(
        Redis->new(
            server    => $self->redis_host . ':' . $self->redis_port,
            reconnect => $self->redis_connect_timeout,
            every     => $self->redis_connect_rate,
            debug     => $self->debug,
        ));

    return;
};

=head2 cache_get

retrieve, base64 decode and thaw complex data from Redis

=cut

sub cache_get {
    my ($self, $key) = @_;

    my $value = $self->redis->get($key);

    return unless defined $value;
    return thaw(decode_base64($value));
}

=head2 cache_set

freeze, base64 encode and store complex data in Redis

=cut

sub cache_set {
    my ($self, $key, $data, $expire) = @_;

    $self->redis->set($key => encode_base64(nfreeze($data)));
    $self->redis->expire($key, ($expire || $self->tycoon_default_expire));

    return 1;
}

=head2 cache_del

delete Redis key

=cut

sub cache_del {
    my ($self, $key);

    return $self->redis->del($key);
}

=head2 lock

=cut

sub lock {    ## no critic (ProhibitBuiltinHomonyms)
    my ($self, $thing) = @_;

    unless (ref \$thing eq 'SCALAR') {
        $self->log("locking failed: argument is not of type SCALAR");
        return;
    }

    my $lock = $thing || $self->name;

    if (my $pid = $self->redis->get($lock)) {
        if ($pid == $$) {
            $self->redis->expire($lock, $self->redis_default_expire);
            $self->log("locking time extended") if $self->debug;
            return 1;
        }
        else {
            $self->log("cannot acquire lock hold by $pid");
            return;
        }
    }
    else {
        $self->redis->set($lock => $$);
        $self->redis->expire($lock, $self->redis_default_expire);
        $self->log("lock acquired") if $self->debug;
        return 1;
    }
}

=head2 unlock

=cut

sub unlock {
    my ($self, $thing) = @_;

    unless (ref \$thing eq 'SCALAR') {
        $self->log("locking failed: argument is not of type SCALAR");
        return;
    }

    my $lock = $thing || $self->name;

    if (my $pid = $self->redis->get($lock)) {
        if ($pid == $$) {
            $self->redis->del($lock);
            $self->log("lock released") if $self->debug;
            return 1;
        }
        else {
            $self->log("lock hold by pid $pid, permission denied");
            return;
        }
    }
    else {
        $self->log("lock was already released") if $self->debug;
        return 1;
    }
}

1;
