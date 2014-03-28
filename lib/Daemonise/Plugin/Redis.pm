package Daemonise::Plugin::Redis;

use Mouse::Role;

# ABSTRACT: Daemonise Redis plugin

use Redis;

BEGIN {
    with("Daemonise::Plugin::HipChat");
}


has 'redis_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);


has 'redis_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 6379 },
);


has 'redis_connect_timeout' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 2 },
);


has 'redis_connect_rate' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 500 },
);


has 'cache_default_expire' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 600 },
);


has 'redis' => (
    is       => 'rw',
    isa      => 'Redis',
    required => 1,
);


after 'configure' => sub {
    my ($self, $reconfig) = @_;

    if ($reconfig) {
        $self->log("closing redis connection") if $self->debug;
        $self->redis->quit;
    }

    $self->log("configuring Redis plugin") if $self->debug;

    if (ref($self->config->{redis}) eq 'HASH') {
        foreach my $conf_key ('host', 'port', 'connect_timeout', 'connect_rate')
        {
            my $attr = "redis_" . $conf_key;
            $self->$attr($self->config->{redis}->{$conf_key})
                if defined $self->config->{redis}->{$conf_key};
        }
        $self->cache_default_expire($self->config->{redis}->{default_expire})
            if defined $self->config->{redis}->{default_expire};
    }

    $self->redis(
        Redis->new(
            server    => $self->redis_host . ':' . $self->redis_port,
            reconnect => $self->redis_connect_timeout,
            every     => $self->redis_connect_rate,
            debug     => $self->debug,
        ));

    # lock cron for 24hours
    if ($self->is_cron) {
        my $expire = $self->cache_default_expire;
        $self->cache_default_expire(24 * 60 * 60);
        die unless $self->lock;
        $self->cache_default_expire($expire);
    }

    return;
};


sub cache_get {
    my ($self, $key) = @_;

    my $value = $self->redis->get($key);

    return unless defined $value;
    return thaw(decode_base64($value));
}


sub cache_set {
    my ($self, $key, $data, $expire) = @_;

    $self->redis->set($key => encode_base64(nfreeze($data)));
    $self->redis->expire($key, ($expire || $self->cache_default_expire));

    return 1;
}


sub cache_del {
    my ($self, $key);

    return $self->redis->del($key);
}


sub lock {    ## no critic (ProhibitBuiltinHomonyms)
    my ($self, $thing) = @_;

    unless (ref \$thing eq 'SCALAR') {
        $self->log("locking failed: argument is not of type SCALAR");
        return;
    }

    my $lock = $thing || $self->name;

    if (my $pid = $self->redis->get($lock)) {
        if ($pid == $$) {
            $self->redis->expire($lock, $self->cache_default_expire);
            $self->log("locking time extended") if $self->debug;
            return 1;
        }
        else {
            $self->notify("cannot acquire lock hold by PID $pid");
            return;
        }
    }
    else {
        $self->redis->set($lock => $$);
        $self->redis->expire($lock, $self->cache_default_expire);
        $self->log("lock acquired") if $self->debug;
        return 1;
    }
}


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

sub DESTROY {
    my ($self) = @_;

    return if (${^GLOBAL_PHASE} eq 'DESTRUCT');

    $self->unlock if $self->is_cron;

    return;
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise::Plugin::Redis - Daemonise Redis plugin

=head1 VERSION

version 1.74

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

=head2 redis_port

=head2 redis_connect_timeout

=head2 redis_connect_rate

=head2 cache_default_expire

=head2 redis

=head1 SUBROUTINES/METHODS provided

=head2 configure

=head2 cache_get

retrieve, base64 decode and thaw complex data from Redis

=head2 cache_set

freeze, base64 encode and store complex data in Redis

=head2 cache_del

delete Redis key

=head2 lock

=head2 unlock

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
