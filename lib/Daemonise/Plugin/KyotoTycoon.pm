package Daemonise::Plugin::KyotoTycoon;

use Mouse::Role;

# ABSTRACT: Daemonise KyotoTycoon plugin

use Cache::KyotoTycoon;
use Try::Tiny;
use MIME::Base64 qw(encode_base64 decode_base64);
use Storable qw/nfreeze thaw/;

BEGIN {
    with("Daemonise::Plugin::HipChat");
}


has 'tycoon_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);


has 'tycoon_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 1978 },
);


has 'tycoon_db' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 0 },
);


has 'tycoon_timeout' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 5 },
);


has 'cache_default_expire' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 600 },
);


has 'tycoon' => (
    is       => 'rw',
    isa      => 'Cache::KyotoTycoon',
    required => 1,
);


after 'configure' => sub {
    my ($self, $reconfig) = @_;

    $self->log("configuring KyotoTycoon plugin") if $self->debug;

    if (ref($self->config->{kyototycoon}) eq 'HASH') {
        foreach my $conf_key ('host', 'port', 'timeout') {
            my $attr = "tycoon_" . $conf_key;
            $self->$attr($self->config->{kyototycoon}->{$conf_key})
                if defined $self->config->{kyototycoon}->{$conf_key};
        }
        $self->cache_default_expire(
            $self->config->{kyototycoon}->{default_expire})
            if defined $self->config->{kyototycoon}->{default_expire};
    }

    $self->tycoon(
        Cache::KyotoTycoon->new(
            host    => $self->tycoon_host,
            port    => $self->tycoon_port,
            timeout => $self->tycoon_timeout,
            db      => $self->tycoon_db,
        ));

    # don't try to lock() again when reconfiguring
    return if $reconfig;

    # lock cron for 24hours
    if ($self->is_cron) {
        my $expire = $self->cache_default_expire;
        $self->cache_default_expire(24 * 60 * 60);
        die 'locking failed' unless $self->lock;
        $self->cache_default_expire($expire);
    }

    return;
};


sub cache_get {
    my ($self, $key) = @_;

    my ($value, $expire);

    if (wantarray) {
        ($value, $expire) = $self->tycoon->get($key);
        return unless defined $value;
        return (thaw(decode_base64($value)), $expire);
    }
    else {
        $value = $self->tycoon->get($key);
        return unless defined $value;
        return thaw(decode_base64($value));
    }

    return;
}


sub cache_set {
    my ($self, $key, $data, $expire) = @_;

    return unless ref $data;

    $self->tycoon->set(
        $key,
        encode_base64(nfreeze($data)),
        ($expire || $self->cache_default_expire));

    return 1;
}


sub cache_del {
    my ($self, $key) = @_;

    return $self->tycoon->remove($key);
}


sub lock {    ## no critic (ProhibitBuiltinHomonyms)
    my ($self, $thing, $lock_value) = @_;

    unless (ref \$thing eq 'SCALAR') {
        $self->log("locking failed: first argument is not of type SCALAR");
        return;
    }

    if (defined $lock_value) {
        unless (ref \$lock_value eq 'SCALAR') {
            $self->log('locking failed: second argument is not of type SCALAR');
            return;
        }
    }

    my $lock = $thing || $self->name;

    # fallback to PID for the lock value
    $lock_value //= $$;

    if (my $value = $self->tycoon->get($lock)) {
        if ($value eq $lock_value) {
            $self->tycoon->replace($lock, $lock_value,
                $self->cache_default_expire);
            $self->log("lock=$lock locking time extended for $value")
                if $self->debug;
            return 1;
        }
        else {
            $self->notify("lock=$lock cannot acquire lock hold by $value");
            return;
        }
    }
    else {
        $self->tycoon->set($lock, $lock_value, $self->cache_default_expire);
        $self->log("lock=$lock lock acquired") if $self->debug;
        return 1;
    }
}


sub unlock {
    my ($self, $thing, $lock_value) = @_;

    unless (ref \$thing eq 'SCALAR') {
        $self->log("locking failed: first argument is not of type SCALAR");
        return;
    }

    if (defined $lock_value) {
        unless (ref \$lock_value eq 'SCALAR') {
            $self->log('locking failed: second argument is not of type SCALAR');
            return;
        }
    }

    my $lock = $thing || $self->name;

    # fallback to PID for the lock value
    $lock_value //= $$;

    if (my $value = $self->tycoon->get($lock)) {
        if ($value eq $lock_value) {
            $self->tycoon->remove($lock);
            $self->log("lock=$lock lock released") if $self->debug;
            return 1;
        }
        else {
            $self->log("lock=$lock lock hold by $value, permission denied");
            return;
        }
    }
    else {
        $self->log("lock=$lock lock was already released") if $self->debug;
        return 1;
    }
}


before 'stop' => sub {
    my ($self) = @_;

    $self->unlock if $self->is_cron;

    return;
};

sub DESTROY {
    my ($self) = @_;

    return unless ref $self;
    return unless $self->{is_cron};

    # the Cache::KyotoTycoon object was already destroyed to we need to create
    # a new one with the existing config
    $self->tycoon(
        Cache::KyotoTycoon->new(
            host    => $self->tycoon_host,
            port    => $self->tycoon_port,
            timeout => $self->tycoon_timeout,
            db      => $self->tycoon_db,
        ));

    $self->unlock;

    return;
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise::Plugin::KyotoTycoon - Daemonise KyotoTycoon plugin

=head1 VERSION

version 1.86

=head1 SYNOPSIS

This plugin conflicts with other plugins that provide caching, like the Redis plugin.

    use Daemonise;
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('KyotoTycoon');
    
    $d->configure;
    
    # get a key
    my $value = $d->tycoon->get("some_key");
    
    # set a key and expire (see Cache::KyotoTycoon module for more)
    $d->tycoon->set("key", "value", 600);
    
    # allow only one instance of this deamon to run at a time
    $d->lock;
    
    # when you are done with mission critical single task stuff
    $d->unlock;

=head1 ATTRIBUTES

=head2 tycoon_host

=head2 tycoon_port

=head2 tycoon_db

=head2 tycoon_connect_timeout

=head2 cache_default_expire

=head2 tycoon

=head1 SUBROUTINES/METHODS provided

=head2 configure

=head2 cache_get

retrieve, base64 decode and thaw complex data from KyotoTycoon

=head2 cache_set

freeze, base64 encode and store complex data in KyotoTycoon

=head2 cache_del

delete KyotoTycoon key

=head2 lock

=head2 unlock

=head2 stop

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
