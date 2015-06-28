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

=cut

has 'tycoon_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

=head2 tycoon_port

=cut

has 'tycoon_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 1978 },
);

=head2 tycoon_db

=cut

has 'tycoon_db' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 0 },
);

=head2 tycoon_connect_timeout

=cut

has 'tycoon_timeout' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 5 },
);

=head2 cache_default_expire

=cut

has 'cache_default_expire' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 600 },
);

=head2 tycoon

=cut

has 'tycoon' => (
    is       => 'rw',
    isa      => 'Cache::KyotoTycoon',
    required => 1,
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

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

=head2 cache_get

retrieve, base64 decode and thaw complex data from KyotoTycoon

=cut

sub cache_get {
    my ($self, $key) = @_;

    my ($value, $expire) = $self->tycoon->get($key);

    return unless defined $value;

    my $data = $value;

    # thaw and decode if it looks like a BASE64 encoding
    $data = thaw(decode_base64($value)) if $value =~ m/^[a-zA-Z0-9\n]+=?=$/s;

    return ($data, $expire) if wantarray;
    return $data;
}

=head2 cache_set

freeze, base64 encode and store complex data in KyotoTycoon

=cut

sub cache_set {
    my ($self, $key, $data, $expire) = @_;

    # if $data is a scalar we can just store it as is
    my $scalar = $data;

    # otherwise freeze and encode
    $scalar = encode_base64(nfreeze($data)) if ref $data;

    $expire //= $self->cache_default_expire;
    $self->tycoon->set($key, $scalar, $expire);

    return 1;
}

=head2 cache_del

delete KyotoTycoon key

=cut

sub cache_del {
    my ($self, $key) = @_;

    return $self->tycoon->remove($key);
}

=head2 lock

=cut

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

=head2 unlock

=cut

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

=head2 stop

=cut

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
