package Daemonise::Plugin::KyotoTycoon;

use Mouse::Role;

# ABSTRACT: Daemonise KyotoTycoon plugin

use Cache::KyotoTycoon;
use Try::Tiny;
use MIME::Base64 qw(encode_base64 decode_base64);
use Storable qw/nfreeze thaw/;

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

=head2 tycoon_default_expire

=cut

has 'tycoon_default_expire' => (
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
    my ($self) = @_;

    $self->log("configuring KyotoTycoon plugin") if $self->debug;

    foreach my $conf_key ('host', 'port', 'timeout', 'default_expire') {
        my $attr = "tycoon_" . $conf_key;
        $self->$attr($self->config->{kyoto_tycoon}->{$conf_key})
            if exists $self->config->{kyoto_tycoon}->{$conf_key};
    }

    $self->tycoon(
        KyotoTycoon->new(
            host    => $self->tycoon_host,
            port    => $self->tycoon_port,
            timeout => $self->tycoon_timeout,
            db      => $self->tycoon_db,
        ));
};

=head2 cache_get

retrieve, base64 decode and thaw complex data from KyotoTycoon

=cut

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

=head2 cache_set

freeze, base64 encode and store complex data in KyotoTycoon

=cut

sub cache_set {
    my ($self, $key, $data, $expire) = @_;

    $self->tycoon->set(
        $key,
        encode_base64(nfreeze($data)),
        ($expire || $self->tycoon_default_expire));

    return 1;
}

=head2 cache_del

delete KyotoTycoon key

=cut

sub cache_del {
    my ($self, $key);

    return $self->tycoon->remove($key);
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

    if (my $pid = $self->tycoon->get($lock)) {
        if ($pid == $$) {
            $self->tycoon->replace($lock, $$, $self->tycoon_default_expire);
            $self->log("locking time extended") if $self->debug;
            return 1;
        }
        else {
            $self->log("cannot acquire lock hold by $pid");
            return;
        }
    }
    else {
        $self->tycoon->set($lock, $$, $self->tycoon_default_expire);
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

    if (my $pid = $self->tycoon->get($lock)) {
        if ($pid == $$) {
            $self->tycoon->remove($lock);
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
