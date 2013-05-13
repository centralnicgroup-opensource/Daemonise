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
    
    # set a key and expire (see Redis perl module for more)
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

    foreach my $conf_key ('host', 'port', 'connect_timeout', 'connect_rate') {
        $self->redis_host($self->config->{redis}->{$conf_key})
            if exists $self->config->{redis}->{$conf_key};
    }

    $self->redis(
        Redis->new(
            server    => $self->redis_host . ':' . $self->redis_port,
            reconnect => $self->redis_connect_timeout,
            every     => $self->redis_connect_rate,
            debug     => $self->debug,
        ));
};

=head2 lock

=cut

sub lock {    ## no critic (ProhibitBuiltinHomonyms)
    my ($self) = @_;

    if (my $pid = $self->redis->get($self->name)) {
        if ($pid == $$) {
            $self->redis->expire($self->name, $self->redis_default_expire);
            $self->log("lock still acquired") if $self->debug;
            return 1;
        }
        else {
            $self->log("cannot acquire lock hold by $pid");
            return;
        }
    }
    else {
        $self->redis->set($self->name => $$);
        $self->redis->expire($self->name, $self->redis_default_expire);
        $self->log("lock acquired") if $self->debug;
        return 1;
    }
}

=head2 unlock

=cut

sub unlock {
    my ($self) = @_;

    if (my $pid = $self->redis->get($self->name)) {
        if ($pid == $$) {
            $self->redis->del($self->name);
            $self->log("lock released") if $self->debug;
            return 1;
        }
        else {
            $self->log("lock hold by pid $pid, cannot remove");
            return;
        }
    }
    else {
        $self->log("lock was already released") if $self->debug;
        return 1;
    }
}

1;
