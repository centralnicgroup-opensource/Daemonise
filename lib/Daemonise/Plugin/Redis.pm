package Daemonise::Plugin::Redis;

use Mouse::Role;
use Redis;

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

has 'redis_default_expire' => (
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

sub lock {
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
