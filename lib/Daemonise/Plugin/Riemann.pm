package Daemonise::Plugin::Riemann;

use Mouse::Role;
use Riemann::Client;
use Scalar::Util qw(looks_like_number);
use Carp;

has 'riemann_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

has 'riemann_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 5555 },
);

has 'riemann_udp' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);

has 'riemann' => (
    is  => 'rw',
    isa => 'Riemann::Client',
);

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring Riemann plugin") if $self->debug;

    my $r;
    eval {
        $r = Riemann::Client->new(
            host    => $self->riemann_host,
            port    => $self->riemann_port,
            use_udp => $self->riemann_udp,
        );
    };
    if ($@) {
        $self->log("connect to riemann failed: $@");
        return;
    }
    else {
        $self->riemann($r);
    }

    return;
};

sub metric {
    my ($self, $service, $state, $metric, $desc) = @_;

    unless (ref(\$service) eq 'SCALAR'
        and ref(\$state)  eq 'SCALAR'
        and ref(\$metric) eq 'SCALAR')
    {
        carp 'missing mandatory parameter! '
            . 'usage: $d->metric($service, $state, $metric)';
        return;
    }

    unless (looks_like_number($metric)) {
        carp "metric has to be an integer or float";
        return;
    }

    if ($desc and ref(\$desc) ne 'SCALAR') {
        carp "description must be of SCALAR type";
        undef $desc;
    }

    if ($self->debug) {
        $self->log("[riemann] $service: $metric ($state)");
        return;
    }

    eval {
        $self->riemann->send({
                service     => $service,
                state       => $state,
                metric      => $metric,
                description => $desc || "metric for $service",
        });
    };
    carp "sending metric failed: $@" if $@;

    return;
}

1;
