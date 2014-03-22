package Daemonise::Plugin::Riemann;

use Mouse::Role;

# ABSTRACT: Daemonise Riemann plugin

use Riemann::Client;
use Scalar::Util qw(looks_like_number);
use Carp;

=head1 SYNOPSIS

This plugin conflicts with other plugins that provide graphing, like the Graphite plugin.

    use Daemonise;
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('Riemann');
    
    $d->configure;
    
    # send a metric to riemann server
    # (service, state, metric, optional description)
    $d->graph("interwebs", "slow", 1.4, "MB/s");


=head1 ATTRIBUTES

=head2 riemann_host

=cut

has 'riemann_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

=head2 riemann_port

=cut

has 'riemann_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 5555 },
);

=head2 riemann_udp

=cut

has 'riemann_udp' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);

=head2 riemann

=cut

has 'riemann' => (
    is  => 'rw',
    isa => 'Riemann::Client',
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self, $reconfig) = @_;

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

=head2 graph

=cut

sub graph {
    my ($self, $service, $state, $metric, $desc) = @_;

    unless (ref \$service eq 'SCALAR'
        and defined $service
        and ref \$state eq 'SCALAR'
        and defined $state
        and ref \$metric eq 'SCALAR'
        and defined $metric)
    {
        carp 'missing or wrong type of mandatory argument! '
            . 'usage: $d->graph("$service", "$state", $metric)';
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

    # $self->async and return;

    eval {
        $self->riemann->send({
            service     => $service,
            state       => $state,
            metric      => $metric,
            description => $desc || "metric for $service",
        });
    };
    carp "sending metric failed: $@" if $@;

    # exit; # async
    return;
}

1;
