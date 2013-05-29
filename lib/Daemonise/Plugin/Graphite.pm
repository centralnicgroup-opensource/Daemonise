package Daemonise::Plugin::Graphite;

use Mouse::Role;

# ABSTRACT: Daemonise Graphite plugin

use Net::Graphite;
use Scalar::Util qw(looks_like_number);
use Carp;

=head1 SYNOPSIS

This plugin conflicts with other plugins that provide graphing, like the Riemann plugin.

Example:

    use Daemonise;
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('Graphite');
    
    $d->configure;
    
    # send a metric to graphite server
    # (service, state, metric, optional description)
    $d->graph("interwebs", "slow", 1.4, "MB/s");


=head1 ATTRIBUTES

=head2 graphite_host

=cut

has 'graphite_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

=head2 graphite_port

=cut

has 'graphite_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 2003 },
);

=head2 graphite_udp

=cut

has 'graphite_proto' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'tcp' },
);

=head2 graphite

=cut

has 'graphite' => (
    is  => 'rw',
    isa => 'Net::Graphite',
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring Graphite plugin") if $self->debug;

    $self->graphite(
        Net::Graphite->new(
            host            => $self->graphite_host,
            port            => $self->graphite_port,
            proto           => $self->graphite_proto,
            fire_and_forget => 1,
        ));

    return;
};

=head2 graph

=cut

sub graph {
    my ($self, $service, $state, $metric, $desc) = @_;

    unless (ref \$service eq 'SCALAR'
        and ref \$metric eq 'SCALAR')
    {
        carp 'missing or wrong type of mandatory argument! '
            . 'usage: $d->graph("$service", "$state", $metric, "$desc")';
        return;
    }

    unless ($service =~ m/\./) {
        carp 'service has to have at least one namespace! '
            . 'e.g.: "site.graph.thing"';
        return;
    }

    unless (looks_like_number($metric)) {
        carp "metric has to be an integer or float";
        return;
    }

    if ($self->debug) {
        $self->log("[graphite] $service: $metric");
        return;
    }

    eval { $self->graphite->send({ path => $service, value => $metric }); };
    carp "sending metric failed: $@" if $@;

    return;
}

1;
