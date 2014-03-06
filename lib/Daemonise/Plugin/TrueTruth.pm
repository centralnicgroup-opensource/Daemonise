package Daemonise::Plugin::TrueTruth;

use Mouse::Role;

# ABSTRACT: Daemonise plugin that wraps True::Truth reusing the KyotoTycoon plugin connection

use True::Truth 1.1;

BEGIN {
    with("Daemonise::Plugin::KyotoTycoon");
}

=head1 SYNOPSIS

    use Daemonise;
    use Data::Dump 'dump';
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('TrueTruth')
    $d->configure;
    
    $d->add_pending_truth(...);
    $d->persist_pending_truth(...);
    

=head1 ATTRIBUTES

=head2 truth

=cut

has 'truth' => (
    is  => 'rw',
    isa => 'True::Truth',
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self, $reconfig) = @_;

    $self->log("configuring TrueTruth plugin") if $self->debug;

    $self->truth(
        True::Truth->new(
            kt    => $self->tycoon,
            debug => $self->debug,
        ),
    );

    return;
};

# expose True::Truth methods as ours for convenience

=head2 add_true_truth

=cut

sub add_true_truth {
    my ($self, @args) = @_;
    return $self->truth->add_true_truth(@args);
}

=head2 add_pending_truth

=cut

sub add_pending_truth {
    my ($self, @args) = @_;
    return $self->truth->add_pending_truth(@args);
}

=head2 persist_pending_truth

=cut

sub persist_pending_truth {
    my ($self, @args) = @_;
    return $self->truth->persist_pending_truth(@args);
}

=head2 remove_pending_truth

=cut

sub remove_pending_truth {
    my ($self, @args) = @_;
    return $self->truth->remove_pending_truth(@args);
}

=head2 get_true_truth

=cut

sub get_true_truth {
    my ($self, @args) = @_;
    return $self->truth->get_true_truth(@args);
}

1;
