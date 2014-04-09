package Daemonise::Plugin::TrueTruth;

use Mouse::Role;

# ABSTRACT: Daemonise plugin that wraps True::Truth reusing the KyotoTycoon plugin connection

use True::Truth 1.1;

BEGIN {
    with("Daemonise::Plugin::KyotoTycoon");
}


has 'truth' => (
    is  => 'rw',
    isa => 'True::Truth',
);


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


sub add_true_truth {
    my ($self, @args) = @_;
    return $self->truth->add_true_truth(@args);
}


sub add_pending_truth {
    my ($self, @args) = @_;
    return $self->truth->add_pending_truth(@args);
}


sub persist_pending_truth {
    my ($self, @args) = @_;
    return $self->truth->persist_pending_truth(@args);
}


sub remove_pending_truth {
    my ($self, @args) = @_;
    return $self->truth->remove_pending_truth(@args);
}


sub get_true_truth {
    my ($self, @args) = @_;
    return $self->truth->get_true_truth(@args);
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise::Plugin::TrueTruth - Daemonise plugin that wraps True::Truth reusing the KyotoTycoon plugin connection

=head1 VERSION

version 1.79

=head1 SYNOPSIS

    use Daemonise;
    
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

=head1 SUBROUTINES/METHODS provided

=head2 configure

=head2 add_true_truth

=head2 add_pending_truth

=head2 persist_pending_truth

=head2 remove_pending_truth

=head2 get_true_truth

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
