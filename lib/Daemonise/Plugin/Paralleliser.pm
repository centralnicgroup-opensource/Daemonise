package Daemonise::Plugin::Paralleliser;

use Mouse::Role;

# ABSTRACT: Daemonise plugin to parallelise certain tasks easily

use Parallel::ForkManager;

=head1 SYNOPSIS

    use Daemonise;
    use Data::Dump 'dump';
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('Paralleliser')
    $d->worker(5);
    $d->configure;
    
    my @input = ('kiwi', 'fern', 'maori', 'marae');
    $d->parallelise(\&loop, @input);
    
    sub loop {
        print dump(@_);
    }

=head1 ATTRIBUTES

=head2 worker

=cut

has 'worker' => (
    is      => 'rw',
    isa     => 'Int',
    default => sub { 5 },
);

=head2 pm

=cut

has 'pm' => (
    is  => 'rw',
    isa => 'Parallel::ForkManager',
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self, $reconfig) = @_;

    $self->log("configuring Paralleliser plugin") if $self->debug;

    return;
};

=head2 parallelise

=cut

sub parallelise {
    my ($self, $code, @input) = @_;

    $self->pm(Parallel::ForkManager->new($self->worker));

    foreach my $input (@input) {
        $self->pm->start and next;

        # reconfigure to setup eventually broken connections
        $self->configure(1);

        # do not respond to previously setup TERM and INT signal traps
        $SIG{TERM} = 'IGNORE';    ## no critic
        $SIG{INT}  = 'IGNORE';    ## no critic

        $code->($input);

        $self->pm->finish;
    }
    $self->pm->wait_all_children;

    return;
}

1;
