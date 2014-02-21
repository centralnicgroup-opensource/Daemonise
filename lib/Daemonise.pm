package Daemonise;

use Mouse;
use FindBin qw($Bin);
use lib "$Bin/../lib";

# ABSTRACT: Daemonise - a general daemoniser for anything...

our $VERSION = '1.62'; # VERSION

use Unix::Syslog;
use Config::Any;
use POSIX qw(strftime SIGTERM SIG_BLOCK SIG_UNBLOCK);


has 'name' => (
    is        => 'rw',
    default   => sub { 'daemon' },
    predicate => 'has_name',
);


has 'hostname' => (
    is      => 'ro',
    isa     => 'Str',
    lazy    => 1,
    default => sub { my $h = `hostname -a`; chomp $h; $h },
);


has 'config_file' => (
    is        => 'rw',
    isa       => "Str",
    predicate => 'has_config_file',
);


has 'config' => (
    is      => 'rw',
    isa     => 'HashRef',
    lazy    => 1,
    default => sub { {} },
);


has 'debug' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);


has 'start_time' => (
    is      => 'rw',
    isa     => 'Int',
    default => sub { time },
);


has 'is_cron' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);


has 'cache_plugin' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'KyotoTycoon' },
);

after 'new' => sub {
    my ($class, %args) = @_;

    # backwards compatibility
    with('Daemonise::Plugin::Daemon')
        unless ($args{no_daemon} or $args{is_cron});

    # load cache plugin required for cron locking
    if ($args{is_cron}) {
        return if %Daemonise::Plugin::KyotoTycoon::;
        my $cache_plugin =
            "Daemonise::Plugin::" . ($args{cache_plugin} || 'KyotoTycoon');
        with($cache_plugin);
    }

    return;
};


sub load_plugin {
    my ($self, $plugin) = @_;

    my $plug = 'Daemonise::Plugin::' . $plugin;
    $self->log("loading $plugin plugin") if $self->debug;
    with($plug);

    return;
}


sub configure {
    my ($self, $reconfig) = @_;

    ### install a signal handler as anchor for clean shutdowns in plugins
    $SIG{TERM} = sub { $self->stop };    ## no critic
    $SIG{INT}  = sub { $self->stop };    ## no critic

    unless ($self->has_config_file) {
        $self->log("config_file unset, nothing to configure");
        return;
    }

    unless (-e $self->config_file) {
        $self->log(
            "ERROR: config file '" . $self->config_file . "' does not exist!");
        return;
    }

    my $conf = Config::Any->load_files({
            files       => [ $self->config_file ],
            use_ext     => 1,
            driver_args => { General => { -InterPolateEnv => 1 } },
    });
    $conf = $conf->[0]->{ $self->config_file } if $conf;

    $self->config($conf);

    return;
}


sub async {
    my ($self) = @_;

    ### block signal for fork
    my $sigset = POSIX::SigSet->new(SIGTERM);
    POSIX::sigprocmask(SIG_BLOCK, $sigset)
        or die "Can't block SIGTERM for fork: [$!]\n";

    ### fork off a child
    my $pid = fork;
    unless (defined $pid) {
        die "Couldn't fork: [$!]\n";
    }

    ### make SIGTERM kill us as it did before
    local $SIG{TERM} = 'DEFAULT';

    ### put back to normal
    POSIX::sigprocmask(SIG_UNBLOCK, $sigset)
        or die "Can't unblock SIGTERM for fork: [$!]\n";

    return $pid;
}


sub log {    ## no critic (ProhibitBuiltinHomonyms)
    my ($self, $msg) = @_;

    chomp($msg);

    # escape newlines when not running in debug mode for log parser convenience
    $msg =~ s/\n/\\n/gs unless $self->debug;

    Unix::Syslog::openlog('Daemonise', Unix::Syslog::LOG_PID,
        Unix::Syslog::LOG_USER);
    Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(),
        'queue=%s %s', $self->name, $msg);

    return;
}


sub stop {
    my ($self) = @_;

    $self->log("good bye cruel world!");

    exit;
}


1;    # End of Daemonise

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise - Daemonise - a general daemoniser for anything...

=head1 VERSION

version 1.62

=head1 SYNOPSIS

    use Daemonise;
    use File::Basename;
    
    my $d = Daemonise->new();
    $d->name(basename($0));
    
    # log/print more debug info
    $d->debug(1);
    
    # stay in foreground, don't actually fork when calling $d->start
    $d->foreground(1) if $d->debug;
    
    # config file style can be whatever Config::Any supports
    $d->config_file('/path/to/some.conf');
    
    # where to store/look for PID file
    $d->pid_file("/var/run/${name}.pid");
    
    # configure everything so far
    $d->configure;
    
    # fork and redirect STDERR/STDOUT to syslog per default
    $d->start;
    
    # load some plugins (refer to plugin documentation for provided methods)
    $d->load_plugin('RabbitMQ');
    $d->load_plugin('CouchDB');
    $d->load_plugin('JobQueue');
    $d->load_plugin('Event');
    $d->load_plugin('Redis');
    $d->load_plugin('HipChat');
    $d->load_plugin('Riemann');
    $d->load_plugin('PagerDuty');
    $d->load_plugin('KyotoTycoon');
    $d->load_plugin('Graphite');
    
    # reconfigure after loading plugins if necessary
    $d->configure;
    
    # do stuff

=head1 ATTRIBUTES

=head2 name

=head2 hostname

=head2 config_file

=head2 config

=head2 debug

=head2 start_time

=head2 is_cron

=head2 cache_plugin

=head1 SUBROUTINES/METHODS

=head2 load_plugin

=head2 configure

=head2 async

=head2 log

=head2 stop

=head1 BUGS

Please report any bugs or feature requests on GitHub's issue tracker L<https://github.com/ideegeo/Daemonise/issues>.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Daemonise

You can also look for information at:

=over 4

=item * GitHub repository

L<https://github.com/ideegeo/Daemonise>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Daemonise>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Daemonise>

=back

=head1 ACKNOWLEDGEMENTS

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
