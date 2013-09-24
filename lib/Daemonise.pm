package Daemonise;

use Mouse;
use FindBin qw($Bin);
use lib "$Bin/../lib";

# ABSTRACT: Daemonise - a general daemoniser for anything...

# VERSION

use Unix::Syslog;
use Config::Any;

=head1 SYNOPSIS

Example:

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

=cut

has 'name' => (
    is        => 'rw',
    default   => sub { 'daemon' },
    predicate => 'has_name',
);

=head2 hostname

=cut

has 'hostname' => (
    is      => 'ro',
    isa     => 'Str',
    lazy    => 1,
    default => sub { my $h = `hostname`; chomp $h; $h },
);

=head2 config_file

=cut

has 'config_file' => (
    is        => 'rw',
    isa       => "Str",
    predicate => 'has_config_file',
);

=head2 config

=cut

has 'config' => (
    is      => 'rw',
    isa     => 'HashRef',
    lazy    => 1,
    default => sub { {} },
);

=head2 debug

=cut

has 'debug' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);

# backwards compatibility
after 'new' => sub {
    my ($class, %args) = @_;

    with('Daemonise::Plugin::Daemon') unless $args{no_daemon};

    return;
};

=head1 SUBROUTINES/METHODS

=head2 load_plugin

=cut

sub load_plugin {
    my ($self, $plugin) = @_;

    my $plug = 'Daemonise::Plugin::' . $plugin;
    $self->log("loading $plugin plugin") if $self->debug;
    with($plug);

    return;
}

=head2 configure

=cut

sub configure {
    my ($self) = @_;

    return unless $self->has_config_file;

    unless (-e $self->config_file) {
        $self->log(
            "ERROR: config file '" . $self->config_file . "' does not exist!");
        return;
    }

    my $conf = Config::Any->load_files({
            files   => [ $self->config_file ],
            use_ext => 1,
            driver_args => { General => { -InterPolateEnv => 1 } }
    });
    $conf = $conf->[0]->{ $self->config_file } if $conf;

    $self->config($conf);

    return;
}

=head2 log

=cut

sub log {    ## no critic (ProhibitBuiltinHomonyms)
    my ($self, $msg) = @_;

    chomp($msg);
    Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(),
        'queue=%s %s', $self->name, $msg);

    return;
}

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

=cut

1;    # End of Daemonise
