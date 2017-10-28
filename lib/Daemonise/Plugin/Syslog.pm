package Daemonise::Plugin::Syslog;

use Mouse::Role;

# ABSTRACT: Daemonise plugin adding syslog functionality

use Sys::Syslog qw(setlogsock :standard :macros);

=head1 SYNOPSIS

    use Daemonise;

    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    $d->load_plugin('Syslog');

    $d->configure;

    # fork and run in background (unless foreground is true)
    $d->start(\&main);

    sub main {
        # check if daemon is running already
        $d->status;
    }

=head1 ATTRIBUTES

=head2 syslog_host

=cut

has 'syslog_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { '127.0.0.1' },
);

=head2 syslog_port

=cut

has 'syslog_port' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { '514' },
);

=head2 syslog_type

=cut

has 'syslog_type' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'tcp' },
);

=head2 syslog_log

=cut

has 'syslog_log' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 1 },
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self, $reconfig) = @_;

    $self->log("configuring Syslog plugin") if $self->debug;

    my $syslog_config;
    if (ref($self->config->{syslog}) eq 'HASH') {
        $syslog_config = 1;
        foreach my $conf_key ('host', 'port', 'type') {
            my $attr = 'syslog_' . $conf_key;
            $self->$attr($self->config->{syslog}->{$conf_key})
                if defined $self->config->{syslog}->{$conf_key};
        }
    }

    setlogsock({
            type => $self->syslog_type,
            host => $self->syslog_host,
            port => $self->syslog_port
        }) if $syslog_config;
    openlog($self->name, 'pid,ndelay', LOG_USER);

    return;
};

=head2 log

=cut

after 'log' => sub {
    my ($self, $msg) = @_;

    return unless $self->syslog_log;

    # escape newlines when not running in debug mode for log parser convenience
    $msg =~ s/\h*\v+\h*/ /gs;

    # encode wide characters as UTF-8
    utf8::encode($msg);

    syslog(LOG_NOTICE, $msg);

    return;
};

=head2 stdout_redirect

=cut

around 'stdout_redirect' => sub {
    my ($orig, $self) = @_;

    unless ($self->syslog_log) {
        $self->$orig();
        return;
    }

    my $tie_syslog;
    eval { require Tie::Syslog; } and do { $tie_syslog = 1 unless $@ };

    # revert to original plan if there is no Tie::Syslog installed
    unless ($tie_syslog) {
        $self->$orig();
        return;
    }

    # FIXME Tie::Syslog does not support the "setlogsock" option
    # so we can't set another syslog server. This is a problem
    # on FreeBSD atm
    $Tie::Syslog::ident = $self->name;
    tie *STDOUT, 'Tie::Syslog', {
        facility => 'LOG_USER',
        priority => 'LOG_NOTICE',
        };
    tie *STDERR, 'Tie::Syslog', {
        facility => 'LOG_USER',
        priority => 'LOG_ERR',
        };

    # inject our own PRINT function into Tie::Syslog so we can remove
    # newlines when not in debug mode so syslog feeds splunk with nice
    # single lines not losing the context
    unless ($self->debug) {
        undef &Tie::Syslog::PRINT;    # silence redefine warnings
        *Tie::Syslog::PRINT = sub {
            my ($s, @msg) = @_;

            warn "Cannot PRINT to a closed filehandle!"
                unless $s->{'is_open'};

            my $msg = join('', @msg);

            # remove vertical (and potentially surrounding horizontal) spaces
            $msg =~ s/\h*\v+\h*/ /gs;

            # Sys::Syslog does not like wide characters and dies
            utf8::encode($msg);

            eval { syslog($s->facility . '|' . $s->priority, $msg); };
            if ($@) {
                syslog($s->facility . '|' . $s->priority,
                    "Tie::Syslog::PRINT failed with errors: $@");
            }

            return;
        };
    }

    return;
};

1;
