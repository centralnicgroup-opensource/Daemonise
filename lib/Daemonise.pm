package Daemonise;

use Mouse;

# ABSTRACT: Daemonise - a general daemoniser for anything...

# VERSION

use POSIX qw(strftime SIGINT SIG_BLOCK SIG_UNBLOCK);
use Config::Any;
use Unix::Syslog;

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
    
    # set file can be Config::Any style
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
    
    # reconfigure after loading plugins if necessary
    $d->configure;
    
    # do stuff


=head1 ATTRIBUTES

=head2 user

=cut

has 'user' => (
    is      => 'rw',
    default => sub { 'root' },
);

=head2 uid

=cut

has 'uid' => (
    is  => 'rw',
    isa => 'Int',
);

=head2 group

=cut

has 'group' => (
    is      => 'rw',
    default => sub { 'root' },
);

=head2 gid

=cut

has 'gid' => (is => 'rw');

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

=head2 pid_file

=cut

has 'pid_file' => (
    is        => 'rw',
    predicate => 'has_pid_file',
);

=head2 running

=cut

has 'running' => (
    is        => 'rw',
    predicate => 'is_running',
);

=head2 config_file

=cut

has 'config_file' => (
    is        => 'rw',
    predicate => 'has_config_file',
);

=head2 config

=cut

has 'config' => (
    is      => 'rw',
    default => sub { },
);

=head2 phase

=cut

has 'phase' => (
    is      => 'rw',
    default => sub { 'starting' },
);

=head2 logfile

=cut

has 'logfile' => (
    is        => 'rw',
    predicate => 'has_logfile',
);

=head2 debug

=cut

has 'debug' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);

=head2 foreground

=cut

has 'foreground' => (
    is      => 'rw',
    isa     => 'Bool',
    default => sub { 0 },
);

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
    my $self = shift;

    $self->log("configuring Daemonise") if $self->debug;

    unless ($self->has_config_file) {
        warn "No config file defined!";
        return;
    }

    my $conf = Config::Any->load_files({
            files   => [ $self->config_file ],
            use_ext => 1,
    });
    $conf = $conf->[0]->{ $self->config_file } if $conf;

    unless ((ref $conf eq 'HASH')
        and (exists $conf->{main})
        and ref $conf->{main} eq 'HASH')
    {
        warn "'main' config section missing!";
        return;
    }

    foreach my $key (keys %{ $conf->{main} }) {
        my $val = $conf->{main}->{$key};

        # set properties if they exist but don't die if they don't
        eval { $self->$key($val) };
        warn "$key not defined as property! $@"
            if ($@ && $self->debug);
    }

    $self->config($conf);
    return $conf;
}

=head2 log

=cut

sub log {    ## no critic (ProhibitBuiltinHomonyms)
    my ($self, $msg) = @_;

    if ($self->foreground) {
        print $self->name . ": $msg\n";
    }
    elsif ($self->has_logfile) {
        open(my $log_file, '>>', $self->logfile)
            or confess "Could not open File (" . $self->logfile . "): $@";
        my $now = strftime "[%F %T]", localtime;
        print $log_file "$now\t$$\t$msg\n";
        close $log_file;
    }
    else {
        chomp($msg);
        Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(),
            'queue=%s %s', $self->name, $msg);
    }

    return;
}

=head2 check_pid_file

=cut

sub check_pid_file {
    my $self = shift;

    ### no pid_file = return success
    return 1 unless -e $self->pid_file;

    ### get the currently listed pid
    open(my $pid_file, '<', $self->pid_file)
        or die "Couldn't open existant pid_file \""
        . $self->pid_file
        . "\" [$!]\n";
    my $pid = <$pid_file>;
    close $pid_file;

    $pid =
          $pid =~ /^(\d{1,10})/
        ? $1
        : die "Couldn't find pid in existing pid_file";

    my $exists;

    ### try a proc file system
    if (-d '/proc') {
        $exists = -e "/proc/$pid";

        ### try ps
        #}elsif( -x '/bin/ps' ){ # not as portable
        # the ps command itself really isn't portable
        # this follows BSD syntax ps (BSD's and linux)
        # this will fail on Unix98 syntax ps (Solaris, etc)
    }
    elsif (`ps p $$ | grep -v 'PID'` =~ /^\s*$$\s+.*$/) {

        # can I play ps on myself ?
        $exists = `ps p $pid | grep -v 'PID'`;

    }

    ### running process exists, ouch
    if ($exists) {

        if ($pid == $$) {
            warn "Pid_file created by this same process. Doing nothing.\n";
            return 1;
        }
        else {
            if ($self->phase eq 'status') {
                $self->running($pid);
                return;
            }
            else {
                die
                    "Pid_file already exists for running process ($pid)... aborting\n";
            }
        }
    }
    else {
        ### remove the pid_file

        warn "Pid_file \""
            . $self->pid_file
            . "\" already exists.  Overwriting!\n";
        unlink $self->pid_file
            || die "Couldn't remove pid_file \""
            . $self->pid_file
            . "\" [$!]\n";

        return 1;
    }
}

=head2 daemonise

=cut

sub daemonise {
    my $self = shift;

    if ($self->foreground) {
        $self->running($$);
        $self->log('staying in forground');
        return;
    }

    $self->phase('starting');
    $self->check_pid_file if $self->has_pid_file;

    $self->uid($self->_get_uid);
    $self->gid($self->_get_gid);
    $self->gid((split /\s+/, $self->gid)[0]);

    my $pid = $self->_safe_fork;

    ### parent process should do the pid file and exit
    if ($pid) {
        $pid && exit(0);
        ### child process will continue on
    }
    else {
        $self->_create_pid_file if $self->has_pid_file;

        ### make sure we can remove the file later
        chown($self->uid, $self->gid, $self->pid_file)
            if $self->has_pid_file;

        ### become another user and group
        $self->_set_user;

        ### close all input/output and separate
        ### from the parent process group
        open(STDIN, '<', '/dev/null')
            or die "Can't open STDIN from /dev/null: [$!]";

        # check for Tie::Syslog for later
        my $tie_syslog;
        eval { require Tie::Syslog; } and do { $tie_syslog = 1 unless $@ };

        if ($self->logfile) {
            my $logfile = $self->logfile;
            open(STDOUT, '>', $logfile)
                or die "Can't redirect STDOUT to $logfile: [$!]";
            open(STDERR, '>', '&STDOUT')
                or die "Can't redirect STDERR to STDOUT: [$!]";
        }
        elsif ($tie_syslog) {
            my $name = $self->name;
            my $y = tie *STDOUT, 'Tie::Syslog', 'local0.info',
                "perl[$$]: queue=$name STDOUT ", 'pid', 'unix';
            my $x = tie *STDERR, 'Tie::Syslog', 'local0.info',
                "perl[$$]: queue=$name STDERR ", 'pid', 'unix';
            $x->ExtendedSTDERR();
            binmode(STDOUT, ":encoding(UTF-8)");
            binmode(STDERR, ":encoding(UTF-8)");
        }
        else {
            open(STDOUT, '>', '/dev/null')
                or die "Can't redirect STDOUT to /dev/null: [$!]";
            open(STDERR, '>', '&STDOUT')
                or die "Can't redirect STDERR to STDOUT: [$!]";
        }

        ### Change to root dir to avoid locking a mounted file system
        ### does this mean to be chroot ?
        chdir '/' or die "Can't chdir to \"/\": [$!]";

        ### Turn process into session leader, and ensure no controlling terminal
        POSIX::setsid();

        if ($self->has_name) {
            Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(),
                "Daemon started (" . $self->name . ") with pid: $$");
        }
        else {
            Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(),
                "Daemon started with pid: $$");
        }

        ### install a signal handler to make sure
        ### SIGINT's remove our pid_file
        local $SIG{INT} = sub { $self->_huntsman }
            if $self->has_pid_file;

        return 1;
    }

    return;
}

=head2 status

=cut

sub status {
    my $self = shift;
    $self->phase('status');
    $self->check_pid_file();
    if ($self->is_running) {
        return $self->running . ' with PID file ' . $self->pid_file . "\n";
    }
    return;
}

=head2 restart

=cut

sub restart {
    my $self = shift;
    $self->stop;
    $self->start;
    return "Restarted Daemon\n";
}

=head2 stop

=cut

sub stop {
    my $self = shift;
    my $msg;
    $self->phase('status');
    $self->check_pid_file();
    if ($self->is_running) {
        my $pid = $self->running;
        system("kill $pid");
        unlink $self->pid_file;
        if ($self->has_name) {
            $msg = "Daemon stopped (" . $self->name . ") with $$";
            Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(), $msg);
        }
        else {
            $msg = "Daemon stopped with $$";
            Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(), $msg);
        }
        return $msg;
    }
    else {
        die "Could not PID!\n";
    }
}

=head2 start

=cut

sub start {
    my $self = shift;
    $self->daemonise;
    return;
}

sub _huntsman {
    my $self = shift;

    unlink $self->pid_file;

    eval {
        Unix::Syslog::syslog(Unix::Syslog::LOG_ERR(), "Exiting on INT signal.");
    };

    exit;
}

sub _get_uid {
    my $self = shift;
    my $uid  = undef;

    if ($self->user =~ /^\d+$/) {
        $uid = $self->user;
    }
    else {
        $uid = getpwnam($self->user);
    }

    die "No such user \"" . $self->user . "\"\n" unless defined $uid;

    return $uid;
}

sub _get_gid {
    my $self = shift;
    my @gid  = ();

    foreach my $group (split(/[, ]+/, join(" ", $self->group))) {
        if ($group =~ /^\d+$/) {
            push @gid, $group;
        }
        else {
            my $id = getgrnam($group);
            die "No such group \"$group\"\n" unless defined $id;
            push @gid, $id;
        }
    }

    die "No group found in arguments.\n" unless @gid;

    return join(" ", $gid[0], @gid);
}

sub _safe_fork {
    my $self = shift;

    ### block signal for fork
    my $sigset = POSIX::SigSet->new(SIGINT);
    POSIX::sigprocmask(SIG_BLOCK, $sigset)
        or die "Can't block SIGINT for fork: [$!]\n";

    ### fork off a child
    my $pid = fork;
    unless (defined $pid) {
        die "Couldn't fork: [$!]\n";
    }

    ### make SIGINT kill us as it did before
    local $SIG{INT} = 'DEFAULT';

    ### put back to normal
    POSIX::sigprocmask(SIG_UNBLOCK, $sigset)
        or die "Can't unblock SIGINT for fork: [$!]\n";

    return $pid;
}

sub _create_pid_file {
    my $self = shift;

    ### see if the pid_file is already there
    $self->check_pid_file;

    open(my $pid_file, '>', $self->pid_file)
        or die "Couldn't open pid file \"" . $self->pid_file . "\" [$!].\n";
    print $pid_file "$$\n";
    close $pid_file;

    die "Pid_file \"" . $self->pid_file . "\" not created.\n"
        unless -e $self->pid_file;

    return 1;
}

sub _set_user {
    my $self = shift;

    $self->_set_gid || return;
    $self->_set_uid || return;

    return 1;
}

sub _set_uid {
    my $self = shift;
    my $uid  = $self->_get_uid;

    POSIX::setuid($uid);

    # check $> also (rt #21262)
    if ($< != $uid || $> != $uid) {

        # try again - needed by some 5.8.0 linux systems (rt #13450)
        local $< = local $> = $uid;
        if ($< != $uid) {
            die "Couldn't become uid \"$uid\": $!\n";
        }
    }

    return 1;
}

sub _set_gid {
    my $self = shift;
    my $gids = $self->_get_gid;
    my $gid  = (split /\s+/, $gids)[0];

    # store all the gids - this is really sort of optional
    eval { local $) = $gids };

    POSIX::setgid($gid);

    # look for any valid gid in the list
    if (!grep { $gid == $_ } split /\s+/, $() {
        die "Couldn't become gid \"$gid\": $!\n";
    }

    return 1;
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
