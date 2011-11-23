package Daemonise;

use Mouse;
use POSIX qw(strftime SIGINT SIG_BLOCK SIG_UNBLOCK);
use Config::Any;
use Unix::Syslog;

our $VERSION = '1.1.2';

has 'user' => (
    is      => 'rw',
    default => sub { 'root' },
);

has 'uid' => (
    is  => 'rw',
    isa => 'Int',
);

has 'group' => (
    is      => 'rw',
    default => sub { 'root' },
);

has 'gid' => (
    is  => 'rw',
);

has 'name' => (
    is        => 'rw',
    default   => sub { 'daemon' },
    predicate => 'has_name',
);

has 'pid_file' => (
    is        => 'rw',
    predicate => 'has_pid_file',
);

has 'running' => (
    is        => 'rw',
    predicate => 'is_running',
);

has 'config_file' => (
    is        => 'rw',
    predicate => 'has_config_file',
);

has 'config' => (
    is      => 'rw',
    default => sub { },
);

has 'phase' => (
    is      => 'rw',
    default => sub { 'starting' },
);

has 'logfile' => (
    is        => 'rw',
    predicate => 'has_logfile',
);

has 'debug' => (
    is        => 'rw',
    isa       => 'Bool',
    default   => 0,
    predicate => 'is_debug'
);

sub load_plugin {
    my ($self, $plugin) = @_;

    my $plug = 'Daemonise::Plugin::' . $plugin;
    print STDERR "Loading $plugin Plugin\n"
        if $self->is_debug;
    with($plug);
    return;
}

sub configure {
    my $self = shift;

    warn "No config file defined!" unless $self->has_config_file;
    return unless $self->has_config_file;

    my $_conf = Config::Any->load_files({
            files   => [ $self->config_file ],
            use_ext => 1,
    });
    my $conf = $_conf->[0]->{ $self->config_file } if $_conf;

    foreach my $key (keys %{ $conf->{main} }) {
        my $val = $conf->{main}->{$key};
        $self->$key($val) or warn "$key not defined as property!";
    }
    $self->config($conf);
    return $conf;
}

sub msg_pub {
    my ($self, $msg, $queue) = @_;
    $self->rabbit_user($self->config->{rabbit}->{user})
        if $self->config->{rabbit}->{user};
    $self->rabbit_pass($self->config->{rabbit}->{pass})
        if $self->config->{rabbit}->{pass};
    $self->rabbit_host($self->config->{rabbit}->{host})
        if $self->config->{rabbit}->{host};
    $self->rabbit_port($self->config->{rabbit}->{port})
        if $self->config->{rabbit}->{port};
    $self->rabbit_vhost($self->config->{rabbit}->{vhost})
        if $self->config->{rabbit}->{vhost};
    $self->rabbit_exchange($self->config->{rabbit}->{exchange})
        if $self->config->{rabbit}->{exchange};
    return;
}

sub msg_rpc {
    my ($self, $msg, $queue, $reply_queue) = @_;
    $self->rabbit_user($self->config->{rabbit}->{user})
        if $self->config->{rabbit}->{user};
    $self->rabbit_pass($self->config->{rabbit}->{pass})
        if $self->config->{rabbit}->{pass};
    $self->rabbit_host($self->config->{rabbit}->{host})
        if $self->config->{rabbit}->{host};
    $self->rabbit_port($self->config->{rabbit}->{port})
        if $self->config->{rabbit}->{port};
    $self->rabbit_vhost($self->config->{rabbit}->{vhost})
        if $self->config->{rabbit}->{vhost};
    $self->rabbit_exchange($self->config->{rabbit}->{exchange})
        if $self->config->{rabbit}->{exchange};
    return $self->rabbit_last_response;
}

sub queue_bind {
    my ($self, $queue) = @_;
    $self->rabbit_user($self->config->{rabbit}->{user})
        if $self->config->{rabbit}->{user};
    $self->rabbit_pass($self->config->{rabbit}->{pass})
        if $self->config->{rabbit}->{pass};
    $self->rabbit_host($self->config->{rabbit}->{host})
        if $self->config->{rabbit}->{host};
    $self->rabbit_port($self->config->{rabbit}->{port})
        if $self->config->{rabbit}->{port};
    $self->rabbit_vhost($self->config->{rabbit}->{vhost})
        if $self->config->{rabbit}->{vhost};
    $self->rabbit_exchange($self->config->{rabbit}->{exchange})
        if $self->config->{rabbit}->{exchange};
    return;
}

sub couchdb {
    warn 'plugin "CouchDB" not loaded';
}

sub lookup {
    my ($self, $key) = @_;
    return $key;
}

sub log {
    my $self = shift;
    my $msg  = shift;
    if ($self->has_logfile) {
        open(LOG, '>>', $self->logfile)
            or confess "Could not open File (" . $self->logfile . "): $@";
        my $now = strftime "[%F %T]", localtime;
        print LOG "$now\t$$\t$msg\n";
        close LOG;
    }
    else {
        Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(), $msg);
    }
}

sub check_pid_file {
    my $self = shift;

    ### no pid_file = return success
    return 1 unless -e $self->pid_file;

    ### get the currently listed pid
    if (!open(_PID, $self->pid_file)) {
        die "Couldn't open existant pid_file \""
            . $self->pid_file
            . "\" [$!]\n";
    }
    my $_current_pid = <_PID>;
    close _PID;
    my $current_pid =
          $_current_pid =~ /^(\d{1,10})/
        ? $1
        : die "Couldn't find pid in existing pid_file";

    my $exists = undef;

    ### try a proc file system
    if (-d '/proc') {
        $exists = -e "/proc/$current_pid";

        ### try ps
        #}elsif( -x '/bin/ps' ){ # not as portable
        # the ps command itself really isn't portable
        # this follows BSD syntax ps (BSD's and linux)
        # this will fail on Unix98 syntax ps (Solaris, etc)
    }
    elsif (`ps p $$ | grep -v 'PID'` =~ /^\s*$$\s+.*$/) {

        # can I play ps on myself ?
        $exists = `ps p $current_pid | grep -v 'PID'`;

    }

    ### running process exists, ouch
    if ($exists) {

        if ($current_pid == $$) {
            warn "Pid_file created by this same process. Doing nothing.\n";
            return 1;
        }
        else {
            if ($self->phase eq 'status') {
                $self->running($current_pid);
                return;
            }
            else {
                die
                    "Pid_file already exists for running process ($current_pid)... aborting\n";
            }
        }

        ### remove the pid_file
    }
    else {

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
    $SIG{INT} = 'DEFAULT';

    ### put back to normal
    POSIX::sigprocmask(SIG_UNBLOCK, $sigset)
        or die "Can't unblock SIGINT for fork: [$!]\n";

    return $pid;
}

sub _create_pid_file {
    my $self = shift;

    ### see if the pid_file is already there
    $self->check_pid_file;

    if (!open(PID, '>', $self->pid_file)) {
        die "Couldn't open pid file \"" . $self->pid_file . "\" [$!].\n";
    }

    ### save out the pid and exit
    print PID "$$\n";
    close PID;

    die "Pid_file \"" . $self->pid_file . "\" not created.\n"
        unless -e $self->pid_file;
    return 1;
}

sub _set_user {
    my $self = shift;

    $self->_set_gid || return undef;
    $self->_set_uid || return undef;

    return 1;
}

sub _set_uid {
    my $self = shift;
    my $uid  = $self->_get_uid;

    POSIX::setuid($uid);
    if ($< != $uid || $> != $uid) {    # check $> also (rt #21262)
        $< = $> =
            $uid;   # try again - needed by some 5.8.0 linux systems (rt #13450)
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
    eval { $) = $gids };  # store all the gids - this is really sort of optional

    POSIX::setgid($gid);

    # look for any valid gid in the list
    if (!grep { $gid == $_ } split /\s+/, $() {
        die "Couldn't become gid \"$gid\": $!\n";
    }

    return 1;
}

sub daemonise {
    my $self = shift;

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
        my $logfile = $self->logfile;
        open STDIN, '</dev/null'
            or die "Can't open STDIN from /dev/null: [$!]\n";
        open STDOUT, ">$logfile"
            or die "Can't open STDOUT to $logfile: [$!]\n";
        open STDERR, '>&STDOUT'
            or die "Can't open STDERR to STDOUT: [$!]\n";

        ### Change to root dir to avoid locking a mounted file system
        ### does this mean to be chroot ?
        chdir '/' or die "Can't chdir to \"/\": [$!]";

        ### Turn process into session leader, and ensure no controlling terminal
        POSIX::setsid();

        if ($self->has_name) {
            Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(),
                "Daemon started (" . $self->name . ") with $$");
        }
        else {
            Unix::Syslog::syslog(Unix::Syslog::LOG_NOTICE(),
                "Daemon started with $$");
        }
        ### install a signal handler to make sure
        ### SIGINT's remove our pid_file
        $SIG{INT} = sub { $self->HUNTSMAN }
            if $self->has_pid_file;
        return 1;
    }
}

sub status {
    my $self = shift;
    $self->phase('status');
    $self->check_pid_file();
    if ($self->is_running) {
        return $self->running . ' with PID file ' . $self->pid_file . "\n";
    }
    return;
}

sub restart {
    my $self = shift;
    $self->stop;
    $self->start;
    return "Restarted Daemon\n";
}

sub stop {
    my $self = shift;
    my $msg;
    $self->phase('status');
    $self->check_pid_file();
    if ($self->is_running) {
        my $pid = $self->running;
        qx{kill $pid};
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

sub start {
    my $self = shift;
    $self->daemonise;
    return;
}

sub HUNTSMAN {
    my $self = shift;
    unlink $self->pid_file;

    eval {
        Unix::Syslog::syslog(Unix::Syslog::LOG_ERR(), "Exiting on INT signal.");
    };

    exit;
}

=head1 NAME

Daemonise - a general daemoniser for anything...

=head1 VERSION

Version 1.1.2.1.2.1.1.0.6.0.5.0.5.0.5

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Daemonise;

    my $foo = Daemonise->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 AUTHOR

Lenz Gschwendtner, C<< <norbu09 at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-daemonise at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Daemonise>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Daemonise


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Daemonise>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Daemonise>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Daemonise>

=item * Search CPAN

L<http://search.cpan.org/dist/Daemonise/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2009 Lenz Gschwendtner.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;    # End of Daemonise
