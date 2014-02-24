package Daemonise::Plugin::Daemon;

use Mouse::Role;

# ABSTRACT: Daemonise Daemon plugin handling PID file, forking, syslog

use POSIX qw(strftime SIGTERM SIG_BLOCK SIG_UNBLOCK);
use Unix::Syslog;
use Scalar::Util qw(looks_like_number);


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


has 'gid' => (is => 'rw');


has 'pid_file' => (
    is        => 'rw',
    predicate => 'has_pid_file',
);


has 'running' => (
    is        => 'rw',
    predicate => 'is_running',
);


has 'phase' => (
    is      => 'rw',
    default => sub { 'starting' },
);


has 'logfile' => (
    is        => 'rw',
    predicate => 'has_logfile',
);


has 'foreground' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);


has 'dont_loop' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);


after 'configure' => sub {
    my ($self, $reconfig) = @_;

    $self->log("configuring Daemon plugin") if $self->debug;

    unless (exists $self->config->{main}
        and ref $self->config->{main} eq 'HASH')
    {
        warn "'main' config section missing!";
        return;
    }

    foreach my $key (keys %{ $self->config->{main} }) {
        my $val = $self->config->{main}->{$key};

        # set properties if they exist but don't die if they don't
        eval { $self->$key($val) };
        warn "$key not defined as property! $@"
            if ($@ && $self->debug);
    }

    return;
};


around 'log' => sub {
    my ($orig, $self, $msg) = @_;

    chomp($msg);

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
        $self->$orig($msg);
    }

    return;
};


sub check_pid_file {
    my ($self) = @_;

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


sub daemonise {
    my ($self) = @_;

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

    my $pid = $self->async;

    ### parent process should do the pid file and exit
    if ($pid) {
        $pid && exit;
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
            $Tie::Syslog::ident = 'Daemonise';
            tie *STDOUT, 'Tie::Syslog', {
                facility => 'LOG_USER',
                priority => 'LOG_INFO',
                };
            tie *STDERR, 'Tie::Syslog', {
                facility => 'LOG_USER',
                priority => 'LOG_ERR',
                };
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

        $self->log("daemon started");

        ### install a signal handler to make sure
        ### SIGTERM's remove our pid_file
        $SIG{TERM} = sub { $self->stop };    ## no critic
        $SIG{INT}  = sub { $self->stop };    ## no critic

        return 1;
    }

    return;
}


sub status {
    my ($self) = @_;

    $self->phase('status');
    $self->check_pid_file();

    if ($self->is_running) {
        return $self->running . ' with PID file ' . $self->pid_file;
    }

    return;
}


before 'stop' => sub {
    my ($self) = @_;

    unlink $self->pid_file if $self->has_pid_file;

    return;
};


sub start {
    my ($self, $code) = @_;

    $self->daemonise;

    # we need to reconfigure, because we are in the child and some plugins
    # start connections in the configure stage
    $self->configure;

    if (ref $code eq 'CODE') {
        if ($self->dont_loop) {
            $code->();
            $self->stop;
        }
        else {
            while (1) {
                $code->();
            }
        }
    }
    else {
        $self->log("first argument of start() is not a CODEREF! existing...");
        $self->stop;
    }

    return;
}

sub _get_uid {
    my ($self) = @_;

    my $uid = undef;
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
    my ($self) = @_;

    my @gid = ();
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

sub _create_pid_file {
    my ($self) = @_;

    # child should also know its PID
    $self->running($$);

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
    my ($self) = @_;

    $self->_set_gid || return;
    $self->_set_uid || return;

    return 1;
}

sub _set_uid {
    my ($self) = @_;

    my $uid = $self->_get_uid;

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
    my ($self) = @_;

    my $gids = $self->_get_gid;
    my $gid = (split /\s+/, $gids)[0];

    # store all the gids - this is really sort of optional
    eval { local $) = $gids };

    POSIX::setgid($gid);

    # look for any valid gid in the list
    if (!grep { $gid == $_ } split /\s+/, $() {
        die "Couldn't become gid \"$gid\": $!\n";
    }

    return 1;
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise::Plugin::Daemon - Daemonise Daemon plugin handling PID file, forking, syslog

=head1 VERSION

version 1.63

=head1 SYNOPSIS

    use Daemonise;
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->configure;
    
    # fork and run in background (unless foreground is true)
    $d->start(\&main);
    
    sub main {
        # check if daemon is running already
        $d->status;
    }

=head1 ATTRIBUTES

=head2 user

=head2 uid

=head2 group

=head2 gid

=head2 pid_file

=head2 running

=head2 phase

=head2 logfile

=head2 foreground

=head2 dont_loop

=head1 SUBROUTINES/METHODS provided

=head2 configure

=head2 log

=head2 check_pid_file

=head2 daemonise

=head2 status

=head2 stop

=head2 start

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
