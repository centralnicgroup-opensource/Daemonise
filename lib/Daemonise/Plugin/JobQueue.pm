package Daemonise::Plugin::JobQueue;

use Mouse::Role;

# ABSTRACT: Daemonise JobQueue plugin

use feature 'switch';
use Data::Dumper;
use Digest::MD5 'md5_hex';
use DateTime;
use Carp;

BEGIN {
    with("Daemonise::Plugin::CouchDB");
    with("Daemonise::Plugin::RabbitMQ");
}


has 'jobqueue_db' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'jobqueue' },
);


has 'job' => (
    is      => 'rw',
    isa     => 'HashRef',
    lazy    => 1,
    default => sub { {} },
);


after 'configure' => sub {
    my ($self, $reconfig) = @_;

    $self->log("configuring JobQueue plugin") if $self->debug;

    $self->jobqueue_db($self->config->{jobqueue}->{db})
        if (exists $self->config->{jobqueue}
        and exists $self->config->{jobqueue}->{db}
        and $self->config->{jobqueue}->{db});

    return;
};


around 'log' => sub {
    my ($orig, $self, $msg) = @_;

    $msg = 'account=' . $self->job->{message}->{meta}->{account} . ' ' . $msg
        if (exists $self->job->{message}->{meta}->{account}
        and defined $self->job->{message}->{meta}->{account});
    $msg = 'user=' . $self->job->{message}->{meta}->{user} . ' ' . $msg
        if (exists $self->job->{message}->{meta}->{user}
        and defined $self->job->{message}->{meta}->{user});
    $msg = 'job=' . $self->job->{message}->{meta}->{id} . ' ' . $msg
        if (exists $self->job->{message}->{meta}->{id}
        and defined $self->job->{message}->{meta}->{id});
    $msg = 'platform=' . $self->job->{message}->{meta}->{platform} . ' ' . $msg
        if (exists $self->job->{message}->{meta}->{platform}
        and defined $self->job->{message}->{meta}->{platform});

    $self->$orig($msg);

    return;
};


sub get_job {
    my ($self, $id) = @_;

    unless ($id) {
        carp "Job ID missing";
        return;
    }

    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    my $job = $self->couchdb->get_doc({ id => $id });
    $self->couchdb->db($old_db);

    unless ($job) {
        carp "job ID not existing: $id";
        return;
    }

    # store job in attribute
    $self->job($job);

    return $job;
}


sub create_job {
    my ($self, $msg) = @_;

    unless ((ref($msg) eq 'HASH')
        and (exists $msg->{meta}->{platform}))
    {
        carp "{meta}->{platform} is missing, can't create job";
        return;
    }

    # set testmode when running in debug mode
    $msg->{data}->{options}->{testmode} = 1 if $self->debug;

    # kill duplicate identical jobs with a hashsum over the input data
    # and a 2 min caching time
    my $created = time;
    my $cached = DateTime->from_epoch(epoch => $created);
    $cached->truncate(to => 'minute');
    $cached->set_minute($cached->minute - ($cached->minute % 2));
    my $dumper = Data::Dumper->new([ $msg->{data}->{options} ]);
    $dumper->Terse(1);
    $dumper->Sortkeys(1);
    $dumper->Indent(0);
    $dumper->Deepcopy(1);
    my $id = md5_hex($dumper->Dump . $cached);

    # return if job ID exists
    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    my $job = $self->couchdb->get_doc({ id => $id });
    if ($job) {
        $self->log("found duplicate job: " . $job->{_id});
        return $job, 1;
    }

    $msg->{meta}->{id} = $id;
    $job = {
        _id      => $id,
        created  => $created,
        updated  => $created,
        message  => $msg,
        platform => $msg->{meta}->{platform},
        status   => 'new',
    };

    $id = $self->couchdb->put_doc({ doc => $job });
    $self->couchdb->db($old_db);

    unless ($id) {
        carp 'creating job failed: ' . $self->couchdb->error;
        return;
    }

    $self->job($job);

    return $job;
}


sub start_job {
    my ($self, $workflow, $platform, $options) = @_;

    unless ($workflow) {
        carp 'workflow not defined';
        return;
    }

    unless ($platform) {
        carp 'platform not defined';
        return;
    }

    $options = {} unless $options;

    my $frame = {
        meta => {
            platform => $platform,
            lang     => 'en',
            user     => $options->{user_id} || undef,
        },
        data => {
            command => $workflow,
            options => $options,
        },
    };

    # tell the new job who created it
    $frame->{meta}->{created_by} = $self->job->{message}->{meta}->{id}
        if exists $self->job->{message}->{meta}->{id};

    $self->log("starting '$workflow' workflow with:\n" . Dumper($frame))
        if $self->debug;
    $self->queue('workflow', $frame);

    return;
}


sub update_job {
    my ($self, $msg, $status) = @_;

    unless ((ref($msg) eq 'HASH') and (exists $msg->{meta}->{id})) {
        $self->log("not a JOB, just a message, nothing to see here")
            if $self->debug;
        return;
    }

    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    my $job = $self->get_job($msg->{meta}->{id});
    $self->couchdb->db($old_db);

    return unless $job;

    $self->job($job);

    $job->{updated} = time;
    $job->{status}  = $status || $job->{status};
    $job->{message} = $msg;

    $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    ($job->{_id}, $job->{_rev}) = $self->couchdb->put_doc({ doc => $job });
    $self->couchdb->db($old_db);

    if ($self->couchdb->has_error) {
        carp 'updating job failed! id: '
            . $msg->{meta}->{id}
            . ', error: '
            . $self->couchdb->error;
        return;
    }

    $self->job($job);

    return $job;
}


sub job_done {
    my ($self, $msg) = @_;

    return $self->update_job($msg, 'done');
}


sub job_failed {
    my ($self, $msg) = @_;

    return $self->update_job($msg, 'failed');
}


sub job_pending {
    my ($self, $msg) = @_;

    return $self->update_job($msg, 'pending');
}


sub log_worker {
    my ($self, $msg) = @_;

    # no log yet? create it
    unless (exists $msg->{meta}->{log}) {
        push(@{ $msg->{meta}->{log} }, $msg->{meta}->{worker} || $self->name);
        return $msg;
    }

    # last worker same as current? ignore
    unless (
        $msg->{meta}->{log}->[-1] eq ($msg->{meta}->{worker} || $self->name))
    {
        push(@{ $msg->{meta}->{log} }, $msg->{meta}->{worker} || $self->name);
    }

    return $msg;
}


sub find_job {
    my ($self, $how, $key, $all) = @_;

    my $result;
    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    $result = $self->couchdb->get_array_view({
            view => "find/$how",
            opts => {
                key          => $key,
                include_docs => 'true',
            },
        });
    $self->couchdb->db($old_db);

    if ($result->[0]) {
        return @{$result} if $all;

        $self->job($result->[0]);
        return $result->[0];
    }

    return;
}


sub find_all_jobs {
    my ($self, @args) = @_;

    return $self->find_job(@args[ 0, 1 ], 'all');
}


sub stop_here {
    my ($self) = @_;

    $self->dont_reply if exists $self->job->{message}->{meta}->{id};

    return;
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise::Plugin::JobQueue - Daemonise JobQueue plugin

=head1 VERSION

version 1.61

=head1 SYNOPSIS

    use Daemonise;
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('JobQueue');
    
    $d->configure;
    
    # fetch job from "jobqueue_db" and put it in $d->job
    my $job_id = '585675aab87f878c9e98779e9e9c9ccadff';
    my $job    = $d->get_job($job_id);
    
    # creates a new job in "jobqueue_db"
    $job = $d->create_job({ some => { structured => 'hash' } });
    
    # starts new job by sending a new job message to workflow worker
    $d->start_job('workflow_name', "some platform identifier", { user => "kevin", command => "play" });
    
    # searches for job in couchdb using view 'find/by_something' and key provided
    $job = $d->find_job('by_bottles', "bottle_id");
    
    # if you REALLY have to persist something in jobqueue right now, rather don't
    $d->update_job($d->job->{message} || $job->{message});
    
    # stops workflow here if it is a job (if it has a message->meta->job_id)
    $d->stop_here;

=head1 ATTRIBUTES

=head2 jobqueue_db

=head2 job

=head1 SUBROUTINES/METHODS provided

=head2 configure

=head2 log

=head2 get_job

=head2 create_job

=head2 start_job

=head2 update_job

=head2 job_done

=head2 job_failed

=head2 job_pending

=head2 log_worker

=head2 find_job

=head2 find_all_jobs

=head2 stop_here

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
