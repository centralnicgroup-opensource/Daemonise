package Daemonise::Plugin::JobQueue;

use Mouse::Role;
use Data::Dumper;
use Carp;

has 'jobqueue_db' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'jobqueue' },
);

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring JobQueue plugin") if $self->debug;

    confess "this plugin requires the CouchDB plugin to be loaded as well"
        unless (%Daemonise::Plugin::CouchDB::);
};

sub get_job {
    my ($self, $id) = @_;

    confess "Job ID missing" unless $id;

    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    my $job = $self->couchdb->get_doc({ id => $id });
    $self->couchdb->db($old_db);

    carp("job ID not existing: $id") unless $job;

    return $job;
}

sub create_job {
    my ($self, $job) = @_;

    confess "{message}->{meta}->{platform} is missing, can't create job"
        unless ((ref($job) eq 'HASH')
        and (exists $job->{message}->{meta}->{platform}));

    $job->{created}     = time;
    $job->{last_update} = time;
    $job->{platform}    = $job->{message}->{meta}->{platform};
    $job->{status}      = 'requested';

    # $job_hash->{type} = split(/_/, $job->{message}->{data}->{command}, 2)->[0];

    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    my ($id, $rev) = $self->couchdb->put_doc({ doc => $job });
    $self->couchdb->db($old_db);

    confess 'creating job failed: ' . $self->couchdb->error unless $id;

    # store job ID within meta data to be able to find jobs easily later
    $job->{message}->{meta}->{id} = $id;
    $job->{_id}                   = $id;
    $job->{_rev}                  = $rev;
    $self->couchdb->db($self->jobqueue_db);
    $self->couchdb->put_doc({ doc => $job });
    $self->couchdb->db($old_db);

    confess 'updating job failed: ' . $self->couchdb->error
        if $self->couchdb->has_error;

    return $job;
}

sub update_job {
    my ($self, $msg, $status) = @_;

    confess "first argument must be a hashref"
        unless ((ref($msg) eq 'HASH') and (exists $msg->{meta}->{id}));

    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    my $job = $self->get_job($msg->{meta}->{id});

    return unless $job;

    $job->{last_update} = time;
    $job->{status} = $status || $job->{status};

    $self->couchdb->put_doc({ doc => $job });
    $self->couchdb->db($old_db);

    confess 'updating job failed! id: '
        . $msg->{meta}->{id}
        . ', error: '
        . $self->couchdb->error
        if $self->couchdb->has_error;

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

1;
