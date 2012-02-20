package Daemonise::Plugin::JobQueue;

use feature 'switch';
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
    my ($self, $msg) = @_;

    confess "{meta}->{platform} is missing, can't create job"
        unless ((ref($msg) eq 'HASH')
        and (exists $msg->{meta}->{platform}));

    my $job = {
        created  => time,
        updated  => time,
        message  => $msg,
        platform => $msg->{meta}->{platform},
        status   => 'requested',
    };

    # $job->{type} = split(/_/, $msg->{data}->{command}, 2)->[0];

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

    unless ((ref($msg) eq 'HASH') and (exists $msg->{meta}->{id})) {
        $self->log("not a JOB, just a message, nothing to see here");
        return;
    }

    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    my $job = $self->get_job($msg->{meta}->{id});
    $self->couchdb->db($old_db);

    return unless $job;

    $job->{updated} = time;
    $job->{status}  = $status || $job->{status};
    $job->{message} = $msg;

    $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
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

sub log_worker {
    my ($self, $msg) = @_;

    push(@{ $msg->{meta}->{log} }, $msg->{meta}->{worker} || $self->name);

    return $msg;
}

sub find_job {
    my ($self, $how, $info) = @_;

    my $result;
    given ($how) {
        when ('by_billing_callback') {
            my $old_db = $self->couchdb->db;
            $self->couchdb->db($self->jobqueue_db);
            $result = $self->couchdb->get_array_view({
                    view => 'billing/billing_callback',
                    opts => {
                        key => [ $info->{transaction_id}, $info->{platform} ]
                    },
                });
            $self->couchdb->db($old_db);
        }
        default { return; }
    }

    return $result->[0] if ($result->[0]);
    return;
}

1;
