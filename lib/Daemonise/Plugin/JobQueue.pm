package Daemonise::Plugin::JobQueue;

use feature 'switch';
use Mouse::Role;
use Data::Dumper;
use Digest::MD5 'md5_hex';
use DateTime;
use Carp;

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
    my ($self) = @_;

    $self->log("configuring JobQueue plugin") if $self->debug;

    confess "this plugin requires the CouchDB plugin to be loaded as well"
        unless (%Daemonise::Plugin::CouchDB::);
    confess "this plugin requires the RabbitMQ plugin to be loaded as well"
        unless (%Daemonise::Plugin::RabbitMQ::);
};

around 'log' => sub {
    my ($orig, $self, $msg) = @_;

    unless ((exists $self->job->{_id}) and (exists $self->job->{platform})) {
        $self->$orig($msg);
        return;
    }

    $msg =
          'platform='
        . $self->job->{platform}
        . ' job_id='
        . $self->job->{_id} . ' '
        . $msg;
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
        carp("job ID not existing: $id");
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
            user     => $options->{user_id},
        },
        data => {
            command => $workflow,
            options => $options,
        },
    };
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
    my ($self, $how, $key) = @_;

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
        $self->job($result->[0]);
        return $result->[0];
    }

    return;
}

1;
