package Daemonise::Plugin::JobQueue;

use Mouse::Role;

# ABSTRACT: Daemonise JobQueue plugin

use Data::Dumper;
use Digest::MD5 'md5_hex';
use DateTime;
use Carp;
use Basket::Calc;
use Scalar::Util qw(looks_like_number);

BEGIN {
    with("Daemonise::Plugin::CouchDB");
    with("Daemonise::Plugin::RabbitMQ");
    with("Daemonise::Plugin::KyotoTycoon");
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


has 'items_key' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'domains' },
);


has 'item_key' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'domain' },
);


has 'mark_worker' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
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

    $self->$orig($msg);

    return;
};


around 'start' => sub {
    my ($orig, $self, $code) = @_;

    my $wrapper = sub {
        my $msg = $self->dequeue;

        # skip processing if message was empty
        return unless $msg;

        $msg = $code->($msg);

        $self->log("ERROR: " . $msg->{error}) if (exists $msg->{error});

        # log worker and update job if we have to
        if ($self->mark_worker) {
            $self->log_worker($msg);
            $self->update_job($msg, delete $msg->{status});
        }

        # reply if needed and wanted
        $self->queue($self->reply_queue, $msg) if $self->wants_reply;

        # at last, ack message
        $self->ack;

        return;
    };

    return $self->$orig($wrapper);
};


around 'dequeue' => sub {
    my ($orig, $self, $tag) = @_;

    my $msg = $self->$orig($tag);
    $self->job({ message => $msg }) unless $tag;

    if (    exists $msg->{meta}
        and ref $msg->{meta} eq 'HASH'
        and exists $msg->{meta}->{id})
    {
        my $key     = 'activejob:' . $msg->{meta}->{id};
        my $value   = $self->name . ':' . $$;
        my $success = $self->lock($key, $value);

        # return undef to prevent any further processing
        unless ($success) {
            $self->ack;
            return;
        }
    }

    return $msg;
};


around 'queue' => sub {
    my ($orig, $self, $queue, $msg, $reply_queue, $exchange) = @_;

    if (    ref $msg eq 'HASH'
        and exists $msg->{meta}
        and ref $msg->{meta} eq 'HASH'
        and exists $msg->{meta}->{id}
        and defined $msg->{meta}->{id})
    {
        my $key   = 'activejob:' . $msg->{meta}->{id};
        my $value = $self->name . ':' . $$;
        $self->unlock($key, $value);
    }

    return $self->$orig($queue, $msg, $reply_queue, $exchange);
};


before 'ack' => sub {
    my ($self) = @_;

    $self->job({});

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

    unless (ref $msg eq 'HASH') {
        carp "msg must be a HASH";
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
        $self->job($job);
        return $job, 1;
    }

    $msg->{meta}->{id} = $id;
    $job = {
        _id     => $id,
        created => $created,
        updated => $created,
        message => $msg,
        status  => 'new',
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
    my ($self, $workflow, $options) = @_;

    unless ($workflow) {
        carp 'workflow not defined';
        return;
    }

    $options = {} unless $options;

    my $frame = {
        meta => {
            lang => 'en',
            user => $options->{user_id} || undef,
        },
        data => {
            command => $workflow,
            options => $options,
        },
    };

    # tell the new job who created it
    $frame->{meta}->{created_by} = $self->job->{message}->{meta}->{id}
        if exists $self->job->{message}->{meta}->{id};

    $self->log("starting '$workflow' workflow with:\n" . $self->dump($frame))
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

    my @result;
    my $old_db = $self->couchdb->db;
    $self->couchdb->db($self->jobqueue_db);
    @result = $self->couchdb->get_view_array({
            view => "find/$how",
            opts => {
                key          => $key,
                include_docs => 'true',
            },
        });
    $self->couchdb->db($old_db);

    if (@result) {
        return @result if $all;

        $self->job($result[0]);
        return $result[0];
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


sub recalculate {
    my ($self) = @_;

    unless (ref $self->job eq 'HASH') {
        carp 'needs a job HashRef in the job attribute!';
        return;
    }

    my $data = $self->job->{message}->{data}->{options};

    unless (exists $data->{ $self->items_key }
        and scalar(@{ $data->{ $self->items_key } || [] }) > 0)
    {
        carp 'no ' . $self->items_key . ' found, can\'t recalculate';
        return;
    }

    $self->log("re-calculating total!");

    my $old_total = $data->{total} || 0;

    my $basket = Basket::Calc->new(
        currency => ($data->{currency} || 'USD'),
        debug => $self->debug,
    );

    # add tax if applicable
    $basket->tax($data->{tax} / 100)
        if (exists $data->{tax} and $data->{tax});

    # add item prices
    for my $item (@{ $data->{ $self->items_key } }) {
        next unless exists $item->{amount};
        next unless looks_like_number($item->{amount});
        next unless $item->{amount} > 0;

        $basket->add_item({
            price    => $item->{amount},
            quantity => ($item->{interval} || 1),
        });
    }

    # add discounts if applicable
    # TODO: support for other discount types, e.g. fixed amount
    $basket->add_discount(
        { type => 'percent', value => $data->{discount} / 100 })
        if (exists $data->{discount} and $data->{discount});

    # calculate and update totals
    my $result = $basket->calculate;
    $data->{total} = $result->{value};

    if ($result->{tax_amount}) {
        $data->{tax_amount} = $result->{tax_amount};
        $data->{net}        = $result->{net};
    }

    $self->log("new total: "
            . $data->{total} . ' '
            . $data->{currency}
            . ' was: '
            . $old_total . ' '
            . $data->{currency});

    return 1;
}


sub find_item_index {
    my ($self, $data, $item) = @_;

    for (my $i = 0; $i <= $#{ $data->{ $self->items_key } }; $i++) {
        if ($data->{ $self->items_key }->[$i]->{ $self->item_key } eq $item) {
            return $i;
        }
    }

    return;
}


sub remove_items {
    my ($self, $data, @items_to_remove) = @_;

    my @removed;
    my $dest_key = 'removed_' . $self->items_key;

    foreach (@items_to_remove) {
        my $i = $self->find_item_index($data, $_);

        # remove this item from job
        push(@removed, splice(@{ $data->{ $self->items_key } }, $i, 1))
            if defined $i;
    }

    # store removed items in removed_items key for record
    if (exists $data->{$dest_key} and ref $data->{$dest_key} eq 'ARRAY') {
        push(@{ $data->{$dest_key} }, $_) for (@removed);
    }
    elsif (@removed) {
        $data->{$dest_key} = \@removed;
    }

    return @removed;
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise::Plugin::JobQueue - Daemonise JobQueue plugin

=head1 VERSION

version 1.86

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
    $d->start_job('workflow_name', { user => "kevin", command => "play" });
    
    # searches for job in couchdb using view 'find/by_something' and key provided
    $job = $d->find_job('by_bottles', "bottle_id");
    
    # if you REALLY have to persist something in jobqueue right now, rather don't
    $d->update_job($d->job->{message} || $job->{message});
    
    # mark job as done and persist
    $d->job_done($d->job->{message});
    
    # stops workflow here if it is a job (if it has a message->meta->job_id)
    $d->stop_here;
    
    # recalculate totals
    $d->recalculate;
    
    # remove items from a job
    $d->remove_items($d->job->{message}->{data}->{options}, qw(item1 item2 item3));

=head1 ATTRIBUTES

=head2 jobqueue_db

=head2 job

=head2 items_key

=head2 item_key

=head2 mark_worker

=head1 SUBROUTINES/METHODS provided

=head2 configure

=head2 log

=head2 start (around)

wrap 

=head2 dequeue (around)

store rabbitMQ message in job attribute after receiving

if message is a job, lock rabbit on it using "activejob:job_id" as key and
"some.rabbit.name:PID" as lock value.

if locking fails, throw error and ack() message to prevent repeating.

=head queue (around)

if message is a job, unlock rabbit from it before sending it on

=head2 ack (before)

empty job attribute before acknowledging a rabbitMQ message

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

=head2 recalculate

=head2 find_item_index

Find the index of a particular item name in the C<$data->{items_key}> array.

Return undef if not found.

=head2 remove_items

Given a list of item names to remove, remove them from C<$data->{$self->items_key}>
array and save them in C<$data->{'removed_'  . $self->items_key}> array.

If item isn't found, it is silently ignored.

Return value is an array of removed item hashes.

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
