package Daemonise::Plugin::Event;

use feature 'switch';
use Mouse::Role;
use Carp;
use JSON;
use Data::UUID;

has 'event_db' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'events' },
);

has 'event_queue' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'event' },
);

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring Events plugin") if $self->debug;

    confess "this plugin requires the CouchDB plugin to be loaded as well"
        unless (%Daemonise::Plugin::CouchDB::);

    confess "this plugin requires the RabbitMQ plugin to be loaded as well"
        unless (%Daemonise::Plugin::RabbitMQ::);

    $self->event_db($self->config->{events}->{db})
        if exists $self->config->{events}->{db};
    $self->event_queue($self->config->{events}->{queue})
        if exists $self->config->{events}->{queue};

    $self->couchdb->db($self->event_db);
};

sub create_event {
    my ($self, $type, $platform, $key, $offset, $data) = @_;

    # base event keys/values
    my $now   = time;
    my $event = {
        type      => 'event',
        platform  => $platform,
        timestamp => $now,
    };

    # add mapped stuff
    given ($type) {
        when ('restart_workflow') {
            $event = {
                %$event,
                backend => 'internal',
                object  => 'workflow',
                action  => 'restart',
                status  => 'none',
                job_id  => $key,
            };
        }
    }

    if ($data) {
        $event->{parsed} = $data->{parsed};
        $event->{raw}    = $data->{raw};
    }

    # offset is number of hours
    if ($offset) {

        # is it a timestamp of an offset?
        $event->{when} =
            length($offset) >= 10 ? int $offset : $now + ($offset * 60 * 60);
        my $old_db = $self->couchdb->db;
        $self->couchdb->db($self->event_db);
        my ($id, $rev) = $self->couchdb->put_doc({ doc => $event });
        if ($self->couchdb->error) {
            $self->log("failed creating $type event: " . $self->couchdb->error);
            return;
        }
        $self->couchdb->db($old_db);
        return $id;
    }
    else {
        my $frame = {
            meta => {
                platform => $event->{platform},
                lang     => 'en',
                reply_to => Data::UUID->new->create_str(),
            },
            data => {
                command => "event_add",
                options => $event,
            },
        };
        my $res = decode_json(
            $self->msg_rpc(
                encode_json($frame), $self->event_queue,
                $frame->{meta}->{reply_to}));
        if ($res->{error}) {
            $self->log(
                "failed to create $type event via queue: " . $res->{error});
            return;
        }

        return $res->{response}->{event_id};
    }
}

1;
