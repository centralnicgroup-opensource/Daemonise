package Daemonise::Plugin::RabbitMQ;

use Mouse::Role;

# ABSTRACT: Daemonise RabbitMQ plugin

use Net::RabbitMQ;
use Carp;
use JSON;
use Try::Tiny;

=head1 SYNOPSIS

Example:

    use Daemonise;
    
    my $d = Daemonise->new();
    
    # this is the rabbitMQ queue name we eventually create and subscribe to
    $d->name("queue_name");
    
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('RabbitMQ');
    
    $d->is_worker(1); # make it create its own queue
    $d->configure;
    
    # fetch next message from our subscribed queue
    $msg = $d->dequeue;
    
    # send message to some_queue and don't wait (rabbitMQ publish)
    # $msg can be anything that encodes into JSON
    $d->queue('some_queue', $msg);
    
    # send message and wait for response (rabbitMQ RPC)
    my $response = $d->queue('some_queue', $msg);
    
    # worker that wants a reply from us, not necessarily from an RPC call
    my $worker = $d->reply_queue;
    
    # does NOT reply to caller, no matter what, usually not what you want
    $d->dont_reply;
    
    # at last, acknowledge message as processed
    $d->ack;

=cut

our $js = JSON->new;
$js->utf8;
$js->allow_blessed;
$js->convert_blessed;
$js->allow_nonref;

=head1 ATTRIBUTES

=head2 is_worker

=cut

has 'is_worker' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);

=head2 rabbit_host

=cut

has 'rabbit_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

=head2 rabbit_port

=cut

has 'rabbit_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 5672 },
);

=head2 rabbit_user

=cut

has 'rabbit_user' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'guest' },
);

=head2 rabbit_pass

=cut

has 'rabbit_pass' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'guest' },
);

=head2 rabbit_vhost

=cut

has 'rabbit_vhost' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { '/' },
);

=head2 rabbit_exchange

=cut

has 'rabbit_exchange' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'amq.direct' },
);

=head2 rabbit_channel

=cut

has 'rabbit_channel' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { int(rand(63999)) + 1 },
);

=head2 rabbit_consumer_tag

=cut

has 'rabbit_consumer_tag' => (
    is      => 'rw',
    lazy    => 1,
    default => sub { '' },
);

=head2 last_delivery_tag

=cut

has 'last_delivery_tag' => (
    is      => 'rw',
    lazy    => 1,
    default => sub { '' },
);

=head2 reply_queue

=cut

has 'reply_queue' => (
    is        => 'rw',
    lazy      => 1,
    clearer   => 'dont_reply',
    predicate => 'wants_reply',
    default   => sub { undef },
);

=head2 mq

=cut

has 'mq' => (
    is      => 'rw',
    isa     => 'Net::RabbitMQ',
    lazy    => 1,
    default => sub { Net::RabbitMQ->new },
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring RabbitMQ plugin") if $self->debug;

    $self->_setup_rabbit_connection;

    return;
};

=head2 queue

=cut

sub queue {
    my ($self, $queue, $hash, $reply_queue) = @_;

    my $rpc;
    $rpc = 1 if defined wantarray;

    unless (defined $hash) {
        $self->log("message hash missing");
        return;
    }

    unless ($queue) {
        $self->log("target queue missing");
        return;
    }

    my $tag;
    my $reply_channel = $self->rabbit_channel + 1;
    if ($rpc) {
        try {
            $self->mq->channel_open($reply_channel);
        } catch {
            warn "Could not open channel 1st time! >>" . $@ ."<<";
            $self->_setup_rabbit_connection;
            $self->mq->channel_open($reply_channel);
        };
        $self->log("opened channel $reply_channel for reply") if $self->debug;

        $reply_queue =
            $self->mq->queue_declare($reply_channel, '',
            { durable => 0, auto_delete => 1, exclusive => 1 });
        $self->log("declared reply queue $reply_queue") if $self->debug;

        $tag = $self->mq->consume($reply_channel, $reply_queue);
        $self->log("got consumer tag: " . $tag) if $self->debug;
    }

    my $props = { content_type => 'application/json' };
    $props->{reply_to} = $reply_queue if defined $reply_queue;

    my $err =
        $self->mq->publish($self->rabbit_channel, $queue, $js->encode($hash),
        undef, $props);

    if ($err) {
        $self->log("sending message failed: $err");
        return;
    }

    $self->log(
        "sent message to '$queue' using channel " . $self->rabbit_channel)
        if $self->debug;

    if ($rpc) {
        my $msg = $self->dequeue($tag);

        $self->mq->channel_close($reply_channel);
        $self->log("closed reply channel $reply_channel") if $self->debug;

        return $msg;
    }

    return;
}

=head2 dequeue

=cut

sub dequeue {
    my ($self, $tag) = @_;

    # clear reply_queue if this not an RPC response
    $self->dont_reply unless defined $tag;

    my $now;
    $now = time if defined $tag;

    my $frame;
    while (1) {
        $frame = $self->mq->recv();
        if (defined $tag) {
            last if ($frame->{consumer_tag} eq $tag);
            return
                if (time - $now) >
                ($self->config->{rabbitmq}->{timeout} || 180);
        }
        else {
            last if ($frame->{consumer_tag} eq $self->rabbit_consumer_tag);
        }

        require Data::Dump;
        $self->log("LOSING MESSAGE: " . Data::Dump::dump($frame));

    }

    # store delivery tag to ack later
    $self->last_delivery_tag($frame->{delivery_tag}) unless $tag;
    $self->reply_queue($frame->{props}->{reply_to})
        if exists $frame->{props}->{reply_to};

    # decode
    return $js->decode($frame->{body});
}

=head2 ack

=cut

sub ack {
    my ($self) = @_;

    $self->mq->ack($self->rabbit_channel, $self->last_delivery_tag);

    return;
}

sub _setup_rabbit_connection {
    my $self = shift;

    if (ref($self->config->{rabbitmq}) eq 'HASH') {
        foreach
            my $conf_key ('user', 'pass', 'host', 'port', 'vhost', 'exchange')
        {
            my $attr = "rabbit_" . $conf_key;
            $self->$attr($self->config->{rabbitmq}->{$conf_key})
                if defined $self->config->{rabbitmq}->{$conf_key};
        }
    }

    eval {
        $self->mq->connect(
            $self->rabbit_host, {
                user     => $self->rabbit_user,
                password => $self->rabbit_pass,
                port     => $self->rabbit_port,
                vhost    => $self->rabbit_vhost,
            });
    };
    my $err = $@;

    if ($err) {
        confess "Could not connect to the RabbitMQ server '"
            . $self->rabbit_host
            . "': $err\n";
    }

    $self->mq->channel_open($self->rabbit_channel);
    $self->log("opened channel " . $self->rabbit_channel) if $self->debug;

    return unless $self->is_worker;

    $self->log("preparing worker queue") if $self->debug;

    $self->mq->basic_qos($self->rabbit_channel, { prefetch_count => 1 });
    $self->mq->queue_declare($self->rabbit_channel, $self->name,
        { durable => 1, auto_delete => 0 });
    $self->rabbit_consumer_tag(
        $self->mq->consume($self->rabbit_channel, $self->name, { no_ack => 0 })
    );

    $self->log("got consumer tag: " . $self->rabbit_consumer_tag)
        if $self->debug;

    return;
}

1;
