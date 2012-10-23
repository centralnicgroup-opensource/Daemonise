package Daemonise::Plugin::RabbitMQ;

use Mouse::Role;
use Net::RabbitMQ;
use Carp;
use JSON;

our $js = JSON->new;
$js->utf8;
$js->allow_blessed;
$js->convert_blessed;
$js->allow_nonref;

has 'is_worker' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 0 },
);

has 'rabbit_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

has 'rabbit_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { 5672 },
);

has 'rabbit_user' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'guest' },
);

has 'rabbit_pass' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'guest' },
);

has 'rabbit_vhost' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { '/' },
);

has 'rabbit_exchange' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'amq.direct' },
);

has 'rabbit_channel' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { int(rand(63999)) + 1 },
);

has 'rabbit_consumer_tag' => (
    is      => 'rw',
    lazy    => 1,
    default => sub { '' },
);

has 'last_delivery_tag' => (
    is      => 'rw',
    lazy    => 1,
    default => sub { '' },
);

has 'reply_queue' => (
    is        => 'rw',
    lazy      => 1,
    clearer   => 'dont_reply',
    predicate => 'wants_reply',
    default   => sub { undef },
);

has 'mq' => (
    is      => 'rw',
    isa     => 'Net::RabbitMQ',
    lazy    => 1,
    default => sub { new Net::RabbitMQ },
);

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring RabbitMQ plugin") if $self->debug;

    $self->rabbit_user($self->config->{rabbitmq}->{user})
        if exists $self->config->{rabbitmq}->{user};
    $self->rabbit_pass($self->config->{rabbitmq}->{pass})
        if exists $self->config->{rabbitmq}->{pass};
    $self->rabbit_host($self->config->{rabbitmq}->{host})
        if exists $self->config->{rabbitmq}->{host};
    $self->rabbit_port($self->config->{rabbitmq}->{port})
        if exists $self->config->{rabbitmq}->{port};
    $self->rabbit_vhost($self->config->{rabbitmq}->{vhost})
        if exists $self->config->{rabbitmq}->{vhost};
    $self->rabbit_exchange($self->config->{rabbitmq}->{exchange})
        if exists $self->config->{rabbitmq}->{exchange};

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
};

sub queue {
    my ($self, $queue, $hash, $reply_queue) = @_;

    my $rpc = 1 if defined wantarray;

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
        $self->mq->channel_open($reply_channel);
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

    $self->mq->publish($self->rabbit_channel, $queue, $js->encode($hash), undef,
        $props);
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

sub dequeue {
    my ($self, $tag) = @_;

    # clear reply_queue if this not an RPC response
    $self->dont_reply unless defined $tag;

    my $now = time if defined $tag;
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
        $self->log("LOSING MESSAGE:\n" . Dumper($frame));
    }

    # store delivery tag to ack later
    $self->last_delivery_tag($frame->{delivery_tag}) unless $tag;
    $self->reply_queue($frame->{props}->{reply_to})
        if exists $frame->{props}->{reply_to};

    # decode
    return $js->decode($frame->{body});
}

sub ack {
    my ($self) = @_;

    $self->mq->ack($self->rabbit_channel, $self->last_delivery_tag);
}

1;
