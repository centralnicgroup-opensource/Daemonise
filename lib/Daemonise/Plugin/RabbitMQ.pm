package Daemonise::Plugin::RabbitMQ;

use Mouse::Role;
use Net::RabbitMQ;
use Carp;

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
    default => sub { 1 },
);

has 'rabbit_last_response' => (
    is      => 'rw',
    lazy    => 1,
    default => '',
);

has 'rabbit_last_consumer_tag' => (
    is      => 'rw',
    lazy    => 1,
    default => '',
);

has 'mq' => (
    is  => 'rw',
    isa => 'Net::RabbitMQ',
);

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring RabbitMQ plugin") if $self->debug;

    $self->rabbit_user($self->config->{rabbitmq}->{user})
        if $self->config->{rabbitmq}->{user};
    $self->rabbit_pass($self->config->{rabbitmq}->{pass})
        if $self->config->{rabbitmq}->{pass};
    $self->rabbit_host($self->config->{rabbitmq}->{host})
        if $self->config->{rabbitmq}->{host};
    $self->rabbit_port($self->config->{rabbitmq}->{port})
        if $self->config->{rabbitmq}->{port};
    $self->rabbit_vhost($self->config->{rabbitmq}->{vhost})
        if $self->config->{rabbitmq}->{vhost};
    $self->rabbit_exchange($self->config->{rabbitmq}->{exchange})
        if $self->config->{rabbitmq}->{exchange};

    return;
};

sub queue_bind {
    my ($self, $queue) = @_;

    my $tag;
    if ($queue) {
        $self->_amqp();
        eval { $tag = $self->mq->consume($self->rabbit_channel, $queue); };
        if ($@) {
            $tag = $self->_consume_queue($queue);
        }
        $self->rabbit_last_consumer_tag($tag);
        $self->log(
            "Bound to queue '$queue' using channel " . $self->rabbit_channel);
        return $self->rabbit_channel;
    }
    else {
        confess "You have to provide a queue to bind to!";
    }
    return;
}

sub msg_pub {
    my ($self, $msg, $queue) = @_;

    my $rep;
    if ($msg) {
        $self->_get_channel;
        eval { $self->mq->channel_open($self->rabbit_channel); };
        if ($@) {
            $self->_amqp();
        }
        eval { $rep = $self->mq->publish($self->rabbit_channel, $queue, $msg); };
        if ($@) {
            carp "Could not send message: $@\n";
            return;
        }
        $self->mq->channel_close($self->rabbit_channel);
    }
    else {
        carp "You have to provide a message to send!";
        return;
    }
    $self->log("Sent message to queue '$queue' with channel "
            . $self->rabbit_channel
            . " ($rep)")
        if $self->debug;
    return;
}

sub msg_rpc {
    my ($self, $msg, $queue, $reply_queue, $_tag) = @_;

    my $rep;
    my @channels;
    my $tag = $_tag;

    $reply_queue = 'reply.' . $$ unless $reply_queue;

    if ($msg) {
        $tag = $self->_consume_queue($reply_queue, 'temp')
            unless $tag;
        my $rep_chan = $self->rabbit_channel;
        my $fwd_chan = $self->_get_channel;
        eval {
            $self->mq->channel_open($fwd_chan);
            sleep 1;
            $rep = $self->mq->publish($fwd_chan, $queue, $msg);
        };
        if ($@) {
            carp "Could not send message: $@\n";
            return;
        }
        my $reply;
        my $now = time;
        while (1) {
            $reply = $self->mq->recv();
            last if ($reply->{consumer_tag} eq $tag);
            last if (time - $now) > 180;
        }
        $self->log("Got reply on queue $reply_queue") if $self->debug;
        $self->rabbit_last_response($reply->{body});
        $self->mq->channel_close($fwd_chan);
        $self->mq->channel_close($rep_chan) unless $_tag;
    }
    else {
        carp "You have to provide a message to send!";
        return;
    }
    $self->log("Sent message to queue '$queue' with channel "
            . $self->rabbit_channel
            . " ($rep)")
        if $self->debug;
    return $self->rabbit_last_response;
}

sub _amqp {
    my $self = shift;

    my $amqp = Net::RabbitMQ->new();
    eval {
        $amqp->connect(
            $self->rabbit_host, {
                user     => $self->rabbit_user,
                password => $self->rabbit_pass,
                port     => $self->rabbit_port,
                vhost    => $self->rabbit_vhost,
            });
    };
    if ($@) {
        carp "Could not connect to the RabbitMQ server! $@\n";
        return;
    }
    $self->_get_channel;
    $self->log("Trying to open channel: " . $self->rabbit_channel);
    $amqp->channel_open($self->rabbit_channel);

    #$d->log("Declaring exchange: " . $self->rabbit_exchange);
    #eval { $amqp->exchange_declare( $self->rabbit_channel, $self->rabbit_exchange, { durable => 1, auto_delete => 0 } ); };
    #if ($@) {
    #    $d->log("Exchange " . $self->rabbit_exchange . " already defined: $@");
    #}
    $self->log("Initialized connection successfully");
    $self->mq($amqp);
    return $amqp;
}

sub _consume_queue {
    my ($self, $queue, $temp) = @_;

    $self->_get_channel;
    eval { $self->mq->channel_open($self->rabbit_channel); };
    if ($@) {
        $self->_amqp();
    }
    my $tag;
    if ($temp) {
        eval {
            $self->mq->queue_declare($self->rabbit_channel, $queue,
                { durable => 0, auto_delete => 1, exclusive => 1 });
            $tag = $self->mq->consume($self->rabbit_channel, $queue);
        };
    }
    else {
        eval {
            $self->mq->queue_declare($self->rabbit_channel, $queue,
                { durable => 1, auto_delete => 0 });

            # $self->mq->exchange_declare($self->rabbit_channel,
            #     $self->rabbit_exchange, { durable => 1, auto_delete => 0 });
            $self->mq->queue_bind(
                $self->rabbit_channel,  $queue,
                $self->rabbit_exchange, $queue
            );
            $tag = $self->mq->consume($self->rabbit_channel, $queue);
        };
    }
    if ($@) {
        carp "Could not setup the listening queue: $@\n";
        return;
    }
    $self->rabbit_last_consumer_tag($tag);
    return $tag;
}

sub _get_channel {
    my $self = shift;

    my $chan = $self->rabbit_channel;
    $chan = int(rand(63999)) + 1;
    $self->rabbit_channel($chan);

    return $chan;
}

1;
