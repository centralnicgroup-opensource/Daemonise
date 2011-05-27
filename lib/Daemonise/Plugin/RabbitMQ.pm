#!/usr/bin/perl -Ilib

package Daemonise::Plugin::RabbitMQ;

use Mouse::Role;
use Net::RabbitMQ;

has 'rabbit_host' => (
    is      => 'rw',
    isa     => 'Str',
    default => sub { 'localhost' },
);

has 'rabbit_port' => (
    is      => 'rw',
    isa     => 'Int',
    default => 5672,
);

has 'rabbit_user' => (
    is      => 'rw',
    isa     => 'Str',
    default => 'guest',
);

has 'rabbit_pass' => (
    is      => 'rw',
    isa     => 'Str',
    default => 'guest',
);

has 'rabbit_vhost' => (
    is      => 'rw',
    isa     => 'Str',
    default => '/',
);

has 'rabbit_exchange' => (
    is      => 'rw',
    isa     => 'Str',
    default => 'amq.direct',
);

has 'rabbit_channel' => (
    is      => 'rw',
    isa     => 'Int',
    default =>  1,
);

has 'mq' => (
    is      => 'rw',
    isa     => 'Net::RabbitMQ',
    default => sub { },
);

after 'queue_bind' => sub {
    my ( $self, $queue ) = @_;

    if ($queue) {
        $self->_amqp();
        eval { $self->mq->consume( $self->rabbit_channel, $queue ); };
        if ($@) {
            $self->rabbit_channel($self->rabbit_channel + 1);
            eval { $self->mq->channel_open( $self->rabbit_channel ); };
            eval { $self->mq->queue_declare( $self->rabbit_channel, $queue, { durable => 1, auto_delete => 0 } ); };
            eval { $self->mq->exchange_declare( $self->rabbit_channel, $self->rabbit_exchange, { durable => 1, auto_delete => 0 } ); };
            eval { $self->mq->queue_bind( $self->rabbit_channel, $queue, $self->rabbit_exchange, $queue ); };
            eval { $self->mq->consume( $self->rabbit_channel, $queue ); };
            if ($@) {
                confess "Could not setup the listening queue: $@\n";
            }
        }
        print STDERR "Bound to queue '$queue' using channel ".$self->rabbit_channel."\n";
        return $self->rabbit_channel;
    }
    else {
        confess "You have to provide a queue to bind to!";
    }
    return;
};

after 'msg_pub' => sub {
    my ( $self, $msg, $queue ) = @_;
    my $rep;
    if ($msg) {
        $self->_get_channel;
        eval{$self->mq->channel_open($self->rabbit_channel);};
        if($@){
            $self->_amqp();
        }
        eval{$rep = $self->mq->publish( $self->rabbit_channel, $queue, $msg);};
        if($@){
            confess "Could not send message: $@\n";
        }
        $self->mq->channel_close($self->rabbit_channel);
    }
    else {
        confess "You have to provide a message to send!";
    }
    print STDERR "Sent message to queue '$queue' with channel ".$self->rabbit_channel." ($rep)\n";
    return;
};

sub _amqp {
    my $self = shift;

    my $amqp = Net::RabbitMQ->new();
    eval {
        $amqp->connect(
            $self->rabbit_host,
            {
                user     => $self->rabbit_user,
                password => $self->rabbit_pass,
                port     => $self->rabbit_port,
                vhost    => $self->rabbit_vhost,
            }
        );
    };
    if ($@) {
        confess "Could not connect to the RabbitMQ server! $@\n";
    }
    print STDERR "Trying to open channel: ".$self->rabbit_channel."\n";
    $amqp->channel_open( $self->rabbit_channel );
    print STDERR "Declaring exchange: ".$self->rabbit_exchange."\n";
    eval { $amqp->exchange_declare( $self->rabbit_channel, $self->rabbit_exchange, { durable => 1, auto_delete => 0 } ); };
    if ($@) {
        print "Exchange " . $self->rabbit_exchange . " already defined: $@\n";
    }
    print STDERR "Initialized connection successfully\n";
    $self->mq($amqp);
    return $amqp;
}

sub _get_channel {
    my $self = shift;
    my $num = $self->rabbit_channel;
    $num++;
    $num = 1 if($num > 64000);
    $self->rabbit_channel($num);
    return $num;
}

1;
