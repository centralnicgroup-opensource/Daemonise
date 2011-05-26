#!/usr/bin/perl -Ilib

package Daemonise::Plugin::RabbitMQ;

use Mouse::Role;
use Net::RabbitMQ;

has 'rabbit_host' => (
    is      => 'rw',
    default => sub { 'localhost' },
);

has 'rabbit_port' => (
    is      => 'rw',
    default => sub { 5672 },
);

has 'rabbit_user' => (
    is      => 'rw',
    default => sub { 'guest' },
);

has 'rabbit_pass' => (
    is      => 'rw',
    default => sub { 'guest' },
);

has 'rabbit_vhost' => (
    is      => 'rw',
    default => sub { '/' },
);

has 'rabbit_exchange' => (
    is      => 'rw',
    default => sub { 'amq.direct' },
);

has 'rabbit_channel' => (
    is      => 'rw',
    default => sub { 2 },
);

has 'queue' => (
    is      => 'rw',
    default => sub { },
);

after 'queue_bind' => sub {
    my ( $self, $queue ) = @_;

    if ($queue) {
        $self->_amqp();
        eval { $self->queue->consume( $self->rabbit_channel, $queue ); };
        if ($@) {
            $self->rabbit_channel($self->rabbit_channel + 1);
            eval { $self->queue->channel_open( $self->rabbit_channel ); };
            eval { $self->queue->queue_declare( $self->rabbit_channel, $queue, { durable => 1, auto_delete => 0 } ); };
            eval { $self->queue->exchange_declare( $self->rabbit_channel, $self->rabbit_exchange, { durable => 1, auto_delete => 0 } ); };
            eval { $self->queue->queue_bind( $self->rabbit_channel, $queue, $self->rabbit_exchange, $queue ); };
            eval { $self->queue->consume( $self->rabbit_channel, $queue ); };
            if ($@) {
                confess "Could not setup the listening queue: $@\n";
            }
        }
        print STDERR "Bound to queue '$queue'\n";
        return $self->rabbit_channel;
    }
    else {
        confess "You have to provide a queue to bind to!";
    }
    return;
};

after 'msg_pub' => sub {
    my ( $self, $queue, $msg ) = @_;
    if ($msg) {
        $self->rabbit_channel($self->rabbit_channel + 1);
        eval{$self->queue->publish( $self->rabbit_channel, $queue, $msg);};
        if($@){
            $self->_amqp();
            eval{$self->queue->publish( $self->rabbit_channel, $queue, $msg);};
            if($@){
                confess "Could not send message: $@\n";
            }
        }
    }
    else {
        confess "You have to provide a message to send!";
    }
    print STDERR "Sent message to queue '$queue'\n";
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
    $self->queue($amqp);
    return $amqp;
}

1;
