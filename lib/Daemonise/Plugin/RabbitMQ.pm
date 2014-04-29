package Daemonise::Plugin::RabbitMQ;

use 5.010;
use Mouse::Role;
use experimental 'smartmatch';

# ABSTRACT: Daemonise RabbitMQ plugin

use Net::RabbitMQ;
use Carp;
use JSON;
use Try::Tiny;


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


has 'admin_queue' => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => sub { 1 },
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
    default => sub { Net::RabbitMQ->new },
);


after 'configure' => sub {
    my ($self, $reconfig) = @_;

    if ($reconfig) {
        $self->log("closing channel " . $self->rabbit_channel) if $self->debug;
        $self->mq->channel_close($self->rabbit_channel);
    }

    $self->log("configuring RabbitMQ plugin") if $self->debug;

    $self->_setup_rabbit_connection;

    return;
};


sub queue {
    my ($self, $queue, $hash, $reply_queue, $exchange) = @_;

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
        }
        catch {
            warn "Could not open channel 1st time! >>" . $@ . "<<";
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

    my $options;
    $options->{exchange} = $exchange if $exchange;

    my $err =
        $self->mq->publish($self->rabbit_channel, $queue, $js->encode($hash),
        $options, $props);

    if ($err) {
        $self->log("sending message failed: $err");
        return;
    }

    $self->log("sent message to '$queue' via channel "
            . $self->rabbit_channel
            . " using "
            . ($exchange ? $exchange : 'amq.direct'))
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

    my $frame;
    my $msg;
    while (1) {
        $frame = $self->mq->recv();
        if (defined $tag) {
            unless (($frame->{consumer_tag} eq $tag)
                or ($frame->{consumer_tag} eq $self->rabbit_consumer_tag))
            {
                $self->log("LOSING MESSAGE: " . $self->dump($frame));
            }
        }

        # ACK all admin messages no matter what
        $self->mq->ack($self->rabbit_channel, $frame->{delivery_tag})
            if ($frame->{routing_key} =~ m/^admin/);

        # decode
        eval { $msg = $js->decode($frame->{body} || '{}'); };
        if ($@) {
            $self->log("JSON parsing error: $@");
            $msg = {};
        }

        last unless ($frame->{routing_key} =~ m/^admin/);

        if (   ($frame->{routing_key} eq 'admin')
            or ($frame->{routing_key} eq 'admin.' . $self->hostname))
        {
            given ($msg->{command} || 'restart') {
                when ('configure') {
                    $self->log("reconfiguring");
                    $self->configure('reconfig');
                }
                when ('restart') {
                    my $name = $self->name;
                    if (grep { $_ eq $name } @{ $msg->{daemons} || [$name] }) {
                        $self->stop;
                    }
                }
            }
        }
    }

    # store delivery tag to ack later
    $self->last_delivery_tag($frame->{delivery_tag}) unless $tag;
    $self->reply_queue($frame->{props}->{reply_to})
        if exists $frame->{props}->{reply_to};

    return $msg;
}


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
    $self->mq->queue_bind(
        $self->rabbit_channel,  $self->name,
        $self->rabbit_exchange, $self->name,
    );
    $self->rabbit_consumer_tag(
        $self->mq->consume($self->rabbit_channel, $self->name, { no_ack => 0 })
    );
    $self->log("got consumer tag: " . $self->rabbit_consumer_tag)
        if $self->debug;

    # setup admin queue per worker using z.queue.host.pid
    # and bind to fanout exchange
    if ($self->admin_queue) {
        my $admin_queue = join('.', 'z', $self->name, $self->hostname, $$);

        $self->log("declaring and binding admin queue " . $admin_queue)
            if $self->debug;

        $self->mq->queue_declare($self->rabbit_channel, $admin_queue,
            { durable => 0, auto_delete => 1 });
        $self->mq->queue_bind($self->rabbit_channel, $admin_queue, 'amq.fanout',
            'admin');
        $self->mq->queue_bind($self->rabbit_channel, $admin_queue, 'amq.fanout',
            'admin.' . $self->hostname);
        $self->mq->consume($self->rabbit_channel, $admin_queue,
            { no_ack => 0 });
    }

    return;
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Daemonise::Plugin::RabbitMQ - Daemonise RabbitMQ plugin

=head1 VERSION

version 1.82

=head1 SYNOPSIS

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

=head1 ATTRIBUTES

=head2 is_worker

=head2 rabbit_host

=head2 rabbit_port

=head2 rabbit_user

=head2 rabbit_pass

=head2 rabbit_vhost

=head2 rabbit_exchange

=head2 rabbit_channel

=head2 rabbit_consumer_tag

=head2 last_delivery_tag

=head2 admin_queue

=head2 reply_queue

=head2 mq

=head1 SUBROUTINES/METHODS provided

=head2 configure

=head2 queue

=head2 dequeue

=head2 ack

=head1 AUTHOR

Lenz Gschwendtner <norbu09@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Lenz Gschwendtner.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
