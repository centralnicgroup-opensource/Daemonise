package Daemonise::Plugin::HipChat;

use Mouse::Role;

# ABSTRACT: Daemonise HipChat plugin

use LWP::UserAgent;

# severity to message colour mapping
my %colour = (
    debug    => 'gray',
    info     => 'green',
    warning  => 'yellow',
    critical => 'purple',
    error    => 'red',
);

=head1 SYNOPSIS

    use Daemonise;
    
    my $d = Daemonise->new();
    $d->debug(1);
    $d->foreground(1) if $d->debug;
    $d->config_file('/path/to/some.conf');
    
    $d->load_plugin('HipChat');
    
    $d->configure;
    
    # send a message to the default 'log' channel
    $d->notify("some text");
    
    # send a message to the specified hipchat channel
    $d->notify("some text", 'channel');


=head1 ATTRIBUTES

=head2 hipchat_url

=cut

has 'hipchat_url' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub {
        'https://api.hipchat.com/v1/rooms/message?format=json&auth_token=';
    },
);

=head2 hipchat_token

=cut

has 'hipchat_token' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

=head2 hipchat_from

=cut

has 'hipchat_from' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'Daemonise' },
);

=head2 hipchat_room

=cut

has 'hipchat_room' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'log' },
);

=head1 SUBROUTINES/METHODS provided

=head2 configure

=cut

after 'configure' => sub {
    my ($self, $reconfig) = @_;

    $self->log("configuring HipChat plugin") if $self->debug;

    if (    ref($self->config->{api}) eq 'HASH'
        and ref($self->config->{api}->{hipchat}) eq 'HASH'
        and ref($self->config->{api}->{hipchat}->{token}) eq 'HASH')
    {
        $self->hipchat_token(
            $self->config->{api}->{hipchat}->{token}->{default})
            if defined $self->config->{api}->{hipchat}->{token}->{default};
        $self->hipchat_room($self->config->{api}->{hipchat}->{room})
            if defined $self->config->{api}->{hipchat}->{room};
    }

    # truncate name to 15 characters as that's the limit for the "From" field...
    $self->hipchat_from(substr($self->name, 0, 15));

    return;
};

=head2 notify

    Arguments: 

    $msg is the message to send.
    $room is the room to send to.
    $severity is the type of message.
    $notify_users is whether the message will mark the room as having unread messages.
    $message_format indicates the format of the message. "html" or "text".

    More details at https://www.hipchat.com/docs/api/method/rooms/message
=cut

sub notify {
    my ($self, $msg, $room, $severity, $notify_users, $message_format) = @_;
    
    $self->log($msg);

    $msg = '[debug] ' . $msg if $self->debug;

    # fork and to the rest asynchronously
    # $self->async and return;

    my $colour = ($colour{ $severity || 'info' }) || 'info';
    my $ua = LWP::UserAgent->new(agent => $self->name);
    my $res = $ua->post(
        $self->hipchat_url . $self->hipchat_token, {
            room_id => $room || $self->hipchat_room,
            from    => $self->hipchat_from,
            message => $self->hostname . ': ' . $msg,
            message_format => $message_format || 'text',
            notify         => $notify_users || 0,
            color          => $colour,
        });

    unless ($res->is_success) {
        $self->log($res->status_line . ': ' . $res->decoded_content);
    }

    # exit; # async
    return;
}

1;
