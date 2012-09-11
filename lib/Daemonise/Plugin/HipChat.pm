package Daemonise::Plugin::HipChat;

use Mouse::Role;
use LWP::UserAgent;
use Carp;
use Data::Dumper;

has 'hipchat_url' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub {
        'https://api.hipchat.com/v1/rooms/message?format=json&auth_token=';
    },
);

has 'hipchat_token' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'hipchat_from' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'Daemonise' },
);

has 'hipchat_room' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'log' },
);

after 'configure' => sub {
    my ($self) = @_;

    $self->log("configuring HipChat plugin") if $self->debug;

    $self->hipchat_token($self->config->{hipchat}->{token});
    $self->hipchat_from($self->name);
    $self->hipchat_room($self->config->{hipchat}->{room})
        if $self->config->{hipchat}->{room};
};

sub notify {
    my ($self, $msg, $room) = @_;

    my $ua = LWP::UserAgent->new(agent => $self->name);
    $ua->post(
        $self->hipchat_url . $self->hipchat_token, {
            room_id => $room || $self->hipchat_room,
            from    => $self->hipchat_from,
            message => $msg,
            message_format => 'text',
            notify         => 0,
            color          => 'green',
        });
}

1;
