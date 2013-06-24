package Daemonise::Plugin::HipChat;

use Mouse::Role;

# ABSTRACT: Daemonise HipChat plugin

use LWP::UserAgent;

=head1 SYNOPSIS

Example:

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
    my ($self) = @_;

    $self->log("configuring HipChat plugin") if $self->debug;

    if (    ref($self->config->{api}) eq 'HASH'
        and ref($self->config->{api}->{hipchat}) eq 'HASH')
    {
        foreach my $conf_key ('token', 'room') {
            my $attr = "hipchat_" . $conf_key;
            $self->$attr($self->config->{api}->{hipchat}->{$conf_key})
                if defined $self->config->{api}->{hipchat}->{$conf_key};
        }
    }

    $self->hipchat_from($self->name);
};

=head2 notify

=cut

sub notify {
    my ($self, $msg, $room) = @_;

    $self->log($msg);

    $msg = '[debug] ' . $msg if $self->debug;

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

    return;
}

1;
