#!/usr/bin/perl -Ilib

package Daemonise::Plugin::CouchDB;

use Mouse::Role;
use Store::CouchDB;
use Data::Dumper;

has 'couch_host' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'localhost' },
);

has 'couch_port' => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => 5984,
);

has 'couch_db' => (
    is  => 'rw',
    isa => 'Str',
);

has 'couch_view' => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub { 'config/backend' },
);

has 'couch_user' => (
    is  => 'rw',
    isa => 'Str',
);

has 'couch_pass' => (
    is  => 'rw',
    isa => 'Str',
);

has 'couchdb' => (
    is  => 'rw',
    isa => 'Store::CouchDB',
);

after 'configure' => sub {
    my ($self) = @_;

    print STDERR "configuring CouchDB plugin\n" if $self->debug;

    $self->couch_host($self->config->{couchdb}->{host})
        if $self->config->{couchdb}->{host};
    $self->couch_port($self->config->{couchdb}->{port})
        if $self->config->{couchdb}->{port};
    $self->couch_user($self->config->{couchdb}->{user})
        if $self->config->{couchdb}->{user};
    $self->couch_pass($self->config->{couchdb}->{pass})
        if $self->config->{couchdb}->{pass};
    $self->couch_db($self->config->{couchdb}->{db})
        if $self->config->{couchdb}->{db};
    $self->couch_view($self->config->{couchdb}->{view})
        if $self->config->{couchdb}->{view};

    my $sc = Store::CouchDB->new(
        host  => $self->couch_host,
        port  => $self->couch_port,
        debug => $self->debug ? 1 : 0,
    );
    $sc->db($self->couch_db) if ($self->couch_db);
    if ($self->couch_user && $self->couch_pass) {
        $sc->user($self->couch_user);
        $sc->pass($self->couch_pass);
    }
    $self->couchdb($sc);

    return;
};

sub lookup {
    my ($self, $key) = @_;

    if ($key) {
        my @path     = split(/\//, $key);
        my $platform = $path[0];
        my $view     = {
            view => $self->couch_view,
            opts => { key => '"' . $platform . '"' },
        };
        my $config = $self->couchdb->get_view($view);
        while (my $part = shift(@path)) {
            return undef unless $config->{$part};
            $config = $config->{$part};
        }
        return $config || undef;
    }
    else {
        confess "You have to provide a key to look up!";
    }
    return;
}

1;
