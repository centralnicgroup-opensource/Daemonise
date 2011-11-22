#!/usr/bin/perl -Ilib

package Daemonise::Plugin::CouchDB;

use Mouse::Role;
use Store::CouchDB;

has 'couch_host' => (
    is      => 'rw',
    default => sub { 'localhost' },
);

has 'couch_port' => (
    is      => 'rw',
    default => sub { 5984 },
);

has 'couch_db' => (
    is        => 'rw',
    predicate => 'has_db',
);

has 'couch_view' => (
    is      => 'rw',
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

after 'load_plugin' => sub {
    my ($self, $key) = @_;

    my $sc = Store::CouchDB->new({
            host => $self->couch_host,
            port => $self->couch_port,
    });
    $sc->db($self->couch_db) if ($self->has_db);
    if ($self->couch_user && $self->couch_pass) {
        $sc->user($self->couch_user);
        $sc->pass($self->couch_pass);
    }
    $self->{couchdb} = $sc;
    return;
};

override 'couchdb' => sub {
    my ($self) = @_;
    return $self->{couchdb};
};

around 'lookup' => sub {
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
};

1;
