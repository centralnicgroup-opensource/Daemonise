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
    is      => 'rw',
    default => sub { 'settings' },
);

has 'couch_view' => (
    is      => 'rw',
    default => sub { 'config/backend' },
);

has 'couch_user' => (
    is => 'rw',
);

has 'couch_pass' => (
    is => 'rw',
);

after 'load_plugin' => sub {
    my ($self, $key) = @_;

    my $db = Store::CouchDB->new({
        host => $self->couch_host,
        port => $self->couch_port,
        db   => $self->couch_db,
    });
    if ($self->couch_user && $self->couch_pass) {
        $db->user($self->couch_user);
        $db->pass($self->couch_pass);
    }
    $self->{couchdb} = $db;
    return;
};

sub couchdb {
    my ($self, $key) = @_;
    return $self->{couchdb};
}

around 'lookup' => sub {
    my ($self, $key) = @_;

    if ($key) {
        my @path = split(/\//, $key);
        my $platform = $path[0];
        my $view = {
            view   => $self->couch_view,
            opts   => { key => '"' . $platform . '"' },
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
