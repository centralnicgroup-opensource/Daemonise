#!/usr/bin/perl -Ilib

package Daemonise::Plugin::CouchDB;

use Moose::Role;
use Store::CouchDB;
use Data::Dumper;

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

around 'lookup' => sub {
    my $orig = shift;
    my ( $self, @args ) = @_;
    my $key = $self->$orig(@args);
    if ($key) {
        my @path = split(/\//, $key);
        my $db = Store::CouchDB->new(
            {
                host => $self->couch_host,
                port => $self->couch_port,
                db   => $self->couch_db
            }
        );
        my $platform = $path[0];
        my $view = {
            view   => $self->couch_view,
            opts   => { key => '"' . $platform . '"' },
        };
        my $config = $db->get_view($view);
        print STDERR Dumper($config);
        while(my $part = shift(@path)){
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
