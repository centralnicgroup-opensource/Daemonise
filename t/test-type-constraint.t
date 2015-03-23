#!/usr/bin/env perl

use strict;
use warnings;

package WithoutConversion;
use Mouse;
use Modern::Perl;

has test_type => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub {
        return '';
    }
);

package WithConversion;
use Mouse;
use Modern::Perl;
use Mouse::Util::TypeConstraints;

subtype 'MathUInt64' => as 'Object' => where {
    say "Checking MathUInt";
    $_->isa('Math::UInt64');
};

subtype 'MathInt64' => as 'Object' => where {
    say "Checking MathInt";
    $_->isa('Math::Int64');
};

coerce 'Str' => from 'MathUInt64' => via {
    return "$_";
} => from 'MathInt64' => via {
    return "$_";
};

no Mouse::Util::TypeConstraints;

has test_type => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    coerce  => 1,
    default => sub {
        return '';
    },
);

use Test::More tests => 3;
use Test::Exception;
use Math::UInt64 qw/uint64/;
my $longint = uint64("12345678901234567890");

isa_ok( $longint, 'Math::UInt64' );

throws_ok {
    my $obj = WithoutConversion->new();
    $obj->test_type($longint);

}
qr/Mouse::Util::throw_error/, 'Threw error because of wrong type.';

lives_ok {
    my $obj = WithConversion->new();
    $obj->test_type($longint);
}
'Initialised class.';

__END__
