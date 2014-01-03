
use strict;
use warnings;

use 5.010;

use ExtUtils::MakeMaker 6.30;



my %WriteMakefileArgs = (
  "ABSTRACT" => "Daemonise - a general daemoniser for anything...",
  "AUTHOR" => "Lenz Gschwendtner <norbu09\@cpan.org>",
  "BUILD_REQUIRES" => {},
  "CONFIGURE_REQUIRES" => {
    "ExtUtils::MakeMaker" => "6.30"
  },
  "DISTNAME" => "Daemonise",
  "EXE_FILES" => [],
  "LICENSE" => "perl",
  "NAME" => "Daemonise",
  "PREREQ_PM" => {
    "Cache::KyotoTycoon" => 0,
    "Carp" => 0,
    "Config::Any" => 0,
    "Data::Dump" => 0,
    "Data::Dumper" => 0,
    "DateTime" => 0,
    "Digest::MD5" => 0,
    "FindBin" => 0,
    "JSON" => 0,
    "LWP::UserAgent" => 0,
    "MIME::Base64" => 0,
    "Mouse" => 0,
    "Mouse::Role" => 0,
    "Net::Graphite" => 0,
    "Net::RabbitMQ" => 0,
    "POSIX" => 0,
    "Redis" => 0,
    "Riemann::Client" => 0,
    "Scalar::Util" => 0,
    "Storable" => 0,
    "Store::CouchDB" => 0,
    "Tie::Syslog" => 0,
    "Try::Tiny" => 0,
    "Unix::Syslog" => 0,
    "WebService::PagerDuty" => 0,
    "experimental" => 0,
    "feature" => 0,
    "lib" => 0
  },
  "TEST_REQUIRES" => {},
  "VERSION" => "1.54",
  "test" => {
    "TESTS" => "t/*.t"
  }
);


unless ( eval { ExtUtils::MakeMaker->VERSION(6.63_03) } ) {
  my $tr = delete $WriteMakefileArgs{TEST_REQUIRES};
  my $br = $WriteMakefileArgs{BUILD_REQUIRES};
  for my $mod ( keys %$tr ) {
    if ( exists $br->{$mod} ) {
      $br->{$mod} = $tr->{$mod} if $tr->{$mod} > $br->{$mod};
    }
    else {
      $br->{$mod} = $tr->{$mod};
    }
  }
}

unless ( eval { ExtUtils::MakeMaker->VERSION(6.56) } ) {
  my $br = delete $WriteMakefileArgs{BUILD_REQUIRES};
  my $pp = $WriteMakefileArgs{PREREQ_PM};
  for my $mod ( keys %$br ) {
    if ( exists $pp->{$mod} ) {
      $pp->{$mod} = $br->{$mod} if $br->{$mod} > $pp->{$mod};
    }
    else {
      $pp->{$mod} = $br->{$mod};
    }
  }
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);



