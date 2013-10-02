requires "Cache::KyotoTycoon" => "0";
requires "Carp" => "0";
requires "Config::Any" => "0";
requires "Data::Dump" => "0";
requires "Data::Dumper" => "0";
requires "DateTime" => "0";
requires "Digest::MD5" => "0";
requires "FindBin" => "0";
requires "JSON" => "0";
requires "LWP::UserAgent" => "0";
requires "MIME::Base64" => "0";
requires "Mouse" => "0";
requires "Mouse::Role" => "0";
requires "Net::Graphite" => "0";
requires "Net::RabbitMQ" => "0";
requires "POSIX" => "0";
requires "Redis" => "0";
requires "Riemann::Client" => "0";
requires "Scalar::Util" => "0";
requires "Storable" => "0";
requires "Store::CouchDB" => "0";
requires "Tie::Syslog" => "0";
requires "Try::Tiny" => "0";
requires "Unix::Syslog" => "0";
requires "WebService::PagerDuty" => "0";
requires "feature" => "0";
requires "lib" => "0";

on 'test' => sub {
  requires "Test::More" => "0";
};

on 'configure' => sub {
  requires "ExtUtils::MakeMaker" => "6.30";
};

on 'develop' => sub {
  requires "Pod::Coverage::TrustPod" => "0";
  requires "Test::Pod" => "1.41";
  requires "Test::Pod::Coverage" => "1.08";
};
