package RcsFastStoreContribSuite;

use strict;
use warnings;

use Unit::TestSuite;
our @ISA = qw( Unit::TestSuite );

sub name { 'RcsFastStoreContribSuite' }

sub include_tests {
  'RcsFastTests',
}

1;
