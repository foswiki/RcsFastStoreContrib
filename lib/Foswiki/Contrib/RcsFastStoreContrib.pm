# Extension for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# RcsFastStoreContrib is Copyright (C) 2024-2025 Michael Daum http://michaeldaumconsulting.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details, published at
# http://www.gnu.org/copyleft/gpl.html

package Foswiki::Contrib::RcsFastStoreContrib;

=begin TML

---+ package Foswiki::Contrib::RcsFastStoreContrib

=cut

use strict;
use warnings;

our $VERSION = '1.05';
our $RELEASE = '%$RELEASE%';
our $SHORTDESCRIPTION = 'A simpler faster RCS store';
our $LICENSECODE = '%$LICENSECODE%';
our $NO_PREFS_IN_TOPIC = 1;

=begin TML

---++ ObjectMethod checkStore() 

=cut

sub checkStore {
  my $session = shift;

  require Foswiki::Contrib::RcsFastStoreContrib::CheckService;
  my $service = Foswiki::Contrib::RcsFastStoreContrib::CheckService->new($session);
  $service->checkStore()
}

1;
