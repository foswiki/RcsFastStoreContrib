# Extension for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# RcsFastStoreContrib is Copyright (C) 2024 Michael Daum http://michaeldaumconsulting.com
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

package Foswiki::Contrib::RcsFastStoreContrib::CheckService;

=begin TML

---+ package Foswiki::Contrib::RcsFastStoreContrib::CheckService

=cut

use strict;
use warnings;

use Foswiki::Func ();
use Foswiki::Store::RcsFast ();

our @BYTE_SUFFIX = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB');

=begin TML

---++ ClassMethod new() -> $core

constructor for a service object

=cut

sub new {
  my ($class, $session) = @_;

  my $this = bless({
    _session => $session,
  }, $class);

  return $this;
}

=begin TML

---++ ObjectMethod session() -> $session

=cut

sub session { 
  return shift->{_session}; 
}

=begin TML

---++ ObjectMethod checkStore() 

=cut

sub checkStore {
  my $this = shift;

  my $request = Foswiki::Func::getRequestObject();
  my $web = $request->param("web");

  $this->{_verbose} = Foswiki::Func::isTrue($request->param("verbose"), 0);
  $this->{_quiet} = Foswiki::Func::isTrue($request->param("quiet"), 0);
  $this->{_unlink} = Foswiki::Func::isTrue($request->param("unlink"), 0);

  $this->{_level} = 1;
  $this->{_level} = 0 if $this->{_quiet};
  $this->{_level} = 2 if $this->{_verbose};

  my $totalSizeTopics = 0;
  my $totalSizeAttachments = 0;
  my $stats = {};
  $stats->{files} = [];

  my @webs = ();

  if ($web) {
    push @webs, $web;
  } else {
    @webs = Foswiki::Func::getListOfWebs()
  }

  foreach my $web (@webs) {
    $this->checkWeb($web, $stats);
    $totalSizeTopics += $stats->{$web."::topics"};
    $totalSizeAttachments += $stats->{$web."::attachments"};
  }

  $this->writeInfo(1, "### total - " .
    $this->formatBytes($totalSizeTopics)." bytes can be spared in topics, " .
    $this->formatBytes($totalSizeAttachments)." in attachments, " .
    $this->formatBytes($totalSizeAttachments+$totalSizeTopics)." bytes total"
  ) if $totalSizeTopics || $totalSizeTopics || $totalSizeTopics;

  my $numFiles = scalar(@{$stats->{files}});
  $this->writeInfo(1, "### $numFiles file(s) ".($this->{_unlink}?"":"can be ")."deleted")
    if $numFiles;

  #print STDERR join("\n", sort @{$stats->{files}})."\n"
  #  if $numFiles && !$this->{_quiet};
}

=begin TML

---++ ObjectMethod checkWeb($web, $stats) -> $stats

=cut

sub checkWeb {
  my ($this, $web, $stats) = @_;

  $stats //= {};
  $stats->{$web."::topics"} = 0;
  $stats->{$web."::attachments"} = 0;

  return $stats unless Foswiki::Func::webExists($web);

  $this->writeInfo(1, "### processing web $web");


  my $totalSizeTopics = 0;
  my $totalSizeAttachments = 0;

  foreach my $topic (Foswiki::Func::getTopicList($web)) {
    $this->checkTopic($web, $topic, $stats);
    $totalSizeTopics = $stats->{$web."::topics"} += $stats->{"$web.$topic"."::topic"} // 0;
    $totalSizeAttachments = $stats->{$web."::attachments"} += $stats->{"$web.$topic"."::attachments"} // 0;
  }

  $this->writeInfo(1, "### $web - " .
    $this->formatBytes($totalSizeTopics)." bytes can be spared in topics, " .
    $this->formatBytes($totalSizeAttachments)." in attachments, " .
    $this->formatBytes($totalSizeAttachments+$totalSizeTopics)." bytes total"
  ) if $totalSizeTopics || $totalSizeAttachments || $totalSizeTopics;

  return $stats;
}

=begin TML

---++ ObjectMethod checkTopic($web, $topic, $stats) -> $stats

=cut

sub checkTopic {
  my ($this, $web, $topic, $stats) = @_;

  $stats //= {};
  $stats->{"$web.$topic"."::topic"} = 0;
  $stats->{"$web.$topic"."::attachments"} = 0;

  return $stats unless Foswiki::Func::topicExists($web, $topic);

  my $info = $this->getTopicInfo($web, $topic);

  unless ($info) {
    $this->writeInfo(2, "broken TOPICINFO in $web.$topic ... must add TOPICINFO version=1");
    return $stats;
  }

  my $file = _getDataDir($web, $topic);
  my $rcsFile = $file . ',v';


  if (-e $rcsFile) {
    my $rcsVersion = $this->session->{store}->_getLatestRevFromHistory($file);
    if ($info->{version} > $rcsVersion) {
      $this->writeInfo(2, "topicinfo version=$info->{version}, but rcs version=$rcsVersion in $web.$topic ... must be downgraded to $rcsVersion");
    } elsif ($info->{version} < $rcsVersion) {
      $this->writeInfo(2, "topicinfo version=$info->{version}, but rcs version=$rcsVersion in $web.$topic ... must be upgraded to $rcsVersion");
    } 

    if ($rcsVersion == 1) {
      $this->writeInfo(2, "only one revision in history of $web.$topic ... can be omitted");
      $stats->{"$web.$topic"."::topic"} += $this->getFileSize($rcsFile);
      push @{$stats->{files}}, $rcsFile;
      if ($this->{_unlink}) {
        $this->writeInfo(2, "... unlinking $rcsFile");
        unlink $rcsFile;
      } else {
        $this->writeInfo(2, "... would unlink $rcsFile");
      }
    }
  } else {
    if ($info->{version} > 1) {
      $this->writeInfo(2, "no revision file for $web.$topic yet version > 1 ... must be downgraded to 1");
    }
  }

  $this->checkAttachments($web, $topic, $stats);

  return $stats;
}

=begin TML

---++ ObjectMethod checkAttachments($web, $topic, $stats)  -> $stats

=cut

sub checkAttachments {
  my ($this, $web, $topic, $stats) = @_;

  $stats //= {};

  my $info = $this->getAttachmentsInfo($web, $topic);
  return $stats unless $info;

  foreach my $name (keys %$info) {
    my $attInfo = $info->{$name};
    my $file = _getPubDir($web, $topic, $name);
    my $rcsFile = $file . ',v';

    if (-e $rcsFile) {
      my $rcsVersion = $this->session->{store}->_getLatestRevFromHistory($file);
      if ($attInfo->{version} > $rcsVersion) {
        $this->writeInfo(2, "att info version=$attInfo->{version}, but rcs version=$rcsVersion in $web.$topic#$name ... must be downgraded to $rcsVersion");
      } elsif ($attInfo->{version} < $rcsVersion) {
        $this->writeInfo(2, "att info version=$attInfo->{version}, but rcs version=$rcsVersion in $web.$topic#$name ... must be upgraded to $rcsVersion");
      } 

      if ($rcsVersion == 1) {
        $this->writeInfo(2, "only one revision in history of $web.$topic#$name ... can be omitted");
        $stats->{"$web.$topic"."::attachments"} += $this->getFileSize($rcsFile);
        push @{$stats->{files}}, $rcsFile;
        if ($this->{_unlink}) {
          $this->writeInfo(2, "... unlinking $rcsFile");
          unlink $rcsFile;
        } else {
          $this->writeInfo(2, "... would unlink $rcsFile");
        }
      }
    } else {
      if ($attInfo->{version} > 1) {
        $this->writeInfo(2, "no revision file for $web.$topic#$name yet version > 1 ... must be downgraded to 1");
      }
    }
  }

  return $stats;
}

=begin TML

---++ ObjectMethod getFileSize($filePath) -> $size

=cut

sub getFileSize {
  my ($this, $filePath) = @_;

  my @stat = stat($filePath);
  my $res = $stat[7] // 0;

  return $res;
}

=begin TML

---++ ObjectMethod getTopicInfo($web, $topic) -> \%info

=cut

sub getTopicInfo {
  my ($this, $web, $topic) = @_;

  my $file = _getDataDir($web, $topic);

  my $data = Foswiki::Func::readFile($file);
  my $info;
  if ($data =~ /^%META:TOPICINFO\{(.*?)\}%$/m) {
     $info = Foswiki::Store::RcsFast::_extractRevInfo($1); 
  }

  return $info;
}

=begin TML

---++ ObjectMethod getAttachmentsInfo($web, $topic) -> \%infos

=cut

sub getAttachmentsInfo {
  my ($this, $web, $topic) = @_;

  my $file = _getDataDir($web, $topic);

  my $data = Foswiki::Func::readFile($file);
  my $info;

  while ($data =~ /^%META:FILEATTACHMENT\{(.*?)\}%$/gm) {
    my $attInfo = Foswiki::Store::RcsFast::_extractRevInfo($1); 
    $info->{$attInfo->{name}} = $attInfo;
  }

  return $info;
}

=begin TML

---++ ObjectMethod formatBytes($value, $params) -> $string

=cut

sub formatBytes {
  my ($this, $value, $params) = @_;

  my $max = $params->{"max"} || '';

  my $magnitude = 0;
  my $suffix;

  while ($magnitude < scalar(@BYTE_SUFFIX)) {
    $suffix = $BYTE_SUFFIX[$magnitude];
    last if $value < 1024;
    last if $max eq $suffix;
    $value = $value/1024;
    $magnitude++;
  };

  my $prec = $params->{"prec"} // 2;

  my $result = sprintf("%.0".$prec."f", $value);
  $result =~ s/\.00$//;
  $result .= ' '. $suffix;

  return $result;
}

=begin TML

---++ ObjectMethod writeInfo($level, $msg) 

=cut

sub writeInfo {
  my ($this, $level, $msg) = @_;

  print STDERR "$msg\n" if $level <= $this->{_level};
}

### static functions

sub _getDataDir {
  my ($web, $topic) = @_;

  return $Foswiki::cfg{DataDir}.'/'.$web.'/'.$topic.'.txt';
}

sub _getPubDir {
  my ($web, $topic, $name) = @_;

  return $Foswiki::cfg{PubDir}.'/'.$web.'/'.$topic.'/'.($name?$name:"");
}

1;
