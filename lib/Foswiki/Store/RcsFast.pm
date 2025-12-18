# Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright (C) 2024-2025 Michael Daum https://michaeldaumconsulting.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version. For
# more details read LICENSE in the root of this distribution.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# As per the GPL, removal of this notice is prohibited.

package Foswiki::Store::RcsFast;

=begin TML

---+ package Foswiki::Store::RcsFast

Single-file implementation of =Foswiki::Store= 

=cut

use strict;
use warnings;

use Foswiki::Store ();
use Foswiki::Users::BaseUserMapping();
use Foswiki::Sandbox ();
use Foswiki::Plugins ();
use Foswiki::Serialise ();
use Foswiki::Iterator::NumberRangeIterator ();
use Foswiki::Time ();
use Foswiki::Attrs ();
use Encode();
use File::Copy::Recursive ();
use File::Spec ();
use File::Path ();
use Assert;
use Error qw(:try);
use IO::String ();
use Fcntl qw( :DEFAULT :flock );

my %REVINFOS = ();

use constant TRACE => 0;
use constant USE_STORABLE => 1;
use constant CREATE_MISSING_STORABLE => 1;

our @ISA = ('Foswiki::Store');

BEGIN {
  eval { use Algorithm::Diff::XS qw( sdiff ) };
  if ($@) {
    eval { use Algorithm::Diff qw( sdiff ) };
    die $@ if $@;
  }

  if ($Foswiki::cfg{UseLocale}) {
    require locale;
    import locale();
  }

  if (USE_STORABLE) {
    require Sereal::Decoder;
    require Sereal::Encoder;
  }
}

=begin TML

---++ ClassMethod new() -> $core

constructor for a store object

=cut

sub new {
  my $class = shift;
  my $this = $class->SUPER::new(@_);

  #_writeDebug("### called new this=$this");

  $this->{_tmpDir} = $Foswiki::cfg{TempfileDir} || $Foswiki::cfg{WorkingDir} . '/tmp';
  $this->{_lockIndex} = 0;
  $this->{_locks} = {};
  $this->{_lockOfFile} = {};

  return $this;
}

=begin TML

---++ ObjectMethod finish() 

=cut

sub finish {
  my $this = shift;

  #_writeDebug("called finish");

  foreach my $lock (values %{$this->{_locks}}) {
    print STDERR "RcsFast: WARNING - lock for $lock->{file} has been left behind ... force closing\n";
    $this->_leaveCritical($lock);
  }

  undef $this->{_encoder};
  undef $this->{_decoder};
  undef $this->{_locks};
  undef $this->{_lockOfFile};
  undef $this->{_tmpDir};

  _writeDebug("... created $this->{_lockIndex} locks during this session");

  $this->SUPER::finish();
}

### storage api ################################################################

=begin TML

---++ ObjectMethod readTopic($meta, $version) -> ($rev, $isLatest)

=cut

sub readTopic {
  my ($this, $meta, $version) = @_;

  #_writeDebug("### called readTopic(path=".$meta->getPath.", version=" . ($version // 'undef') . ")");

  my $lock = $this->_enterCritical(meta => $meta);

  my $error;
  my $isLatest;

  try {
    my $text;
    ($text, $version) = $this->_getTopic($meta, $version);

    my $info = $this->_getRevInfo($meta);
    my $maxRev = $info ? $info->{version} : undef;
    $isLatest = (!$maxRev || !$version || $version >= $maxRev) ? 1:0;

    if (defined $text) {
      if (!$isLatest || !$this->_readMetaDB($meta)) {
        Foswiki::Serialise::deserialise($text, 'Embedded', $meta);
        $meta->setLoadStatus($version, $isLatest);
        if ($isLatest && CREATE_MISSING_STORABLE) {
          #_writeDebug("creating missing storable for ".$meta->getPath);
          $this->_writeMetaDB($meta);
        }
      }
    } else {
      $version = undef;
      $meta->setLoadStatus();
    }
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  #_writeDebug("### done readTopic()");
  return ($version, $isLatest);
}

=begin TML

---++ ObjectMethod saveTopic( $meta, $cUID, $options  ) -> $integer

=cut

sub saveTopic {
  my ($this, $meta, $cUID, $opts) = @_;

  #_writeDebug("### called saveTopic");
  my $lock = $this->_enterCritical(
    meta => $meta, 
    mode => LOCK_EX
  );

  my $nextRev;
  my $error;

  try {

    my $topicExists = $this->topicExists($meta->web, $meta->topic);

    die "Attempting to save a topic that already exists, and forceinsert specified"
      if $opts->{forceinsert} && $topicExists;

    my $info = $meta->get("TOPICINFO");
    my $file = $this->_getPath(meta => $meta);
    my $rcsFile = $file . ',v';

    # first check in rev 1 before we create rev 2
    if ( defined $info
      && !-e $rcsFile
      && defined $info->{version}
      && $topicExists
      && $info->{version} == 2)
    {
      my $firstInfo = $this->_getRevInfo($meta, undef, 1);
      $this->_checkIn($meta, undef, $firstInfo->{comment}, $firstInfo->{author}, $firstInfo->{date});
    }

    $nextRev = $this->_getNextRevision($meta);

    my $comment = $opts->{comment} || '';
    my $date = $opts->{forcedate} || time();

    if (defined $info) {
      $info->{version} = $nextRev;
      $info->{author} = $cUID;
      $info->{comment} = $comment;
      $info->{date} = $date;
    } else {
      $meta->setRevisionInfo(
        version => $nextRev,
        author => $cUID,
        comment => $comment,
        date => $date,
      );
    }
    $meta->setLoadStatus($nextRev, 1);

    my $text = Foswiki::Serialise::serialise($meta, 'Embedded');
    $this->_saveFile($file, $text);
    $this->_writeMetaDB($meta);

    if ($info->{version} > 1) {
      $this->_checkIn($meta, undef, $comment, $cUID, $opts->{forcedate});
    }
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $nextRev;
}

=begin TML

---++ ObjectMethod moveTopic(  $oldTopicObject, $newTopicObject, $cUID )

=cut

sub moveTopic {
  my ($this, $oldMeta, $newMeta) = @_;

  #_writeDebug("called moveTopic");

  my $lock1 = $this->_enterCritical(
    meta => $oldMeta, 
    mode => LOCK_EX
  );
  my $lock2 = $this->_enterCritical(
    meta => $newMeta, 
    mode => LOCK_EX
  );

  my $error;

  try {

    my $oldFile = $this->_getPath(meta => $oldMeta);
    my $newFile = $this->_getPath(meta => $newMeta);
    $this->_move($oldFile, $newFile);

    my $oldRcsFile = $oldFile . ',v';
    my $newRcsFile = $newFile . ',v';
    $this->_move($oldRcsFile, $newRcsFile) if -e $oldRcsFile;

    my $oldPub = $this->_getPath(meta => $oldMeta, attachment => "");
    my $newPub = $this->_getPath(meta => $newMeta, attachment => "");
    $this->_move($oldPub, $newPub) if -d $oldPub;

    $this->_deleteMetaDB($oldMeta);
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock2);
    $this->_leaveCritical($lock1);
  };

  die "RcsFast: $error" if $error;
}

=begin TML

---++ ObjectMethod topicExists( $web, $topic ) -> $boolean

=cut

sub topicExists {
  my ($this, $web, $topic) = @_;

  #_writeDebug("called topicExists");

  return 0 unless defined $web && $web ne '';
  return 0 unless defined $topic && $topic ne '';

  my $file = $this->_getPath(web => $web, topic => $topic);
  return -e $file;
}

=begin TML

---++ ObjectMethod openAttachment( $meta, $attachment, $mode, %opts  ) -> $text

=cut

sub openAttachment {
  my ($this, $meta, $attachment, $mode, %opts) = @_;

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  return unless -e $file;

  #_writeDebug("called openAttachment");
  my $lock = $this->_enterCritical(
    meta => $meta,
    attachment => $attachment
  );

  my $error;
  my $stream;

  try {
    my $version = $opts{version};
    my $maxRev = $this->_getLatestRev($meta, $attachment);

    if ($mode eq '<' && $version && $version < $maxRev) {
      $stream = IO::String->new($this->_readAttachment($meta, $attachment, $version));
    } else {
      _mkPathTo($file) if $mode =~ />/;

      die "stream open $file failed: Read requested on directory." 
        if -d $file;

      open($stream, $mode, $file)
        or die "stream open $file failed: $!";

      binmode $stream;
    }
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $stream;
}

=begin TML

---++ ObjectMethod saveAttachment( $meta, $attachment, $stream, $cUID, \%options ) -> $revNum

=cut

sub saveAttachment {
  my ($this, $meta, $name, $stream, $cUID, $opts) = @_;

  #_writeDebug("called saveAttachment");

  my $lock = $this->_enterCritical(
    meta => $meta, 
    attachment => $name, 
    mode => LOCK_EX
  );

  my $nextRev;
  my $error;

  try {
    my $attachmentExists = $this->attachmentExists($meta, $name) ? 1 : 0;
    my $file = $this->_getPath(meta => $meta, attachment => $name);
    my $rcsFile = $file . ',v';

    my $info = $meta->get('FILEATTACHMENT', $name);

    # first check in rev 1 before we create rev 2
    if ( defined $info
      && !-e $rcsFile
      && defined $info->{version}
      && $attachmentExists
      && $info->{version} == 1)
    {
      my $firstInfo = $this->_getRevInfo($meta, $name, 1);
      $this->_checkIn($meta, $name, $firstInfo->{comment}, $firstInfo->{author}, $firstInfo->{date});
    }

    $this->_saveStream($file, $stream);

    # only check in if there is an first rev already
    $nextRev = 1;

    if ($attachmentExists) {
      $nextRev = $this->_checkIn($meta, $name, $opts->{comment} || '', $cUID, $opts->{forcedate});
      $nextRev //= $this->_getNextRevision($meta, $name);
    }
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $nextRev;
}


=begin TML

---++ ObjectMethod moveAttachment( $oldTopicObject, $oldAttachment, $newTopicObject, $newAttachment, $cUID  )

=cut

sub moveAttachment {
  my ($this, $oldMeta, $oldAttachment, $newMeta, $newAttachment) = @_;

  #_writeDebug("called moveAttachment");
  ASSERT($oldAttachment) if DEBUG;
  ASSERT($newAttachment) if DEBUG;

  my $lock1 = $this->_enterCritical(
    meta => $oldMeta, 
    attachment => $oldAttachment, 
    mode => LOCK_EX
  );
  my $lock2 = $this->_enterCritical(
    meta => $newMeta, 
    attachment => $newAttachment, 
    mode =>LOCK_EX
  );

  my $error;

  try {
    my $oldFile = $this->_getPath(meta => $oldMeta, attachment => $oldAttachment);
    if (-e $oldFile) {
      my $newFile = $this->_getPath(meta => $newMeta, attachment => $newAttachment);
      die "cannot move file $oldFile onto itself"
        if $oldFile eq $newFile;

      $this->_move($oldFile, $newFile);
      $oldFile .= ',v';
      $newFile .= ',v';
      $this->_move($oldFile, $newFile) if -e $oldFile;
    }
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock1);
    $this->_leaveCritical($lock2);
  };

  die "RcsFast: $error" if $error;
}

=begin TML

---++ ObjectMethod copyAttachment( $oldTopicObject, $oldAttachment, $newTopicObject, $newAttachment  )

=cut

sub copyAttachment {
  my ($this, $oldMeta, $oldAttachment, $newMeta, $newAttachment) = @_;

  #_writeDebug("called copyAttachment");

  my $oldFile = $this->_getPath(meta => $oldMeta, attachment => $oldAttachment);
  return unless -e $oldFile;

  my $newFile = $this->_getPath(meta => $newMeta, attachment => $newAttachment);

  die "RcsFile: cannot copy file $oldFile onto itself"
    if $oldFile eq $newFile;

  my $lock1 = $this->_enterCritical(
    meta => $oldMeta, 
    attachment => $oldAttachment, 
    mode => LOCK_EX
  );
  my $lock2 = $this->_enterCritical(
    meta => $newMeta, 
    attachment => $newAttachment, 
    mode => LOCK_EX
  );

  my $error;

  try {
    $this->_copy($oldFile, $newFile);
    $oldFile .= ",v";
    $newFile .= ",v";
    $this->_copy($oldFile, $newFile) if -e $oldFile;
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock2);
    $this->_leaveCritical($lock1);
  };

  die "RcsFast: $error" if $error;
}

=begin TML

---++ ObjectMethod attachmentExists( $meta, $att ) -> $boolean

=cut

sub attachmentExists {
  my ($this, $meta, $attachment) = @_;

  #_writeDebug("called attachmentExists");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  return -e $file;
}

=begin TML

---++ ObjectMethod testAttachment( $meta, $attachment, $test ) -> $value

=cut

sub testAttachment {
  my ($this, $meta, $attachment, $test) = @_;

  #_writeDebug("called testAttachment");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  return eval "-$test '$file'";
}

=begin TML

---++ ObjectMethod moveWeb( $oldWebObject, $newWebObject, $cUID )

=cut

sub moveWeb {
  my ($this, $oldMeta, $newMeta) = @_;

  #_writeDebug("called moveWeb");

  my $lock1 = $this->_enterCritical(
    meta => $oldMeta, 
    mode => LOCK_EX
  );
  my $lock2 = $this->_enterCritical(
    meta => $newMeta, 
    mode => LOCK_EX
  );

  my $error;

  try {

    my $oldBase = $this->_getPath(meta => $oldMeta);
    my $newBase = $this->_getPath(meta => $newMeta);

    die "cannot move web onto itself" if $oldBase eq $newBase;

    $this->_move($oldBase, $newBase);

    $oldBase = $this->_getPath(meta => $oldMeta, attachment => "");
    $newBase = $this->_getPath(meta => $newMeta, attachment => "");

    $this->_move($oldBase, $newBase) if -d $oldBase;

  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock2);
    $this->_leaveCritical($lock1);
  };

  die "RcsFast: $error" if $error;
}

=begin TML

---++ ObjectMethod webExists( $web ) -> $boolean

=cut

sub webExists {
  my ($this, $web) = @_;

  #_writeDebug("called webExists");

  return $this->topicExists($web, $Foswiki::cfg{WebPrefsTopicName});
}

=begin TML

---++ ObjectMethod getRevisionHistory ( $meta [, $attachment]  ) -> $iterator

=cut

sub getRevisionHistory {
  my ($this, $meta, $attachment) = @_;

  #_writeDebug("### called getRevisionHistory");
  
  my $lock = $this->_enterCritical(
    meta => $meta,
    attachment => $attachment
  );
  
  my $error;
  my $it;

  try {
    my $info = $this->_getRevInfo($meta, $attachment);
    $it = Foswiki::Iterator::NumberRangeIterator->new($info ? $info->{version} : 1, 1);
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  #_writeDebug("### done getRevisionHistory");

  return $it;
}

=begin TML

---++ ObjectMethod getNextRevision ($meta  ) -> $revision

=cut

sub getNextRevision {
  my ($this, $meta, $attachment) = @_;

  #_writeDebug("called getNextRevision");

  my $lock = $this->_enterCritical(
    meta => $meta, 
    attachment => $attachment
  );

  my $error;
  my $nextRev;

  try {

    $nextRev = $this->_getNextRevision($meta, $attachment);

  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $nextRev;
}

sub _getNextRevision {
  my ($this, $meta, $attachment) = @_;

  #_writeDebug("called getNextRevision");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  my $rcsFile = $file . ',v';

  my $info = $this->_getRevInfo($meta, $attachment);
  my $rev = $info ? $info->{version} : 0;
  my $maxRev = $this->_getLatestRevFromHistory($file);

  if ($maxRev > 1) {
    if (_mtime($file) > _mtime($rcsFile) || $rev > $maxRev) {
      $rev = $maxRev;
    }
  }

  return $rev + 1;
}

=begin TML

---++ ObjectMethod getRevisionDiff ( $meta, $rev2, $contextLines  ) -> \@diffArray

=cut

sub getRevisionDiff {
  my ($this, $meta, $rev2, $contextLines) = @_;

  #_writeDebug("called getRevisionDiff");

  my $lock = $this->_enterCritical(
    meta => $meta
  );

  my $error;
  my $diff;

  try {
    my $rev1 = $meta->getLoadedRev // "";
    my $text1 = $this->_getTopic($meta, $rev1);
    my $text2 = $this->_getTopic($meta, $rev2);

    my $lNew = _split($text1);
    my $lOld = _split($text2);

    $diff = sdiff($lNew, $lOld);

  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $diff;
}

=begin TML

---++ ObjectMethod getVersionInfo($meta, $rev, $attachment) -> \%info

=cut

sub getVersionInfo {
  my ($this, $meta, $version, $attachment) = @_;

  #_writeDebug("### called getVersionInfo");

  my $lock = $this->_enterCritical(
    meta => $meta, 
    attachment => $attachment
  );

  my $error;
  my $info;

  try {
    $info = $this->_getRevInfo($meta, $attachment, $version);
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  #_writeDebug("### done getVersionInfo");

  return $info;
}

=begin TML

---++ ObjectMethod repRev( $meta, $cUID, %options ) -> $rev

=cut

sub repRev {
  my ($this, $meta, $cUID, %opts) = @_;

  #_writeDebug("### called repRev");

  my $lock = $this->_enterCritical(
    meta => $meta, 
    mode => LOCK_EX
  );

  my $error;
  my $maxRev;

  try {

    my $file = $this->_getPath(meta => $meta);
    my $rcsFile = $file . ',v';

    my $comment = $opts{comment} || 'reprev';
    my $date = $opts{forcedate} || time();

    my $info = $meta->get("TOPICINFO");
    $info->{author} = $cUID;
    $info->{comment} = $comment;
    $info->{date} = $date;

    $maxRev = $this->_getLatestRevFromHistory($file);
    if ($maxRev <= 1) {
      # initial revision, so delete repository file and start again
      unlink $rcsFile;
    } else {
      $this->_deleteRevision($meta, $maxRev);
      $info->{version} = $maxRev;
    }

    #print STDERR "info->{version}=$info->{version}, loadedVersion=".$meta->getLoadedRev()."\n";

    my $text = Foswiki::Serialise::serialise($meta, 'Embedded');
    $this->_saveFile($file, $text);
    $this->_writeMetaDB($meta);

    if ($info->{version} > 1) {
      $this->_rcsLock($file);

      $date = Foswiki::Time::formatTime($date, '$rcs', 'gmtime');
      #_writeDebug("calling ciDateCmd");
      my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
        $Foswiki::cfg{RcsFast}{ciDateCmd},
        DATE => $date,
        USERNAME => $cUID,
        FILENAME => $file,
        COMMENT => $comment
      );

      if ($exit) {
        $stdout = $Foswiki::cfg{RcsFast}{ciDateCmd} . "\n" . $stdout;
        return $stdout;
      }

      chmod($Foswiki::cfg{Store}{filePermission}, $file);
    }
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $maxRev;
}

=begin TML

---++ ObjectMethod delRev( $meta, $cUID ) -> $rev

=cut

sub delRev {
  my ($this, $meta, $cUID) = @_;

  #_writeDebug("called delRev");

  my $lock = $this->_enterCritical(
    meta => $meta, 
    mode => LOCK_EX
  );

  my $error;
  my $info;

  try {
    $info = $meta->get("TOPICINFO");

    die "cannot delete non existing version" unless $info;
    die "cannot delete initial revision of " . $meta->getPath()
      if $info->{version} <= 1;

    my $file = $this->_getPath(meta => $meta);
    my $rcsFile = $file . ',v';
    my $maxRev = $this->_getLatestRevFromHistory($file);

    if ($maxRev <= 1) {
      # initial revision, so delete repository file and start again
      unlink $rcsFile;
      $maxRev = 1;
    } elsif ($info->{version} <= $maxRev) {
      $this->_deleteRevision($meta, $maxRev);
    } 
    $info->{version} = $maxRev;

  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  # reload the topic object
  $meta->unload();
  $meta->loadVersion();
  return $info->{version};
}

=begin TML

---++ ObjectMethod atomicLockInfo( $meta ) -> ($cUID, $time)

=cut

sub atomicLockInfo {
  my ($this, $meta) = @_;

  #_writeDebug("called atomicLockInfo");

  my $file = $this->_getPath(meta => $meta, subdir => ".store", extension => '.lock');
  return (undef, undef) unless -e $file;

  my $text = $this->_readFile($file);
  return split(/\s+/, $text, 2);
}

=begin TML

---++ ObjectMethod atomicLock( $meta, $cUID )

=cut

sub atomicLock {
  my ($this, $meta, $cUID) = @_;

  my $file = $this->_getPath(meta => $meta, subdir => ".store", extension => '.lock');
  $this->_saveFile($file, $cUID . "\n" . time());
}

=begin TML

---++ ObjectMethod atomicUnlock( $meta )

=cut

sub atomicUnlock {
  my ($this, $meta, $cUID) = @_;

  my $file = $this->_getPath(meta => $meta, subdir => ".store", extension => '.lock');
  if (-e $file) {
    unlink $file 
      or die "RcsFast: failed to delete $file: $!";
  }
}

=begin TML

---++ ObjectMethod getApproxRevTime (  $web, $topic  ) -> $epochSecs

=cut

sub getApproxRevTime {
  my ($this, $web, $topic) = @_;

  #_writeDebug("called getApproxRevTime");

  my $file = $this->_getPath(web => $web, topic => $topic);
  my @e = stat($file);

  return $e[9] || 0;
}

=begin TML

---++ ObjectMethod getRevisionAtTime( $meta, $time ) -> $rev

=cut

sub getRevisionAtTime {
  my ($this, $meta, $time) = @_;

  #_writeDebug("called getRevisionAtTime");

  my $lock = $this->_enterCritical(meta => $meta);

  my $error;
  my $rev;

  try {

    my $file = $this->_getPath(meta => $meta);
    my $rcsFile = $file . ',v';

    unless (-e $rcsFile) {
      return ($time >= _mtime($file)) ? 1 : 0;
    }

    my $date = Foswiki::Time::formatTime($time, '$rcs', 'gmtime');

    #_writeDebug("calling rlogDateCmd");
    my $cmd = $Foswiki::cfg{RcsFast}{rlogDateCmd};
    my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
      $cmd,
      DATE => $date,
      FILENAME => $file,
    );

    die "rlogDateCmd of $file failed: $stdout $stderr" if $exit;

    if ($stdout =~ m/revision \d+\.(\d+)/) {
      $rev = $1;
    }

  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $rev;
}

=begin TML

---++ ObjectMethod eachChange( $meta, $time ) -> $iterator

=cut

sub eachChange {
  my ($this, $webOrMeta, $since) = @_;

  #_writeDebug("called eachChange");
  my $web = ref($webOrMeta) ? $webOrMeta->web : $webOrMeta;
  my $lock = $this->_enterCritical(
    web => $web,
    file => ".changes",
  );

  my $error;
  my $it;

  try {
    my @changes = ();

    my $file = $this->_getPath(web => $web, file => '.changes');
    $since //= 0;
    @changes = reverse grep { $_->{time} >= $since } @{$this->_readChanges($file)}
      if -r $file;

    $it = Foswiki::ListIterator->new(\@changes);
  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;

  return $it;
}

=begin TML

---++ ObjectMethod recordChange(%args)

=cut

sub recordChange {
  my ($this, %args) = @_;

  #_writeDebug("called recordChange");

  my $web = $args{path};
  if ($web =~ m/\./) {
    ($web) = Foswiki->normalizeWebTopicName(undef, $web);
  }

  # Can't log changes in a non_existent web
  my $webDir = $this->_getPath(web => $web);
  return unless -e $webDir;

  my $lock = $this->_enterCritical(
    web => $web,
    file => ".changes",
    mode => LOCK_EX,
  );

  my $error;

  try {
    my $file = $webDir . '/.changes';
    my $changes;

    if (-e $file) {
      $changes = $this->_readChanges($file);

      # Trim old entries
      my $cutoff = time - $Foswiki::cfg{Store}{RememberChangesFor};
      while (scalar(@$changes) && $changes->[0]{time} < $cutoff) {
        shift(@$changes);
      }
    } else {
      $changes = [];
    }

    # Add the new change to the end of the file
    $args{time} = time;
    push @$changes, \%args;

    $this->_saveFile($file, $this->_json->encode($changes));

  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;
}

=begin TML

---++ ObjectMethod eachAttachment( $meta ) -> \$iterator

=cut

sub eachAttachment {
  my ($this, $meta) = @_;

  #_writeDebug("called eachAttachment");

  # deliberately _not_ reading from filesystem and use in memory infos instead
  $meta->loadVersion() unless $meta->latestIsLoaded();

  my @list = ();
  foreach my $name (map { $_->{name} } $meta->find('FILEATTACHMENT')) {
    my $file = $this->_getPath(meta => $meta, attachment => $name);
    next unless -e $file;
    push @list, $name;
  }

  return Foswiki::ListIterator->new(\@list);
}

=begin TML

---++ ObjectMethod eachTopic( $webObject ) -> $iterator

=cut

sub eachTopic {
  my ($this, $meta) = @_;

  #_writeDebug("called eachTopic");

  my $web = ref($meta) ? $meta->web : $meta;

  my $dh;
  opendir($dh, $this->_getPath(web => $web))
    or return Foswiki::ListIterator->new([]);

  # the name filter is used to ensure we don't return filenames
  # that contain illegal characters as topic names.
  my @list =
    map {
      my $tmp = Encode::decode_utf8($_);
      $tmp =~ s/\.txt$//;
      $tmp;
    }
    grep { !/$Foswiki::cfg{NameFilter}/ && /\.txt$/ } readdir($dh);

  closedir($dh);

  return Foswiki::ListIterator->new(\@list);
}

=begin TML

---++ ObjectMethod eachWeb($webObject, $all ) -> $iterator

=cut

sub eachWeb {
  my ($this, $web, $all) = @_;

  #_writeDebug("called eachWeb");

  my $list = $this->_getWebs($web, $all);
  return Foswiki::ListIterator->new($list);
}

=begin TML

---++ ObjectMethod remove( $cUID, $om, $attachment )

=cut

sub remove {
  my ($this, $cUID, $meta, $attachment) = @_;

  #_writeDebug("called remove");
  
  my $lock = $this->_enterCritical(
    meta => $meta, 
    attachment => $attachment, 
    mode => LOCK_EX
  );

  my $error;

  try {

    if ($meta->topic) {
      my $file = $this->_getPath(meta => $meta, attachment => $attachment);
      my $rcsFile = $file . ",v";
      my $pubDir = $this->_getPath(meta => $meta, attachment => "");

      unlink($file);
      unlink($rcsFile) if -e $rcsFile;
      $this->_deleteMetaDB($meta);
      _rmtree($pubDir) unless $attachment;

    } else {
      my $dataDir = $this->_getPath(meta => $meta);
      my $pubDir = $this->_getPath(meta => $meta, attachment => "");

      # Web
      _rmtree($dataDir);
      _rmtree($pubDir);
    }

  } catch Error with {
    $error = shift;
  } finally {
    $this->_leaveCritical($lock);
  };

  die "RcsFast: $error" if $error;
}

=begin TML

---++ ObjectMethod query($query, $inputTopicSet, $session, \%options) -> $outputTopicSet

=cut

sub query {
  my ($this, $query, $inputTopicSet, $session, $opts) = @_;

  #_writeDebug("called query");

  my $engine;
  if ($query->isa('Foswiki::Query::Node')) {
    unless ($this->{queryObj}) {
      my $module = $Foswiki::cfg{Store}{QueryAlgorithm};
      eval "require $module";
      die "Bad {Store}{QueryAlgorithm}; suggest you run configure and select a different algorithm\n$@"
        if $@;
      $this->{queryObj} = $module->new();
    }
    $engine = $this->{queryObj};
  } else {
    ASSERT($query->isa('Foswiki::Search::Node')) if DEBUG;
    unless ($this->{searchQueryObj}) {
      my $module = $Foswiki::cfg{Store}{SearchAlgorithm};
      eval "require $module";
      die "Bad {Store}{SearchAlgorithm}; suggest you run configure and select a different algorithm\n$@"
        if $@;
      $this->{searchQueryObj} = $module->new();
    }
    $engine = $this->{searchQueryObj};
  }

  no strict 'refs';
  return $engine->query($query, $inputTopicSet, $session, $opts);
  use strict 'refs';
}

=begin TML

---++ ObjectMethod getLease( $meta ) -> $lease

=cut

sub getLease {
  my ($this, $meta) = @_;

  #_writeDebug("called getLease");

  my $lease;
  my $file = $this->_getPath(meta => $meta, extension => ".lease");

  if (-e $file) {
    my $text = $this->_readFile($file);
    $lease = {split(/\r?\n/, $text)};
  }

  return $lease;
}

=begin TML

---++ ObjectMethod setLease( $meta, $length )

=cut

sub setLease {
  my ($this, $meta, $lease) = @_;

  #_writeDebug("called setLease");

  my $file = $this->_getPath(meta => $meta, extension => '.lease');

  if ($lease) {
    $this->_saveFile($file, join("\n", %$lease));
  } elsif (-e $file) {
    unlink($file)
      or die "RcsFast: failed to delete $file: $!";
  }
}

=begin TML

---++ ObjectMethod removeSpuriousLeases( $web )

=cut

sub removeSpuriousLeases {
  my ($this, $web) = @_;

  #_writeDebug("called removeSpuriousLeases");

  my $webDir = $this->_getPath(web => $web);

  my $W;
  return unless opendir($W, $webDir);

  foreach my $f (readdir($W)) {

    my $file = $webDir . Encode::decode_utf8($f);
    next unless $file =~ m/^(.*)\.lease$/;

    my $txtFile = $1 . '.txt';
    next if -e $txtFile;

    unlink $file;
  }

  closedir($W);
}

### internal api ###############################################################

=begin TML

---++ ObjectMethod _json() -> $json

returns a JSON object

=cut

sub _json {
  my $this = shift;

  #_writeDebug("called _json");

  $this->{_json} //= JSON->new->pretty(0);

  return $this->{_json};
}

=begin TML

---++ ObjectMethod _encoder() -> $encoder

returns a Sereal::Encoder object

=cut

sub _encoder {
  my $this = shift;

  $this->{_encoder} //= Sereal::Encoder->new();

  return $this->{_encoder};
}

=begin TML

---++ ObjectMethod _decoder() -> $decoder

returns a Sereal::Decoder object

=cut

sub _decoder {
  my $this = shift;

  $this->{_decoder} //= Sereal::Decoder->new();

  return $this->{_decoder};
}

=begin TML

---++ ObjectMethod _enterCritical(%args) -> $lock

The args hash consists of:

  * meta (either meta or web, topic)
  * web 
  * topic 
  * file
  * attachment (optional)
  * mode

See _getPath().

This establishes the start of a critical transaction onto the given resource.
The resource is depicted by =$meta= and an optional =$attachement= name.

The =$mode= param can either =LOCK_EX= or =LOCK_SH=, defaulting to =LOCK_SH=. 
The resource will be locked exclusively using =LOCK_EX= or shared with a
=LOCK_SH= mode. An exclusive lock is required to establish safe read/write
access to a resource. The same resource cannot be locked by any other critical
path. A shared lock protect the resource from writing while concurrent
read-only operations are fine.

The function returns a =$lock= object to be used in =_leaveCritical()=. Note
that everytime you enter a critical transaction you must exlicitly leave
it as well. While all remaining locks left behind are being cleared in
the deconstructor, this is considered an error.

=cut

sub _enterCritical {
  my ($this, %args) = @_;

  my $file = $this->_getMutexFile(%args);
  $args{mode} //= LOCK_SH;

  #print STDERR "RcsFast: mutex file $file already exists ...\n"
  #  if -$args{mode} == LOCK_EX and -e $file;

  if (TRACE) {
    my ($package, undef, $line) = caller;
    _writeDebug("### entering critical exclusive mode for $file via $package,$line")
      if $args{mode} == LOCK_EX;
    _writeDebug("### entering critical shared mode for $file via $package,$line")
      if $args{mode} == LOCK_SH;
  }

  die "cannot lock the same resouce twice exclusively: $file"
    if $args{mode} == LOCK_EX && exists $this->{_lockOfFile}{$file};

  my $fh;
  open($fh, ">", $file) 
    or die "RcsFast: failed to open file $file: $!";

  #print STDERR "RcsFast: waiting on exclusive lock ... ".time()."\n"
  #  if $args{mode} == LOCK_EX;

  flock($fh, $args{mode}) 
    or die "RcsFast: failed to lock file $file: $!";

  #print STDERR "RcsFast: passed exclusive lock ...     ".time()."\n"
  #  if $args{mode} == LOCK_EX;

  seek($fh, 0, 0);

  my $id = $this->{_lockIndex}++;
  my $lock = $this->{_locks}{$id} = {
    id => $id,
    file => $file,
    fh => $fh,
    mode => $args{mode},
  };

  $this->{_lockOfFile}{$file} = $lock if $args{mode} == LOCK_EX;

  return $lock;
}

=begin TML

---++ ObjectMethod _leaveCritical($lock) 

This method destroys the lock with the given id.

=cut

sub _leaveCritical {
  my ($this, $lock) = @_;

  if (TRACE) {
    _writeDebug("### leaving critical exclusive mode for $lock->{file}")
      if $lock->{mode} == LOCK_EX;
    _writeDebug("### leaving critical shared mode for $lock->{file}")
      if $lock->{mode} == LOCK_SH;
  }

  die "undefined lock" unless defined $lock;

  # don't die 
  flock($lock->{fh}, LOCK_UN) 
    or warn "RcsFast: WARNING - failed to unlock file $lock->{file}: $!";

  # don't die
  close($lock->{fh})
    or warn "RcsFast: WARNING - failed to close file $lock->{file}: $!";

  # don't die, it is ok if it is already gone 
  unlink $lock->{file};

  delete $this->{_lockOfFile}{$lock->{file}};
  delete $this->{_locks}{$lock->{id}};
}

=begin TML

---++ ObjectMethod _getTopic($meta, $version) -> ($tex, $version)

=cut

sub _getTopic {
  my ($this, $meta, $version) = @_;

  #_writeDebug("called _getTopic(" . ($version // 'undef') . ")");

  my $file = $this->_getPath(meta => $meta);
  my $rcsFile = $file . ',v';
  my $info = $this->_getRevInfo($meta, undef, $version);
  return unless defined $info;

  my $text;

  unless ($version && -e $rcsFile) {
    $text = $this->_readTopic($meta);
    return wantarray ? ($text, $info->{version}) : $text;
  }

  my $coCmd = $Foswiki::cfg{RcsFast}{coCmd};
  my $status;
  my $stderr;

  # read from rcs
  #_writeDebug("calling coCmd");
  ($text, $status, $stderr) = Foswiki::Sandbox->sysCommand(
    $coCmd,
    REVISION => '1.' . $version,
    FILENAME => $file
  );
  $text = Encode::decode_utf8($text);

  # test revision against top rev
  if (defined $stderr && $stderr =~ /revision 1\.(\d+)/s) {
    $version = $1;
  }

  return wantarray ? ($text, $version) : $text;
}

=begin TML

---++ ObjectMethod _getRevInfo($meta, $attachment, $version) -> \%info

=cut

sub _getRevInfo {
  my ($this, $meta, $attachment, $version) = @_;

  #_writeDebug("called _getRevInfo");

  my $text = $this->_readTopic($meta);
  my $info;

  if ($attachment) {
    # cache attachments info

    $info = $this->_getRevInfoFromHistory($meta, $attachment, $version)
      if $version;

    if (!$info && $text) {
      if ($text =~ /^%META:FILEATTACHMENT\{(name="\Q$attachment\E".*)?\}%$/gm) {
        $info = _extractRevInfo($1); 
      }
    }

  } else {
    # cache topic info

    $info = $this->_getRevInfoFromHistory($meta, undef, $version)
      if $version;

    if (!$info && $text) {
      # cache topic info

      if ($text =~ /^%META:TOPICINFO\{(.*?)\}%$/m) {
        $info = _extractRevInfo($1); 
      } else {
        $info //= {
          author => $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID,
          date => time(),
          version => 1
        };
      }
    }
  }

  return $info;
}

=begin TML

---++ ObjectMethod _getRevInfoFromHistory($meta, $attachment, $version)  -> \%info

=cut

sub _getRevInfoFromHistory {
  my ($this, $meta, $attachment, $version) = @_;

  #_writeDebug("calling infoCmd");
  my $rcsFile = $this->_getPath(meta => $meta, attachment => $attachment) . ",v";
  return unless -e $rcsFile;

  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
    $Foswiki::cfg{RcsFast}{infoCmd},
    REVISION => '1.' . $version,
    FILENAME => $rcsFile,
  );

  die "infoCmd of $rcsFile failed: $stdout $stderr" if $exit;

  my $info;

  if ($stdout =~ /^.*?date: ([^;]+);  author: ([^;]*);[^\n]*\n([^\n]*)\n/s) {
    $info = {
      version => $version,
      date => Foswiki::Time::parseTime($1),
      author => $2,
      comment => $3,
    };
    if ($stdout =~ /revision 1.([0-9]*)/) {
      $info->{version} = $1;
    }
  }

  return $info;
}


=begin TML

---++ ObjectMethod _readAttachment($meta, $attachment, $version) -> $data

=cut

sub _readAttachment {
  my ($this, $meta, $attachment, $version) = @_;

  #_writeDebug("called _readAttachment");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  my $data;

  if ($version) {
    my $coCmd = $Foswiki::cfg{RcsFast}{coCmd};
    my $status;
    my $stderr;

    # read from rcs
    #_writeDebug("calling coCmd (attachment)");
    ($data, $status, $stderr) = Foswiki::Sandbox->sysCommand(
      $coCmd,
      REVISION => '1.' . $version,
      FILENAME => $file
    );
  } else {
    $data = $this->_readFile($file, 1);
  }

  return $data;
}

=begin TML

---++ ObjectMethod _readMetaDB($meta) -> $boolean

loads the meta object by decoding the Sereal object stored in 
meta.db fie. returns true on success. 

=cut

sub _readMetaDB {
  my ($this, $meta) = @_;

  return 0 unless USE_STORABLE;
  my $file = $this->_getPath(meta => $meta, subdir => ".store", file => "meta.db");
  return 0 unless -e $file;

  my $topicFile = $this->_getPath(meta => $meta);
  return 0 if _mtime($file) < _mtime($topicFile);

  my $fh;
  open($fh, "<", $file) 
    or die "failed to open file $file: $!";

  flock($fh, LOCK_SH) 
    or die "failed to lock file $file: $!";

  my $data = do { local $/; <$fh> };

  flock($fh, LOCK_UN) 
    or die "failed to unlock file $file: $!";

  close($fh)
    or die "failed to close file $file: $!";

  my $tmpMeta = $this->_decoder->decode($data);

  _copyMeta($tmpMeta, $meta);
  #_writeDebug("reading meta from storable, version=".$meta->getLoadedRev());

  return 1;
}

=begin TML

---++ ObjectMethod _writeMetaDB($meta) 

writes the meta object by encoding it into a Sereal file

=cut

sub _writeMetaDB {
  my ($this, $meta) = @_;

  return unless USE_STORABLE;
  my $file = $this->_getPath(meta => $meta, subdir => ".store", file => "meta.db");
  #_writeDebug("writing meta to storable, version=".$meta->getLoadedRev());
  _mkPathTo($file);

  my $tmpMeta = Foswiki::Meta->new( $Foswiki::Plugins::SESSION, $meta->web, $meta->topic, $meta->text);
  _copyMeta($meta, $tmpMeta);
  delete $tmpMeta->{_session};

  my $fh;
  open($fh, ">", $file) 
    or die "failed to open file $file: $!";

  flock($fh, LOCK_EX) 
    or die "failed to lock file $file: $!";

  print $fh $this->_encoder->encode($tmpMeta)
      or die "failed to encode to '$file': $!";

  flock($fh, LOCK_UN) 
    or die "failed to unlock file $file: $!";

  close($fh)
    or die "failed to close file $file: $!";
}

=begin TML

---++ ObjectMethod _deleteMetaDB($meta) 

=cut

sub _deleteMetaDB {
  my ($this, $meta) = @_;

  return unless USE_STORABLE;

  my $file = $this->_getPath(meta => $meta, subdir => ".store", file => "meta.db");
  return unless -e $file;

  unlink($file);
}

=begin TML

---++ ObjectMethod _getPath(%args)  -> $filepath

returns the path to an object on the store. args may contain keys:

   * meta: object to get the store file for
   * web, topic: either meta or web, topic params are required
   * subdir: intermedium directory
   * attachment: name of an attachment
   * extension: defaults to .txt
   * file: explicit filename

note that the return value has been encoded to utf8

=cut

sub _getPath {
  my ($this, %args) = @_;

  #_writeDebug("called _getPath");

  my $web;
  my $topic;

  if ($args{meta}) {
    $web = $args{meta}->web;
    $topic = $args{meta}->topic;
  } else {
    $web = $args{web};
    $topic = $args{topic};
  }

  $web =~ s#\.#/#g if $web;

  my @path = ();

  if (defined $args{attachment}) {
    push @path, $Foswiki::cfg{PubDir};
    push @path, $web if $web;
    push @path, $topic if $topic;
    push @path, $args{subdir} if $args{subdir};
    push @path, $args{attachment} if $args{attachment};
  } else {
    push @path, $Foswiki::cfg{DataDir};
    push @path, $web if $web;

    if ($args{subdir}) {
      push @path, $args{subdir};

      if ($topic) {
        push @path, $topic . ($args{extension} // '');
      } elsif ($args{file}) {
        push @path, $args{file} . ($args{extension} // '');
        undef $args{file};
      } else {
        push @path, "web" . ($args{extension} // '');
      }

    } else {
      push @path, $topic . ($args{extension} // '.txt') if $topic;
    }
  }

  push @path, $args{file} if $args{file};

  return Encode::encode_utf8(join("/", @path));
}

=begin TML

---++ ObjectMethod _getMutexFile(%args) -> $filePath

returns the file path used to create a mutex while entering a critical area

args:

   * meta
   * web
   * topic
   * attachment
   * file

=cut

sub _getMutexFile {
  my ($this, %args) = @_;

  my $web;
  my $topic;

  if ($args{meta}) {
    $web = $args{meta}->web;
    $topic = $args{meta}->topic;
  } else {
    $web = $args{web};
    $topic = $args{topic};
  }

  $web =~ s#\/#_#g if $web;

  my @path = ();

  push @path, $web if $web;
  push @path, $topic if $topic;
  push @path, $args{attachment} if $args{attachment};
  push @path, $args{file} if $args{file};
 
  $path[-1] .= ".mtx";

  return $this->{_tmpDir} . "/" . join("_", @path);
}

=begin TML

---++ ObjectMethod _readTopic($meta)  -> $text

=cut

sub _readTopic {
  my ($this, $meta) = @_;

  my $file = $this->_getPath(meta => $meta);
  return unless -e $file;

  my $text = $this->_readFile($file);
  $text =~ s/\r//g;

  return $text;
}

=begin TML

---++ ObjectMethod _readFile($file, $isBinary) -> $data

reads a file from disck

=cut

sub _readFile {
  my ($this, $file, $isBinary) = @_;

  #_writeDebug("called _readFile($file)");

  my $fh;
  open($fh, '<', $file)
    or die "failed to read $file: $!";

  binmode($fh);

  flock($fh, LOCK_SH) 
    or die "failed to lock file $file: $!";

  my $data = do { local $/; <$fh> };

  flock($fh, LOCK_UN) 
    or die "failed to unlock file $file: $!";

  close($fh);

  $data //= '';
  $data = Encode::decode_utf8($data) unless $isBinary;

  return $data;
}

=begin TML

---++ ObjectMethod _move($from, $to) 

=cut

sub _move {
  my ($this, $from, $to) = @_;

  #_writeDebug("called _move");

  _mkPathTo($to);

  File::Copy::Recursive::rmove($from, $to)
    or die "move $from to $to failed: $!";
}

=begin TML

---++ ObjectMethod _copy($from, to) 

=cut

sub _copy {
  my ($this, $from, $to) = @_;

  #_writeDebug("called _copy");

  _mkPathTo($to);
  
  File::Copy::Recursive::rcopy($from, $to)
    or die "copy $from to $to failed: $!";
}

=begin TML

---++ ObjectMethod _checkIn($meta, $attachment, $comment, $user, $data) 

=cut

sub _checkIn {
  my ($this, $meta, $attachment, $comment, $user, $date) = @_;

  #_writeDebug("called _checkIn");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  $this->_rcsLock($file);

  $comment ||= 'none';

  my ($cmd, $stdout, $exit, $stderr);
  if (defined($date)) {
    $date = Foswiki::Time::formatTime($date, '$rcs', 'gmtime');
    $cmd = $Foswiki::cfg{RcsFast}{ciDateCmd};
    #_writeDebug("calling ciDateCmd");
    ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
      $cmd,
      USERNAME => $user,
      FILENAME => $file,
      COMMENT => $comment,
      DATE => $date
    );
  } else {
    #_writeDebug("calling ciCmd");
    $cmd = $Foswiki::cfg{RcsFast}{ciCmd};
    ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
      $cmd,
      USERNAME => $user,
      FILENAME => $file,
      COMMENT => $comment
    );
  }


  $stdout ||= '';

  die "ciCmd/ciDateCmd of $file failed: $stdout $stderr" if $exit;

  chmod($Foswiki::cfg{Store}{filePermission}, $file);

  if ($stderr =~ /reverting to previous revision 1\.(\d+)/) {
    return $1;
  }

  return;
}

=begin TML

---++ ObjectMethod _rcsLock($file) 

=cut

sub _rcsLock {
  my ($this, $file) = @_;

  #_writeDebug("called _rcsLock");

  my $rcsFile = $file . ',v';
  return unless -e $rcsFile;

  # Try and get a lock on the file
  #_writeDebug("1 - calling lockCmd");
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RcsFast}{lockCmd}, FILENAME => $file);

  if ($exit) {

    # if the lock has been set more than 24h ago, let's try to break it
    # and then retry.  Should not happen unless in Cairo upgrade
    # scenarios - see Item2102

    if ((time - _mtime($rcsFile)) > 3600) {
      warn 'Automatic recovery: breaking lock for ' . $file;
      #_writeDebug("2 - calling lockCmd");
      Foswiki::Sandbox->sysCommand($Foswiki::cfg{RcsFast}{breaklockCmd}, FILENAME => $file);
      ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RcsFast}{lockCmd}, FILENAME => $file);
    }
    if ($exit) {

      # still no luck - bailing out
      $stdout ||= '';
      die "lockCmd failed: $stdout $stderr";
    }
  }

  chmod($Foswiki::cfg{Store}{filePermission}, $file);
}

=begin TML

---++ ObjectMethod _rcsUnlock($file) 

=cut

sub _rcsUnlock {
  my ($this, $file) = @_;

  #_writeDebug("calling _rcsUnlock");
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RcsFast}{unlockCmd}, FILENAME => $file);

  die "unlockCmd failed: $stdout $stderr" if $exit;

  chmod($Foswiki::cfg{Store}{filePermission}, $file);
}

=begin TML

---++ ObjectMethod _saveFile($file, $text) 

=cut

sub _saveFile {
  my ($this, $file, $text) = @_;

  #_writeDebug("called _saveFile($file)");
  #_writeDebug("... text=$text");

  _mkPathTo($file);

  my $fh;
  open($fh, ">", $file) 
    or die "failed to open file $file: $!";

  binmode($fh)
    or die "failed to binmode $file: $!";

  flock($fh, LOCK_EX) 
    or die "failed to lock file $file: $!";

  print $fh Encode::encode_utf8($text)
    or die "failed to print to $file: $!";

  flock($fh, LOCK_UN) 
    or die "failed to unlock file $file: $!";

  close($fh)
    or die "failed to close file $file: $!";

  chmod($Foswiki::cfg{Store}{filePermission}, $file);

  return;
}

=begin TML

---++ ObjectMethod _saveStream($file, $fh) 

=cut

sub _saveStream {
  my ($this, $file, $fh) = @_;

  #_writeDebug("called _saveStream");

  _mkPathTo($file);

  my $F;

  open($F, '>', $file) or die "open $file failed: $!";

  binmode($F) || die "failed to binmode $file: $!";

  flock($fh, LOCK_EX) or die "failed to lock file $file: $!";

  my $data;

  while (read($fh, $data, 1024)) {
    print $F $data;
  }

  flock($fh, LOCK_UN) 
    or die "failed to unlock file $file: $!";

  close($F) or die "close $file failed: $!";

  chmod($Foswiki::cfg{Store}{filePermission}, $file);
}

=begin TML

---++ ObjectMethod _readChanges($files) ->> \@changes

=cut

sub _readChanges {
  my ($this, $file) = @_;

  #_writeDebug("called _readChanges");

  my $session = $Foswiki::Plugins::SESSION;
  my $text = $this->_readFile($file);

  # Look at the first line to deduce format
  my $changes;
  eval { $changes = $this->_json->decode($text); };
  print STDERR "Corrupt $file: $@\n" if $@;

  foreach my $entry (@$changes) {
    if ($entry->{path} && $entry->{path} =~ m/^(.*)\.(.*)$/) {
      $entry->{topic} = $2;
    } elsif ($entry->{oldpath} && $entry->{oldpath} =~ m/^(.*)\.(.*)$/) {
      $entry->{topic} = $2;
    }
    $entry->{user} = $session ? $session->{users}->getWikiName($entry->{cuid}) : $entry->{cuid};
    $entry->{more} = ($entry->{minor} ? 'minor ' : '') . ($entry->{comment} || '');
  }

  return \@$changes;
}

=begin TML

---++ ObjectMethod _getWebs($meta, $all, $result) -> $result

gather all webs; if all is true then recursively

=cut

sub _getWebs {
  my ($this, $meta, $all, $result) = @_;

  #_writeDebug("called _getWebs");
  $result //= [];

  my $web = ref($meta) ? $meta->web : $meta;
  $web //= "";

  my $wptn = $Foswiki::cfg{WebPrefsTopicName} . ".txt";
  my $dir = $this->_getPath(web => $web) . "/*/$wptn";

  my @list = map {
    my $tmp = Encode::decode_utf8($_);
    $tmp =~ s/^.*\/(.*)\/$wptn$/$1/;
    $tmp
  } glob($dir);

  my $root = $web ? "$web/" : '';

  while (my $wp = shift(@list)) {
    push @$result, $root . $wp;
    push @$result, @{$this->_getWebs($root . $wp, $all)} if $all;
  }

  return $result;
}

=begin TML

---++ ObjectMethod _deleteRevision($meta, $rev) 

=cut

sub _deleteRevision {
  my ($this, $meta, $rev) = @_;

  #_writeDebug("called _deleteRevision");

  my $file = $this->_getPath(meta => $meta);
  my $rcsFile = $file . ",v";
  return unless -e $rcsFile;

  # delete latest revision (unlink (may not be needed), delete revision)
  $this->_rcsUnlock($file);

  #_writeDebug("calling delRevCmd");
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
    $Foswiki::cfg{RcsFast}{delRevCmd},
    REVISION => '1.' . $rev,
    FILENAME => $file
  );

  if ($exit) {
    print STDERR "RcsFast: delRevCmd of $file failed, rev=$rev: $stdout $stderr\n";
    #die "delRevCmd of $file failed, rev=$rev: $stdout $stderr");
    return;
  }

  $rev--;

  #_writeDebug("calling coCmd");
  ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
    $Foswiki::cfg{RcsFast}{coCmd},
    REVISION => '1.' . $rev,
    FILENAME => $file
  );

  die "coCmd of $file failed: $stdout $stderr" if $exit;

  $this->_saveFile($file, $stdout);
  $this->_deleteMetaDB($meta);
}

=begin TML

---++ ObjectMethod _getLatestRevFromHistory($file) -> $rev

=cut

sub _getLatestRevFromHistory {
  my ($this, $file) = @_;

  my $rcsFile = $file . ',v';
  return 1 unless -e $rcsFile;

  #_writeDebug("calling histCmd");
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RcsFast}{histCmd}, FILENAME => $rcsFile);

  die "histCmd of $rcsFile failed: $stdout $stderr" if $exit;

  if ($stdout =~ /head:\s+\d+\.(\d+)\n/) {
    return $1;
  }
  if ($stdout =~ /total revisions: (\d+)\n/) {
    print STDERR "WARNING: lasest revision not found in head, using total revisions in rcs file $rcsFile\n";
    return $1;
  }
  print STDERR "WARNING: no head: or total revisions: information found in file $rcsFile\n";

  return 1;
}

sub _getLatestRev {
  my ($this, $meta, $attachment) = @_;

  my $info = $this->_getRevInfo($meta, $attachment);
  return $info->{version} if $info;
  
  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  return $this->_getLatestRevFromHistory($file);
}

### static helper #####
sub _extractRevInfo {
  my $string = shift;

  my $attrs = Foswiki::Attrs->new($string);
  my $info = {%$attrs};

  $info->{author} //= $info->{user};
  $info->{version} = 1 unless $info->{version} =~ /^\d+$/;

  delete $info->{_RAW};
  delete $info->{_DEFAULT};
  delete $info->{user};

  $info->{author} //= $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID;
  $info->{date} //= time();
  $info->{version} //= 1;

  return $info;
}

sub _split {
  my $text = shift;

  #_writeDebug("called _split");

  my @list = ();
  return \@list unless defined $text;

  my $nl = 1;
  foreach my $i (split(/(\n)/, $text)) {
    if ($i eq "\n") {
      push(@list, '') if $nl;
      $nl = 1;
    } else {
      push(@list, $i);
      $nl = 0;
    }
  }
  push @list, '' if $nl;

  return \@list;
}

sub _mtime {
  my $file = shift;

  return (stat($file))[9] || 0;
}

sub _rmtree {
  my $root = shift;

  #_writeDebug("called _rmtree");

  my $D;
  if (opendir($D, $root)) {

    # Don't need to decode the directory entries, we're not
    # passing them back
    foreach my $entry (grep { !/^\.+$/ } readdir($D)) {
      $entry =~ m/^(.*)$/;
      $entry = "$root/$1";
      if (-d $entry) {
        _rmtree($entry);
      } elsif (!unlink($entry) && -e $entry) {
        my $mess = 'RcsFast: Failed to delete file ' . Encode::decode_utf8($entry) . ": $!";
        if ($Foswiki::cfg{OS} ne 'WINDOWS') {
          die $mess;
        } else {

          # Windows sometimes fails to delete files when
          # subprocesses haven't exited yet, because the
          # subprocess still has the file open. Live with it.
          warn $mess;
        }
      }
    }
    closedir($D);

    if (!rmdir($root)) {
      if ($Foswiki::cfg{OS} ne 'WINDOWS') {
        die 'failed to delete ' . Encode::decode_utf8($root) . ": $!";
      } else {
        warn 'RcsFast: failed to delete ' . Encode::decode_utf8($root) . ": $!";
      }
    }
  }
}

sub _copyMeta {
  my ($source, $target) = @_;

  $target->copyFrom($source);

  foreach my $key (qw(_web _topic _loadedRev _latestIsLoaded _text)) {
    my $val = $source->{$key};
    $target->{$key} = $val if defined $val;
  }

  return $target;
}

sub _mkPathTo {
  my $file = shift;

  ASSERT(File::Spec->file_name_is_absolute($file)) if DEBUG;

  my ($volume, $path, undef) = File::Spec->splitpath($file);
  $path = File::Spec->catpath($volume, $path, '');

  eval { File::Path::mkpath($path, 0, $Foswiki::cfg{Store}{dirPermission}); };
  if ($@) {
    die "failed to create $path: $!";
  }
}

sub _writeDebug {
  return unless TRACE;
  print STDERR "RcsFast: $_[0]\n";
}

1;
