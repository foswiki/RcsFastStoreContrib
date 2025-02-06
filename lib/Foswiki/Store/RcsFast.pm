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
use Encode();
use File::Copy ();
use File::Copy::Recursive ();
use File::Spec ();
use File::Path ();
use Assert;
use IO::String ();
use Error qw( :try );
use Fcntl qw( :DEFAULT :flock );

my %REVINFOS = ();
my $cacheCounter = 0;

use constant TRACE => 0;
use constant USE_MEMORY_CACHE => 0; # experimental
use constant WARN_REDUNDANT_HISTORY => 0;

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
}

=begin TML

---++ ClassMethod new() -> $core

constructor for a store object

=cut

sub new {
  my $class = shift;
  my $this = $class->SUPER::new(@_);

  #_writeDebug("called new");

  if (USE_MEMORY_CACHE) {
    $this->{_refInfos} = \%REVINFOS;
  } else {
    $this->{_revInfos} = {};
  }

  return $this;
}

=begin TML

---++ ObjectMethod finish() 

=cut

sub finish {
  my $this = shift;

  #_writeDebug("called finish");
  $this->SUPER::finish();

  undef $this->{_revInfos} unless USE_MEMORY_CACHE;
  undef $this->{_json};

  print STDERR "RcsFast: $cacheCounter cache hits\n" if USE_MEMORY_CACHE;
}

### storage api ################################################################

=begin TML

---++ ObjectMethod readTopic($meta, $version) -> ($rev, $isLatest)

=cut

sub readTopic {
  my ($this, $meta, $version) = @_;

  #_writeDebug("### called readTopic(path=".$meta->getPath.", version=" . ($version // 'undef') . ")");
  my $text;
  my $isLatest;

  ($text, $isLatest, $version) = $this->_getTopic($meta, $version);

  unless (defined $text) {
    $meta->setLoadStatus(undef, $isLatest);
    return (undef, $isLatest);
  }

  Foswiki::Serialise::deserialise($text, 'Embedded', $meta);

  $meta->setLoadStatus($version, $isLatest);

  return ($version, $isLatest);
}

=begin TML

---++ ObjectMethod moveAttachment( $oldTopicObject, $oldAttachment, $newTopicObject, $newAttachment, $cUID  )

=cut

sub moveAttachment {
  my ($this, $oldMeta, $oldAttachment, $newMeta, $newAttachment) = @_;

  #_writeDebug("called moveAttachment");
  ASSERT($oldAttachment) if DEBUG;
  ASSERT($newAttachment) if DEBUG;

  my $oldFile = $this->_getPath(meta => $oldMeta, attachment => $oldAttachment);
  return unless -e $oldFile;

  my $newFile = $this->_getPath(meta => $newMeta, attachment => $newAttachment);

  $this->_moveFile($oldFile, $newFile);

  $oldFile .= ',v';
  $newFile .= ',v';
  $this->_moveFile($oldFile, $newFile) if -e $oldFile;
}

=begin TML

---++ ObjectMethod copyAttachment( $oldTopicObject, $oldAttachment, $newTopicObject, $newAttachment  )

=cut

sub copyAttachment {
  my ($this, $oldMeta, $oldAttachment, $newMeta, $newAttachment) = @_;

  #_writeDebug("called copyAttachment");

  my $oldFile = $this->_getPath(meta => $oldMeta, attachment => $oldAttachment);
  my $newFile = $this->_getPath(meta => $newMeta, attachment => $newAttachment);

  return unless -e $oldFile;

  $this->_copyFile($oldFile, $newFile);

  $oldFile .= ",v";
  $newFile .= ",v";

  $this->_copyFile($oldFile, $newFile) if -e $oldFile;
}

=begin TML

---++ ObjectMethod attachmentExists( $$metat, $att ) -> $boolean

=cut

sub attachmentExists {
  my ($this, $meta, $attachment) = @_;

  #_writeDebug("called attachmentExists");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  return -e $file;
}

=begin TML

---++ ObjectMethod moveTopic(  $oldTopicObject, $newTopicObject, $cUID )

=cut

sub moveTopic {
  my ($this, $oldMeta, $newMeta) = @_;

  #_writeDebug("called moveTopic");

  my $oldFile = $this->_getPath(meta => $oldMeta);
  my $newFile = $this->_getPath(meta => $newMeta);

  $this->_moveFile($oldFile, $newFile);

  $oldFile .= ',v';
  $newFile .= ',v';

  $this->_moveFile($oldFile, $newFile) if -e $oldFile;

  my $oldPub = $this->_getPath(meta => $oldMeta, attachment => "");
  my $newPub = $this->_getPath(meta => $newMeta, attachment => "");

  $this->_moveFile($oldPub, $newPub) if -d $oldPub;
}

=begin TML

---++ ObjectMethod moveWeb( $oldWebObject, $newWebObject, $cUID )

=cut

sub moveWeb {
  my ($this, $oldMeta, $newMeta) = @_;

  #_writeDebug("called moveWeb");

  my $oldBase = $this->_getPath(meta => $oldMeta);
  my $newBase = $this->_getPath(meta => $newMeta);

  $this->_moveFile($oldBase, $newBase);

  $oldBase = $this->_getPath(meta => $oldMeta, attachment => "");
  $newBase = $this->_getPath(meta => $newMeta, attachment => "");

  $this->_moveFile($oldBase, $newBase) if -d $oldBase;
}

=begin TML

---++ ObjectMethod testAttachment( $$metat, $attachment, $test ) -> $value

=cut

sub testAttachment {
  my ($this, $meta, $attachment, $test) = @_;

  #_writeDebug("called testAttachment");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  return eval "-$test '$file'";
}

=begin TML

---++ ObjectMethod openAttachment( $$metat, $attachment, $mode, %opts  ) -> $text

=cut

sub openAttachment {
  my ($this, $meta, $attachment, $mode, %opts) = @_;

  #_writeDebug("called openAttachment");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  return unless -e $file;

  my $stream;
  if ($mode eq '<' && $opts{version}) {
    $stream = IO::String->new($this->_readAttachment($meta, $attachment, $opts{version}));
  } else {
    $this->_mkPathTo($file) if $mode =~ />/;

    throw Error::Simple('RcsFast: stream open ' . $file . ' failed: ' . 'Read requested on directory.')
      if -d $file;

    throw Error::Simple('RcsFast: stream open ' . $file . ' failed: ' . $!)
      unless open($stream, $mode, $file);

    binmode $stream;
  }

  return $stream;
}

=begin TML

---++ ObjectMethod getRevisionHistory ( $$metat [, $attachment]  ) -> $iterator

=cut

sub getRevisionHistory {
  my ($this, $meta, $attachment) = @_;

  #_writeDebug("called getRevisionHistory");

  my $info = $this->_getRevInfo($meta, $attachment);
  return Foswiki::Iterator::NumberRangeIterator->new($info ? $info->{version} : 1, 1);
}

=begin TML

---++ ObjectMethod getNextRevision ( $$metat  ) -> $revision

=cut

sub getNextRevision {
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

---++ ObjectMethod getRevisionDiff ( $$metat, $rev2, $contextLines  ) -> \@diffArray

=cut

sub getRevisionDiff {
  my ($this, $meta, $rev2, $contextLines) = @_;

  #_writeDebug("called getRevisionDiff");

  my $rev1 = $meta->getLoadedRev // "";
  my ($text1) = $this->_getTopic($meta, $rev1);
  my ($text2) = $this->_getTopic($meta, $rev2);

  my $lNew = _split($text1);
  my $lOld = _split($text2);

  return sdiff($lNew, $lOld);
}

=begin TML

---++ ObjectMethod getVersionInfo($$metat, $rev, $attachment) -> \%info

=cut

sub getVersionInfo {
  my ($this, $meta, $version, $attachment) = @_;

  return $this->_getRevInfo($meta, $attachment, $version);
}

=begin TML

---++ ObjectMethod saveAttachment( $$metat, $attachment, $stream, $cUID, \%options ) -> $revNum

=cut

sub saveAttachment {
  my ($this, $meta, $name, $stream, $cUID, $opts) = @_;

  #_writeDebug("called saveAttachment");

  my $verb = $this->attachmentExists($meta, $name) ? 'update' : 'insert';
  my $file = $this->_getPath(meta => $meta, attachment => $name);
  my $rcsFile = $file . ',v';

  my $info = $meta->get('FILEATTACHMENT', $name);

  # first check in rev 1 before we create rev 2
  if ( defined $info
    && !-e $rcsFile
    && defined $info->{version}
    && $verb eq 'update'
    && $info->{version} == 1)
  {
    my $firstInfo = $this->_getRevInfo($meta, $name, 1);
    $this->_checkIn($meta, $name, $firstInfo->{comment}, $firstInfo->{author}, $firstInfo->{date});
  }

  $this->_saveStream($file, $stream);

  # only check in if there is an first rev already
  my $nextRev = 1;

  if ($verb eq 'update') {
    $this->_checkIn($meta, $name, $opts->{comment} || '', $cUID, $opts->{forcedate});
    $nextRev = $this->getNextRevision($meta, $name);
  } else {
    $this->_hasRedundantHistory($meta, $name) if WARN_REDUNDANT_HISTORY;

  }

  $this->_unsetRevInfo($meta, $name);

  return $nextRev;
}

=begin TML

---++ ObjectMethod saveTopic( $$metat, $cUID, $options  ) -> $integer

=cut

sub saveTopic {
  my ($this, $meta, $cUID, $opts) = @_;

  #_writeDebug("### called saveTopic");

  my $verb = $this->topicExists($meta->web, $meta->topic) ? 'update' : 'insert';

  throw Error::Simple("RcsFast: Attempting to save a topic that already exists, and forceinsert specified")
    if $opts->{forceinsert} && $verb eq 'update';

  my $info = $meta->get("TOPICINFO");
  my $file = $this->_getPath(meta => $meta);
  my $rcsFile = $file . ',v';

  # first check in rev 1 before we create rev 2
  if ( defined $info
    && !-e $rcsFile
    && defined $info->{version}
    && $verb eq 'update'
    && $info->{version} == 2)
  {
    my $firstInfo = $this->_getRevInfo($meta, undef, 1);
    $this->_checkIn($meta, undef, $firstInfo->{comment}, $firstInfo->{author}, $firstInfo->{date});
  }

  # SMELL: must set TOPICINFO
  my $nextRev = $this->getNextRevision($meta);

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

  my $text = Foswiki::Serialise::serialise($meta, 'Embedded');
  $this->_saveFile($file, $text);

  if ($info->{version} > 1) {
    $this->_checkIn($meta, undef, $comment, $cUID, $opts->{forcedate});
  }

  $this->_hasRedundantHistory($meta) if WARN_REDUNDANT_HISTORY;
  $this->_unsetRevInfo($meta);

  return $nextRev;
}

=begin TML

---++ ObjectMethod repRev( $$metat, $cUID, %options ) -> $rev

=cut

sub repRev {
  my ($this, $meta, $cUID, %opts) = @_;

  #_writeDebug("### called repRev");

  my $file = $this->_getPath(meta => $meta);
  my $rcsFile = $file . ',v';

  my $comment = $opts{comment} || 'reprev';
  my $date = $opts{forcedate} || time();

  my $info = $meta->get("TOPICINFO");
  $info->{author} = $cUID;
  $info->{comment} = $comment;
  $info->{date} = $date;

  my $maxRev = $this->_getLatestRevFromHistory($file);

  if ($info->{version} <= 1) {
    # initial revision, so delete repository file and start again
    unlink $rcsFile;
  } else {
    $this->_deleteRevision($meta, $maxRev);
    $info->{version} = $maxRev;
  }

  my $text = Foswiki::Serialise::serialise($meta, 'Embedded');
  $this->_saveFile($file, $text);

  if ($info->{version} > 1) {
    $this->_lock($file);

    $date = Foswiki::Time::formatTime($date, '$rcs', 'gmtime');
    #_writeDebug("calling ciDateCmd");
    my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
      $Foswiki::cfg{RCS}{ciDateCmd},
      DATE => $date,
      USERNAME => $cUID,
      FILENAME => $file,
      COMMENT => $comment
    );

    if ($exit) {
      $stdout = $Foswiki::cfg{RCS}{ciDateCmd} . "\n" . $stdout;
      return $stdout;
    }

    chmod($Foswiki::cfg{Store}{filePermission}, $file);
  }

  $this->_hasRedundantHistory($meta) if WARN_REDUNDANT_HISTORY;
  $this->_unsetRevInfo($meta);
}

=begin TML

---++ ObjectMethod delRev( $$metat, $cUID ) -> $rev

=cut

sub delRev {
  my ($this, $meta, $cUID) = @_;

  #_writeDebug("called delRev");

  my $info = $meta->get("TOPICINFO");

  throw Error::Simple("RcsFast: cannot delete non existing version")
    unless $info;

  throw Error::Simple("RcsFast: cannot delete initial revision of " . $meta->getPath())
    if $info->{version} <= 1;

  my $file = $this->_getPath(meta => $meta);
  my $maxRev = $this->_getLatestRevFromHistory($file);

  if ($info->{version} <= $maxRev) {
    $this->_deleteRevision($meta, $info->{version});
  } else {
    $info->{version} = $maxRev;
  }

  # reload the topic object
  $meta->unload();
  $meta->loadVersion();

  return $info->{version};
}

=begin TML

---++ ObjectMethod atomicLockInfo( $$metat ) -> ($cUID, $time)

=cut

sub atomicLockInfo {
  my ($this, $meta) = @_;

  #_writeDebug("called atomicLockInfo");

  my $file = $this->_getPath(meta => $meta, extension => '.lock');
  return (undef, undef) unless -e $file;

  my $text = $this->_readFile($file);

  return split(/\s+/, $text, 2);
}

=begin TML

---++ ObjectMethod atomicLock( $$metat, $cUID )

=cut

sub atomicLock {
  my ($this, $meta, $cUID) = @_;

  #_writeDebug("called atomicLock");

  my $file = $this->_getPath(meta => $meta, extension => '.lock');

  $this->_saveFile($file, $cUID . "\n" . time);
}

=begin TML

---++ ObjectMethod atomicUnlock( $$metat )

=cut

sub atomicUnlock {
  my ($this, $meta, $cUID) = @_;

  #_writeDebug("called atomicUnlock");

  my $file = $this->_getPath(meta => $meta, extension => '.lock');

  if (-e $file) {
    unlink $file
      or die "RcsFast: failed to delete $file: $!";
  }
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

---++ ObjectMethod eachChange( $meta, $time ) -> $iterator

=cut

sub eachChange {
  my ($this, $webOrMeta, $since) = @_;

  #_writeDebug("called eachChange");
  my $web = ref($webOrMeta) ? $webOrMeta->web : $webOrMeta;
  my $file = $this->_getPath(web => $web, file => '.changes');
  $since //= 0;

  my @changes;
  @changes = reverse grep { $_->{time} >= $since } @{$this->_readChanges($file)}
    if -r $file;

  return Foswiki::ListIterator->new(\@changes);
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
}

=begin TML

---++ ObjectMethod eachAttachment( $$metat ) -> \$iterator

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

  if ($meta->topic) {
    my $file = $this->_getPath(meta => $meta, attachment => $attachment);
    my $rcsFile = $file . ",v";
    my $pubDir = $this->_getPath(meta => $meta, attachment => "");

    unlink($file);
    unlink($rcsFile) if -e $rcsFile;
    _rmtree($pubDir) unless $attachment;

  } else {
    my $dataDir = $this->_getPath(meta => $meta);
    my $pubDir = $this->_getPath(meta => $meta, attachment => "");

    # Web
    _rmtree($dataDir);
    _rmtree($pubDir);
  }
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

---++ ObjectMethod getRevisionAtTime( $$metat, $time ) -> $rev

=cut

sub getRevisionAtTime {
  my ($this, $meta, $time) = @_;

  #_writeDebug("called getRevisionAtTime");

  my $file = $this->_getPath(meta => $meta);
  my $rcsFile = $file . ',v';

  unless (-e $rcsFile) {
    return ($time >= _mtime($file)) ? 1 : 0;
  }

  my $date = Foswiki::Time::formatTime($time, '$rcs', 'gmtime');

  #_writeDebug("calling rlogDateCmd");
  my $cmd = $Foswiki::cfg{RCS}{rlogDateCmd};
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
    $cmd,
    DATE => $date,
    FILENAME => $file,
  );

  throw Error::Simple("RcsFast: rlogDateCmd of $file failed: $stdout $stderr")
    if $exit;

  my $version;
  if ($stdout =~ m/revision \d+\.(\d+)/) {
    $version = $1;
  }

  return $version;
}

=begin TML

---++ ObjectMethod getLease( $$metat ) -> $lease

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

---++ ObjectMethod setLease( $$metat, $length )

=cut

sub setLease {
  my ($this, $meta, $lease) = @_;

  #_writeDebug("called setLease");

  my $file = $this->_getPath(meta => $meta, extension => '.lease');

  if ($lease) {
    $this->_saveFile($file, join("\n", %$lease));
  } elsif (-e $file) {
    unlink($file)
      or throw Error::Simple("RcsFast: failed to delete $file: $!");
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

=cut

sub _json {
  my $this = shift;

  #_writeDebug("called _json");

  $this->{_json} //= JSON->new->pretty(0);

  return $this->{_json};
}

=begin TML

---++ ObjectMethod _getTopic($meta, $version) -> ($tex, $isLatest, $version)

=cut

sub _getTopic {
  my ($this, $meta, $version) = @_;

  #_writeDebug("called _getTopic(" . ($version // 'undef') . ")");

  my $file = $this->_getPath(meta => $meta);
  my $rcsFile = $file . ',v';
  my $info = $this->_getRevInfo($meta, undef, $version);
  my $text;

  return (undef, 0) unless defined $info;

  unless ($version && -e $rcsFile) {
    $text = $this->_readTopic($meta);
    return ($text, 1, $info->{version});
  }

  my $isLatest = 0;
  my $coCmd = $Foswiki::cfg{RCS}{coCmd};
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
    if ($version > $1) {
      $isLatest = 1;
    } else {
      $isLatest = ($1 == $info->{version});
    }
    $version = $1;
  }

  return ($text, $isLatest, $version);
}

=begin TML

---++ ObjectMethod _getRevInfo($meta, $attachment, $version) -> \%info

=cut

sub _getRevInfo {
  my ($this, $meta, $attachment, $version) = @_;

  #_writeDebug("called _getRevInfo");

  my $key = _getRevInfosKey($meta, $attachment, $version);
  my $info = $this->{_revInfos}{$key};

  if (defined $info) {
    # found in cache
    $cacheCounter++;
    return $info;
  }

  my $text = $this->_readTopic($meta);

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
        # broken storage file, assuming there is no history
        $info = {
          author => $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID,
          date => time(),
          version => 1
        };
      }
    }
  }

  # set cache
  $this->_setRevInfo($meta, $attachment, $version, $info);

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
    $Foswiki::cfg{RCS}{infoCmd},
    REVISION => '1.' . $version,
    FILENAME => $rcsFile,
  );

  throw Error::Simple("RcsFast: infoCmd of $rcsFile failed: $stdout $stderr")
    if $exit;

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

---++ ObjectMethod _setRevInfo($meta, $attachment, $version, $info) -> $info

=cut

sub _setRevInfo {
  my ($this, $meta, $attachment, $version, $info) = @_;

  $this->{_revInfos}{_getRevInfosKey($meta, $attachment, $version)} = $info
    if defined $info;

  return $info;
}

=begin TML

---++ ObjectMethod _unsetRevInfo($meta, $attachment, $version) 

=cut

sub _unsetRevInfo {
  my ($this, $meta, $attachment, $version) = @_;

  my $key = _getRevInfosKey($meta, $attachment, $version);

  undef $this->{_revInfos}{$key};
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
    my $coCmd = $Foswiki::cfg{RCS}{coCmd};
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

---++ ObjectMethod _getPath(%args)  -> $filepath

returns the path to an object on the store. args may contain keys:

   * meta: object to get the store file for
   * web, topic: either meta or web, topic params are required
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

  my $attachment = $args{attachment};
  my @path = ();
  push @path, (defined $attachment ? $Foswiki::cfg{PubDir} : $Foswiki::cfg{DataDir});
  push @path, $web if $web;

  if ($topic) {
    if (defined $attachment) {
      push @path, $topic, $attachment;
    } else {
      push @path, $topic . ($args{extension} || '.txt');
    }
  }

  push @path, $args{file} if $args{file};

  return Encode::encode_utf8(join("/", @path));
}

=begin TML

---++ ObjectMethod _readTopic($meta)  -> $text

=cut

sub _readTopic {
  my ($this, $meta) = @_;

  #_writeDebug("called _readTopic");

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
  my ($this, $name, $isBinary) = @_;

  #_writeDebug("called _readFile");

  my $IN_FILE;

  open($IN_FILE, '<', $name)
    or die "RcsFast: failed to read $name: $!";

  binmode($IN_FILE);
  local $/ = undef;
  my $data = <$IN_FILE>;
  close($IN_FILE);

  $data //= '';

  return $data if $isBinary;
  return Encode::decode_utf8($data);
}

=begin TML

---++ ObjectMethod _mkPathTo($file) 

creates a path the the given file

=cut

sub _mkPathTo {
  my ($this, $file) = @_;

  #_writeDebug("called _mkPathTo");

  ASSERT(File::Spec->file_name_is_absolute($file)) if DEBUG;

  my ($volume, $path, undef) = File::Spec->splitpath($file);
  $path = File::Spec->catpath($volume, $path, '');

  eval { File::Path::mkpath($path, 0, $Foswiki::cfg{Store}{dirPermission}); };
  if ($@) {
    throw Error::Simple("RcsFast: failed to create ${path}: $!");
  }
}

=begin TML

---++ ObjectMethod _moveFile($from, $to) 

=cut

sub _moveFile {
  my ($this, $from, $to) = @_;

  #_writeDebug("called _moveFile");

  $this->_mkPathTo($to);

  unless (File::Copy::Recursive::rmove($from, $to)) {
    throw Error::Simple('RcsFast: move ' . $from . ' to ' . $to . ' failed: ' . $!);
  }
}

=begin TML

---++ ObjectMethod _copyFile($from, to) 

=cut

sub _copyFile {
  my ($this, $from, $to) = @_;

  #_writeDebug("called _copyFile");

  $this->_mkPathTo($to);
  unless (File::Copy::copy($from, $to)) {
    throw Error::Simple('RcsFast: copy ' . $from . ' to ' . $to . ' failed: ' . $!);
  }
}

=begin TML

---++ ObjectMethod _checkIn($meta, $attachment, $comment, $user, $data) 

=cut

sub _checkIn {
  my ($this, $meta, $attachment, $comment, $user, $date) = @_;

  #_writeDebug("called _checkIn");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  $this->_lock($file);

  $comment ||= 'none';

  my ($cmd, $stdout, $exit, $stderr);
  if (defined($date)) {
    $date = Foswiki::Time::formatTime($date, '$rcs', 'gmtime');
    $cmd = $Foswiki::cfg{RCS}{ciDateCmd};
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
    $cmd = $Foswiki::cfg{RCS}{ciCmd};
    ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
      $cmd,
      USERNAME => $user,
      FILENAME => $file,
      COMMENT => $comment
    );
  }

  $stdout ||= '';

  throw Error::Simple("RcsFast: ciCmd/ciDateCmd of $file failed: $stdout $stderr")
    if $exit;

  chmod($Foswiki::cfg{Store}{filePermission}, $file);
}

=begin TML

---++ ObjectMethod _lock($file) 

=cut

sub _lock {
  my ($this, $file) = @_;

  #_writeDebug("called _lock");

  my $rcsFile = $file . ',v';
  return unless -e $rcsFile;

  # Try and get a lock on the file
  #_writeDebug("calling lockCmd");
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RCS}{lockCmd}, FILENAME => $file);

  if ($exit) {

    # if the lock has been set more than 24h ago, let's try to break it
    # and then retry.  Should not happen unless in Cairo upgrade
    # scenarios - see Item2102

    if ((time - _mtime($rcsFile)) > 3600) {
      warn 'Automatic recovery: breaking lock for ' . $file;
      #_writeDebug("calling lockCmd");
      Foswiki::Sandbox->sysCommand($Foswiki::cfg{RCS}{breaklockCmd}, FILENAME => $file);
      ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RCS}{lockCmd}, FILENAME => $file);
    }
    if ($exit) {

      # still no luck - bailing out
      $stdout ||= '';
      throw Error::Simple("RcsFast: lockCmd failed: $stdout $stderr");
    }
  }

  chmod($Foswiki::cfg{Store}{filePermission}, $file);
}

=begin TML

---++ ObjectMethod _revisionHistoryExists($meta, $attachment) 

=cut

sub _revisionHistoryExists {
  my ($this, $meta, $attachment) = @_;

  #_writeDebug("called _revisionHistoryExists");

  my $file = $this->_getPath(meta => $meta, attachment => $attachment) . ',v';

  return -e $file;
}

=begin TML

---++ ObjectMethod _saveFile($file, $text) 

=cut

sub _saveFile {
  my ($this, $file, $text) = @_;

  #_writeDebug("called _saveFile($file)");
  #_writeDebug("... text=$text");

  $this->_mkPathTo($file);

  my $fh;

  open($fh, '>', $file)
    or die("RcsFast: failed to create file $file: $!");

  flock($fh, LOCK_EX)
    or die("RcsFast: failed to lock file $file: $!");

  binmode($fh)
    or die("RcsFast: failed to binmode $file: $!");

  print $fh Encode::encode_utf8($text)
    or die("RcsFast: failed to print to $file: $!");

  close($fh)
    or die("RcsFast: failed to close file $file: $!");

  chmod($Foswiki::cfg{Store}{filePermission}, $file);

  return;
}

=begin TML

---++ ObjectMethod _saveStream($file, $fh) 

=cut

sub _saveStream {
  my ($this, $file, $fh) = @_;

  #_writeDebug("called _saveStream");

  $this->_mkPathTo($file);

  my $F;

  open($F, '>', $file)
    || throw Error::Simple('RcsFast: open ' . $file . ' failed: ' . $!);

  binmode($F)
    || throw Error::Simple('RcsFast: failed to binmode ' . $file . ': ' . $!);

  my $data;

  while (read($fh, $data, 1024)) {
    print $F $data;
  }

  close($F)
    || throw Error::Simple('RcsFast: close ' . $file . ' failed: ' . $!);

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
  print STDERR "Corrupt $file: $@\n" if ($@);

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

  # delete latest revision (unlock (may not be needed), delete revision)
  #_writeDebug("calling unlockCmd");
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RCS}{unlockCmd}, FILENAME => $file);

  throw Error::Simple("RcsFast: unlockCmd failed: $stdout $stderr")
    if $exit;

  chmod($Foswiki::cfg{Store}{filePermission}, $file);

  #_writeDebug("calling delRevCmd");
  ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
    $Foswiki::cfg{RCS}{delRevCmd},
    REVISION => '1.' . $rev,
    FILENAME => $file
  );

  throw Error::Simple("RcsFast: delRevCmd of $file failed: $stdout $stderr")
    if $exit;

  $rev--;
  #_writeDebug("calling coCmd");
  ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand(
    $Foswiki::cfg{RCS}{coCmd},
    REVISION => '1.' . $rev,
    FILENAME => $file
  );

  throw Error::Simple("RcsFast: coCmd of $file failed: $stdout $stderr")
    if $exit;

  $this->_unsetRevInfo($meta, undef, $rev);

  $this->_saveFile($file, $stdout);
}

=begin TML

---++ ObjectMethod _getLatestRevFromHistory($file) -> $rev

=cut

sub _getLatestRevFromHistory {
  my ($this, $file) = @_;

  my $rcsFile = $file . ',v';
  return 1 unless -e $rcsFile;

  #_writeDebug("calling histCmd");
  my ($stdout, $exit, $stderr) = Foswiki::Sandbox->sysCommand($Foswiki::cfg{RCS}{histCmd}, FILENAME => $rcsFile);

  throw Error::Simple("RcsFast: histCmd of $rcsFile failed: $stdout $stderr")
    if $exit;

  if ($stdout =~ /head:\s+\d+\.(\d+)\n/) {
    return $1;
  }
  if ($stdout =~ /total revisions: (\d+)\n/) {
    return $1;
  }

  return 1;
}

=begin TML

---++ ObjectMethod _hasRedundantHistory($meta, $attachment) -> $boolean

checks for a redundant rcs file

=cut

sub _hasRedundantHistory {
  my ($this, $meta, $attachment) = @_;

  my $res = 0;

  my $info = $this->_getRevInfo($meta, $attachment);
  return 0 unless $info;
  return 0 if $attachment && $info->{version} > 1;

  my $file = $this->_getPath(meta => $meta, attachment => $attachment);
  my $rcsFile = $file . ',v';
  my $webTopic = $meta->getPath;

  if ($attachment) {
    if (-e $rcsFile) {
      $res ||= 1;
      print STDERR "WARNING: version 1 attachment $attachment at $webTopic doesn't need an rcs file yet: $rcsFile\n";
    }
  } else {
    if ($info->{version} == 1 && -e $rcsFile) {
      $res ||= 1;
      print STDERR "WARNING: version 1 of $webTopic doesn't need an rcs file yet: $rcsFile\n";
    }

    foreach my $name (map { $_->{name} } $meta->find('FILEATTACHMENT')) {
      $res = 1 if $this->_hasRedundantHistory($meta, $name);
    }
  }

  return $res;
}

### static helper #####
sub _getRevInfosKey {
  my ($meta, $attachment, $version) = @_;

  return $meta->getPath() . '::' . ($attachment // 'topic') . '::' . ($version || '0');
}

sub _extractRevInfo {
  my $string = shift;

  #_writeDebug("called _extractRevInfo($string)");

  my $info = {};

  while ($string =~ /(author|user|comment|name|date|version)="(.*?)"/g) {
    my $key = $1;
    my $val = $2;

    if ($key eq "version") {
      next unless $val =~ /^\d+$/;
    } elsif ($key eq "user") {
      $key = "author";
    }

    $info->{$key} = $val;
  }

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

  return (stat($file))[9];
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
        die 'RcsFast: Failed to delete ' . Encode::decode_utf8($root) . ": $!";
      } else {
        warn 'RcsFast: Failed to delete ' . Encode::decode_utf8($root) . ": $!";
      }
    }
  }
}

sub _writeDebug {
  print STDERR "RcsFast: $_[0]\n" if TRACE;
}

1;
