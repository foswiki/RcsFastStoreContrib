# Tests that specifically target the ticklish area of rev number management.
# These tests acknowledge the possibility that loading a topic may not
# return the true revision number of the topic, but some cached number
# that may or may not be correct. They also verify that if
# the topic is *force loaded* with a specific revision (which may or may
# not be in the range of known revisions) that a "true" revision
# will be loaded.
#
# These tests are conducted at the Foswiki::Meta object level, so need
# to be run for each different RCS store implementation.
#
package RcsFastTests;

use strict;
use warnings;
use utf8;

use Foswiki::Store::RcsFast ();
use Foswiki::Meta ();
use Foswiki::Func ();
use FoswikiFnTestCase();
use Encode ();
use Foswiki();
use Foswiki::Store();
use Foswiki::Request::Upload ();
use File::Temp;

our @ISA = ('FoswikiFnTestCase');

sub new {
  my $self = shift()->SUPER::new(@_);
  return $self;
}

sub loadExtraConfig {
  my $this = shift;

  $this->SUPER::loadExtraConfig(@_);
  $Foswiki::cfg{Store}{Implementation} = 'Foswiki::Store::RcsFast';
}

sub createTopic {
  my ($this, $topic) = @_;

  $topic //= 'SomeTopic';
  return Foswiki::Meta->new($this->{session}, $this->{test_web}, $topic );
}

sub createWeb {
  my ($this, $web, $base) = @_;

  $web //= 'SomeWeb';
  $base //= "_empty";

  my $meta = Foswiki::Meta->new($this->{session}, "$this->{test_web}/$web");
  $meta->populateNewWeb("_empty");

  return $meta;
}

sub readWeb {
  my ($this, $web) = @_;

  my $fullWeb = $this->{test_web};
  $fullWeb .= ".$web" if $web;
  
  return Foswiki::Meta->new($this->{session}, $fullWeb);
}

sub readTopic {
  my ($this, $topic, $rev) = @_;

  $topic //= 'SomeTopic';
  my ($meta) = Foswiki::Func::readTopic($this->{test_web}, $topic, $rev);

  return $meta;
}

sub getPath {
  my ($this, $topic, $attachment) = @_;

  $topic //= 'SomeTopic';

  return "$Foswiki::cfg{PubDir}/$this->{test_web}/$topic/$attachment" if $attachment;
  return "$Foswiki::cfg{DataDir}/$this->{test_web}/$topic.txt";
}

sub readRawTopic {
  my $this = shift;

  return Foswiki::Func::readFile($this->getPath(@_), 1);
}

sub saveRawTopic {
  my ($this, $topic, $text) = @_;

  my $file = $this->getPath($topic);
  Foswiki::Func::saveFile($file, $text);
}

sub saveAttachment {
  my ($this, $topic, $name, $data) = @_;

  my $meta = $this->createTopic($topic);
  $topic //= "SomeTopic";
  $name //= "SomeAttachment.txt";
  $data //= "";

  my $fh = File::Temp->new();
  print $fh $data;
  seek($fh, 0, 0);
  my $size = (stat($fh->filename))[7];

  $meta->attach(
    name => $name,
    file => $fh->filename,
    filesize => $size
  );
  close $fh;

  return $meta;
}

sub readAttachment {
  my ($this, $topicOrMeta, $name) = @_;

  my $meta;
  my $topic;

  if (ref($topicOrMeta)) {
    $meta = $topicOrMeta;
  } else {
    $topic = $topicOrMeta;
    $topic //= "SomeTopic";
    $meta = $this->createTopic($topic);
  }

  $name //= "SomeAttachment.txt";

  my $fh = $meta->openAttachment($name, "<");
  return unless $fh;

  my $data;
  {
      local $/ = undef;
      $data = <$fh>;
      close $fh;
  }
  
  return $data;
}

sub test_store {
  my $this = shift;

  $this->assert_equals('Foswiki::Store::RcsFast', $Foswiki::cfg{Store}{Implementation});
  $this->assert_matches(qr/^Foswiki::Store::RcsFast$/, ref($this->{session}{store}));
}

sub test_save_topic {
  my $this = shift;

  my $meta = $this->createTopic;
  $this->assert_equals(undef, $meta->getLoadedRev());
  $this->assert(!$meta->existsInStore);

  $meta->text("hello world");

  $meta->save;
  $meta->finish;

  $this->assert($meta->existsInStore);

  my $file = $this->getPath;
  my $rcsFile = $file . ',v';

  $this->assert(-e $file);
  $this->assert(!-e $rcsFile, "there should be no rev file yet");

  $meta = $this->readTopic;
  $this->assert_equals("hello world", $meta->text);
  $this->assert_equals(1, $meta->getLoadedRev());

  $meta->text("hello world 2");
  $meta->save;
  $meta->finish;

  $meta = $this->readTopic;
  $this->assert_equals("hello world 2", $meta->text);
  $this->assert_equals(1, $meta->getLoadedRev());

  $this->assert(!-e $rcsFile, "there should still be no rev file yet");

  $meta = $this->readTopic;
  $meta->text("hello world 3");
  $meta->save(forcenewrevision => 1);
  $meta->finish;

  $this->assert(-e $rcsFile, "now there should be a rev file");

  $meta = $this->readTopic;
  $this->assert_equals("hello world 3", $meta->text);
  $this->assert_equals(2, $meta->getLoadedRev());
  $this->assert_equals(2, $this->{session}{store}->_getLatestRevFromHistory($this->getPath));

  # trigger a repRev
  $meta->finish();
  $meta = $this->readTopic;
  $meta->text("hello world 4");
  $meta->save;

  $this->assert_equals(2, $meta->getLoadedRev, "revprev changed the rev");
  $this->assert_equals(2, $this->{session}{store}->_getLatestRevFromHistory($this->getPath), "revprev changed the history rev");
}

sub test_version_info {
  my $this = shift;
  
  my $meta = $this->createTopic;
  $this->assert_equals(undef, $meta->getLoadedRev);

  $meta->text("hello world");
  $meta->save;

  my $info = $meta->getRevisionInfo;
  $this->assert($info);
  $this->assert_equals('BaseUserMapping_666', $info->{author});
  $this->assert($info->{date});
  $this->assert_equals(1, $info->{version});

  $meta->save(forcenewrevision => 1);
  $info = $meta->getRevisionInfo;
  $this->assert_equals(2, $info->{version});

  $meta->save(forcenewrevision => 1);
  $info = $meta->get("TOPICINFO");
  $this->assert_equals(3, $info->{version});
  $info = $meta->getRevisionInfo;
  $this->assert_equals(3, $info->{version});
  $meta->finish;

  $meta = $this->readTopic(undef, 1);
  $info = $meta->get("TOPICINFO");
  $this->assert_equals(1, $info->{version});
  $info = $meta->getRevisionInfo;
  $this->assert_equals(1, $info->{version});
  $meta->finish;

  $meta = $this->readTopic(undef, 2);
  $info = $meta->get("TOPICINFO");
  $this->assert_equals(2, $info->{version});
  $info = $meta->getRevisionInfo;
  $this->assert_equals(2, $info->{version});
  $meta->finish;

  $meta = $this->readTopic(undef, 3);
  $info = $meta->get("TOPICINFO");
  $this->assert_equals(3, $info->{version});
  $info = $meta->getRevisionInfo;
  $this->assert_equals(3, $info->{version});
  $meta->finish;
}

sub test_version_history {
  my $this = shift;
  
  my $meta = $this->createTopic;
  $this->assert_equals(undef, $meta->getLoadedRev);

  $meta->text("hello world");
  $meta->save;
  $meta->text("hello world 1");
  $meta->save(forcenewrevision => 1);
  $meta->text("hello world 2");
  $meta->save(forcenewrevision => 1);

  my $info = $meta->getRevisionInfo;
  $this->assert($info);
  $this->assert_equals('BaseUserMapping_666', $info->{author});
  $this->assert($info->{date});
  $this->assert_equals(3, $info->{version});

  my $it = $meta->getRevisionHistory();
  $this->assert($it);
  my $all = join(",", $it->all);
  $this->assert_equals("3,2,1", $all);

  $meta->finish;
  $meta = $this->createTopic;

  $it = $meta->getRevisionHistory();
  $this->assert($it);
  $all = join(",", $it->all);
  $this->assert_equals("3,2,1", $all);

  my $text = $meta->text;
  $this->assert_equals("hello world 2", $text);
}

# Topic has not been saved. Loaded rev should be undef *even after
# a load*
sub test_phantom_topic {
  my $this = shift;

  my $meta = $this->createTopic("PhantomTopic");

  $this->assert_equals(undef, $meta->getLoadedRev());
  $meta->load();

  $this->assert_equals(undef, $meta->getLoadedRev());
  $meta->load(1);

  $this->assert_equals(undef, $meta->getLoadedRev());

  $meta->finish();
  $meta = $this->readTopic("PhantomTopic");

  $this->assert_equals(undef, $meta->getLoadedRev());
}

# Topic has been saved. Loaded rev should be defined after a load,
sub test_good_topic {
  my $this = shift;

  my $meta = $this->createTopic("GoodTopic");
  $meta->text('Let there be light');

  # We haven't loaded a rev yet, so the loaded rev should be undef
  $this->assert_equals(undef, $meta->getLoadedRev());

  # Now save. The loaded rev should be set.
  $this->assert_equals(1, $meta->save());
  $this->assert_equals(1, $meta->getLoadedRev());

  # Create a new unloaded object for what we just saved
  $meta->finish();
  $meta = $this->createTopic("GoodTopic");
  $this->assert_equals(undef, $meta->getLoadedRev());

  $meta->load();
  $this->assert_equals(1, $meta->getLoadedRev());

  $meta->finish();
  $meta = $this->readTopic("GoodTopic", 0);
  $this->assert_equals(1, $meta->getLoadedRev());

  $meta->finish();
  $meta = $this->readTopic("GoodTopic", 1);
  $this->assert_equals(1, $meta->getLoadedRev());
  $meta->finish();

  $meta = $this->readTopic("GoodTopic", 2);
  $this->assert_equals(1, $meta->getLoadedRev());
}

# Save a topic with borked TOPICINFO. The TOPICINFO should be corrected
# during the save.
sub test_borked_TOPICINFO_save {
  my $this = shift;

  my $meta = $this->createTopic("BorkedTOPICINFO");
  $meta->text(<<SICK);
%META:TOPICINFO{version="3"}%
Houston, we may have a problem here
SICK

  # We haven't loaded a rev yet, so the loaded rev should be undef
  $this->assert_equals(undef, $meta->getLoadedRev());

  $meta->save(forcenewrevision => 1);

  # Now we *have* saved, and the rev should have been force-corrected
  $this->assert_equals(1, $meta->getLoadedRev());

  # Load it again to make sure
  $meta->finish();

  $meta = $this->readTopic("BorkedTOPICINFO");
  $this->assert_equals(1, $meta->getLoadedRev());
}

sub test_no_history {
  my $this = shift;

  $this->saveRawTopic("NoHistory", <<WHEE);
%META:TOPICINFO{version="1.3"}%
Blue. No, Green!
WHEE

  my $meta = $this->readTopic("NoHistory");
  $this->assert_equals(1, $meta->getLoadedRev());

  $meta->finish();
  $meta = $this->createTopic("NoHistory");
  $meta->load(3);

  # We asked for an out-of-range version; even though that's the rev no
  # in the topic, it deosn't exist as a version so the loaded rev
  # should rewind to the "true" version.
  $this->assert_equals(1, $meta->getLoadedRev());

  # Reload out-of-range
  $meta->finish();
  $meta = $this->createTopic("NoHistory");
  $meta->load(4);
  $this->assert_equals(1, $meta->getLoadedRev());

  # Reload undef
  $meta->finish();
  $meta = $this->createTopic("NoHistory");
  $meta->load();
  $this->assert_equals(1, $meta->getLoadedRev());

  # Reload 0
  $meta->finish();
  $meta = $this->createTopic("NoHistory");
  $meta->load(0);
  $this->assert_equals(1, $meta->getLoadedRev());
}

# Topic exists on disk, but the topic cache was saved by an external
# process and META:TOPICINFO is behind the latest topic in the DB.
# This case is specifically aimed at stores that decouple
# the revision history from the topic text.
# When the topic is first loaded, the version number will be imaginary.
sub test_borked_TOPICINFO_load_behind {
  my $this = shift;

  # Start by creating a topic with a valid rev no (1)
  # Rev 1: Your grandmother smells of elderberries
  my $meta = $this->createTopic("BorkedTOPICINFO");
  $meta->text(<<SICK);
Your grandmother smells of elderberries
SICK
  $meta->save();
  $this->assert_equals(1, $meta->getLoadedRev());

  # Rev 2: We are the knights who say Ni!
  $meta->text('ere, Dennis, there some lovely muck over ere');
  $meta->save(forcenewrevision => 1);
  $this->assert_equals(2, $meta->getLoadedRev());
  $this->assert_equals(2, $this->{session}{store}->_getLatestRevFromHistory($this->getPath("BorkedTOPICINFO")));

  # Stomp the cache

  # Wait for the clock to tick. This used to be a 1 second tick,
  # but r16350 added a 1s grace period to the file time checks,
  # so it had to be upped to 2
  my $x = time;
  while (time == $x) {
    sleep 2;
  }

  # .txt TOPICINFO borked: We are the knights who say Ni!
  # In PFS, this will create rev 3 when the repair is done
  $this->saveRawTopic("BorkedTOPICINFO",<<SICK);
%META:TOPICINFO{version="1"}%
We are the knights who say Ni!
SICK

  # The load will NOT repair 
  $meta->finish();
  $meta = $this->readTopic("BorkedTOPICINFO");
  $this->assert_equals(1, $meta->getLoadedRev());
  $this->assert_matches(qr/knights who say Ni/, $meta->text());

  # Save will repair
  $meta->save();
  $meta->finish();

  $meta = $this->readTopic("BorkedTOPICINFO");
  $this->assert_equals(3, $meta->getLoadedRev());
  $this->assert_equals(3, $this->{session}{store}->_getLatestRevFromHistory($this->getPath("BorkedTOPICINFO")));

  # Now if we load the latest, we will see a rev number of
  # 1 (because it's reading the .txt), but if we force-load any other
  # rev we should see a correct rev number
  $meta = $this->readTopic("BorkedTOPICINFO", 2);
  $this->assert_equals(2, $meta->getLoadedRev());
  $this->assert_matches(qr/lovely muck/, $meta->text());

  # load explicit number. This is the same rev as is is in the TOPICINFO
  # for the .txt, but that is invalid TOPICINFO so we should be loading
  # the 'true' rev 1: Your mother smells of elderberries
  $meta->finish();
  $meta = $this->readTopic("BorkedTOPICINFO", 1);
  $this->assert_equals(1, $meta->getLoadedRev());
  $this->assert_matches(qr/elderberries/, $meta->text());

  $meta->finish();
  $meta = $this->readTopic("BorkedTOPICINFO", 2);
  $this->assert_equals(2, $meta->getLoadedRev());
  $this->assert_matches(qr/lovely muck/, $meta->text());

  # load latest rev
  $meta->finish();
  $meta = $this->readTopic("BorkedTOPICINFO", 3);
  $this->assert_equals(3, $meta->getLoadedRev());
  $this->assert_matches(qr/knights who say Ni/, $meta->text());

  # load out of range rev
  $meta->finish();
  $meta = $this->readTopic("BorkedTOPICINFO", 4);
  $this->assert_equals(3, $meta->getLoadedRev());
  $this->assert_matches(qr/knights who say Ni/, $meta->text());

  #  commit the pending checkin
  $meta->save(forcenewrevision => 1);
  $meta->finish();

  # testing rev info
  $meta = $this->readTopic("BorkedTOPICINFO", 0);
  $this->assert_equals(4, $meta->getLoadedRev()); # that's the real revision now, the pending checkin got stored to rev 3

  my $info = $meta->getRevisionInfo();
  $this->assert_equals($Foswiki::Users::BaseUserMapping::DEFAULT_USER_CUID, $info->{author});
  $this->assert($info->{date});
  $this->assert_equals(4, $info->{version});

  # If we now save it, we should be back to corrected rev nos
  $meta->save(forcenewrevision => 1);
  $meta->finish();
  $meta = $this->readTopic("BorkedTOPICINFO", 0);
  $this->assert_equals(5, $meta->getLoadedRev());
}

sub test_fix_topicinfo {
  my $this = shift;


  my $meta = $this->createTopic;
  $this->assert_equals(undef, $meta->getLoadedRev());
  $meta->save;
  $meta->save(forcenewrevision => 1);

  my $info = $meta->getRevisionInfo();
  $this->assert_equals(2, $info->{version});

  my $file = $this->getPath();  

  # botch it 
  my $raw = $meta->getEmbeddedStoreForm;
  $raw =~ s/version="2"/version="3"/g;
  $this->saveRawTopic(undef, $raw);

  # re-read it
  $meta->finish;
  $meta = $this->readTopic;
  $info = $meta->getRevisionInfo();
  $this->assert_equals(3, $info->{version});

  # this will fix the version info in the topic
  $meta->save;
  $info = $meta->getRevisionInfo();
  $this->assert_equals(2, $info->{version});
  $meta->save(forcenewrevision => 1);

  # botch it again
  $raw =~ s/version="3"/version="2"/g;
  $this->saveRawTopic(undef, $raw);

  # re-read it
  $meta->finish;
  $meta = $this->readTopic;
  $info = $meta->getRevisionInfo();
  $this->assert_equals(2, $info->{version});

  # fix it again
  $meta = $this->readTopic;
  $meta->save;
  $info = $meta->getRevisionInfo();
  $this->assert_equals(4, $info->{version});
}

sub test_unicode_topic {
  my $this = shift;

  my $meta = $this->createTopic("Frühstück");
  $this->assert_equals(undef, $meta->getLoadedRev());

  $meta->text("um zwölf");
  $meta->save;
  $meta->finish;

  my $file = Encode::encode_utf8($this->getPath("Frühstück"));
  $this->assert(-e $file);
}

sub test_each_topic {
  my $this = shift;

  my $webObj = Foswiki::Meta->new($this->{session}, $this->{test_web});

  my $it = $webObj->eachTopic();
  my @topics = $it->all();
  $this->assert_equals(2, scalar(@topics));

  $this->assert_matches(qr/\bWebPreferences\b/, join(",", @topics));

  my $topicObj = $this->createTopic("Frühstück");
  $topicObj->save();

  $it = $webObj->eachTopic();
  @topics = $it->all();
  $this->assert_equals(3, scalar(@topics));

  $it->reset;
  while ($it->hasNext) {
    my $topic = $it->next;
    #print STDERR "reading $topic\n";
    my $meta = $this->readTopic($topic);
    $this->assert($meta->existsInStore, "topic $topic not found in store");
    $meta->finish;
  }
}

sub test_webs {
  my $this = shift;

  my $webObj = $this->createWeb("SomeWeb");
  my $rootWeb = $this->readWeb;

  # eachWeb
  my $it = $rootWeb->eachWeb();
  my @webs = $it->all;
  $this->assert_equals(1, scalar(@webs));
  $this->assert_equals("SomeWeb", join(",", @webs));

  # webExists
  $this->assert($webObj->existsInStore());

  # move web
  my $newWebObj = $this->readWeb("OtherWeb");
  $this->assert(!$newWebObj->existsInStore());
  $this->assert($webObj->existsInStore());
  $webObj->move($newWebObj);
  $this->assert(!$webObj->existsInStore());
  $this->assert($newWebObj->existsInStore());

  # remove web
  $newWebObj->removeFromStore();
  $this->assert(!$newWebObj->existsInStore());
  $this->assert($rootWeb->existsInStore());
}

sub test_remove_topic {
  my $this = shift;

  my $meta = $this->createTopic;
  $this->assert_equals(undef, $meta->getLoadedRev());
  $this->assert(!$meta->existsInStore);

  my $file = $this->getPath;
  my $rcsFile = $file . ',v';

  $this->assert(!-e $file);
  $this->assert(!-e $rcsFile);

  $meta->save;
  $this->assert($meta->existsInStore);
  $this->assert(-e $file);
  $this->assert(!-e $rcsFile);

  $meta->save(forcenewrevision => 1);
  $this->assert(-e $rcsFile);

  $this->assert($meta->existsInStore);
  $meta->removeFromStore;

  $this->assert(!$meta->existsInStore);
  $this->assert(!-e $file, "topic file still exists at $file");
  $this->assert(!-e $rcsFile, "rcs file still exists at $rcsFile");
}

sub test_del_rev_topic {
  my $this = shift;

  my $meta = $this->createTopic;
  $meta->text("hello world 1");
  $meta->save;

  my $info = $meta->getRevisionInfo;
  $this->assert_equals(1, $info->{version});

  $meta->text("hello world 2");
  $meta->save(forcenewrevision => 1);

  $info = $meta->get("TOPICINFO");
  $this->assert_equals(2, $info->{version});

  $info = $meta->getRevisionInfo;
  $this->assert_equals(2, $info->{version});

  $meta->deleteMostRecentRevision;
  $info = $meta->getRevisionInfo;
  $this->assert_equals(1, $info->{version});

  $info = $meta->get("TOPICINFO");
  $this->assert_equals(1, $info->{version});
}

sub test_move_topic {
  my $this = shift;

  my $meta = $this->createTopic;
  $meta->save;
  $this->assert($meta->existsInStore);

  my $webObject = $this->readWeb;
  my $it = $webObject->eachTopic;
  my %topics = map {$_ => 1} $it->all();
  $this->assert($topics{"SomeTopic"});

  my $newMeta = $this->createTopic("SomeMoreTopic");
  $meta->move($newMeta);

  $this->assert(!$meta->existsInStore);
  $this->assert($newMeta->existsInStore);

  $it = $webObject->eachTopic;
  %topics = map {$_ => 1} $it->all();
  $this->assert(!$topics{"SomeTopic"});
  $this->assert($topics{"SomeMoreTopic"});
}

sub test_attachments {
  my $this = shift;

  my $meta = $this->saveAttachment("SomeTopic", "SomeAttachment.txt", <<HERE);
hello world
HERE

  $this->assert($meta->hasAttachment("SomeAttachment.txt"));

  my $info = $meta->get("FILEATTACHMENT", "SomeAttachment.txt");
  $this->assert($info);

  $info = $meta->getAttachmentRevisionInfo("SomeAttachment.txt");
  $this->assert($info);
  $this->assert_equals(1, $info->{version});

  my $file = $this->getPath("SomeTopic", "SomeAttachment.txt");
  $this->assert(-e $file);
  $this->assert($meta->testAttachment("SomeAttachment.txt", "r"));
  $this->assert($meta->testAttachment("SomeAttachment.txt", "w"));

  my $rcsFile = $file . ',v';
  $this->assert(!-e $rcsFile, "version 1 should not create an rcs file yet");

  my $text = <<HERE;
hello world 2
HERE
  $meta = $this->saveAttachment("SomeTopic", "SomeAttachment.txt", $text);
  $this->assert($meta->hasAttachment("SomeAttachment.txt"));

  $info = $meta->get("FILEATTACHMENT", "SomeAttachment.txt");
  $this->assert($info);
  $this->assert_equals(2, $info->{version});
  $info = $meta->getAttachmentRevisionInfo("SomeAttachment.txt");
  $this->assert($info);
  $this->assert_equals(2, $info->{version});

  $this->assert(-e $rcsFile, "version 2 should have an rcs file now");

  my $data = $this->readAttachment($meta, "SomeAttachment.txt");
  $this->assert_equals($text, $data);

  $meta->moveAttachment("SomeAttachment.txt", $meta, new_name => "SomeOtherAttachment.txt");
  $this->assert(!-e $file);
  $this->assert(!-e $rcsFile);

  $file = $this->getPath("SomeTopic", "SomeOtherAttachment.txt");
  $rcsFile = $file . ",v";
  $this->assert(-e $file);
  $this->assert(-e $rcsFile);

  $info = $meta->get("FILEATTACHMENT", "SomeAttachment.txt");
  $this->assert(!$info);
  $this->assert(!$meta->hasAttachment("SomeAttachment.txt"));

  $data = $this->readAttachment($meta, "SomeAttachment.txt");
  $this->assert(!$data);

  $data = $this->readAttachment($meta, "SomeOtherAttachment.txt");
  $this->assert_equals($text, $data);
}

sub test_copy_attachment {
  my $this = shift;

  my $meta = $this->saveAttachment("SomeTopic", "SomeAttachment.txt", <<HERE);
hello world
HERE
  my $file = $this->getPath("SomeTopic", "SomeAttachment.txt");
  my $rcsFile = $file . ',v';
  $this->assert(-e $file);
  $this->assert(!-e $rcsFile);

  my $info = $meta->get("FILEATTACHMENT", "SomeAttachment.txt");
  $this->assert($info);
  $this->assert_equals(1, $info->{version});

  $meta->copyAttachment("SomeAttachment.txt", $meta, new_name => "Copy.txt");

  $info = $meta->get("FILEATTACHMENT", "SomeAttachment.txt");
  $this->assert($info);

  $info = $meta->get("FILEATTACHMENT", "Copy.txt");
  $this->assert($info);
  $this->assert_equals(1, $info->{version});

  $this->assert(-e $file);
  $this->assert(!-e $rcsFile);

  $file = $this->getPath("SomeTopic", "Copy.txt");
  $rcsFile = $file . ',v';
  $this->assert(-e $file);
  $this->assert(!-e $rcsFile);

  $meta = $this->saveAttachment("SomeTopic", "SomeAttachment.txt", <<HERE);
hello world 1
HERE
  $info = $meta->get("FILEATTACHMENT", "SomeAttachment.txt");
  $this->assert($info);
  $this->assert_equals(2, $info->{version});

  $file = $this->getPath("SomeTopic", "SomeAttachment.txt");
  $rcsFile = $file . ',v';
  $this->assert(-e $file);
  $this->assert(-e $rcsFile);

  $meta->copyAttachment("SomeAttachment.txt", $meta, new_name => "Copy.txt");
  $file = $this->getPath("SomeTopic", "Copy.txt");
  $rcsFile = $file . ',v';
  $this->assert(-e $file);
  $this->assert(-e $rcsFile);

  $info = $meta->get("FILEATTACHMENT", "Copy.txt");
  $this->assert($info);
  $this->assert_equals(2, $info->{version});

  my $data = $this->readAttachment($meta, "Copy.txt");
  $this->assert_equals("hello world 1\n", $data);

  my $it = $meta->eachAttachment();
  my %list = map {$_=> 1} $it->all();
  $this->assert_equals(2, scalar(keys %list));
  $this->assert($list{"SomeAttachment.txt"});
  $this->assert($list{"Copy.txt"});
}

sub test_getRevisionAtTime {
  my $this = shift;

  my $meta = $this->createTopic;
  $meta->save;
  
  my $info1 = $meta->getRevisionInfo;

  $this->assert($info1);
  $this->assert($info1->{date});
  $this->assert($info1->{version});

  sleep 2;

  $meta->save(forcenewrevision => 1);
  my $info2 = $meta->getRevisionInfo;

  $this->assert($info2);
  $this->assert($info2->{date});
  $this->assert($info2->{version});
  $this->assert($info2->{version} > $info1->{version});
  $this->assert($info2->{date} > $info1->{date});
 
  my $rev = $meta->getRevisionAtTime($info1->{date});
  $this->assert_equals(1, $rev, "rev for date=$info1->{date} should be 1");

  $rev = $meta->getRevisionAtTime($info2->{date});
  $this->assert_equals(2, $rev, "rev for date=$info2->{date} should be 2");

  $rev = $meta->getRevisionAtTime($info1->{date} - 10);
  $this->assert(!$rev, "no revision before $info1->{date}");

  $rev = $meta->getRevisionAtTime($info2->{date} + 10);
  $this->assert_equals(2, $rev, "rev 2 after $info2->{date}");
}

sub test_eachChange {
  my $this = shift;

  my $webMeta = $this->readWeb;
  $this->assert($webMeta->existsInStore());

  my $it = $webMeta->eachChange();
  $this->assert($it);

  my @all = $it->all();
  $this->assert(scalar(@all) eq 2, "there should be two initial change logs");

  sleep 1;

  my $now = time();
  $it = $webMeta->eachChange($now);
  @all = $it->all();
  $this->assert(!scalar(@all), "there shouldn't be any additional changes");

  my $meta = $this->createTopic;
  $meta->save;
  $meta->finish;

  $it = $webMeta->eachChange($now);
  my $log = $it->next();

  @all = $it->all();
  $this->assert(!scalar(@all), "there should be exactly one change log");
  $this->assert_matches(qr/SomeTopic/, $log->{path}, "didn't find recent change");
}

1;
