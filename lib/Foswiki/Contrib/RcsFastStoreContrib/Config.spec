# ---+ Extensions
# ---++ RcsFastStoreContrib

# **STRING 20 CHECK="undefok emptyok" LABEL="Extension"**
# Specifies the extension to use on RCS files. Set to -x,v on Windows, leave
# blank on other platforms.
$Foswiki::cfg{RcsFast}{ExtOption} = '';

# **COMMAND LABEL="Break Lock Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast break a file lock.
$Foswiki::cfg{RcsFast}{breaklockCmd} = '/usr/bin/rcs $Foswiki::cfg{RcsFast}{ExtOption} -u -M %FILENAME|F%';

# **COMMAND LABEL="Ci Command" EXPERT CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast check-in.
# %USERNAME|S% will be expanded to the username.
# %COMMENT|U% will be expanded to the comment.
$Foswiki::cfg{RcsFast}{ciCmd} = '/usr/bin/ci $Foswiki::cfg{RcsFast}{ExtOption} -m%COMMENT|U% -t-none -w%USERNAME|S% -u %FILENAME|F%';

# **COMMAND LABEL="Ci Date Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast check in, forcing the date.
# %DATE|D% will be expanded to the date.
$Foswiki::cfg{RcsFast}{ciDateCmd} = '/usr/bin/ci $Foswiki::cfg{RcsFast}{ExtOption} -m%COMMENT|U% -t-none -d%DATE|D% -u -w%USERNAME|S% %FILENAME|F%';

# **COMMAND LABEL="Co Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast check out.
# %REVISION|N% will be expanded to the revision number
$Foswiki::cfg{RcsFast}{coCmd} = 'co $Foswiki::cfg{RcsFast}{ExtOption} -p%REVISION|N% -ko %FILENAME|F%';

# **COMMAND LABEL="Delete Revision Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast delete a specific revision.
$Foswiki::cfg{RcsFast}{delRevCmd} = '/usr/bin/rcs $Foswiki::cfg{RcsFast}{ExtOption} -o%REVISION|N% %FILENAME|F%';

# **COMMAND LABEL="History Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast file history.
$Foswiki::cfg{RcsFast}{histCmd} = '/usr/bin/rlog $Foswiki::cfg{RcsFast}{ExtOption} -h %FILENAME|F%';

# **COMMAND LABEL="Info Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast revision info about the file.
$Foswiki::cfg{RcsFast}{infoCmd} = '/usr/bin/rlog $Foswiki::cfg{RcsFast}{ExtOption} -r%REVISION|N% %FILENAME|F%';

# **COMMAND LABEL="Lock Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast lock a file.
$Foswiki::cfg{RcsFast}{lockCmd} = '/usr/bin/rcs $Foswiki::cfg{RcsFast}{ExtOption} -l %FILENAME|F%';

# **COMMAND LABEL="Unlock Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast unlock a file.
$Foswiki::cfg{RcsFast}{unlockCmd} = '/usr/bin/rcs $Foswiki::cfg{RcsFast}{ExtOption} -u %FILENAME|F%';

# **COMMAND LABEL="Info Date Command" CHECK_ON_CHANGE="{Store}{Implementation}" DISPLAY_IF {Store}{Implementation}=='Foswiki::Store::RcsFast'**
# RcsFast revision info about the revision that existed at a given date.
# %REVISIONn|N% will be expanded to the revision number.
# %CONTEXT|N% will be expanded to the number of lines of context.
$Foswiki::cfg{RcsFast}{rlogDateCmd} = '/usr/bin/rlog $Foswiki::cfg{RcsFast}{ExtOption} -d%DATE|D% %FILENAME|F%';

1;
