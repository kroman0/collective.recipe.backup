Example usage
=============

Some needed imports:

    >>> from datetime import datetime
    >>> import os
    >>> import sys
    >>> import time

Just to isolate some test differences, we run an empty buildout once::

    >>> ignore = system(buildout)

The simplest way to use it is to add a part in ``buildout.cfg`` like this::

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... """)

Running the buildout adds a backup, snapshotbackup, restore and
snapshotrestore scripts to the ``bin/`` directory and, by default, it
creates the ``var/backups`` and ``var/snapshotbackups`` dirs::

    >>> print system(buildout)
    Installing backup.
    backup: Created /sample-buildout/var/backups
    backup: Created /sample-buildout/var/snapshotbackups
    Generated script '/sample-buildout/bin/backup'.
    Generated script '/sample-buildout/bin/snapshotbackup'.
    Generated script '/sample-buildout/bin/restore'.
    Generated script '/sample-buildout/bin/snapshotrestore'.
    <BLANKLINE>
    >>> ls('var')
    d  backups
    d  snapshotbackups
    >>> ls('bin')
    -  backup
    -  buildout
    -  restore
    -  snapshotbackup
    -  snapshotrestore

Backup
------

Calling ``bin/backup`` results in a normal repozo backup. We put in place a
mock repozo script that prints the options it is passed (and make it
executable). It is horridly unix-specific at the moment.

    >>> write('bin', 'repozo',
    ...       "#!%s\nimport sys\nprint ' '.join(sys.argv[1:])" % sys.executable)
    >>> dontcare = system('chmod u+x bin/repozo')

By default, backups are done in ``var/backups``::

    >>> print system('bin/backup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups --gzip
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/backups
    <BLANKLINE>


Restore
-------

You can restore the very latest backup with ``bin/restore``::

    >>> print system('bin/restore', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    Are you sure? (yes/No)?
    INFO: Please wait while restoring database file: /sample-buildout/var/backups to /sample-buildout/var/filestorage/Data.fs
    <BLANKLINE>

You can restore the very latest snapshotbackup with ``bin/snapshotrestore``::

    >>> print system('bin/snapshotrestore', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    Are you sure? (yes/No)?
    INFO: Please wait while restoring database file: /sample-buildout/var/snapshotbackups to /sample-buildout/var/filestorage/Data.fs

You can also restore the backup as of a certain date. Just pass a date
argument. According to repozo: specify UTC (not local) time.  The format is
``yyyy-mm-dd[-hh[-mm[-ss]]]``.

    >>> print system('bin/restore 1972-12-25', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups -D 1972-12-25
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    Are you sure? (yes/No)?
    INFO: Date restriction: restoring state at 1972-12-25.
    INFO: Please wait while restoring database file: /sample-buildout/var/backups to /sample-buildout/var/filestorage/Data.fs

Note that restoring a blobstorage to a specific date only works since
release 2.3.  We will test that a bit further on.


Snapshots
---------

For quickly grabbing the current state of a production database so you can
download it to your development laptop, you want a full backup. But
you shouldn't interfere with the regular backup regime. Likewise, a quick
backup just before updating the production server is a good idea. For that,
the ``bin/snapshotbackup`` is great. It places a full backup in, by default,
``var/snapshotbackups``.

    >>> print system('bin/snapshotbackup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups -F --gzip
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/snapshotbackups
    <BLANKLINE>


Names of created scripts
------------------------

A backup part will normally be called ``[backup]``, leading to a
``bin/backup`` and ``bin/snapshotbackup``.  Should you name your part
something else,  the script names will also be different as will the created
``var/`` directories (since version 1.2):

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = plonebackup
    ...
    ... [plonebackup]
    ... recipe = collective.recipe.backup
    ... """)
    >>> print system(buildout)
    Uninstalling backup.
    Installing plonebackup.
    backup: Created /sample-buildout/var/plonebackups
    backup: Created /sample-buildout/var/plonebackup-snapshots
    Generated script '/sample-buildout/bin/plonebackup'.
    Generated script '/sample-buildout/bin/plonebackup-snapshot'.
    Generated script '/sample-buildout/bin/plonebackup-restore'.
    Generated script '/sample-buildout/bin/plonebackup-snapshotrestore'.

Note that the ``restore``, ``snapshotbackup`` and ``snapshotrestore`` script name used when the
name is ``[backup]`` is now prefixed with the part name:

    >>> ls('bin')
    -  buildout
    -  plonebackup
    -  plonebackup-restore
    -  plonebackup-snapshot
    -  plonebackup-snapshotrestore
    -  repozo

In the var/ directory, the existing backups and snapshotbackups directories
are still present.  The recipe of course never removes that kind of directory!
The different part name *did* result in two directories named after the part:

    >>> ls('var')
    d  backups
    d  plonebackup-snapshots
    d  plonebackups
    d  snapshotbackups

For the rest of the tests we use the ``[backup]`` name again.  And we clean up
the ``var/plonebackups`` and ``var/plonebackup-snaphots`` dirs:

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... """)
    >>> dont_care = system(buildout)
    >>> rmdir('var/plonebackups')
    >>> rmdir('var/plonebackup-snapshots')


Supported options
=================

The recipe supports the following options, none of which are needed by
default. The most common one to change is ``location``, as that allows you to
place your backups in some system-wide directory like
``/var/zopebackups/instancename/``.

location
    Location where backups are stored. Defaults to ``var/backups`` inside the
    buildout directory.

keep
    Number of full backups to keep. Defaults to ``2``, which means that the
    current and the previous full backup are kept. Older backups are removed,
    including their incremental backups. Set it to ``0`` to keep all backups.

datafs
    In case the ``Data.fs`` isn't in the default ``var/filestorage/Data.fs``
    location, this option can overwrite it.

full
    By default, incremental backups are made. If this option is set to 'true',
    bin/backup will always make a full backup.

debug
    In rare cases when you want to know exactly what's going on, set debug to
    'true' to get debug level logging of the recipe itself. Repozo is also run
    with ``--verbose`` if this option is enabled.

snapshotlocation
    Location where snapshot defaults are stored. Defaults to
    ``var/snapshotbackups`` inside the buildout directory.

gzip
    Use repozo's zipping functionality. 'true' by default. Set it to 'false'
    and repozo will notgzip its files. Note that gzipped databases are called
    ``*.fsz``, not ``*.fs.gz``. **Changed in 0.8**: the default used to be
    false, but it so totally makes sense to gzip your backups that we changed
    the default.

additional_filestorages
    Advanced option, only needed when you have split for instance a
    ``catalog.fs`` out of the regular ``Data.fs``. Use it to specify the extra
    filestorages. (See explanation further on).

enable_snapshotrestore
    Having a snapshotrestore script is very useful in development
    environments, but can be harmful in a production buildout. The
    script restores the latest snapshot directly to your filestorage
    and it used to do this without asking any questions whatsoever
    (this has been changed to require an explicit 'yes' as answer).
    If you don't want a snapshotrestore, set this option to false.

blob_storage
    Location of the directory where the blobs (binary large objects)
    are stored.  This is used in Plone 4 and higher, or on Plone 3 if
    you use plone.app.blob.  This option is ignored if backup_blobs is
    false.  The location is not set by default.  When there is a part
    using plone.recipe.zope2instance, we check if that has a
    blob-storage option and use that as default.

blob-storage
    Alternative spelling for the preferred blob_storage, as
    plone.recipe.zope2instance spells it as blob-storage.  Pick one.

backup_blobs
    Backup the blob storage.  This requires the blob_storage location
    to be set.  If no blob_storage location has been set and we cannot
    find one by looking in a plone.recipe.zope2instance part, we
    default to False, otherwise to True.

only_blobs
    Only backup blob storage, not the Data.fs.  False by default.  May
    be a useful option if for example you want to create one
    bin/filestoragebackup script and one bin/blobstoragebackup script,
    using only_blobs in one and backup_blobs in the other.


We'll use all options, except the blob options for now::

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... location = ${buildout:directory}/myproject
    ... keep = 2
    ... datafs = subfolder/myproject.fs
    ... full = true
    ... debug = true
    ... snapshotlocation = snap/my
    ... gzip = false
    ... enable_snapshotrestore = true
    ... """)
    >>> print system(buildout)
    Uninstalling backup.
    Installing backup.
    backup: Created /sample-buildout/myproject
    backup: Created /sample-buildout/snap/my
    Generated script '/sample-buildout/bin/backup'.
    Generated script '/sample-buildout/bin/snapshotbackup'.
    Generated script '/sample-buildout/bin/restore'.
    Generated script '/sample-buildout/bin/snapshotrestore'.
    <BLANKLINE>

Backups are now stored in the ``/myproject`` folder inside buildout and the
Data.fs location is handled correctly despite not being an absolute path::

    >>> output = system('bin/backup')
    >>> print output
    --backup -f /sample-buildout/subfolder/myproject.fs -r /sample-buildout/myproject -F --verbose
    INFO: Please wait while backing up database file: /sample-buildout/subfolder/myproject.fs to /sample-buildout/myproject...

The '...' could filter out too many things, so we explicitly look for
errors here::

    >>> if 'ERROR' in output: print output

The same is true for the snapshot backup.

    >>> output = system('bin/snapshotbackup')
    >>> print output
    --backup -f /sample-buildout/subfolder/myproject.fs -r /sample-buildout/snap/my -F --verbose
    INFO: Please wait while making snapshot backup: /sample-buildout/subfolder/myproject.fs to /sample-buildout/snap/my
    >>> if 'ERROR' in output: print output

Untested in this file, as it would create directories in your root or your
home dir, are absolute links (starting with a '/') or directories in your home
dir or relative (``../``) path. They do work, of course. Also ``~`` and
``$BACKUP``-style environment variables are expanded.


Cron job integration
====================

``bin/backup`` is of course ideal to put in your cronjob instead of a whole
``bin/repozo ....`` line. But you don't want the "INFO" level logging that you
get, as you'll get that in your mailbox. In your cronjob, just add ``-q`` or
``--quiet`` and ``bin/backup`` will shut up unless there's a problem.

    >>> print system('bin/backup -q')
    --backup -f /sample-buildout/subfolder/myproject.fs -r /sample-buildout/myproject -F --verbose
    >>> print system('bin/backup --quiet')
    --backup -f /sample-buildout/subfolder/myproject.fs -r /sample-buildout/myproject -F --verbose

In our case the ``--backup ...`` lines above are just the mock repozo script
that still prints something. So it proves that the command is executed, but it
won't end up in the output.

Speaking of cron jobs?  Take a look at `zc.recipe.usercrontab
<http://pypi.python.org/pypi/z3c.recipe.usercrontab>`_ if you want to handle
cronjobs from within your buildout.  For example::

    [backupcronjob]
    recipe = z3c.recipe.usercrontab
    times = 0 12 * * *
    command = ${buildout:directory}/bin/backup


Advanced usage: multiple Data.fs files
======================================

Sometimes, a Data.fs is split into several files. Most common reason is to
have a regular Data.fs and a catalog.fs which contains the
portal_catalog. This is supported with the ``additional_filestorages``
option::

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... additional_filestorages =
    ...     catalog
    ...     another
    ... """)

The additional backups have to be stored separate from the ``Data.fs``
backup. That's done by appending the file's name and creating extra backup
directories named that way::

    >>> print system(buildout)
    Uninstalling backup.
    Installing backup.
    backup: Created /sample-buildout/var/backups_catalog
    backup: Created /sample-buildout/var/snapshotbackups_catalog
    backup: Created /sample-buildout/var/backups_another
    backup: Created /sample-buildout/var/snapshotbackups_another
    Generated script '/sample-buildout/bin/backup'.
    Generated script '/sample-buildout/bin/snapshotbackup'.
    Generated script '/sample-buildout/bin/restore'.
    Generated script '/sample-buildout/bin/snapshotrestore'.
    <BLANKLINE>
    >>> ls('var')
    d  backups
    d  backups_another
    d  backups_catalog
    d  snapshotbackups
    d  snapshotbackups_another
    d  snapshotbackups_catalog

The various backups are done one after the other. They cannot be done at the
same time with repozo. So they are not completely in sync. The "other"
databases are backed up first as a small difference in the catalog is just
mildly irritating, but the other way around users can get real errors::

    >>> print system('bin/backup')
    --backup -f /sample-buildout/var/filestorage/catalog.fs -r /sample-buildout/var/backups_catalog --gzip
    --backup -f /sample-buildout/var/filestorage/another.fs -r /sample-buildout/var/backups_another --gzip
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups --gzip
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/catalog.fs to /sample-buildout/var/backups_catalog
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/another.fs to /sample-buildout/var/backups_another
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/backups
    <BLANKLINE>

Same with snapshot backups::

    >>> print system('bin/snapshotbackup')
    --backup -f /sample-buildout/var/filestorage/catalog.fs -r /sample-buildout/var/snapshotbackups_catalog -F --gzip
    --backup -f /sample-buildout/var/filestorage/another.fs -r /sample-buildout/var/snapshotbackups_another -F --gzip
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups -F --gzip
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/catalog.fs to /sample-buildout/var/snapshotbackups_catalog
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/another.fs to /sample-buildout/var/snapshotbackups_another
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/snapshotbackups
    <BLANKLINE>

And a restore restores all three backups::

    >>> print system('bin/restore', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/catalog.fs -r /sample-buildout/var/backups_catalog
    --recover -o /sample-buildout/var/filestorage/another.fs -r /sample-buildout/var/backups_another
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    Are you sure? (yes/No)?
    INFO: Please wait while restoring database file: /sample-buildout/var/backups_catalog to /sample-buildout/var/filestorage/catalog.fs
    INFO: Please wait while restoring database file: /sample-buildout/var/backups_another to /sample-buildout/var/filestorage/another.fs
    INFO: Please wait while restoring database file: /sample-buildout/var/backups to /sample-buildout/var/filestorage/Data.fs
    <BLANKLINE>

We fake three old backups in all the (snapshot)backup directories to
test if the 'keep' parameter is working correctly.

    >>> def getblobs(dir):
    ...     return [os.path.join(dir, i) for i in sorted(os.listdir(dir)) if "blobs" in i]
    >>> next_mod_time = time.time() - 1000
    >>> def add_backup(dir, name):  # same as in the tests in repozorunner.py
    ...     global next_mod_time
    ...     write(dir, name, 'sample fs')
    ...     # Change modification time, every new file is 10 seconds older.
    ...     os.utime(join(dir, name), (next_mod_time, next_mod_time))
    ...     next_mod_time += 10
    >>> dirs = ('var/backups', 'var/snapshotbackups')
    >>> for tail in ('', '_catalog', '_another'):
    ...     for dir in dirs:
    ...         dir = dir + tail
    ...         for i in reversed(range(3)):
    ...             add_backup(dir, '%d.fs' % i)
    >>> ls('var/backups')  # Before
    -  0.fs
    -  1.fs
    -  2.fs
    >>> print system('bin/backup')
    --backup -f /sample-buildout/var/filestorage/catalog.fs -r /sample-buildout/var/backups_catalog --gzip
    --backup -f /sample-buildout/var/filestorage/another.fs -r /sample-buildout/var/backups_another --gzip
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups --gzip
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/catalog.fs to /sample-buildout/var/backups_catalog
    INFO: Removed 1 file(s) belonging to old backups, the latest 2 full backups have been kept.
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/another.fs to /sample-buildout/var/backups_another
    INFO: Removed 1 file(s) belonging to old backups, the latest 2 full backups have been kept.
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/backups
    INFO: Removed 1 file(s) belonging to old backups, the latest 2 full backups have been kept.
    <BLANKLINE>
    >>> ls('var/backups')  # After
    -  0.fs
    -  1.fs

Same for the snapshot backups:

    >>> print system('bin/snapshotbackup')
    --backup -f /sample-buildout/var/filestorage/catalog.fs -r /sample-buildout/var/snapshotbackups_catalog -F --gzip
    --backup -f /sample-buildout/var/filestorage/another.fs -r /sample-buildout/var/snapshotbackups_another -F --gzip
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups -F --gzip
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/catalog.fs to /sample-buildout/var/snapshotbackups_catalog
    INFO: Removed 1 file(s) belonging to old backups, the latest 2 full backups have been kept.
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/another.fs to /sample-buildout/var/snapshotbackups_another
    INFO: Removed 1 file(s) belonging to old backups, the latest 2 full backups have been kept.
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/snapshotbackups
    INFO: Removed 1 file(s) belonging to old backups, the latest 2 full backups have been kept.
    <BLANKLINE>

Test disabling the snapshotrestore script.  We generate a new buildout
with enable_snapshotrestore set to false. The script should not be
generated now (and buildout will actually remove the previously
generated script).
    
    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... enable_snapshotrestore = false
    ... """)

    >>> print system(buildout)
    Uninstalling backup.
    Installing backup.
    Generated script '/sample-buildout/bin/backup'.
    Generated script '/sample-buildout/bin/snapshotbackup'.
    Generated script '/sample-buildout/bin/restore'.
    <BLANKLINE>
    >>> ls('bin')
    -  backup
    -  buildout
    -  repozo
    -  restore
    -  snapshotbackup


Blob storage
------------

New in this recipe is that we backup the blob storage.  Plone 4 uses a
blob storage to store files on the file system.  In Plone 3 this is
optional.  When this is used, it should be backed up of course.  You
must specify the source blob_storage directory where Plone (or Zope)
stores its blobs.  When we do not set it specifically, we try to get
the location from the plone.recipe.zope2instance recipe (or a
zeoserver recipe) when it is used::

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... # For some reason this is now needed:
    ... index = http://b.pypi.python.org/simple
    ... # Avoid suddenly updating zc.buildout or other packages:
    ... newest = false
    ... parts = instance backup
    ... versions = versions
    ...
    ... [versions]
    ... # A slightly older version that does not rely on the Zope2 egg
    ... plone.recipe.zope2instance = 3.9
    ... mailinglogger = 3.3
    ... 
    ... [instance]
    ... recipe = plone.recipe.zope2instance
    ... user = admin:admin
    ... blob-storage = ${buildout:directory}/var/somewhere
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... """)

We need a mock mkzopeinstance script in the bin directory for the
zope2instance recipe to work:

    >>> write('bin/mkzopeinstance', """
    ... import sys
    ... import os
    ... path = sys.argv[2]
    ... os.mkdir(path)
    ... os.mkdir(os.path.join(path, 'etc'))
    ... """)

We run the buildout (and set a timeout as we need a few new packages
and apparently a few servers are currently down so a timeout helps
speed things up a bit):

    >>> print system('bin/buildout -t 5')
    Setting socket time out to 5 seconds
    Getting distribution for 'plone.recipe.zope2instance==3.9'...
    Got plone.recipe.zope2instance 3.9.
    Getting distribution for 'mailinglogger==3.3'...
    Got mailinglogger 3.3.0.
    Uninstalling backup.
    Installing instance.
    Generated script '/sample-buildout/bin/instance'.
    Installing backup.
    Generated script '/sample-buildout/bin/repobo'.
    Generated script '/sample-buildout/bin/backup'.
    Generated script '/sample-buildout/bin/snapshotbackup'.
    Generated script '/sample-buildout/bin/restore'.
    Generated script '/sample-buildout/bin/snapshotrestore'.
    <BLANKLINE>
    >>> ls('bin')
    -  backup
    -  buildout
    -  instance
    -  mkzopeinstance
    -  repobo
    -  repozo
    -  restore
    -  snapshotbackup
    -  snapshotrestore

We can override the blob source location if needed:

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... blob_storage = ${buildout:directory}/var/blobstorage
    ... keep = 3
    ... """)
    >>> print system(buildout)
    Uninstalling backup.
    Uninstalling instance.
    Installing backup.
    Generated script '/sample-buildout/bin/repobo'.
    Generated script '/sample-buildout/bin/backup'.
    Generated script '/sample-buildout/bin/snapshotbackup'.
    Generated script '/sample-buildout/bin/restore'.
    Generated script '/sample-buildout/bin/snapshotrestore'.
    <BLANKLINE>
    >>> ls('bin')
    -  backup
    -  buildout
    -  instance
    -  mkzopeinstance
    -  repobo
    -  repozo
    -  restore
    -  snapshotbackup
    -  snapshotrestore
    >>> mkdir('var/blobstorage')
    >>> write('var', 'blobstorage', 'blob1.txt', "Sample blob 1.")


Test the snapshotbackup first, as that should be easiest.

    >>> print system('bin/snapshotbackup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups -F --gzip
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/snapshotbackups
    INFO: Please wait while making snapshot of blobs: /sample-buildout/var/blobstorage to /sample-buildout/var/snapshotbackups
    <BLANKLINE>
    >>> ls('var/snapshotbackups')
    -  0.fs
    -  1.fs
    ...

Let's try that some more, with a second in between so we can more
easily test restoring to a specific time later.

    >>> time.sleep(1)
    >>> write('var', 'blobstorage', 'blob2.txt', "Sample blob 2.")
    >>> print system('bin/snapshotbackup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups -F --gzip
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/snapshotbackups
    INFO: Please wait while making snapshot of blobs: /sample-buildout/var/blobstorage to /sample-buildout/var/snapshotbackups
    <BLANKLINE>
    >>> ls('var/snapshotbackups')
    -  0.fs
    -  1.fs
    -  ...
    -  ...

Now remove an item:

    >>> time.sleep(1)
    >>> remove('var', 'blobstorage', 'blob2.txt')
    >>> print system('bin/snapshotbackup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups -F --gzip
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/snapshotbackups
    INFO: Please wait while making snapshot of blobs: /sample-buildout/var/blobstorage to /sample-buildout/var/snapshotbackups
    <BLANKLINE>
    >>> ls('var/snapshotbackups')
    -  0.fs
    -  1.fs
    -  ...
    -  ...
    -  ...

Let's see how a bin/backup goes:

    >>> print system('bin/backup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups --gzip
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/backups
    INFO: Please wait while backing up blobs: /sample-buildout/var/blobstorage to /sample-buildout/var/backups
    <BLANKLINE>
    >>> ls('var/backups')
    -  0.fs
    -  1.fs
    -  ...

We try again with an extra 'blob':

    >>> time.sleep(1)
    >>> write('var', 'blobstorage', 'blob2.txt', "Sample blob 2.")
    >>> print system('bin/backup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups --gzip
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/backups
    INFO: Please wait while backing up blobs: /sample-buildout/var/blobstorage to /sample-buildout/var/backups
    <BLANKLINE>
    >>> ls('var/backups')
    -  0.fs
    -  1.fs
    -  ...
    -  ...

Now try a restore::

    >>> print system('bin/restore', input='no\n')
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    This will replace the blobstorage.
    Are you sure? (yes/No)?
    INFO: Not restoring.
    <BLANKLINE>
    >>> print system('bin/restore', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    This will replace the blobstorage.
    Are you sure? (yes/No)? INFO: Please wait while restoring database file: /sample-buildout/var/backups to /sample-buildout/var/filestorage/Data.fs
    INFO: Please wait while restoring blobs: /sample-buildout/var/backups to /sample-buildout/var/blobstorage
    <BLANKLINE>
    >>> ls('var/blobstorage')
    -  blob1.txt
    -  blob2.txt

Since release 2.3 we can also restore blobs to a specific date/time.

    >>> mod_time_0 = os.path.getmtime(getblobs('var/backups')[0])
    >>> mod_time_1 = os.path.getmtime(getblobs('var/backups')[1])
    >>> mod_time_0 < mod_time_1
    True
    >>> time_string = '-'.join([str(t) for t in datetime.utcfromtimestamp(mod_time_1).timetuple()[:6]])
    >>> print system('bin/restore %s' % time_string, input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups -D ...
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    This will replace the blobstorage.
    Are you sure? (yes/No)? INFO: Date restriction: restoring state at ...
    INFO: Please wait while restoring database file: /sample-buildout/var/backups to /sample-buildout/var/filestorage/Data.fs
    INFO: Please wait while restoring blobs: /sample-buildout/var/backups to /sample-buildout/var/blobstorage
    <BLANKLINE>

The second blob file is now no longer in the blob storage.

    >>> ls('var/blobstorage')
    -  blob1.txt
    -  blob2.txt

The snapshotrestore works too::

    >>> print system('bin/snapshotrestore', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    This will replace the blobstorage.
    Are you sure? (yes/No)? INFO: Please wait while restoring database file: /sample-buildout/var/snapshotbackups to /sample-buildout/var/filestorage/Data.fs
    INFO: Please wait while restoring blobs: /sample-buildout/var/snapshotbackups to /sample-buildout/var/blobstorage
    <BLANKLINE>

Check that this fits what is in the most recent snapshot::

    >>> ls('var/blobstorage')
    -  blob1.txt
    >>> ls('var/snapshotbackups')
    -  0.fs
    -  1.fs
    -  ...
    -  ...
    -  ...

Since release 2.3 we can also restore blob snapshots to a specific date/time.

    >>> mod_time_0 = os.path.getmtime(getblobs('var/snapshotbackups')[0])
    >>> mod_time_1 = os.path.getmtime(getblobs('var/snapshotbackups')[1])
    >>> mod_time_2 = os.path.getmtime(getblobs('var/snapshotbackups')[2])
    >>> mod_time_0 < mod_time_1
    True
    >>> mod_time_1 < mod_time_2
    True
    >>> time_string = '-'.join([str(t) for t in datetime.utcfromtimestamp(mod_time_1).timetuple()[:6]])
    >>> print system('bin/snapshotrestore %s' % time_string, input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/snapshotbackups -D ...
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    This will replace the blobstorage.
    Are you sure? (yes/No)? INFO: Date restriction: restoring state at ...
    INFO: Please wait while restoring database file: /sample-buildout/var/snapshotbackups to /sample-buildout/var/filestorage/Data.fs
    INFO: Please wait while restoring blobs: /sample-buildout/var/snapshotbackups to /sample-buildout/var/blobstorage
    <BLANKLINE>

The second blob file was only in blobstorage snapshot number 1 when we
started and now it is also in the main blobstorage again.

    >>> ls('var/blobstorage')
    -  blob1.txt

When repozo cannot find a Data.fs backup with files from before the
given date string it will quit with an error.  We should not restore
the blobs then either.  We test that with a special bin/repozo
script that simply quits::

    >>> write('bin', 'repozo', "#!%s\nimport sys\nsys.exit(1)" % sys.executable)
    >>> dontcare = system('chmod u+x bin/repozo')
    >>> print system('bin/snapshotrestore 1972-12-25', input='yes\n')
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    This will replace the blobstorage.
    Are you sure? (yes/No)?
    INFO: Date restriction: restoring state at 1972-12-25.
    INFO: Please wait while restoring database file: /sample-buildout/var/snapshotbackups to /sample-buildout/var/filestorage/Data.fs
    ERROR: Repozo command failed. See message above.
    ERROR: Halting execution due to error; not restoring blobs.
    <BLANKLINE>

Restore the original bin/repozo::

    >>> write('bin', 'repozo',
    ...       "#!%s\nimport sys\nprint ' '.join(sys.argv[1:])" % sys.executable)
    >>> dontcare = system('chmod u+x bin/repozo')


We can tell buildout that we only want to backup blobs or specifically
do not want to backup the blobs.

When we explicitly set backup_blobs to true, we must have a
blob_storage option, otherwise buildout quits::

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... backup_blobs = true
    ... """)
    >>> print system(buildout)
    Uninstalling backup.
    Installing backup.
    While:
      Installing backup.
    Error: backup_blobs is true, but no blob_storage could be found.
    <BLANKLINE>

Combining blob_backup=false and only_blobs=true will not work::

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = backup
    ...
    ... [backup]
    ... recipe = collective.recipe.backup
    ... blob_storage = ${buildout:directory}/var/blobstorage
    ... backup_blobs = false
    ... only_blobs = true
    ... """)
    >>> print system(buildout)
    Installing backup.
    While:
      Installing backup.
    Error: Cannot have backup_blobs false and only_blobs true.
    <BLANKLINE>

Specifying backup_blobs and only_blobs might be useful in case you
want to separate this into several scripts::

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... newest = false
    ... parts = filebackup blobbackup
    ...
    ... [filebackup]
    ... recipe = collective.recipe.backup
    ... blob_storage = ${buildout:directory}/var/blobstorage
    ... backup_blobs = false
    ... 
    ... [blobbackup]
    ... recipe = collective.recipe.backup
    ... blob_storage = ${buildout:directory}/var/blobstorage
    ... only_blobs = true
    ... """)
    >>> print system(buildout)
    Installing filebackup.
    backup: Created /sample-buildout/var/filebackups
    backup: Created /sample-buildout/var/filebackup-snapshots
    Generated script '/sample-buildout/bin/filebackup'.
    Generated script '/sample-buildout/bin/filebackup-snapshot'.
    Generated script '/sample-buildout/bin/filebackup-restore'.
    Generated script '/sample-buildout/bin/filebackup-snapshotrestore'.
    Installing blobbackup.
    backup: Created /sample-buildout/var/blobbackups
    backup: Created /sample-buildout/var/blobbackup-snapshots
    Generated script '/sample-buildout/bin/repobo'.
    Generated script '/sample-buildout/bin/blobbackup'.
    Generated script '/sample-buildout/bin/blobbackup-snapshot'.
    Generated script '/sample-buildout/bin/blobbackup-restore'.
    Generated script '/sample-buildout/bin/blobbackup-snapshotrestore'.
    <BLANKLINE>

Now we test it.  First the backup.  The filebackup now only backs up
the filestorage::

    >>> print system('bin/filebackup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/filebackups --gzip
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/filebackups
    <BLANKLINE>
    >>> print system('bin/filebackup-snapshot')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/filebackup-snapshots -F --gzip
    INFO: Please wait while making snapshot backup: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/filebackup-snapshots
    <BLANKLINE>

And blobbackup only backs up the blobstorage::

    >>> print system('bin/blobbackup')
    INFO: Please wait while backing up blobs: /sample-buildout/var/blobstorage to /sample-buildout/var/blobbackups
    <BLANKLINE>
    >>> print system('bin/blobbackup-snapshot')
    INFO: Please wait while making snapshot of blobs: /sample-buildout/var/blobstorage to /sample-buildout/var/blobbackup-snapshots
    <BLANKLINE>

Now test the restore::

    >>> print system('bin/filebackup-restore', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/filebackups
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    Are you sure? (yes/No)?
    INFO: Please wait while restoring database file: /sample-buildout/var/filebackups to /sample-buildout/var/filestorage/Data.fs
    <BLANKLINE>
    >>> print system('bin/filebackup-snapshotrestore', input='yes\n')
    --recover -o /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/filebackup-snapshots
    <BLANKLINE>
    This will replace the filestorage (Data.fs).
    Are you sure? (yes/No)?
    INFO: Please wait while restoring database file: /sample-buildout/var/filebackup-snapshots to /sample-buildout/var/filestorage/Data.fs
    <BLANKLINE>
    >>> print system('bin/blobbackup-restore', input='yes\n')
    <BLANKLINE>
    This will replace the blobstorage.
    Are you sure? (yes/No)? INFO: Please wait while restoring blobs: /sample-buildout/var/blobbackups to /sample-buildout/var/blobstorage
    <BLANKLINE>
    >>> print system('bin/blobbackup-snapshotrestore', input='yes\n')
    <BLANKLINE>
    This will replace the blobstorage.
    Are you sure? (yes/No)? INFO: Please wait while restoring blobs: /sample-buildout/var/blobbackup-snapshots to /sample-buildout/var/blobstorage
    <BLANKLINE>


zc.buildout 1.5
---------------

Script generation in zc.buildout 1.5 could give problems.  For the
moment, zc.buildout 1.4.x is used above.  We will try 1.5 below.

    >>> write('buildout.cfg',
    ... """
    ... [buildout]
    ... index = http://b.pypi.python.org/simple
    ... # allow updating to newer versions:
    ... newest = true
    ... parts = backup
    ... versions = versions
    ...
    ... [versions]
    ... zc.buildout = 1.5.2
    ... zc.recipe.egg = 1.3.2
    ... 
    ... [backup]
    ... recipe = collective.recipe.backup
    ... """)
    >>> print system('bin/buildout -t 5')
    Setting socket time out to 5 seconds
    ...
    Upgraded:
    ...
    restarting.
    Generated script '/sample-buildout/bin/buildout'.
    Setting socket time out to 5 seconds
    ...
    Installing backup.
    Generated script '/sample-buildout/bin/backup'.
    ...
    <BLANKLINE>


Now, the most important thing about this test is that a bin/backup
call does not give a python exception due to the script being wrongly
configured::

    >>> print system('bin/backup')
    --backup -f /sample-buildout/var/filestorage/Data.fs -r /sample-buildout/var/backups --gzip
    INFO: Please wait while backing up database file: /sample-buildout/var/filestorage/Data.fs to /sample-buildout/var/backups
    <BLANKLINE>
