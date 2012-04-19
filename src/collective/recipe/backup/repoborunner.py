"""Wrapper that invokes repobo.

There are three main methods, these get called by the generated scripts. So
backup_main() for bin/backup, snapshot_main() for bin/snapshotbackup and
restore_main() for bin/restore.

backup_arguments() and restore_arguments() determine the arguments that are to
be passed to bin/repobo.

cleanup() empties old backups from the backup directory to prevent it from
filling up the harddisk.

"""
from operator import itemgetter
import logging
import os
import sys

logger = logging.getLogger('repoborunner')


def quote_command(command):
    # Quote the program name, so it works even if it contains spaces
    command = " ".join(['"%s"' % x for x in command])
    if sys.platform[:3].lower() == 'win':
        # odd, but true: the windows cmd processor can't handle more than
        # one quoted item per string unless you add quotes around the
        # whole line.
        command = '"%s"' % command
    return command


def backup_main(bin_dir, blobs, backup_location, keep, full, verbose, gzip):
    """Main method, gets called by generated bin/backup."""
    repobo = os.path.join(bin_dir, 'repobo')

    logger.info("Please wait while backing up blobs: %s to %s",
                blobs, backup_location)
    result = os.system(quote_command([repobo] +
                            backup_arguments(blobs,
                                             backup_location, full,
                                             verbose, gzip,
                                             as_list=True)))
    logger.debug("Repobo command executed.")
    if result:
        logger.error("Repobo command failed. See message above.")
        return result
    cleanup(backup_location, keep)


def snapshot_main(bin_dir, blobs, snapshot_location, keep, verbose, gzip):
    """Main method, gets called by generated bin/snapshotbackup."""
    repobo = os.path.join(bin_dir, 'repobo')

    logger.info("Please wait while making snapshot backup: %s to %s",
                blobs, snapshot_location)
    result = os.system(quote_command([repobo] +
                            backup_arguments(blobs, snapshot_location,
                                             full=True, verbose=verbose,
                                             gzip=gzip, as_list=True)))
    logger.debug("Repobo command executed.")
    if result:
        logger.error("Repobo command failed. See message above.")
        return result
    cleanup(snapshot_location, keep)


def restore_main(bin_dir, blobs, backup_location, verbose, date=None):
    """Main method, gets called by generated bin/restore."""
    repobo = os.path.join(bin_dir, 'repobo')
    logger.debug("If things break: did you stop zope?")

    logger.info("Please wait while restoring blobs: %s to %s",
                backup_location, blobs)
    result = os.system(quote_command([repobo] +
                            restore_arguments(blobs, backup_location,
                                              date, verbose, as_list=True)))
    logger.debug("Repobo command executed.")
    if result:
        logger.error("Repobo command failed. See message above.")
    return result


def backup_arguments(blobs=None,
                     backup_location=None,
                     full=False,
                     verbose=False,
                     gzip=False,
                     as_list=False):
    """
      >>> backup_arguments()
      Traceback (most recent call last):
      ...
      RuntimeError: Missing locations.
      >>> backup_arguments(blobs='in', backup_location='out')
      '--backup - in -r out'
      >>> backup_arguments(blobs='in', backup_location='out',
      ...                  full=True)
      '--backup -b in -r out -F'

    """
    if blobs is None or backup_location is None:
        raise RuntimeError("Missing locations.")
    arguments = []
    arguments.append('--backup')
    arguments.append('-b')
    arguments.append(blobs)
    arguments.append('-r')
    arguments.append(backup_location)
    if full:
        # By default, there's an incremental backup, if possible.
        arguments.append('-F')
    else:
        logger.debug("You're not making a full backup. Note that if there "
                     "are no changes since the last backup, there won't "
                     "be a new incremental backup file.")
    if verbose:
        arguments.append('--verbose')
    if gzip:
        arguments.append('--gzip')

    logger.debug("Repobo arguments used: %s", ' '.join(arguments))
    if as_list:
        return arguments
    return ' '.join(arguments)


def restore_arguments(blobs=None,
                      backup_location=None,
                      date=None,
                      verbose=False,
                      as_list=False):
    """
      >>> restore_arguments()
      Traceback (most recent call last):
      ...
      RuntimeError: Missing locations.
      >>> restore_arguments(blobs='in', backup_location='out')
      '--recover -o in -r out'

    """
    if blobs is None or backup_location is None:
        raise RuntimeError("Missing locations.")
    arguments = []
    arguments.append('--recover')
    arguments.append('-o')
    arguments.append(blobs)
    arguments.append('-r')
    arguments.append(backup_location)

    if date is not None:
        logger.debug("Restore as of date %r requested.", date)
        arguments.append('-D')
        arguments.append(date)
    if verbose:
        arguments.append('--verbose')
    if as_list:
        return arguments
    logger.debug("Repobo arguments used: %s", ' '.join(arguments))
    if as_list:
        return arguments
    return ' '.join(arguments)


def cleanup(backup_location, keep=0):
    """Clean up old backups

    For the test, we create a backup dir using buildout's test support methods:

      >>> backup_dir = 'back'
      >>> mkdir(backup_dir)

    And we'll make a function that creates a backup file for us and that also
    sets the file modification dates to a meaningful time.

      >>> import time
      >>> import os
      >>> next_mod_time = time.time() - 1000
      >>> def add_backup(name):
      ...     global next_mod_time
      ...     write(backup_dir, name, 'dummycontents')
      ...     # Change modification time, every new file is 10 seconds older.
      ...     os.utime(join(backup_dir, name), (next_mod_time, next_mod_time))
      ...     next_mod_time += 10

    Calling 'cleanup' without a keep arguments will just return without doing
    anything.

      >>> cleanup(backup_dir)

    Cleaning an empty directory won't do a thing.

      >>> cleanup(backup_dir, keep=1)

    Adding one backup file and cleaning the directory won't remove it either:

      >>> add_backup('1.blobs')
      >>> cleanup(backup_dir, keep=1)
      >>> ls(backup_dir)
      - 1.blobs

    Adding a second backup file means the first one gets removed.

      >>> add_backup('2.blobs')
      >>> cleanup(backup_dir, keep=1)
      >>> ls(backup_dir)
      - 2.blobs

    If there are more than one file to remove, the results are OK, too:

      >>> add_backup('3.blobs')
      >>> add_backup('4.blobs')
      >>> add_backup('5.blobs')
      >>> cleanup(backup_dir, keep=1)
      >>> ls(backup_dir)
      - 5.blobs

    Every other file older than the last full backup that is kept is deleted,
    too. This includes deltas for incremental backups and '.dat' files. Deltas
    and other files added after the last full retained backup are always kept.

      >>> add_backup('5-something.deltablobs')
      >>> add_backup('5.dat')
      >>> add_backup('6.blobs')
      >>> add_backup('6-something.deltablobs')
      >>> cleanup(backup_dir, keep=1)
      >>> ls(backup_dir)
      - 6-something.deltablobs
      - 6.blobs

    Keeping more than one file is also supported.

      >>> add_backup('7.blobs')
      >>> add_backup('7-something.deltablobs')
      >>> add_backup('7.dat')
      >>> add_backup('8.blobs')
      >>> add_backup('8-something.deltablobs')
      >>> add_backup('8.dat')
      >>> add_backup('9.blobs')
      >>> add_backup('9-something.deltablobs')
      >>> add_backup('9.dat')
      >>> cleanup(backup_dir, keep=2)
      >>> ls(backup_dir)
      -  8-something.deltablobs
      -  8.blobs
      -  9-something.deltablobs
      -  9.blobs

    Keep = 0 doesn't delete anything.

      >>> cleanup(backup_dir, keep=0)
      >>> ls(backup_dir)
      -  8-something.deltablobs
      -  8.blobs
      -  9-something.deltablobs
      -  9.blobs

    Back to keep=2, we test that .blobs files (made with repobo's ``--gzip``
    option) are also treated as full backups.

      >>> add_backup('10.blobs')
      >>> cleanup(backup_dir, keep=2)
      >>> ls(backup_dir)
      -  10.blobs
      -  9-something.deltablobs
      -  9.blobs

      >>> remove(backup_dir)

    """
    keep = int(keep)  # Making sure.
    if not keep:
        logger.debug(
            "Value of 'keep' is %r, we don't want to remove anything.", keep)
        return
    logger.debug("Trying to clean up old backups.")
    filenames = os.listdir(backup_location)
    logger.debug("Looked up filenames in the target dir: %s found. %r.",
              len(filenames), filenames)
    num_backups = int(keep)
    logger.debug("Max number of backups: %s.", num_backups)
    files_modtimes = []
    for filename in filenames:
        mod_time = os.path.getmtime(os.path.join(backup_location, filename))
        file_ = (filename, mod_time)
        files_modtimes.append(file_)
    # we are only interested in full backups
    fullbackups = [f for f in files_modtimes
                   if f[0].endswith('.blobs')]
    logger.debug("Filtered out full backups (*.blobs): %r.",
              [f[0] for f in fullbackups])
    fullbackups = sorted(fullbackups, key=itemgetter(1))
    logger.debug("%d fullbackups: %r", len(fullbackups), fullbackups)
    if len(fullbackups) > num_backups and num_backups != 0:
        logger.debug("There are older backups that we can remove.")
        fullbackups.reverse()
        logger.debug("Full backups, sorted by date, newest first: %r.",
                  [f[0] for f in fullbackups])
        oldest_backup_to_keep = fullbackups[(num_backups - 1)]
        logger.debug("Oldest backup to keep: %s", oldest_backup_to_keep[0])
        last_date_to_keep = oldest_backup_to_keep[1]
        logger.debug("The oldest backup we get to keep is from %s.",
                  last_date_to_keep)
        deleted = 0
        # Note: this also deletes now outdated .deltablobs and .dat
        # files, so we may easily delete more items than there are
        # fullbackups (so num_backups + deleted may be more than
        # len(fullbackups).
        for filename, modtime in files_modtimes:
            if modtime < last_date_to_keep:
                filepath = os.path.join(backup_location, filename)
                os.remove(filepath)
                logger.debug("Deleted %s.", filepath)
                deleted += 1
        logger.info("Removed %d file(s) belonging to old backups, the latest "
                    "%s full backups have been kept.", deleted,
                    str(num_backups))
        if deleted == 0:
            # This may be a programming/testing error.
            logger.error("We should have deleted something, but didn't...")
    else:
        logger.debug("Not removing backups.")
        if len(fullbackups) <= num_backups:
            logger.debug("Reason: #backups (%s) <= than max (%s).",
                      len(fullbackups), num_backups)
        if num_backups == 0:
            logger.debug("Reason: max # of backups is 0, so that is a "
                      "sign to us to not remove backups.")
