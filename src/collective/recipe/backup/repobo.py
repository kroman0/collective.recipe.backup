#!/usr/bin/env python

# repobo.py -- incremental and full backups of a blobstorage.
#
# Written by Roman Kozlovkiy

"""repobo.py -- incremental and full backups of a blobstorage.

Usage: %(program)s [options]
Where:

    Exactly one of -B or -R must be specified:

    -B / --backup
        Backup current blobstorage.

    -R / --recover
        Restore a blobstorage from a backup.

    -v / --verbose
        Verbose mode.

    -h / --help
        Print this text and exit.

    -r dir
    --repository=dir
        Repository directory containing the backup files.  This argument
        is required.  The directory must already exist.  You should not
        edit the files in this directory, or add your own files to it.

Options for -B/--backup:
    -b blobstorage_dir
    --blobstorage=blobstorage_dir
        Source blobstorage.  This argument is required.

    -F / --full
        Force a full backup.  By default, an incremental backup is made
        if possible (e.g., if a pack has occurred since the last
        incremental backup, a full backup is necessary).

    -z / --gzip
        Compress with gzip the backup files.  Uses the default uncompressed
        level.  By default, gzip compression is not used.

    -k / --kill-old-on-full
        If a full backup is created, remove any prior full or incremental
        backup files from the repository directory.

Options for -R/--recover:
    -D str
    --date=str
        Recover state as of this date.  Specify UTC (not local) time.
            yyyy-mm-dd[-hh[-mm[-ss]]]
        By default, current time is used.

    -o blobstorage_dir
    --output=blobstorage_dir
        Write recovered blobsorage to given directory.  This argument is
        required.
"""

import os
import sys
import time
import getopt
import tarfile

program = sys.argv[0]

BACKUP = 1
RECOVER = 2

COMMASPACE = ', '
VERBOSE = False


class WouldOverwriteFiles(Exception):
    pass


class NoFiles(Exception):
    pass


def usage(code, msg=''):
    outfp = sys.stderr
    if code == 0:
        outfp = sys.stdout

    print >> outfp, __doc__ % globals()
    if msg:
        print >> outfp, msg

    sys.exit(code)


def log(msg, *args):
    if VERBOSE:
        # Use stderr here so that -v flag works with -R and no -o
        print >> sys.stderr, msg % args


def parseargs(argv):
    global VERBOSE
    try:
        opts, args = getopt.getopt(argv, 'BRvhr:b:FzkD:o:',
                                   ['backup',
                                    'recover',
                                    'verbose',
                                    'help',
                                    'repository=',
                                    'blob=',
                                    'full',
                                    'gzip',
                                    'kill-old-on-full',
                                    'date=',
                                    'output=',
                                   ])
    except getopt.error, msg:
        usage(1, msg)

    class Options:
        mode = None         # BACKUP or RECOVER
        blob = None         # name of directory holding blobstorage
        repository = None   # name of directory holding backups
        full = False        # True forces full backup
        date = None         # -D argument, if any
        output = None       # where to write recovered data; None = stdout
        gzip = False        # -z flag state
        killold = None      # -k flag state

    options = Options()

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage(0)
        elif opt in ('-v', '--verbose'):
            VERBOSE = True
        elif opt in ('-R', '--recover'):
            if options.mode is not None:
                usage(1, '-B and -R are mutually exclusive')
            options.mode = RECOVER
        elif opt in ('-B', '--backup'):
            if options.mode is not None:
                usage(1, '-B and -R are mutually exclusive')
            options.mode = BACKUP
        elif opt in ('-b', '--blobstorage'):
            options.blob = arg
        elif opt in ('-r', '--repository'):
            options.repository = arg
        elif opt in ('-F', '--full'):
            options.full = True
        elif opt in ('-D', '--date'):
            options.date = arg
        elif opt in ('-o', '--output'):
            options.output = arg
        elif opt in ('-z', '--gzip'):
            options.gzip = True
        elif opt in ('-k', '--kill-old-on-full'):
            options.killold = True
        else:
            assert False, (opt, arg)

    # Any other arguments are invalid
    if args:
        usage(1, 'Invalid arguments: ' + COMMASPACE.join(args))

    # Sanity checks
    if options.mode is None:
        usage(1, 'Either --backup or --recover is required')
    if options.repository is None:
        usage(1, '--repository is required')
    if options.mode == BACKUP:
        if options.date is not None:
            log('--date option is ignored in backup mode')
            options.date = None
        if options.output is not None:
            log('--output option is ignored in backup mode')
            options.output = None
    else:
        assert options.mode == RECOVER
        if options.blob is not None:
            log('--blobstorage option is ignored in recover mode')
            options.blob = None
        if options.killold is not None:
            log('--kill-old-on-full option is ignored in recover mode')
            options.killold = None
    return options


def concat(files):
    # Concatenate a bunch of files from the repository.
    names = []
    for f in files:
        # Auto uncompress
        log("Reading list %s", f)
        x = tarfile.open(f, 'r:*')
        names.extend(x.getnames())
        x.close()
        log(".")
    return set(names)


def gen_filename(options, ext=None):
    if ext is None:
        if options.full:
            ext = '.blobs'
        else:
            ext = '.deltablobs'
    # Hook for testing
    now = getattr(options, 'test_now', time.gmtime()[:6])
    t = now + (ext,)
    return '%04d-%02d-%02d-%02d-%02d-%02d%s' % t

def listd(base="", prefix="", ll=[]):
    if prefix:
        ll.append(prefix)
    if os.path.isdir(os.path.join(base, prefix)):
        for i in os.listdir(os.path.join(base, prefix)):
            listd(base, os.path.join(prefix, i), ll)
    return ll

# Return a list of files needed to reproduce state at time options.date.
# This is a list, in chronological order, of the .blobs and .deltablobs
# files, from the time of the most recent full backup preceding
# options.date, up to options.date.

import re
is_data_file = re.compile(r'\d{4}(?:-\d\d){5}\.(?:delta)?blobs$').match
del re

def find_files(options):
    when = options.date
    if not when:
        when = gen_filename(options, '')
    log('looking for files between last full backup and %s...', when)
    all = filter(is_data_file, os.listdir(options.repository))
    all.sort()
    all.reverse()   # newest file first
    # Find the last full backup before date, then include all the
    # incrementals between that full backup and "when".
    needed = []
    for fname in all:
        root, ext = os.path.splitext(fname)
        if root <= when:
            needed.append(fname)
            if ext in ('.blobs'):
                break
    # Make the file names relative to the repository directory
    needed = [os.path.join(options.repository, f) for f in needed]
    # Restore back to chronological order
    needed.reverse()
    if needed:
        log('files needed to recover state as of %s:', when)
        for f in needed:
            log('\t%s', f)
    else:
        log('no files found')
    return needed


def delete_old_backups(options):
    # Delete all full backup files except for the most recent full backup file
    all = filter(is_data_file, os.listdir(options.repository))
    all.sort()

    deletable = []
    full = []
    for fname in all:
        root, ext = os.path.splitext(fname)
        if ext in ('.blobs'):
            full.append(fname)
        if ext in ('.blobs', '.deltablobs'):
            deletable.append(fname)

    # keep most recent full
    if not full:
        return

    recentfull = full.pop(-1)
    deletable.remove(recentfull)

    for fname in deletable:
        log('removing old backup file %s', fname)
        os.unlink(os.path.join(options.repository, fname))


def do_full_backup(options):
    options.full = True
    dest = os.path.join(options.repository, gen_filename(options))
    if os.path.exists(dest):
        raise WouldOverwriteFiles('Cannot overwrite existing file: %s' % dest)
    log('writing full backup to %s', dest)
    fs = tarfile.open(dest, options.gzip and 'w:gz' or 'w:')
    for item in os.listdir(options.blob):
        fs.add(os.path.join(options.blob, item), item)
    fs.close()
    if options.killold:
        delete_old_backups(options)


def do_incremental_backup(options, delta, repofiles):
    options.full = False
    dest = os.path.join(options.repository, gen_filename(options))
    if os.path.exists(dest):
        raise WouldOverwriteFiles('Cannot overwrite existing file: %s' % dest)
    fs = tarfile.open(dest, options.gzip and 'w:gz' or 'w:')
    for item in delta:
        fs.add(os.path.join(options.blob, item), item)
    fs.close()


def do_backup(options):
    repofiles = find_files(options)
    # See if we need to do a full backup
    if options.full or not repofiles:
        log('doing a full backup')
        do_full_backup(options)
        return
    blobfiles = set(listd(options.blob, "", []))
    backupfiles = concat(repofiles)
    # Has nothing changed?
    if backupfiles == blobfiles:
        log('No changes, nothing to do')
        return
    # Has the file shrunk, probably because of a pack?
    if not backupfiles.issubset(blobfiles):
        log('blobstorage changed, possibly because of a pack (full backup)')
        do_full_backup(options)
        return
    log('doing incremental')
    do_incremental_backup(options, blobfiles.difference(backupfiles), repofiles)
    return


def do_recover(options):
    # Find the first full backup at or before the specified date
    repofiles = find_files(options)
    if not repofiles:
        if options.date:
            raise NoFiles('No files in repository before %s', options.date)
        else:
            raise NoFiles('No files in repository')
    if options.output is None:
        log('--output is required')
        return
    log('Recovering files to %s', options.output)
    blobfiles = set(listd(options.output, "", []))
    backupfiles = []
    for item in repofiles:
        f = tarfile.open(item, 'r:*')
        f.extractall(options.output)
        backupfiles.extend(f.getnames())
        f.close()
        log('Recovered %s', item)
    oldblobs = blobfiles - set(backupfiles)
    for blob in oldblobs:
        os.remove(os.path.join(options.output, blob))


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    options = parseargs(argv)
    if options.mode == BACKUP:
        try:
            do_backup(options)
        except WouldOverwriteFiles, e:
            print >> sys.stderr, str(e)
            sys.exit(1)
    else:
        assert options.mode == RECOVER
        try:
            do_recover(options)
        except NoFiles, e:
            print >> sys.stderr, str(e)
            sys.exit(1)


if __name__ == '__main__':
    main()
