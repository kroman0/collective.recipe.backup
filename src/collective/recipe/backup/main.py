"""Functions that invoke repozo and/or the blob backup.
"""
import logging
import sys

from collective.recipe.backup import repozorunner
from collective.recipe.backup import repoborunner
from collective.recipe.backup import utils

logger = logging.getLogger('backup')


def backup_main(bin_dir, datafs, backup_location, keep, full, verbose, gzip,
                additional, blob_storage_source, backup_blobs, only_blobs,
                **kwargs):
    """Main method, gets called by generated bin/backup."""
    if not only_blobs:
        result = repozorunner.backup_main(
            bin_dir, datafs, backup_location, keep, full, verbose, gzip,
            additional)
        if result and backup_blobs:
            logger.error("Halting execution due to error; not backing up "
                         "blobs.")

    if not backup_blobs:
        return
    if not blob_storage_source:
        logger.error("No blob storage source specified")
        sys.exit(1)
    result = repoborunner.backup_main(bin_dir, blob_storage_source,
                                      backup_location, keep, full, verbose,
                                      gzip)


def snapshot_main(bin_dir, datafs, snapshot_location, keep, verbose, gzip,
                  additional, blob_storage_source, backup_blobs, only_blobs,
                  **kwargs):
    """Main method, gets called by generated bin/snapshotbackup."""
    if not only_blobs:
        result = repozorunner.snapshot_main(
            bin_dir, datafs, snapshot_location, keep, verbose, gzip,
            additional)
        if result and backup_blobs:
            logger.error("Halting execution due to error; not backing up "
                         "blobs.")
    if not backup_blobs:
        return
    if not blob_storage_source:
        logger.error("No blob storage source specified")
        sys.exit(1)
    result = repoborunner.backup_main(bin_dir, blob_storage_source,
                                      snapshot_location, keep, full, verbose,
                                      gzip)


def restore_main(bin_dir, datafs, backup_location, verbose, additional,
                 blob_storage_source, backup_blobs, only_blobs, **kwargs):
    """Main method, gets called by generated bin/restore."""
    date = None
    if len(sys.argv) > 1:
        date = sys.argv[1]
        logger.debug("Argument passed to bin/restore, we assume it is "
                     "a date that we have to pass to repozo: %s.", date)
        logger.info("Date restriction: restoring state at %s." % date)

    question = '\n'
    if not only_blobs:
        question += "This will replace the filestorage (Data.fs).\n"
    if backup_blobs:
        question += "This will replace the blobstorage.\n"
    question += "Are you sure?"
    if not utils.ask(question, default=False, exact=True):
        logger.info("Not restoring.")
        sys.exit(0)

    if not only_blobs:
        result = repozorunner.restore_main(
            bin_dir, datafs, backup_location, verbose, additional, date)
        if result and backup_blobs:
            logger.error("Halting execution due to error; not restoring "
                         "blobs.")
            sys.exit(1)

    if not backup_blobs:
        return
    if not blob_storage_source:
        logger.error("No blob storage source specified")
        sys.exit(1)
    result = repoborunner.restore_main(bin_dir, blob_storage_source,
                                       backup_location, verbose, date)


def snapshot_restore_main(*args, **kwargs):
    """Main method, gets called by generated bin/snapshotrestore.

    Difference with restore_main is that we get need to use the
    snapshot_location and blob_snapshot_location.
    """
    # Override the locations:
    kwargs['backup_location'] = kwargs['snapshot_location']
    return restore_main(*args, **kwargs)
