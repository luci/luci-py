#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Result Helper.

A basic class to assist with storing test results in the datastore. This helper
acts similiar to the normal blobstore, but the values all stay in the datastore.
"""



import datetime
import logging


from google.appengine.ext import ndb

from common import swarm_constants

# The number of days to keep result chunks around before assuming they are
# orphaned and can be safely deleted. This value should always be more than
# SWARM_OLD_RESULTS_TIME_TO_LIVE_DAYS to ensure they are orphans.
SWARM_RESULT_CHUNK_OLD_TIME_TO_LIVE_DAYS = (
    swarm_constants.SWARM_OLD_RESULTS_TIME_TO_LIVE_DAYS + 5)


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be easily mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.utcnow()


class ResultChunk(ndb.Model):
  """A chunk of the results."""
  chunk = ndb.BlobProperty(compressed=False)

  # Don't use auto_now_add so we control exactly what the time is set to
  # (since we later need to compare this value, so we need to know if it was
  # made with .now() or .utcnow()).
  created = ndb.DateProperty()

  def _pre_put_hook(self):  # pylint: disable=g-bad-name
    """Stores the creation time for this model."""
    if not self.created:
      self.created = datetime.datetime.utcnow().date()


class Results(ndb.Model):
  """A simple wrapper class that contains all the results for a given test.

  The results are stored in several chunks referenced here, since app engine
  doesn't allow a model to be larger than 1MB.
  """
  chunk_keys = ndb.KeyProperty(kind=ResultChunk, repeated=True)

  # Don't use auto_now_add so we control exactly what the time is set to
  # (since we later need to compare this value, so we need to know if it was
  # made with .now() or .utcnow()).
  created = ndb.DateProperty()

  @classmethod
  def _pre_delete_hook(cls, key):  # pylint: disable=g-bad-name
    """Deletes the associated chunk before deleting the results.

    Args:
      key: The key of the Results to be deleted.
    """
    results = key.get()
    if not results:
      return

    ndb.delete_multi(results.chunk_keys)

  def _pre_put_hook(self):  # pylint: disable=g-bad-name
    """Stores the creation time for this model."""
    if not self.created:
      self.created = datetime.datetime.utcnow().date()

  def GetResults(self):
    """Return the results stored in this model.

    Returns:
      The results from this model.
    """
    return ''.join(key.get().chunk for key in self.chunk_keys)


def StoreResults(results_data):
  """Create a new result model with the given results.

  Args:
    results_data: The data to store in the model.

  Returns:
    A model containing the given data.
  """
  chunk_futures = []
  if results_data:
    chunk_futures = [
        ResultChunk(
            chunk=results_data[x:x+swarm_constants.MAX_CHUNK_SIZE]).put_async()
        for x in range(0, len(results_data), swarm_constants.MAX_CHUNK_SIZE)
    ]

  new_results = Results(
      chunk_keys=[future.get_result() for future in chunk_futures])
  new_results.put()

  return new_results


def DeleteOldResults():
  """Deletes old results from the database.

  Returns:
    The list of Futures for all the async deletes.
  """
  logging.debug('DeleteOldResults starting.')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(
          days=swarm_constants.SWARM_OLD_RESULTS_TIME_TO_LIVE_DAYS))

  old_results_query = Results.query(
      Results.created < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))

  futures = ndb.delete_multi_async(old_results_query)

  logging.debug('DeleteOldResults done.')

  return futures


def DeleteOldResultChunks():
  """Deletes old result chunks from the database.

  This function shouldn't find orphans very often, since they can only get
  created in StoreResults, if after a chunk is created we fail to create
  the remaining chunks or the Results object (which could happen due to
  datastore times or other AE specific errors).

  Returns:
    The list of Futures for all the async deletes.
  """
  logging.debug('DeleteOldResultChunks starting.')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(
          days=SWARM_RESULT_CHUNK_OLD_TIME_TO_LIVE_DAYS))

  old_result_chunks_query = ResultChunk.query(
      ResultChunk.created < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))

  futures = ndb.delete_multi_async(old_result_chunks_query)

  logging.debug('DeleteOldResultChunks done.')

  return futures
