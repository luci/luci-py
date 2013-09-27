#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Result Helper.

A basic class to assist with storing test results in the datastore. This helper
acts similiar to the normal blobstore, but the values all stay in the datastore.
"""





from google.appengine.ext import ndb

# The maximum size a chunk may be, according to app engine.
MAX_CHUNK_SIZE = 768 * 1024


class ResultChunk(ndb.Model):
  """A chunk of the results."""
  chunk = ndb.BlobProperty(compressed=False)


class Results(ndb.Model):
  """A simple wrapper class that contains all the results for a given test.

  The results are stored in several chunks referenced here, since app engine
  doesn't allow a model to be larger than 1MB.
  """
  chunk_keys = ndb.KeyProperty(kind=ResultChunk, repeated=True)

  @classmethod
  def _pre_delete_hook(cls, key):  # pylint: disable=g-bad-name
    """Deletes the associated chunk before deleting the results.

    Args:
      key: The key of the Results to be deleted.
    """
    results = key.get()

    for key in results.chunk_keys:
      key.delete_async()

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
    # Store the results as just raw bytes because otherwise app engine won't
    # accept unicode string.
    chunk_futures = [
        ResultChunk(chunk=bytes(results_data[x:x+MAX_CHUNK_SIZE])).put_async()
        for x in range(0, len(results_data), MAX_CHUNK_SIZE)
    ]

  new_results = Results(
      chunk_keys=[future.get_result() for future in chunk_futures])
  new_results.put()

  return new_results
