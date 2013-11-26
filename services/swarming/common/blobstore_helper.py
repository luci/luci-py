# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Blobstore Helper.

A basic class to assist with some basic blobstore operations.
"""


import logging
import time


from google.appengine import runtime
from google.appengine.api import files
from google.appengine.ext import blobstore


# The maximum number of times to try to write something to the blobstore before
# giving up.
MAX_BLOBSTORE_WRITE_TRIES = 5


def CreateBlobstore(blobstore_data):
  """Create a blostore with the desired value and return the key to it.

     This uses the experimental blobstore file api, which can cause problems.
     The main issues that I've seen are memory leaks (was able to repo on dev
     server), ApiTemporaryUnavailableError exceptions and hangs in create
     (which lead to DeadlineExceededError exceptions).

  Args:
    blobstore_data: The data to add to the blobstore.

  Returns:
    The blob key to access the stored blobstore, or None if it was unable to
    successfully write to the blobstore.
  """
  for attempt in range(MAX_BLOBSTORE_WRITE_TRIES):
    try:
      filename = files.blobstore.create('text/plain')
      with files.open(filename, 'a') as f:
        f.write(blobstore_data.encode('utf-8'))
      files.finalize(filename)

      return files.blobstore.get_blob_key(filename)
    except files.ApiTemporaryUnavailableError:
      logging.error('An exception while trying to store results in the '
                    'blobstore. Attempt %d', attempt)
      time.sleep(3)
    except files.ExistenceError:
      # Log the exception as well since there are different types of exitence
      # errors.
      logging.warning('The file seems to not exist, attempt %d.', exc_info=True)
    except files.UnknownError:
      logging.warning('Encountered an unknown error. Attempt %d', attempt)
    except runtime.DeadlineExceededError:
      logging.exception('The blobstore file api took too long to write the '
                        'file, aborting')
      break

  return None


def GetBlobstore(blob_key):
  """Retrieve the blob referenced by the given blob key.

  Args:
    blob_key: The key to the blob.

  Returns:
    The contents of the blob or None if the key doesn't point to a valid blob.
  """
  try:
    blob_reader = blobstore.BlobReader(blob_key)
    return blob_reader.read().decode('utf-8')

  except (ValueError, TypeError, blobstore.BlobNotFoundError,
          blobstore.InternalError) as e:
    logging.warning('Problem getting blobstore entry.\n%s', e)
    return None
