# Copyright 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""Accesses files on Google Cloud Storage via AppEngine GS API.

References:
  https://developers.google.com/appengine/docs/python/googlestorage/
  https://developers.google.com/appengine/docs/python/googlestorage/functions
  https://developers.google.com/appengine/docs/python/googlestorage/exceptions
"""

import logging
import os

# pylint: disable=E0611,F0401
from google.appengine.api import files
# pylint: enable=E0611,F0401


# The limit is 32 megs but it's a tad on the large side. Use 512kb chunks
# instead to not clog memory when there's multiple concurrent requests being
# served.
CHUNK_SIZE = 1 << 19


def to_filepath(bucket, filename):
  return u'/gs/%s/%s' % (bucket, filename)


def store_content(bucket, filename, content):
  """Store the given content in GS.

  Returns True on success, False on failure.
  """
  try:
    filepath = to_filepath(bucket, filename)
    logging.debug('%s', filepath)
    # First create the file, then open it for write.
    obj = files.gs.create(
        filepath,
        content_disposition=u'attachment; filename="%s"' % os.path.basename(
          filename))
    try:
      with files.open(obj, 'a') as f:
        f.write(content)
    finally:
      # Finalize the file anyhow so it can be deleted in case of failure.
      # Finalization itself can fail too but there's nothing that can be done
      # against that.
      files.finalize(obj)
    return True
  except Exception as e:
    logging.error(
        'Exception while trying to store %s (%d) in GS: %s',
        filepath, len(content), e)
    # Try to delete the file.
    try:
      files.delete(filepath)
    except Exception as e:
      logging.error('Failed to delete: %s', e)
    return False


def open_file_for_reading(bucket, filename):
  """Reads a GS file and yield the data in chunks."""
  count = 0
  try:
    filepath = to_filepath(bucket, filename)
    with files.open(filepath, 'r') as f:
      while True:
        # TODO(maruel): Start the read on the next request while the processing
        # is being done to reduce the effect of latency on the throughput.
        data = f.read(CHUNK_SIZE)
        if not data:
          break
        count += len(data)
        yield data
  except Exception:
    logging.debug('Read %d bytes', count)
    raise


def list_files(bucket, subdir):
  """Yields file names."""
  marker = None
  max_keys = 100
  while True:
    items = files.listdir(
        u'/gs/%s' % bucket,
        prefix=subdir or None,
        marker=marker,
        max_keys=max_keys)
    if not items:
      break
    for item in items:
      yield item


def delete_files(bucket, filenames):
  """Deletes multiple files stored in GS at once.

  This is synchronous.
  """
  filepaths = [to_filepath(bucket, i) for i in filenames]
  files.delete(*filepaths)

  # Returns an empty list so this function can be used with functions that
  # expect the RPC to return a Future.
  return []
