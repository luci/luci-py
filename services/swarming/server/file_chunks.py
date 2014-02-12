# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""File Chunks.

Contains the models required to store a file on the server. This is mainly
intended to contain scripts that will be run on the slaves/
"""

from google.appengine.ext import ndb

# The maximum size a chunk should be when creating chunk models. Although App
# Engine allows bigger, this gives some wiggle room in case something needs to
# be added to a chunk model.
MAX_CHUNK_SIZE = 768 * 1024


class FileChunk(ndb.Model):
  """A model representing a chunk of a file."""
  chunk = ndb.BlobProperty(compressed=False)


class File(ndb.Model):
  """A model representing a file."""


def StoreFile(file_id, contents):
  """Store a File model with the given id and data.

  Args:
    file_id: The id of the file model should have.
    contents: The contents of the file.
  """
  file_model = File.get_or_insert(file_id)

  # Delete any chunks associated with the old version (if they exists).
  ndb.delete_multi(FileChunk.query(
      ancestor=file_model.key,
      default_options=ndb.QueryOptions(keys_only=True)))

  chunk_futures = []
  for chunk_start in range(0, len(contents), MAX_CHUNK_SIZE):
    chunk_contents = contents[chunk_start:chunk_start+MAX_CHUNK_SIZE]

    file_chunk = FileChunk(parent=file_model.key,
                           id=chunk_start,
                           chunk=chunk_contents)

    chunk_futures.append(file_chunk.put_async())

  ndb.Future.wait_all(chunk_futures)


def RetrieveFile(file_id):
  """Returns the store file with the given id.

  Args:
    file_id: The id of the file to retrieve.

  Returns:
    The currently stored file. If no file exists, returns None.
  """
  chunks = FileChunk.query(ancestor=ndb.Key(File, file_id))

  # Check that at least 1 chunk exists to see if the file exists.
  if chunks.count(limit=1) == 0:
    return None

  # The chunks are ordered by their id, so make sure they are in the
  # correct order.
  sorted_chunks = sorted(chunks, cmp=lambda x, y: cmp(x.key.id(), y.key.id()))

  return ''.join(file_chunk.chunk for file_chunk in sorted_chunks)
