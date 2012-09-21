#!/usr/bin/env python
# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import datetime
import logging
import re
import webapp2

from google.appengine.api import files
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext.webapp import blobstore_handlers

# The maximum number of hash entries that can be queried in a single
# has request.
MAX_HASHKEYS_PER_HAS = 1000

# The minimum size, in bytes, a hash entry must be before it gets stored in the
# blobstore, otherwise it is stored as a blob property.
MIN_SIZE_FOR_BLOBSTORE = 20 * 8

VALID_SHA1_RE = re.compile(r'[a-f0-9]{40}')


class ContentNamespace(db.Model):
  """Used as an ancestor of HashEntry to create mutiple content-addressed
  "tables"."""
  is_testing = db.BooleanProperty()


class HashEntry(db.Model):
  """Represents the hash content."""
  # The reference pointing to the hash content, which is stored inside the
  # blobstore. This is only valid if the hash content was at least
  # MIN_SIZE_FOR_BLOBSTORE.
  hash_content_reference = blobstore.BlobReferenceProperty()

  # The hash content. This is only valid if the hash content was smaller than
  # MIN_SIZE_FOR_BLOBSTORE.
  hash_content = db.BlobProperty()

  # The day the content was last accessed. This is used to determine when
  # data is old and should be cleared.
  last_access = db.DateProperty(auto_now_add=True)

  # TODO(csharp): This function should be removed and the caller should be in
  # charge of deleting the entities and blobs (they can then be done
  # asynchronously).
  def delete(self):
    # If this model is deleted, then also delete the blobstore it references,
    # since no other model uses it.
    if self.hash_content_reference:
      self.hash_content_reference.delete()

    db.Model.delete(self)


def GetContentByHash(hash_key, namespace):
  """Returns the HashEntry with the given key, or None if it no HashEntry
  matches."""
  if not VALID_SHA1_RE.match(hash_key):
    logging.error('Given an invalid sha-1 value, %s', hash_key)
    return None

  try:
    namespace_model = ContentNamespace.get_or_insert(
        namespace, is_testing=namespace.startswith('temporary'))
    key = db.Key.from_path('HashEntry', hash_key, parent=namespace_model.key())
    return HashEntry.get(key)
  except (db.BadKeyError, db.KindError):
    pass

  return None


def StoreValueInBlobstore(value):
  """Store the given content in the blobstore, returning the key if it is
  successfully stored, otherwise return None."""
  # TODO(csharp): don't use the experimental API, instead create a blobstore
  # upload url and have the url use that instead.
  try:
    file_name = files.blobstore.create(mime_type='application/octet-stream')
    with files.open(file_name, 'a') as f:
      f.write(value)
    files.finalize(file_name)
    return files.blobstore.get_blob_key(file_name)
  except files.ApiTemporaryUnavailableError:
    logging.warning('An exception while trying to store results in the '
                    'blobstore.')
  return None


def CreateHashEntry(request, response):
  """Generates a new hash entry from the request if one doesn't exist. Returns
  None if there is a problem generating the entry or if an entry already exists
  with the given key."""
  hash_key = request.get('hash_key')
  if not hash_key:
    msg = 'No hash key given'
    logging.info(msg)

    response.out.write(msg)
    response.set_status(402)
    return None

  namespace = request.get('namespace', 'default')

  namespace_model = ContentNamespace.get_or_insert(
      namespace, is_testing=namespace.startswith('temporary'))
  key = db.Key.from_path('HashEntry', hash_key, parent=namespace_model.key())

  if HashEntry.all(keys_only=True).filter('__key__ =', key).get():
    msg = 'Hash entry already stored, no need to store again.'
    logging.info(msg)

    response.out.write(msg)
    return None

  return HashEntry(key=key)


class ContainsHashHandler(webapp2.RequestHandler):
  """A simple handler for saying if a hash is already stored or not."""
  def get(self):
    hash_keys = self.request.get('hash_key').split()
    namespace = self.request.get('namespace', 'default')
    logging.debug('Checking namespace %s for the following keys:\n%s',
                  namespace, str(hash_keys))

    if len(hash_keys) > MAX_HASHKEYS_PER_HAS:
      msg = (
          'Requested more than %d hashkeys in a single has request, aborting' %
          len(hash_keys))
      logging.error(msg)
      self.response.out.write(msg)
      self.response.out.set_status(402)
      return

    contains = (
        str(bool(GetContentByHash(hash_key, namespace)))
        for hash_key in hash_keys
    )

    self.response.out.write('\n'.join(contains))


class GenerateBlobstoreHandler(webapp2.RequestHandler):
  """Generate an upload url to directly load files into the blobstore."""
  def get(self):
    self.response.out.write(
        blobstore.create_upload_url('/content/store_blobstore'))


class StoreBlobstoreContentByHashHandler(
    blobstore_handlers.BlobstoreUploadHandler):
  """Assigns the newly stored blobstore entry to the correct hash key."""
  def post(self):
    upload_hash_contents = self.get_uploads('hash_contents')
    if len(upload_hash_contents) != 1:
      msg = ('Found %d hash_contents, there should only be 1.' %
             len(upload_hash_contents))
      logging.error(msg)

      self.response.write.out(msg)
      self.response.set_status(402)

      # Delete all upload files since they aren't linked to anything.
      db.delete_async(upload_hash_contents)
      return

    hash_entry = CreateHashEntry(self.request, self.response)

    if not hash_entry:
      # Delete all upload files since they aren't linked to anything.
      db.delete_async(upload_hash_contents)
      return

    hash_entry.hash_content_reference = upload_hash_contents[0]
    hash_entry.put()

    logging.info('Uploaded data stored directly into blobstore')
    self.response.out.write('hash content saved.')


class StoreContentByHashHandler(webapp2.RequestHandler):
  """The handler for adding hash contents."""
  def post(self):
    hash_entry = CreateHashEntry(self.request, self.response)

    if not hash_entry:
      return

    # webapp2 doesn't like reading the body if it's empty.
    if self.request.headers.get('content-length'):
      hash_content = self.request.body
    else:
      hash_content = ''

    if len(hash_content) < MIN_SIZE_FOR_BLOBSTORE:
      logging.info('Storing hash content in model')
      hash_entry.hash_content = hash_content
    else:
      logging.info('Storing hash content in blobstore')
      hash_entry.hash_content_reference = StoreValueInBlobstore(hash_content)
      if not hash_entry.hash_content_reference:
        msg = 'Unable to save the hash to the blobstore.'
        logging.error(msg)

        self.response.out.write(msg)
        self.response.set_status(402)
        return

    hash_entry.put()
    self.response.out.write('hash content saved.')


class RemoveContentByHashHandler(webapp2.RequestHandler):
  """The handler for removing hash contents from the server."""
  def post(self):
    hash_key = self.request.get('hash_key')
    namespace = self.request.get('namespace', 'default')
    hash_entry = GetContentByHash(hash_key, namespace)

    if not hash_entry:
      msg = 'Unable to find a hash with key \'%s\'.' % hash_key
      logging.info(msg)

      self.response.out.write(msg)
      return

    hash_entry.delete()
    logging.info('Deleted hash entry')


class RetriveContentByHashHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """The handlers for retrieving hash contents."""
  def get(self):
    hash_key = self.request.get('hash_key')
    namespace = self.request.get('namespace', 'default')
    hash_entry = GetContentByHash(hash_key, namespace)
    # TODO(csharp): High priority requests, 0, should be loaded from memcache.
    priority = self.request.get('priority', '1')

    if not hash_entry:
      msg = 'Unable to find a hash with key \'%s\'.' % hash_key
      logging.info(msg)

      self.response.out.write(msg)
      self.response.set_status(402)
      return

    if hash_entry.last_access != datetime.date.today():
      hash_entry.last_access = datatime.date.today()
      hash_entry.put_async()

    if hash_entry.hash_content is None:
      logging.info('Returning hash content from blobstore')
      self.send_blob(hash_entry.hash_content_reference)
    else:
      logging.info('Returning hash content from model')
      self.response.out.write(hash_entry.hash_content)


def CreateApplication():
  return webapp2.WSGIApplication([
      ('/content/contains', ContainsHashHandler),
      ('/content/generate_blobstore_url', GenerateBlobstoreHandler),
      ('/content/store', StoreContentByHashHandler),
      ('/content/store_blobstore', StoreBlobstoreContentByHashHandler),
      ('/content/remove', RemoveContentByHashHandler),
      ('/content/retrieve', RetriveContentByHashHandler)])

app = CreateApplication()
