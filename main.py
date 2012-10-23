#!/usr/bin/env python
# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import binascii
import datetime
import logging
import re

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
import webapp2
from google.appengine.api import datastore_errors
from google.appengine.api import files
from google.appengine.api import memcache
from google.appengine.api import users
from google.appengine.api import taskqueue
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext.webapp import blobstore_handlers
# pylint: enable=E0611,F0401

# The maximum number of hash entries that can be queried in a single
# has request.
MAX_HASH_DIGESTS_PER_HAS = 1000

# The minimum size, in bytes, a hash entry must be before it gets stored in the
# blobstore, otherwise it is stored as a blob property.
MIN_SIZE_FOR_BLOBSTORE = 20 * 8

# The number of days a datamodel must go unaccessed for before it is deleted.
DATASTORE_TIME_TO_LIVE_IN_DAYS = 7

# The maximum number of blobs to delete at a time.
MAX_BLOBS_TO_DELETE_ASYNC = 100

# The domains that are allowed to access this application.
VALID_DOMAINS = (
    'chromium.org',
    'google.com',
)

HASH_DIGEST_LENGTH = 20
HASH_HEXDIGEST_LENGTH = 40
VALID_SHA1_RE = re.compile(r'[a-f0-9]{' + str(HASH_HEXDIGEST_LENGTH) + r'}')


class ContentNamespace(db.Model):
  """Used as an ancestor of HashEntry to create mutiple content-addressed
  "tables".

  There's primarily three tables. The table name is its key name:
  - default:    The default CAD.
  - gzip:       This namespace contains the content in gzip'ed format.
  - temporary*: This family of namespace is a discardable namespace for testing
                purpose only.

  All the tables in the temporary* family should have is_testing==True and the
  others is_testing==False.
  """
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



def GetContentNamespaceKey(namespace):
  """Returns the ContentNamespace key for the namespace value.

  Makes sure the entity exists in the datastore.
  memcache.
  """
  return ContentNamespace.get_or_insert(
      namespace, is_testing=namespace.startswith('temporary'))


def GetContentByHash(hash_key, namespace):
  """Returns the HashEntry with the given key, or None if it no HashEntry
  matches."""
  if not VALID_SHA1_RE.match(hash_key):
    logging.error('Given an invalid sha-1 value, %s', hash_key)
    return None

  try:
    namespace_model_key = GetContentNamespaceKey(namespace)
    key = db.Key.from_path('HashEntry', hash_key, parent=namespace_model_key)
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

  namespace_model_key = GetContentNamespaceKey(namespace)
  key = db.Key.from_path('HashEntry', hash_key, parent=namespace_model_key)

  if HashEntry.all(keys_only=True).filter('__key__ =', key).get():
    msg = 'Hash entry already stored, no need to store again.'
    logging.info(msg)

    response.out.write(msg)
    return None

  return HashEntry(key=key)


class ACLRequestHandler(webapp2.RequestHandler):
  """Adds ACL to the request handler to ensure only valid users can use
  the handlers."""
  def dispatch(self):
    """Ensures that only users from valid domains can continue, and that users
    from invalid domains receive an error message."""
    email = users.get_current_user().email()
    domain = email.partition('@')[2]
    if domain not in VALID_DOMAINS:
      msg = 'Invalid domain, %s' % domain
      logging.error(msg)
      self.response.out.write(msg)
      self.response.set_status(402)
      return

    return webapp2.RequestHandler.dispatch(self)


class RestrictedCleanupWorkerHandler(webapp2.RequestHandler):
  """Removes the old data from the blobstore and datastore. Only
  a task queue task can use this handler."""
  def post(self):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    # Remove old datastore entries.
    logging.info('Deleting old datastore entries')
    old_cutoff = datetime.datetime.today() - datetime.timedelta(
        days=DATASTORE_TIME_TO_LIVE_IN_DAYS)
    # Each gql query returns a maximum of 1000 entries, so loop until there
    # are no elements left to delete.
    while True:
      db_query = HashEntry.gql('WHERE last_access < :1', old_cutoff)
      if not db_query.count():
        break
      db.delete_async(db_query)

    # Remove orphaned blobs.
    logging.info('Deleting orphaned blobs')
    count = 0
    orphaned_blobs = []
    blobstore_query = blobstore.BlobInfo.all()
    while True:
      try:
        for blob_key in blobstore_query.run(batch_size=1000):
          count += 1
          if count % 1000 == 0:
            logging.info('Scanned %d elements', count)
          if not HashEntry.gql('WHERE hash_content_reference = :1',
                               blob_key.key()).count(limit=1):
            orphaned_blobs.append(blob_key.key())
            if len(orphaned_blobs) == MAX_BLOBS_TO_DELETE_ASYNC:
              logging.info('Deleting %d orphan blobs', len(orphaned_blobs))
              blobstore.delete_async(orphaned_blobs)
              orphaned_blobs = []
        # All the blobs have been examined so we can stop.
        break
      except datastore_errors.BadRequestError:
        blobstore_query.with_cursor(blobstore_query.cursor())
        logging.info('Request timed out, retrying')

    if orphaned_blobs:
      logging.info('Deleting %d orphan blobs', len(orphaned_blobs))
      blobstore.delete_async(orphaned_blobs)



class RestrictedCleanupHandler(webapp2.RequestHandler):
  """Removes old unused data from the blob store and datastore.
  Only cronjob from the server can access this handler."""
  def get(self):
    # App engine cron jobs are always GET requests, so we cleanup as a get
    # even though it modifies the server's state.
    if self.request.headers.get('X-AppEngine-Cron') != 'true':
      self.abort(405, detail='Only internal cron jobs can do this')

    taskqueue.add(url='/restricted/cleanup_worker')


class ContainsHashHandler(ACLRequestHandler):
  """A simple handler for saying if a hash is already stored or not."""
  def post(self):
    """This is a POST even though it doesn't modify any data, but it makes
    it easier for python scripts."""
    hash_digests = self.request.body
    namespace = self.request.get('namespace', 'default')

    if len(hash_digests) % HASH_DIGEST_LENGTH:
      msg = ('Hash digests must all be of length %d, last digest was of '
             'length %d' %
             (HASH_DIGEST_LENGTH, len(hash_digests) % HASH_DIGEST_LENGTH))
      self.abort(400, detail=msg)

    hash_digest_count = len(hash_digests) / HASH_DIGEST_LENGTH
    logging.info('Checking namespace %s for %d hash digests', namespace,
                 hash_digest_count)

    if hash_digest_count > MAX_HASH_DIGESTS_PER_HAS:
      msg = (
          'Requested more than %d hash digests in a single has request, '
          'aborting' % hash_digest_count)
      self.abort(400, detail=msg)

    namespace_model_key = GetContentNamespaceKey(namespace)

    # Extract all the hashes.
    hashes = (
        binascii.hexlify(
            hash_digests[i * HASH_DIGEST_LENGTH: (i + 1) * HASH_DIGEST_LENGTH])
        for i in range(hash_digest_count)
    )

    # Convert to entity keys.
    keys = (
        db.Key.from_path('HashEntry', hash_digest, parent=namespace_model_key)
        for hash_digest in hashes
    )

    # Convert to byte, chr(0) if not present, chr(1) if it is.
    contains = (
        bool(HashEntry.all(keys_only=True).filter('__key__ =', key).get())
        for key in keys)

    self.response.out.write(bytearray(contains))


class GenerateBlobstoreHandler(ACLRequestHandler):
  """Generate an upload url to directly load files into the blobstore."""
  def get(self):
    self.response.out.write(
        blobstore.create_upload_url('/content/store_blobstore'))


class StoreBlobstoreContentByHashHandler(
  ACLRequestHandler,
  blobstore_handlers.BlobstoreUploadHandler):
  """Assigns the newly stored blobstore entry to the correct hash key."""
  def post(self):
    upload_hash_contents = self.get_uploads('hash_contents')
    if len(upload_hash_contents) != 1:
      # Delete all upload files since they aren't linked to anything.
      blobstore.delete_async(upload_hash_contents)
      msg = ('Found %d hash_contents, there should only be 1.' %
             len(upload_hash_contents))
      self.abort(400, detail=msg)

    hash_entry = CreateHashEntry(self.request, self.response)

    if not hash_entry:
      # Delete all upload files since they aren't linked to anything.
      blobstore.delete_async(upload_hash_contents)
      return

    hash_entry.hash_content_reference = upload_hash_contents[0]
    hash_entry.put()

    logging.info('Uploaded data stored directly into blobstore')
    self.response.out.write('hash content saved.')


class StoreContentByHashHandler(ACLRequestHandler):
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

    try:
      priority = int(self.request.get('priority'))
    except ValueError:
      priority = 1

    if priority == 0:
      try:
        # TODO(maruel): Use namespace='table_%s' % namespace.
        if memcache.set(self.request.get('hash_key'), hash_content,
                        namespace=self.request.get('namespace', 'default')):
          logging.info('Storing hash content in memcache')
        else:
          logging.error('Attempted to save hash contents in memcache '
                        'but failed')
      except ValueError as e:
        logging.error(e)

    if len(hash_content) < MIN_SIZE_FOR_BLOBSTORE:
      logging.info('Storing hash content in model')
      hash_entry.hash_content = hash_content
    else:
      logging.info('Storing hash content in blobstore')
      hash_entry.hash_content_reference = StoreValueInBlobstore(hash_content)
      if not hash_entry.hash_content_reference:
        self.abort(507, detail='Unable to save the hash to the blobstore.')

    hash_entry.put()
    self.response.out.write('hash content saved.')


class RemoveContentByHashHandler(ACLRequestHandler):
  """The handler for removing hash contents from the server."""
  def post(self):
    hash_key = self.request.get('hash_key')
    namespace = self.request.get('namespace', 'default')
    hash_entry = GetContentByHash(hash_key, namespace)
    # TODO(maruel): Use namespace='table_%s' % namespace.
    memcache.delete(hash_key, namespace=namespace)

    if not hash_entry:
      msg = 'Unable to find a hash with key \'%s\'.' % hash_key
      logging.info(msg)

      self.response.out.write(msg)
      return

    hash_entry.delete()
    logging.info('Deleted hash entry')


class RetrieveContentByHashHandler(ACLRequestHandler,
                                  blobstore_handlers.BlobstoreDownloadHandler):
  """The handlers for retrieving hash contents."""
  def get(self):
    hash_key = self.request.get('hash_key')
    namespace = self.request.get('namespace', 'default')
    # TODO(maruel): Use namespace='table_%s' % namespace.
    memcache_entry = memcache.get(hash_key, namespace=namespace)

    if memcache_entry:
      logging.info('Returning hash contents from memcache')
      self.response.out.write(memcache_entry)
      return

    hash_entry = GetContentByHash(hash_key, namespace)
    if not hash_entry:
      msg = 'Unable to find a hash with key \'%s\'.' % hash_key
      self.abort(404, detail=msg)

    if hash_entry.last_access != datetime.date.today():
      hash_entry.last_access = datetime.date.today()
      db.put_async(hash_entry)

    if hash_entry.hash_content is None:
      logging.info('Returning hash content from blobstore')
      self.send_blob(hash_entry.hash_content_reference)
    else:
      logging.info('Returning hash content from model')
      self.response.out.write(hash_entry.hash_content)


def CreateApplication():
  return webapp2.WSGIApplication([
      (r'/restricted/cleanup_worker', RestrictedCleanupWorkerHandler),
      (r'/restricted/cleanup', RestrictedCleanupHandler),

      ('/content/contains', ContainsHashHandler),
      ('/content/generate_blobstore_url', GenerateBlobstoreHandler),
      ('/content/store', StoreContentByHashHandler),
      ('/content/store_blobstore', StoreBlobstoreContentByHashHandler),
      ('/content/remove', RemoveContentByHashHandler),
      ('/content/retrieve', RetrieveContentByHashHandler)])

app = CreateApplication()
