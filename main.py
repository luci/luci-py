#!/usr/bin/env python
# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import binascii
import datetime
import hashlib
import logging
import os
import re
import zlib

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
import webapp2
from google.appengine import runtime
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
# request.
MAX_HASH_DIGESTS_PER_CALL = 1000

# The minimum size, in bytes, a hash entry must be before it gets stored in the
# blobstore, otherwise it is stored as a blob property.
MIN_SIZE_FOR_BLOBSTORE = 20 * 1024

# The number of days a datamodel must go unaccessed for before it is deleted.
DATASTORE_TIME_TO_LIVE_IN_DAYS = 7

# The maximum number of items to delete at a time.
ITEMS_TO_DELETE_ASYNC = 100

# The domains that are allowed to access this application.
VALID_DOMAINS = (
    'chromium.org',
    'google.com',
)

HASH_DIGEST_LENGTH = 20
HASH_HEXDIGEST_LENGTH = 40
# The SHA-1 must be lower case. This simplifies the code, e.g. the keys are
# unambiguous, no need to constantly call .lower().
VALID_SHA1_RE = r'[a-f0-9]{' + str(HASH_HEXDIGEST_LENGTH) + r'}'
VALID_SHA1_RE_COMPILED = re.compile(VALID_SHA1_RE)
VALID_NAMESPACE_RE = r'[a-z0-9A-Z\-]+'


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
  creation = db.DateTimeProperty(auto_now=True)


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

  creation = db.DateTimeProperty(auto_now=True)

  # It is an .isolated file.
  is_isolated = db.BooleanProperty(default=False)

  # It's content was verified to not be corrupted.
  is_verified = db.BooleanProperty(default=False)

  @property
  def is_compressed(self):
    """Is it the raw data or was it modified in any form, e.g. compressed, so
    that the SHA-1 doesn't match.
    """
    self.parent_key().name().endswith(('bzip2', 'gzip'))


class WhitelistedIP(db.Model):
  """Items where the IP address is allowed."""
  # The IP of the machine to whitelist. Can be either IPv4 or IPv6.
  ip = db.StringProperty()

  # Is only for maintenance purpose, not used.
  comment = db.StringProperty()


def GetContentNamespaceKey(namespace):
  """Returns the ContentNamespace key for the namespace value.

  Makes sure the entity exists in the datastore.
  memcache.
  """
  return ContentNamespace.get_or_insert(
      namespace, is_testing=namespace.startswith('temporary')).key()


def GetContentByHash(namespace, hash_key):
  """Returns the HashEntry with the given key, or None if it no HashEntry
  matches."""
  if not VALID_SHA1_RE_COMPILED.match(hash_key):
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


def CreateHashEntry(namespace, hash_key):
  """Generates a new hash entry from the request if one doesn't exist.

  Returns None if there is a problem generating the entry or if an entry already
  exists with the given key.
  """
  namespace_model_key = GetContentNamespaceKey(namespace)
  key = db.Key.from_path('HashEntry', hash_key, parent=namespace_model_key)

  if HashEntry.all(keys_only=True).filter('__key__ =', key).get():
    return None
  return HashEntry(key=key)


class ACLRequestHandler(webapp2.RequestHandler):
  """Adds ACL to the request handler to ensure only valid users can use
  the handlers."""
  def dispatch(self):
    """Ensures that only users from valid domains can continue, and that users
    from invalid domains receive an error message."""
    user = users.get_current_user()
    if not user:
      # Verify if its IP is whitelisted.
      query = WhitelistedIP.gql('WHERE ip = :1', self.request.remote_addr)
      if not query.count():
        logging.warning('Blocking IP %s', self.request.remote_addr)
        self.abort(401, detail='Please login first.')
    else:
      domain = user.email().partition('@')[2]
      if domain not in VALID_DOMAINS:
        logging.warning('Disallowing %s, invalid domain' % user.email())
        self.abort(403, detail='Invalid domain, %s' % domain)

    return webapp2.RequestHandler.dispatch(self)


def payload_to_hashes(request):
  """Loads the hash provided on the payload concatenated in binary form."""
  hash_digests = request.request.body
  if len(hash_digests) % HASH_DIGEST_LENGTH:
    msg = (
        'Hash digests must all be of length %d, had %d bytes total, last '
        'digest was of length %d' % (
              HASH_DIGEST_LENGTH,
              len(hash_digests),
              len(hash_digests) % HASH_DIGEST_LENGTH))
    logging.error(msg)
    request.abort(400, detail=msg)

  hash_digest_count = len(hash_digests) / HASH_DIGEST_LENGTH
  if hash_digest_count > MAX_HASH_DIGESTS_PER_CALL:
    msg = (
        'Requested more than %d hash digests in a single request, '
        'aborting' % hash_digest_count)
    logging.warning(msg)
    request.abort(400, detail=msg)
  return hash_digests, hash_digest_count


def read_blob(blob, callback):
  """Reads a BlobInfo/BlobKey and pass the data through callback.

  Returns the amount of data read.
  """
  position = 0
  chunk_size = blobstore.MAX_BLOB_FETCH_SIZE
  while True:
    data = blobstore.fetch_data(blob, position, position + chunk_size - 1)
    callback(data)
    position += len(data)
    if len(data) < chunk_size:
      break
  return position


### Restricted handlers


def delete_blobinfo_async(blobinfos):
  """Deletes BlobInfo properly.

  blobstore.delete*() do not accept a list of BlobInfo, they only accept a list
  BlobKey.
  """
  blobstore.delete_async((b.key() for b in blobinfos))


def incremental_delete(query, delete, check=None):
  """Applies |delete| to objects in a query asynchrously.

  Returns True if at least one object was found.
  """
  to_delete = []
  found = False
  count = 0
  for item in query:
    count += 1
    if not (count % 1000):
      logging.debug('Found %d items', count)
    if check and not check(item):
      continue
    to_delete.append(item)
    found = True
    if len(to_delete) == ITEMS_TO_DELETE_ASYNC:
      logging.info('Deleting %s entries', len(to_delete))
      delete(to_delete)
      to_delete = []
  if to_delete:
    logging.info('Deleting %s entries', len(to_delete))
    delete(to_delete)
  return found


class RestrictedCleanupOldEntriesWorkerHandler(webapp2.RequestHandler):
  """Removes the old data from the datastore.

  Only a task queue task can use this handler.
  """
  def post(self):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    logging.info('Deleting old datastore entries')
    old_cutoff = datetime.datetime.today() - datetime.timedelta(
        days=DATASTORE_TIME_TO_LIVE_IN_DAYS)

    def delete_hash_and_blobs(to_delete):
      """Deletes HashEntry and their blobs."""
      # Note all the blobs to delete first.
      blobs_to_delete = [
        i.hash_content_reference for i in to_delete if i.hash_content_reference
      ]
      delete_blobinfo_async(blobs_to_delete)
      # Then delete the entities.
      db.delete_async(to_delete)

    incremental_delete(
        HashEntry.all(keys_only=True).filter('last_access <', old_cutoff),
        delete=delete_hash_and_blobs)
    logging.info('Done deleting old entries')


class RestrictedCleanupTestingEntriesWorkerHandler(webapp2.RequestHandler):
  """Removes the testing data from the datastore.

  Keep stuff under testing for only one full day.

  Only a task queue task can use this handler.
  """
  def post(self):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    logging.info('Deleting testing entries')
    old_cutoff_testing = datetime.datetime.today() - datetime.timedelta(days=1)
    # For each testing namespace.
    namespace_query = ContentNamespace.all(keys_only=True).filter(
        'is_testing =', True)
    orphaned_namespaces = []
    for namespace in namespace_query:
      logging.debug('Namespace %s', namespace.name())
      found = incremental_delete(
          HashEntry.all(keys_only=True).ancestor(
              namespace).filter(
              'last_access <', old_cutoff_testing),
          delete=db.delete_async)
      if not found:
        orphaned_namespaces.append(namespace)
    if orphaned_namespaces:
      # Since delete_async() is used, the stale ContentNamespace will
      # likely stay for another full day, so keep it an extra day.
      logging.info('Deleting %s testing namespaces', len(orphaned_namespaces))
      db.delete_async(orphaned_namespaces)
    logging.info('Done deleting testing namespaces')


class RestrictedCleanupOrphanedBlobsWorkerHandler(webapp2.RequestHandler):
  """Removes the orphaned blobs from the blobstore.

  Only a task queue task can use this handler.
  """
  def post(self):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    logging.info('Deleting orphaned blobs')
    blobstore_query = blobstore.BlobInfo.all().order('creation')

    def check(blob_info):
      """Looks if the corresponding entry exists."""
      return not HashEntry.gql(
          'WHERE hash_content_reference = :1', blob_info.key()).count(limit=1)

    while True:
      try:
        incremental_delete(
            blobstore_query, check=check, delete=delete_blobinfo_async)
        # Didn't throw, can now move on.
        break
      except datastore_errors.BadRequestError:
        blobstore_query.with_cursor(blobstore_query.cursor())
        logging.info('Request timed out, retrying')
    logging.info('Done deleting orphaned blobs')


class RestrictedObliterateWorkerHandler(webapp2.RequestHandler):
  """Deletes all the stuff."""
  def post(self):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    logging.info('Deleting blobs')
    incremental_delete(
        blobstore.BlobInfo.all().order('creation'),
        delete_blobinfo_async)

    logging.info('Deleting HashEntry')
    incremental_delete(
        HashEntry.all(keys_only=True).order('creation'),
        db.delete_async)

    logging.info('Deleting Namespaces')
    incremental_delete(
        ContentNamespace.all(keys_only=True).order('creation'),
        db.delete_async)
    logging.info('Finally done!')


class RestrictedCleanupTriggerHandler(webapp2.RequestHandler):
  """Triggers a taskqueue to clean up."""
  def get(self, name):
    if name in ('obliterate', 'old', 'orphaned', 'testing'):
      url = '/restricted/taskqueue/cleanup/' + name
      # The push task queue name must be unique over a ~7 days period so use
      # the date at second precision, there's no point in triggering each of
      # time more than once a second anyway.
      now = datetime.datetime.utcnow().strftime('%Y-%m-%d_%I-%M-%S')
      taskqueue.add(url=url, queue_name='cleanup', name=name + '_' + now)
      self.response.out.write('Triggered %s' % url)
    else:
      self.abort(404, 'Unknown job')


class RestrictedTagWorkerHandler(webapp2.RequestHandler):
  """Tags .last_access for HashEntries tested for with /content/contains.

  This makes sure they are not evicted from the LRU cache too fast.
  """
  def post(self, namespace, year, month, day):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    hash_digests, hash_digest_count = payload_to_hashes(self)
    logging.info(
        'Stamping %d entries in namespace %s', hash_digest_count, namespace)

    # Extract all the hashes.
    hashes = (
        binascii.hexlify(
            hash_digests[i * HASH_DIGEST_LENGTH: (i + 1) * HASH_DIGEST_LENGTH])
        for i in range(hash_digest_count)
    )
    today = datetime.date(int(year), int(month), int(day))
    parent_key = GetContentNamespaceKey(namespace)
    to_save = []
    for hash_digest in hashes:
      key = db.Key.from_path('HashEntry', hash_digest, parent=parent_key)
      item = HashEntry.get(key)
      if item and item.last_access != today:
        item.last_access = today
        to_save.append(item)
    db.put(to_save)
    logging.info('Done timestamping %d entries', len(to_save))


class RestrictedVerifyWorkerHandler(webapp2.RequestHandler):
  """Verify the SHA-1 matches for an object stored in BlobStore."""
  def post(self, namespace, hash_key):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')

    hash_entry = GetContentByHash(namespace, hash_key)
    if not hash_entry:
      logging.error('Failed to find entity')
      return
    if hash_entry.is_verified:
      logging.info('Was already verified')
      return
    if not hash_entry.hash_content_reference or hash_entry.hash_content:
      logging.error('Should not be called with inline content')
      return

    digest = hashlib.sha1()
    if namespace.endswith('-gzip'):
      # Decompress before hashing.
      zlib_state = zlib.decompressobj()
      callback = lambda data: digest.update(zlib_state.decompress(data))
    else:
      callback = digest.update

    is_verified = False
    count = 0
    try:
      # Start a loop where it reads the data in block.
      count = read_blob(hash_entry.hash_content_reference, callback)

      # Need a fixup for zipped content to complete the decompression.
      if namespace.endswith('-gzip'):
        digest.update(zlib_state.flush())
      is_verified = digest.hexdigest() == hash_key
    except runtime.DeadlineExceededError:
      # Failed to read it through. If it's compressed, at least no zlib error
      # was thrown so the object is fine.
      logging.warning('Got DeadlineExceededError, giving up')
      return
    except zlib.error as e:
      # It's broken. At that point, is_verified is False.
      logging.error(e)

    if not is_verified:
      # Delete the entity since it's corrupted.
      logging.error('SHA-1 and data do not match, %d bytes', count)
      blobstore.delete(hash_entry.hash_content_reference.key())
      db.delete(hash_entry)
    else:
      logging.info('%d bytes verified', count)
      hash_entry.is_verified = True
      hash_entry.put()


class RestrictedWhitelistHandler(webapp2.RequestHandler):
  """Whitelists the current IP."""
  def post(self):
    ip = self.request.remote_addr
    if WhitelistedIP.gql('WHERE ip = :1', ip).get():
      self.response.out.write('Already present: %s' % ip)
      return
    WhitelistedIP(ip=ip).put()
    self.response.out.write('Success: %s' % ip)


### Non-restricted handlers


class ContainsHashHandler(ACLRequestHandler):
  """Returns a 'string' of chr(1) or chr(0) for each hash in the request body,
  signaling if the hash content is present in the namespace or not.
  """
  def post(self, namespace):
    """This is a POST even though it doesn't modify any data, but it makes
    it easier for python scripts.
    """
    hash_digests, hash_digest_count = payload_to_hashes(self)
    logging.info('Checking namespace %s for %d hash digests', namespace,
                 hash_digest_count)
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

    # Start the queries in parallel. It must be a list so the calls are executed
    # right away.
    queries = [
        HashEntry.all().filter('__key__ =', key).run(
            read_policy=db.EVENTUAL_CONSISTENCY,
            limit=1,
            batch_size=1,
            keys_only=True)
        for key in keys
    ]

    # Convert to True/False. It's a bit annoying because run() returns a
    # ResultsIterator.
    def IteratorToBool(itr):
      # TODO(maruel): Return if the entity is verified or not. Or add a filter
      # to the query above?
      for _ in itr:
        return True
      return False

    # Convert to byte, chr(0) if not present, chr(1) if it is.

    contains = [IteratorToBool(q) for q in queries]
    self.response.out.write(bytearray(contains))
    found = sum(contains, 0)
    logging.info('%d hit, %d miss', found, hash_digest_count - found)
    if found:
      # For all the ones that exist, update their last_access in a task queue.
      hashes_to_tag = ''.join(
          hash_digests[i * HASH_DIGEST_LENGTH: (i + 1) * HASH_DIGEST_LENGTH]
          for i in xrange(hash_digest_count) if contains[i])
      url = '/restricted/taskqueue/tag/%s/%s' % (
          namespace, datetime.date.today())
      taskqueue.add(url=url, payload=hashes_to_tag, queue_name='tag')


class GenerateBlobstoreHandler(ACLRequestHandler):
  """Generate an upload url to directly load files into the blobstore."""
  def get(self, namespace, hash_key):
    self.response.out.write(
        blobstore.create_upload_url(
            '/content/store_blobstore/%s/%s' % (namespace, hash_key)))


class StoreBlobstoreContentByHashHandler(
    ACLRequestHandler,
    blobstore_handlers.BlobstoreUploadHandler):
  """Assigns the newly stored blobstore entry to the correct hash key."""
  def post(self, namespace, hash_key):
    upload_hash_contents = self.get_uploads('hash_contents')
    if len(upload_hash_contents) != 1:
      # Delete all upload files since they aren't linked to anything.
      delete_blobinfo_async(upload_hash_contents)
      msg = ('Found %d hash_contents, there should only be 1.' %
             len(upload_hash_contents))
      self.abort(400, detail=msg)

    hash_entry = CreateHashEntry(namespace, hash_key)
    if not hash_entry:
      msg = 'Hash entry already stored, no need to store %d bytes again.' % (
          upload_hash_contents[0].size)
      logging.warning(msg)
      self.response.out.write(msg)
      # Delete all upload files since they aren't linked to anything.
      delete_blobinfo_async(upload_hash_contents)
      return

    try:
      priority = int(self.request.get('priority'))
    except ValueError:
      priority = 1

    hash_entry.hash_content_reference = upload_hash_contents[0]
    # TODO(maruel): Add a new parameter.
    hash_entry.is_isolated = (priority == 0)
    hash_entry.put()

    # Trigger a verification. It can't be done inline since it could be too
    # long to complete.
    url = '/restricted/taskqueue/verify/%s/%s' % (namespace, hash_key)
    taskqueue.add(url=url, queue_name='verify')

    logging.info(
        '%d bytes uploaded directly into blobstore',
        hash_entry.hash_content_reference.size)
    self.response.out.write('hash content saved.')


class StoreContentByHashHandler(ACLRequestHandler):
  """The handler for adding hash contents."""
  def post(self, namespace, hash_key):
    # webapp2 doesn't like reading the body if it's empty.
    if self.request.headers.get('content-length'):
      hash_content = self.request.body
    else:
      hash_content = ''

    hash_entry = CreateHashEntry(namespace, hash_key)
    if not hash_entry:
      msg = 'Hash entry already stored, no need to store %d bytes again.' % (
          len(hash_content))
      logging.info(msg)
      self.response.out.write(msg)
      return

    try:
      priority = int(self.request.get('priority'))
    except ValueError:
      priority = 1

    # Verify the data while at it since it's already in memory but before
    # storing it in memcache and datastore.
    raw_data = hash_content
    if namespace.endswith('-gzip'):
      try:
        raw_data = zlib.decompress(hash_content)
      except zlib.error as e:
        logging.error(e)
        self.abort(400, str(e))

    if hashlib.sha1(raw_data).hexdigest() != hash_key:
      msg = 'SHA-1 and data do not match'
      logging.error(msg)
      self.abort(400, msg)

    if priority == 0:
      try:
        # TODO(maruel): Use namespace='table_%s' % namespace.
        if memcache.set(hash_key, hash_content, namespace=namespace):
          logging.info(
              'Storing %d bytes of content in memcache', len(hash_content))
        else:
          logging.error(
              'Attempted to save %d bytes of content in memcache but failed',
              len(hash_content))
      except ValueError as e:
        logging.error(e)

    if len(hash_content) < MIN_SIZE_FOR_BLOBSTORE:
      logging.info('Storing %d bytes of content in model', len(hash_content))
      hash_entry.hash_content = hash_content
      # TODO(maruel): Add a new parameter.
      hash_entry.is_isolated = (priority == 0)
    else:
      logging.info(
          'Storing %d bytes of content in blobstore', len(hash_content))
      hash_entry.hash_content_reference = StoreValueInBlobstore(hash_content)
      if not hash_entry.hash_content_reference:
        self.abort(507, detail='Unable to save the hash to the blobstore.')

    hash_entry.is_verified = True
    hash_entry.put()
    self.response.out.write('hash content saved.')


class RemoveContentByHashHandler(ACLRequestHandler):
  """Removes hash contents from the server."""
  def post(self, namespace, hash_key):
    hash_entry = GetContentByHash(namespace, hash_key)
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
  def get(self, namespace, hash_key):
    # TODO(maruel): Use namespace='table_%s' % namespace.
    memcache_entry = memcache.get(hash_key, namespace=namespace)

    if memcache_entry:
      logging.info('Returning %d bytes from memcache', len(memcache_entry))
      self.response.out.write(memcache_entry)
      return

    hash_entry = GetContentByHash(namespace, hash_key)
    if not hash_entry:
      msg = 'Unable to find a hash with key \'%s\'.' % hash_key
      self.abort(404, detail=msg)

    if hash_entry.last_access != datetime.date.today():
      hash_entry.last_access = datetime.date.today()
      db.put_async(hash_entry)

    if hash_entry.hash_content is None:
      logging.info(
          'Returning %d bytes from blobstore',
          hash_entry.hash_content_reference.size)
      self.send_blob(hash_entry.hash_content_reference, save_as=hash_key)
    else:
      logging.info(
          'Returning %d bytes from model', len(hash_entry.hash_content))
      self.response.headers['Content-Disposition'] = (
          'attachment; filename="%s"' % hash_key)
      self.response.out.write(hash_entry.hash_content)


class RootHandler(webapp2.RequestHandler):
  """Tells the user to RTM."""
  def get(self):
    url = 'http://dev.chromium.org/developers/testing/isolated-testing'
    self.response.write(
        '<html><body>Hi! Please read <a href="%s">%s</a>.</body></html>' %
        (url, url))


def CreateApplication():
  if os.environ['SERVER_SOFTWARE'].startswith('Development') :
    # Add example.com as a valid domain when testing.
    global VALID_DOMAINS
    VALID_DOMAINS = ('example.com',) + VALID_DOMAINS

  # Namespace can be letters and numbers.
  namespace = r'/<namespace:' + VALID_NAMESPACE_RE + '>'
  hashkey = r'/<hash_key:' + VALID_SHA1_RE + '>'
  namespace_key = namespace + hashkey
  return webapp2.WSGIApplication([
      webapp2.Route(
          r'/restricted/cleanup/trigger/<name:[a-z]+>',
          RestrictedCleanupTriggerHandler),
      webapp2.Route(
          r'/restricted/taskqueue/cleanup/old',
          RestrictedCleanupOldEntriesWorkerHandler),
      webapp2.Route(
          r'/restricted/taskqueue/cleanup/testing',
          RestrictedCleanupTestingEntriesWorkerHandler),
      webapp2.Route(
          r'/restricted/taskqueue/cleanup/orphaned',
          RestrictedCleanupOrphanedBlobsWorkerHandler),
      webapp2.Route(
          r'/restricted/taskqueue/cleanup/obliterate',
          RestrictedObliterateWorkerHandler),
      webapp2.Route(
          r'/restricted/taskqueue/tag' + namespace +
            r'/<year:\d\d\d\d>-<month:\d\d>-<day:\d\d>',
          RestrictedTagWorkerHandler),
      webapp2.Route(
          r'/restricted/taskqueue/verify' + namespace_key,
          RestrictedVerifyWorkerHandler),
      webapp2.Route(r'/restricted/whitelist', RestrictedWhitelistHandler),

      webapp2.Route(r'/content/contains' + namespace, ContainsHashHandler),
      webapp2.Route(
          r'/content/generate_blobstore_url' + namespace_key,
          GenerateBlobstoreHandler),
      webapp2.Route(
          r'/content/store' + namespace_key, StoreContentByHashHandler),
      webapp2.Route(
          r'/content/store_blobstore' + namespace_key,
          StoreBlobstoreContentByHashHandler),
      webapp2.Route(
          r'/content/remove' + namespace_key, RemoveContentByHashHandler),
      webapp2.Route(
          r'/content/retrieve' + namespace_key, RetrieveContentByHashHandler),
      webapp2.Route(r'/', RootHandler),
  ])


app = CreateApplication()
