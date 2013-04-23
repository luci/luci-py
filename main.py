# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import binascii
import datetime
import hashlib
import logging
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
from google.appengine.api import taskqueue
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext.webapp import blobstore_handlers
# pylint: enable=E0611,F0401

import acl

# The maximum number of entries that can be queried in a single request.
MAX_KEYS_PER_CALL = 1000

# The minimum size, in bytes, an entry must be before it gets stored in the
# blobstore, otherwise it is stored as a blob property.
MIN_SIZE_FOR_BLOBSTORE = 20 * 1024

# The number of days a datamodel must go unaccessed for before it is deleted.
DATASTORE_TIME_TO_LIVE_IN_DAYS = 7

# The maximum number of items to delete at a time.
ITEMS_TO_DELETE_ASYNC = 100


#### Models


class ContentNamespace(db.Model):
  """Used as an ancestor of ContentEntry to create mutiple content-addressed
  "tables".

  Eventually, the table name could have a prefix to determine the hashing
  algorithm, like 'sha1-'.

  There's usually only one table name:
  - default:    The default CAD.
  - temporary*: This family of namespace is a discardable namespace for testing
                purpose only.

  The table name can have suffix:
  - -gzip:      The namespace contains the content in gzip'ed format. The
                content key is the hash of the uncompressed data, not the
                compressed one. That is why it is in a separate namespace.

  All the tables in the temporary* family must have is_testing==True and the
  others is_testing==False.
  """
  is_testing = db.BooleanProperty()
  creation = db.DateTimeProperty(auto_now=True)


class ContentEntry(db.Model):
  """Represents the content, keyed by its SHA-1 hash."""
  # The reference pointing to the content, which is stored inside the
  # blobstore. This is only valid if the content was at least
  # MIN_SIZE_FOR_BLOBSTORE in size.
  content_reference = blobstore.BlobReferenceProperty()

  # The content stored inline. This is only valid if the content was smaller
  # than MIN_SIZE_FOR_BLOBSTORE.
  content = db.BlobProperty()

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


def GetContentNamespaceKey(namespace):
  """Returns the ContentNamespace key for the namespace value.

  Makes sure the entity exists in the datastore.
  memcache.
  """
  return ContentNamespace.get_or_insert(
      namespace, is_testing=namespace.startswith('temporary')).key()


def GetContentByHash(namespace, hash_key):
  """Returns the ContentEntry with the given hex encoded SHA-1 hash |hash_key|.

  Returns None if it no ContentEntry matches.
  """
  length = GetHashAlgo(namespace).digest_size * 2
  if not re.match(r'[a-f0-9]{' + str(length) + r'}', hash_key):
    logging.error('Given an invalid key, %s', hash_key)
    return None

  try:
    namespace_model_key = GetContentNamespaceKey(namespace)
    key = db.Key.from_path('ContentEntry', hash_key, parent=namespace_model_key)
    return ContentEntry.get(key)
  except (db.BadKeyError, db.KindError):
    pass

  return None


### Utility


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


def CreateEntry(namespace, hash_key):
  """Generates a new ContentEntry from the request if one doesn't exist.

  Returns None if there is a problem generating the entry or if an entry already
  exists with the given hex encoded SHA-1 hash |hash_key|.
  """
  length = GetHashAlgo(namespace).digest_size * 2
  if not re.match(r'[a-f0-9]{' + str(length) + r'}', hash_key):
    logging.error('Given an invalid key, %s', hash_key)
    return None

  namespace_model_key = GetContentNamespaceKey(namespace)
  key = db.Key.from_path('ContentEntry', hash_key, parent=namespace_model_key)

  if ContentEntry.all(keys_only=True).filter('__key__ =', key).get():
    return None
  return ContentEntry(key=key)


def delete_entry_and_blobs(to_delete):
  """Deletes ContentEntry and their blobs."""
  # Note all the blobs to delete first.
  blobs_to_delete = [
      i.content_reference for i in to_delete if i.content_reference
  ]
  DeleteBlobinfoAsync(blobs_to_delete)
  # Then delete the entities.
  db.delete_async(to_delete)


def GetHashAlgo(_namespace):
  """Returns an instance of the hashing algorithm for the namespace."""
  # TODO(maruel): Support other algorithms.
  return hashlib.sha1()


def SplitPayload(request, chunk_size, max_chunks):
  """Splits a binary payload into elements of |chunk_size| length.

  Returns each chunks.
  """
  content = request.request.body
  if len(content) % chunk_size:
    msg = (
        'Payload must be in increments of %d bytes, had %d bytes total, last '
        'chunk was of length %d' % (
              chunk_size,
              len(content),
              len(content) % chunk_size))
    logging.error(msg)
    request.abort(400, detail=msg)

  count = len(content) / chunk_size
  if count > max_chunks:
    msg = (
        'Requested more than %d hash digests in a single request, '
        'aborting' % count)
    logging.warning(msg)
    request.abort(400, detail=msg)

  return [content[i * chunk_size: (i + 1) * chunk_size] for i in xrange(count)]


def PayloadToHashes(request, namespace):
  """Converts a raw payload into SHA-1 hashes as bytes."""
  return SplitPayload(
      request, GetHashAlgo(namespace).digest_size, MAX_KEYS_PER_CALL)


def ReadBlob(blob, callback):
  """Reads a BlobInfo/BlobKey and pass the data through callback.

  Returns the amount of data read.
  """
  position = 0
  chunk_size = blobstore.MAX_BLOB_FETCH_SIZE
  while True:
    try:
      data = blobstore.fetch_data(blob, position, position + chunk_size - 1)
    except (blobstore.InternalError, taskqueue.InternalError) as e:
      logging.warning('Exception while reading blobstore data, retrying\n%s', e)
      continue
    logging.debug('Read %d bytes', len(data))
    callback(data)
    position += len(data)
    if len(data) < chunk_size:
      break
    # Make sure it's unallocated. Otherwise the python subsystem could keep the
    # data in the heap for a while waiting for the next GC but the AppEngine
    # subsystem could decide that this instance is using too much memory.
    # Deleting this 1mb chunk explicitly helps work around this issue.
    del data
  return position


def DeleteBlobinfoAsync(blobinfos):
  """Deletes BlobInfo properly.

  blobstore.delete*() do not accept a list of BlobInfo, they only accept a list
  BlobKey.
  """
  blobstore.delete_async((b.key() for b in blobinfos))


def IncrementalDelete(query, delete, check=None):
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


### Restricted handlers


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

    IncrementalDelete(
        ContentEntry.all().filter('last_access <', old_cutoff),
        delete=delete_entry_and_blobs)
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
      found = IncrementalDelete(
          ContentEntry.all(keys_only=True).ancestor(
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
      return not ContentEntry.gql(
          'WHERE content_reference = :1', blob_info.key()).count(limit=1)

    while True:
      try:
        IncrementalDelete(
            blobstore_query, check=check, delete=DeleteBlobinfoAsync)
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
    IncrementalDelete(
        blobstore.BlobInfo.all().order('creation'),
        DeleteBlobinfoAsync)

    logging.info('Deleting ContentEntry')
    IncrementalDelete(
        ContentEntry.all(keys_only=True).order('creation'),
        db.delete_async)

    logging.info('Deleting Namespaces')
    IncrementalDelete(
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
    raw_hash_digests = PayloadToHashes(self, namespace)
    logging.info(
        'Stamping %d entries in namespace %s', len(raw_hash_digests), namespace)

    today = datetime.date(int(year), int(month), int(day))
    parent_key = GetContentNamespaceKey(namespace)
    to_save = []
    for raw_hash_digest in raw_hash_digests:
      hash_digest = binascii.hexlify(raw_hash_digest)
      key = db.Key.from_path('ContentEntry', hash_digest, parent=parent_key)
      item = ContentEntry.get(key)
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

    entry = GetContentByHash(namespace, hash_key)
    if not entry:
      logging.error('Failed to find entity')
      return
    if entry.is_verified:
      logging.info('Was already verified')
      return
    if not entry.content_reference or entry.content:
      logging.error('Should not be called with inline content')
      return

    digest = GetHashAlgo(namespace)
    if namespace.endswith('-gzip'):
      # Decompress before hashing.
      zlib_state = zlib.decompressobj()
      def gzip_decompress(data):
        decompressed_data = zlib_state.decompress(data)
        digest.update(decompressed_data)
        # Make sure the memory is unallocated.
        del decompressed_data
      callback = gzip_decompress
    else:
      callback = digest.update

    is_verified = False
    count = 0
    try:
      # Start a loop where it reads the data in block.
      count = ReadBlob(entry.content_reference, callback)

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
      blobstore.delete(entry.content_reference.key())
      db.delete(entry)
    else:
      logging.info('%d bytes verified', count)
      entry.is_verified = True
      entry.put()


### Non-restricted handlers


class ContainsHashHandler(acl.ACLRequestHandler):
  """Returns the presence of each hash key in the payload as a binary string.

  For each SHA-1 hash key in the request body in binary form, a corresponding
  chr(1) or chr(0) is in the 'string' returned.
  """
  def post(self, namespace):
    """This is a POST even though it doesn't modify any data[1], but it makes
    it easier for python scripts.

    [1] It does modify the timestamp of the objects.
    """
    raw_hash_digests = PayloadToHashes(self, namespace)
    logging.info(
        'Checking namespace %s for %d hash digests',
        namespace, len(raw_hash_digests))
    namespace_model_key = GetContentNamespaceKey(namespace)

    # Convert to entity keys.
    keys = (
        db.Key.from_path(
            'ContentEntry',
            binascii.hexlify(raw_hash_digest),
            parent=namespace_model_key)
        for raw_hash_digest in raw_hash_digests
    )

    # Start the queries in parallel. It must be a list so the calls are executed
    # right away.
    queries = [
        ContentEntry.all().filter('__key__ =', key).run(
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
    logging.info('%d hit, %d miss', found, len(raw_hash_digests) - found)
    if found:
      # For all the ones that exist, update their last_access in a task queue.
      hashes_to_tag = ''.join(
          raw_hash_digest for i, raw_hash_digest in enumerate(raw_hash_digests)
          if contains[i])
      url = '/restricted/taskqueue/tag/%s/%s' % (
          namespace, datetime.date.today())
      try:
        taskqueue.add(url=url, payload=hashes_to_tag, queue_name='tag')
      except (taskqueue.Error, runtime.DeadlineExceededError) as e:
        logging.warning('Problem adding task to update last_access. These '
                        'objects may get deleted sooner than intended.\n%s', e)


class GenerateBlobstoreHandler(acl.ACLRequestHandler):
  """Generate an upload url to directly load files into the blobstore."""
  def post(self, namespace, hash_key):
    self.response.headers['Content-Type'] = 'text/plain'
    url = '/content/store_blobstore/%s/%s/%d/%s?token=%s' % (
        namespace, hash_key, int(self.is_user), self.access_id, self.token)
    logging.info('Url: %s', url)
    self.response.out.write(blobstore.create_upload_url(url))


class StoreBlobstoreContentByHashHandler(
    acl.ACLRequestHandler,
    blobstore_handlers.BlobstoreUploadHandler):
  """Assigns the newly stored blobstore entry to the correct hash key."""

  # Requires special processing.
  enforce_token_on_post = False

  def dispatch(self):
    """Disable ACLRequestHandler.dispatch() checks here because the originating
    IP is always an AppEngine IP, which confuses the authentication code.
    """
    return webapp2.RequestHandler.dispatch(self)

  # pylint: disable=W0221
  def post(self, namespace, hash_key, is_user, original_access_id):
    # In particular, do not use self.request.remote_addr because the request
    # has as original an AppEngine local IP.
    if is_user == '1':
      self.check_user_id(original_access_id, None)
    else:
      self.check_ip(original_access_id)
    self.enforce_valid_token()
    contents = self.get_uploads('content')
    if not contents:
      # TODO(maruel): Remove, only kept for short term compatibility.
      contents = self.get_uploads('hash_contents')
    if len(contents) != 1:
      # Delete all upload files since they aren't linked to anything.
      DeleteBlobinfoAsync(contents)
      msg = 'Found %d files, there should only be 1.' % len(contents)
      self.abort(400, detail=msg)

    entry = CreateEntry(namespace, hash_key)
    if not entry:
      msg = 'Hash entry already stored, no need to store %d bytes again.' % (
          contents[0].size)
      logging.warning(msg)
      self.response.out.write(msg)
      # Delete all upload files since they aren't linked to anything.
      DeleteBlobinfoAsync(contents)
      return

    try:
      priority = int(self.request.get('priority'))
    except ValueError:
      priority = 1

    entry.content_reference = contents[0]
    # TODO(maruel): Add a new parameter.
    entry.is_isolated = (priority == 0)
    entry.put()

    if entry.content_reference.size < MIN_SIZE_FOR_BLOBSTORE:
      logging.error(
          'User stored a file too small %d in blobstore, fix client code.',
          entry.content_reference.size)

    # Trigger a verification. It can't be done inline since it could be too
    # long to complete.
    url = '/restricted/taskqueue/verify/%s/%s' % (namespace, hash_key)
    try:
      taskqueue.add(url=url, queue_name='verify')
    except runtime.DeadlineExceededError as e:
      msg = 'Unable to add task to verify blob.\n%s' % e
      logging.warning(msg)
      self.response.out.write(msg)
      self.response.set_status(500)

      delete_entry_and_blobs([entry])
      return

    logging.info(
        '%d bytes uploaded directly into blobstore',
        entry.content_reference.size)
    self.response.out.write('Content saved.')


class StoreContentByHashHandler(acl.ACLRequestHandler):
  """The handler for adding content."""
  def post(self, namespace, hash_key):
    # webapp2 doesn't like reading the body if it's empty.
    if self.request.headers.get('content-length'):
      content = self.request.body
    else:
      content = ''

    entry = CreateEntry(namespace, hash_key)
    if not entry:
      msg = 'Hash entry already stored, no need to store %d bytes again.' % (
          len(content))
      logging.info(msg)
      self.response.out.write(msg)
      return

    try:
      priority = int(self.request.get('priority'))
    except ValueError:
      priority = 1

    # Verify the data while at it since it's already in memory but before
    # storing it in memcache and datastore.
    raw_data = content
    if namespace.endswith('-gzip'):
      try:
        raw_data = zlib.decompress(content)
      except zlib.error as e:
        logging.error(e)
        self.abort(400, str(e))

    digest = GetHashAlgo(namespace)
    digest.update(raw_data)
    if digest.hexdigest() != hash_key:
      msg = 'SHA-1 and data do not match'
      logging.error(msg)
      self.abort(400, msg)

    if priority == 0:
      try:
        # TODO(maruel): Use namespace='table_%s' % namespace.
        if memcache.set(hash_key, content, namespace=namespace):
          logging.info(
              'Storing %d bytes of content in memcache', len(content))
        else:
          logging.error(
              'Attempted to save %d bytes of content in memcache but failed',
              len(content))
      except ValueError as e:
        logging.error(e)

    if len(content) < MIN_SIZE_FOR_BLOBSTORE:
      logging.info('Storing %d bytes of content in model', len(content))
      entry.content = content
      # TODO(maruel): Add a new parameter.
      entry.is_isolated = (priority == 0)
    else:
      logging.info(
          'Storing %d bytes of content in blobstore', len(content))
      entry.content_reference = StoreValueInBlobstore(content)
      if not entry.content_reference:
        self.abort(507, detail='Unable to save the content to the blobstore.')

    entry.is_verified = True
    entry.put()
    self.response.out.write('Content saved.')


class RemoveContentByHashHandler(acl.ACLRequestHandler):
  """Removes content by its SHA-1 hash key from the server."""
  def post(self, namespace, hash_key):
    entry = GetContentByHash(namespace, hash_key)
    # TODO(maruel): Use namespace='table_%s' % namespace.
    memcache.delete(hash_key, namespace=namespace)

    if not entry:
      msg = 'Unable to find a ContentEntry with key \'%s\'.' % hash_key
      logging.info(msg)

      self.response.out.write(msg)
      return

    entry.delete()
    logging.info('Deleted ContentEntry')


class RetrieveContentByHashHandler(acl.ACLRequestHandler,
                                   blobstore_handlers.BlobstoreDownloadHandler):
  """The handlers for retrieving contents by its SHA-1 hash |hash_key|."""
  def get(self, namespace, hash_key):  #pylint: disable=W0221
    # TODO(maruel): Use namespace='table_%s' % namespace.
    memcache_entry = memcache.get(hash_key, namespace=namespace)

    if memcache_entry:
      logging.info('Returning %d bytes from memcache', len(memcache_entry))
      self.response.out.write(memcache_entry)
      return

    entry = GetContentByHash(namespace, hash_key)
    if not entry:
      msg = 'Unable to find an ContentEntry with key \'%s\'.' % hash_key
      self.abort(404, detail=msg)

    if entry.content is None:
      logging.info(
          'Returning %d bytes from blobstore', entry.content_reference.size)
      self.send_blob(entry.content_reference, save_as=hash_key)
    else:
      logging.info('Returning %d bytes from model', len(entry.content))
      self.response.headers['Content-Disposition'] = (
          'attachment; filename="%s"' % hash_key)
      self.response.out.write(entry.content)


class RootHandler(webapp2.RequestHandler):
  """Tells the user to RTM."""
  def get(self):
    url = 'http://dev.chromium.org/developers/testing/isolated-testing'
    self.response.write(
        '<html><body>Hi! Please read <a href="%s">%s</a>.</body></html>' %
        (url, url))


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    self.response.write('ok')


def CreateApplication():
  acl.bootstrap()

  # Namespace can be letters and numbers.
  namespace = r'/<namespace:[a-z0-9A-Z\-]+>'
  # Do not enforce a length limit.
  hashkey = r'/<hash_key:[a-f0-9]{4,}>'
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
      webapp2.Route(
          r'/restricted/whitelistip', acl.RestrictedWhitelistIPHandler),
      webapp2.Route(
          r'/restricted/whitelistdomain', acl.RestrictedWhitelistDomainHandler),

      webapp2.Route(
          r'/content/contains' + namespace,
          ContainsHashHandler),
      webapp2.Route(
          r'/content/generate_blobstore_url' + namespace_key,
          GenerateBlobstoreHandler),
      webapp2.Route(r'/content/get_token', acl.GetTokenHandler),
      webapp2.Route(
          r'/content/store' + namespace_key, StoreContentByHashHandler),
      webapp2.Route(
          r'/content/store_blobstore' + namespace_key +
            r'/<is_user:[01]>/<original_access_id:[^\/]+>',
          StoreBlobstoreContentByHashHandler),
      webapp2.Route(
          r'/content/remove' + namespace_key, RemoveContentByHashHandler),
      webapp2.Route(
          r'/content/retrieve' + namespace_key, RetrieveContentByHashHandler),
      webapp2.Route(r'/', RootHandler),

      webapp2.Route(r'/_ah/warmup', WarmupHandler),
  ])


app = CreateApplication()
