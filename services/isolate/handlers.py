# Copyright 2012 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines all isolateserver url handlers."""

import binascii
import collections
import datetime
import functools
import hashlib
import hmac
import json
import logging
import random
import re
import time
import urllib
import zlib

import webapp2
from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

import acl
import config
import gcs
import map_reduce_jobs
import stats
import template
from components import ereporter2
from components import auth
from components import auth_ui
from components import sharding
from components import utils


# Version of isolate protocol returned to clients in /handshake request.
ISOLATE_PROTOCOL_VERSION = '1.0'


# The maximum number of entries that can be queried in a single request.
MAX_KEYS_PER_CALL = 1000

# The minimum size, in bytes, an entry must be before it gets stored in Google
# Cloud Storage, otherwise it is stored as a blob property.
MIN_SIZE_FOR_GS = 501

# The minimum size, in bytes, for entry that get's uploaded directly to Google
# Cloud Storage, bypassing App engine layer.
# This effectively disable inline upload. This is because urlfetch is too flaky
# in practice so it is not worth the associated downtime.
MIN_SIZE_FOR_DIRECT_GS = MIN_SIZE_FOR_GS

# The maximum number of items to delete at a time.
ITEMS_TO_DELETE_ASYNC = 100

# Limit the namespace to 29 characters because app engine seems unable to
# find the blobs in cloud storage if the namespace is longer.
# TODO(csharp): Find a way to support namespaces greater than 29 characters.
MAX_NAMESPACE_LEN = 29

# Maximum size of file stored in GS to be saved in memcache. The value must be
# small enough so that the whole content can safely fit in memory.
MAX_MEMCACHE_ISOLATED = 500*1024


# Ignore these failures, there's nothing to do.
# TODO(maruel): Store them in the db and make this runtime configurable.
IGNORED_LINES = (
  # And..?
  '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
      'appengine/_internal/django/template/__init__.py:729: UserWarning: '
      'api_milliseconds does not return a meaningful value',
)

# Ignore these exceptions.
IGNORED_EXCEPTIONS = (
  'CancelledError',
  'DeadlineExceededError',
  # These are DeadlineExceededError wrapped up by
  # third_party/cloudstorage/storage_api.py.
  'TimeoutError',
  ereporter2.SOFT_MEMORY,
)


# Valid namespace key.
NAMESPACE_RE = r'[a-z0-9A-Z\-._]+'

# Valid hash keys.
HASH_LETTERS = set('0123456789abcdef')


#### Models


class ContentEntry(ndb.Model):
  """Represents the content, keyed by its SHA-1 hash.

  Parent is a ContentShard.

  Key is '<namespace>-<hash>'.

  Eventually, the table name could have a prefix to determine the hashing
  algorithm, like 'sha1-'.

  There's usually only one table name:
    - default:    The default CAD.
    - temporary*: This family of namespace is a discardable namespace for
                  testing purpose only.

  The table name can have suffix:
    - -deflate: The namespace contains the content in deflated format. The
                content key is the hash of the uncompressed data, not the
                compressed one. That is why it is in a separate namespace.
  """
  # Cache the file size for statistics purposes.
  compressed_size = ndb.IntegerProperty(indexed=False)

  # The value is the Cache the expanded file size for statistics purposes. Its
  # value is different from size *only* in compressed namespaces. It may be -1
  # if yet unknown.
  expanded_size = ndb.IntegerProperty(indexed=False)

  # Set to True once the entry's content has been verified to match the hash.
  is_verified = ndb.BooleanProperty()

  # The content stored inline. This is only valid if the content was smaller
  # than MIN_SIZE_FOR_GS.
  content = ndb.BlobProperty()

  # Moment when this item expires and should be cleared. This is the only
  # property that has to be indexed.
  expiration_ts = ndb.DateTimeProperty()

  # Moment when this item should have its expiration time updatd.
  next_tag_ts = ndb.DateTimeProperty()

  # Moment when this item was created.
  creation_ts = ndb.DateTimeProperty(indexed=False, auto_now=True)

  # It is an important item, normally .isolated file.
  is_isolated = ndb.BooleanProperty(default=False)

  @property
  def is_compressed(self):
    """Is it the raw data or was it modified in any form, e.g. compressed, so
    that the SHA-1 doesn't match.
    """
    return self.key.parent().id().endswith(('-bzip2', '-deflate', '-gzip'))


### Utility


class Accumulator(object):
  """Accumulates output from a generator."""
  def __init__(self, source):
    self.accumulated = []
    self._source = source

  def __iter__(self):
    for i in self._source:
      self.accumulated.append(i)
      yield i
      del i


def utcnow():
  """Returns a datetime, used for testing."""
  return datetime.datetime.utcnow()


def check_hash(hash_key, length):
  """Checks the validity of an hash_key. Doesn't use a regexp for speed.

  Raises in case of non-validity.
  """
  # It is faster than running a regexp.
  if len(hash_key) != length or not HASH_LETTERS.issuperset(hash_key):
    raise ValueError('Invalid \'%s\' as ContentEntry key' % hash_key)


def entry_key(namespace, hash_key):
  """Returns a valid ndb.Key for a ContentEntry."""
  check_hash(hash_key, get_hash_algo(namespace).digest_size * 2)
  return entry_key_from_id('%s/%s' % (namespace, hash_key))


def entry_key_from_id(key_id):
  """Returns the ndb.Key for the key_id."""
  hash_key = key_id.rsplit('/', 1)[1]
  N = config.settings().sharding_letters
  return ndb.Key(
      ContentEntry, key_id,
      parent=sharding.shard_key(hash_key, N, 'ContentShard'))


def create_entry(key):
  """Generates a new ContentEntry from the request if one doesn't exist.

  This function is synchronous.

  Returns None if there is a problem generating the entry or if an entry already
  exists with the given hex encoded SHA-1 hash |hash_key|.
  """
  # Entity was present, can't insert.
  if key.get():
    return None
  # Entity was not present. Create a new one.
  expiration, next_tag = expiration_jitter(
      utcnow(), config.settings().default_expiration)
  return ContentEntry(key=key, expiration_ts=expiration, next_tag_ts=next_tag)


def expiration_jitter(now, expiration):
  """Returns expiration/next_tag pair to set in a ContentEntry."""
  jittered = random.uniform(1, 1.2) * expiration
  expiration = now + datetime.timedelta(seconds=jittered)
  next_tag = now + datetime.timedelta(seconds=jittered*0.1)
  return expiration, next_tag


def delete_entry_and_gs_entry(keys_to_delete):
  """Deletes synchronously a list of ContentEntry and their GS files.

  It deletes the ContentEntry first, then the files in GS. The worst case is
  that the GS files are left behind and will be reaped by a lost GS task
  queue. The reverse is much worse, having a ContentEntry pointing to a
  deleted GS entry will lead to lookup failures.
  """
  try:
    # Always delete ContentEntry first.
    ndb.delete_multi(keys_to_delete)
    # Deleting files that do not exist is not a problem.
    gcs.delete_files(
        config.settings().gs_bucket, (i.id() for i in keys_to_delete))
    return []
  except:
    logging.error('Failed to delete items! DB will be inconsistent!')
    raise


def get_hash_algo(_namespace):
  """Returns an instance of the hashing algorithm for the namespace."""
  # TODO(maruel): Support other algorithms.
  return hashlib.sha1()


def split_payload(request, chunk_size, max_chunks):
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


def payload_to_hashes(request, namespace):
  """Converts a raw payload into SHA-1 hashes as bytes."""
  return split_payload(
      request, get_hash_algo(namespace).digest_size, MAX_KEYS_PER_CALL)


def expand_content(namespace, source):
  """Yields expanded data from source."""
  # TODO(maruel): Add bzip2.
  # TODO(maruel): Remove '-gzip' since it's a misnomer.
  if namespace.endswith(('-deflate', '-gzip')):
    zlib_state = zlib.decompressobj()
    for i in source:
      data = zlib_state.decompress(i, gcs.CHUNK_SIZE)
      yield data
      del data
      while zlib_state.unconsumed_tail:
        data = zlib_state.decompress(
            zlib_state.unconsumed_tail, gcs.CHUNK_SIZE)
        yield data
        del data
      del i
    data = zlib_state.flush()
    yield data
    del data
    # Forcibly delete the state.
    del zlib_state
  else:
    # Returns the source as-is.
    for i in source:
      yield i
      del i


def hash_content(content, namespace):
  """Decompresses and hashes given |content|.

  Returns tuple (hex digest, expanded size).

  Raises ValueError in case of errors.
  """
  expanded_size = 0
  digest = get_hash_algo(namespace)
  try:
    for data in expand_content(namespace, [content]):
      expanded_size += len(data)
      digest.update(data)
      # Make sure the data is GC'ed.
      del data
    return digest.hexdigest(), expanded_size
  except zlib.error as e:
    raise ValueError('Data is corrupted: %s' % e)


def incremental_delete(query, delete, check=None):
  """Applies |delete| to objects in a query asynchrously.

  This function is itself synchronous.

  Arguments:
  - query: iterator of items to process.
  - delete: callback that accepts a list of objects to delete and returns a list
            of objects that have a method .wait() to make sure all calls
            completed.
  - check: optional callback that can filter out items from |query| from
          deletion.

  Returns the number of objects found.
  """
  to_delete = []
  count = 0
  deleted_count = 0
  futures = []
  for item in query:
    count += 1
    if not (count % 1000):
      logging.debug('Found %d items', count)
    if check and not check(item):
      continue
    to_delete.append(item)
    deleted_count += 1
    if len(to_delete) == ITEMS_TO_DELETE_ASYNC:
      logging.info('Deleting %s entries', len(to_delete))
      futures.extend(delete(to_delete))
      to_delete = []

    # That's a lot of on-going operations which could take a significant amount
    # of memory. Wait a little on the oldest operations.
    # TODO(maruel): Profile memory usage to see if a few thousands of on-going
    # RPC objects is a problem in practice.
    while len(futures) > 10 * ITEMS_TO_DELETE_ASYNC:
      futures.pop(0).wait()

  if to_delete:
    logging.info('Deleting %s entries', len(to_delete))
    futures.extend(delete(to_delete))

  ndb.Future.wait_all(futures)
  return deleted_count


def save_in_memcache(namespace, hash_key, content, async=False):
  namespace_key = 'table_%s' % namespace
  if async:
    return ndb.get_context().memcache_set(
        hash_key, content, namespace=namespace_key)
  try:
    if not memcache.set(hash_key, content, namespace=namespace_key):
      msg = 'Failed to save content to memcache.\n%s\\%s %d bytes' % (
          namespace_key, hash_key, len(content))
      if len(content) < 100*1024:
        logging.error(msg)
      else:
        logging.warning(msg)
  except ValueError as e:
    logging.error(e)


def enqueue_task(url, queue_name, payload=None, name=None,
                 use_dedicated_module=True):
  """Adds a task to a task queue.

  If |use_dedicated_module| is True (default) a task will be executed by
  a separate backend module instance that runs same version as currently
  executing instance. Otherwise it will run on a current version of default
  module.

  Returns True if a task was successfully added, logs error and returns False
  if task queue is acting up.
  """
  try:
    headers = None
    if use_dedicated_module:
      headers = {'Host': config.get_task_queue_host()}
    # Note that just using 'target=module' here would redirect task request to
    # a default version of a module, not the currently executing one.
    taskqueue.add(
        url=url,
        queue_name=queue_name,
        payload=payload,
        name=name,
        headers=headers)
    return True
  except (
      taskqueue.Error,
      runtime.DeadlineExceededError,
      runtime.apiproxy_errors.CancelledError,
      runtime.apiproxy_errors.DeadlineExceededError,
      runtime.apiproxy_errors.OverQuotaError) as e:
    logging.warning(
        'Problem adding task \'%s\' to task queue \'%s\' (%s): %s',
        url, queue_name, e.__class__.__name__, e)
    return False


def render_template(template_path, env=None):
  """Renders a template with some common variables in template env.

  Should be used for templates that extend base.html.

  Parameters:
    template_path: path to a template relative to templates/.
    env: a dict that will be added to default template environment.

  Returns:
    Rendered template as str.
  """
  default_env = {
      'app_revision_url': config.get_app_revision_url(),
      'app_version': config.get_app_version(),
  }
  if env:
    default_env.update(env)
  return template.get(template_path).render(default_env)


should_ignore_error_record = functools.partial(
    ereporter2.should_ignore_error_record, IGNORED_LINES, IGNORED_EXCEPTIONS)


### Restricted handlers


class InternalCleanupOldEntriesWorkerHandler(webapp2.RequestHandler):
  """Removes the old data from the datastore.

  Only a task queue task can use this handler.
  """
  def post(self):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    total = incremental_delete(
        ContentEntry.query(
            ContentEntry.expiration_ts < utcnow()).iter(keys_only=True),
        delete=delete_entry_and_gs_entry)
    logging.info('Deleting %s expired entries', total)


class InternalObliterateWorkerHandler(webapp2.RequestHandler):
  """Deletes all the stuff."""
  def post(self):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    logging.info('Deleting ContentEntry')
    incremental_delete(
        ContentEntry.query().iter(keys_only=True), ndb.delete_multi_async)

    gs_bucket = config.settings().gs_bucket
    logging.info('Deleting GS bucket %s', gs_bucket)
    incremental_delete(
        (i[0] for i in gcs.list_files(gs_bucket)),
        lambda filenames: gcs.delete_files(gs_bucket, filenames))

    logging.info('Flushing memcache')
    # High priority (.isolated files) are cached explicitly. Make sure ghosts
    # are zapped too.
    memcache.flush_all()
    logging.info('Finally done!')


class InternalCleanupTrimLostWorkerHandler(webapp2.RequestHandler):
  """Removes the GS files that are not referenced anymore.

  It can happen for example when a ContentEntry is deleted without the file
  properly deleted.

  Only a task queue task can use this handler.
  """
  def post(self):
    """Enumerates all GS files and delete those that do not have an associated
    ContentEntry.
    """
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    gs_bucket = config.settings().gs_bucket

    def filter_missing():
      futures = {}
      cutoff = time.time() - 60*60
      for filepath, filestats in gcs.list_files(gs_bucket):
        # If the file was uploaded in the last hour, ignore it.
        if filestats.st_ctime >= cutoff:
          continue

        # This must match the logic in entry_key(). Since this request will in
        # practice touch every item, do not use memcache since it'll mess it up
        # by loading every items in it.
        # TODO(maruel): Batch requests to use get_multi_async() similar to
        # component_utils.page_queries().
        future = entry_key_from_id(filepath).get_async(
            use_cache=False, use_memcache=False)
        futures[future] = filepath

        if len(futures) > 20:
          future = ndb.Future.wait_any(futures)
          filepath = futures.pop(future)
          if future.get_result():
            continue
          yield filepath
      while futures:
        future = ndb.Future.wait_any(futures)
        filepath = futures.pop(future)
        if future.get_result():
          continue
        yield filepath

    gs_delete = lambda filenames: gcs.delete_files(gs_bucket, filenames)
    total = incremental_delete(filter_missing(), gs_delete)
    logging.info('Deleted %d lost GS files', total)
    # TODO(maruel): Find all the empty directories that are old and remove them.
    # We need to safe guard against the race condition where a user would upload
    # to this directory.


class InternalCleanupTriggerHandler(webapp2.RequestHandler):
  """Triggers a taskqueue to clean up."""
  def get(self, name):
    if name in ('obliterate', 'old', 'orphaned', 'trim_lost'):
      url = '/internal/taskqueue/cleanup/' + name
      # The push task queue name must be unique over a ~7 days period so use
      # the date at second precision, there's no point in triggering each of
      # time more than once a second anyway.
      now = utcnow().strftime('%Y-%m-%d_%I-%M-%S')
      if enqueue_task(url, 'cleanup', name=name + '_' + now):
        self.response.out.write('Triggered %s' % url)
      else:
        self.abort(500, 'Failed to enqueue a cleanup task, see logs')
    else:
      self.abort(404, 'Unknown job')


class InternalTagWorkerHandler(webapp2.RequestHandler):
  """Tags .expiration_ts and .next_tag_ts for ContentEntry tested for with
  /content/contains.

  This makes sure they are not evicted from the LRU cache too fast.
  """
  def post(self, namespace, timestamp):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')

    digests = []
    now = utils.timestamp_to_datetime(long(timestamp))
    expiration = config.settings().default_expiration
    try:
      digests = payload_to_hashes(self, namespace)
      # Requests all the entities at once.
      futures = ndb.get_multi_async(
          entry_key(namespace, binascii.hexlify(d)) for d in digests)

      to_save = []
      while futures:
        # Return opportunistically the first entity that can be retrieved.
        future = ndb.Future.wait_any(futures)
        futures.remove(future)
        item = future.get_result()
        if item and item.next_tag_ts < now:
          # Update the timestamp. Add a bit of pseudo randomness.
          item.expiration_ts, item.next_tag_ts = expiration_jitter(
              now, expiration)
          to_save.append(item)
      if to_save:
        ndb.put_multi(to_save)
      logging.info(
          'Timestamped %d entries out of %s', len(to_save), len(digests))
    except (
        datastore_errors.InternalError,
        datastore_errors.Timeout,
        datastore_errors.TransactionFailedError,
        runtime.DeadlineExceededError) as e:
      # No need to print a stack trace. Return 500 so it is retried
      # automatically. Disable this from an error reporting standpoint because
      # we can't do anything about it.
      # TODO(maruel): Split the request in smaller chunk, since it seems to
      # timeout frequently.
      logging.warning('Failed to stamp %d entries: %s', len(digests), e)
      self.abort(500, detail='Timed out while tagging.')
    except Exception as e:
      logging.error('Failed to stamp %d entries: %s', len(digests), e)
      raise


class InternalVerifyWorkerHandler(webapp2.RequestHandler):
  """Verify the SHA-1 matches for an object stored in Cloud Storage."""

  @staticmethod
  def purge_entry(entry, message, *args):
    """Logs error message, deletes |entry| from datastore and GS."""
    logging.error(
        'Verification failed for %s: %s', entry.key.id(), message % args)
    ndb.Future.wait_all(delete_entry_and_gs_entry([entry.key]))

  def post(self, namespace, hash_key):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')

    entry = entry_key(namespace, hash_key).get()
    if not entry:
      logging.error('Failed to find entity')
      return
    if entry.is_verified:
      logging.warning('Was already verified')
      return
    if entry.content is not None:
      logging.error('Should not be called with inline content')
      return

    # Get GS file size.
    gs_bucket = config.settings().gs_bucket
    gs_file_info = gcs.get_file_info(gs_bucket, entry.key.id())

    # It's None if file is missing.
    if not gs_file_info:
      # According to the docs, GS is read-after-write consistent, so a file is
      # missing only if it wasn't stored at all or it was deleted, in any case
      # it's not a valid ContentEntry.
      self.purge_entry(entry, 'No such GS file')
      return

    # Expected stored length and actual length should match.
    if gs_file_info.size != entry.compressed_size:
      self.purge_entry(entry,
          'Bad GS file: expected size is %d, actual size is %d',
          entry.compressed_size, gs_file_info.size)
      return

    save_to_memcache = (
        entry.compressed_size <= MAX_MEMCACHE_ISOLATED and entry.is_isolated)
    expanded_size = 0
    digest = get_hash_algo(namespace)
    data = None

    try:
      # Start a loop where it reads the data in block.
      stream = gcs.read_file(gs_bucket, entry.key.id())
      if save_to_memcache:
        # Wraps stream with a generator that accumulates the data.
        stream = Accumulator(stream)

      for data in expand_content(namespace, stream):
        expanded_size += len(data)
        digest.update(data)
        # Make sure the data is GC'ed.
        del data

      # Hashes should match.
      if digest.hexdigest() != hash_key:
        self.purge_entry(entry,
            'SHA-1 do not match data (%d bytes, %d bytes expanded)',
            entry.compressed_size, expanded_size)
        return

    except runtime.DeadlineExceededError:
      # Failed to read it through. If it's compressed, at least no zlib error
      # was thrown so the object is fine.
      logging.warning('Got DeadlineExceededError, giving up')
      # Abort so the job is retried automatically.
      return self.abort(500)

    except gcs.NotFoundError as e:
      # Somebody deleted a file between get_file_info and read_file calls.
      self.purge_entry(entry, 'File was unexpectedly deleted')
      return

    except gcs.TransientError as e:
      # Don't delete the file yet since it's an API issue.
      logging.warning(
          'CloudStorage is acting up (%s): %s', e.__class__.__name__, e)
      # Abort so the job is retried automatically.
      return self.abort(500)

    except (gcs.ForbiddenError, gcs.AuthorizationError) as e:
      # Misconfiguration in Google Storage ACLs. Don't delete an entry, it may
      # be fine. Maybe ACL problems would be fixed before the next retry.
      logging.warning(
          'CloudStorage auth issues (%s): %s', e.__class__.__name__, e)
      # Abort so the job is retried automatically.
      return self.abort(500)

    # ForbiddenError and AuthorizationError inherit FatalError, so this except
    # block should be last.
    except (gcs.FatalError, zlib.error, IOError) as e:
      # It's broken or unreadable.
      self.purge_entry(entry,
          'Failed to read the file (%s): %s', e.__class__.__name__, e)
      return

    # Verified. Data matches the hash.
    entry.expanded_size = expanded_size
    entry.is_verified = True
    future = entry.put_async()
    logging.info(
        '%d bytes (%d bytes expanded) verified',
        entry.compressed_size, expanded_size)
    if save_to_memcache:
      save_in_memcache(namespace, hash_key, ''.join(stream.accumulated))
    future.wait()


class RestrictedAdminUIHandler(auth.AuthenticatingHandler):
  """Root admin UI page."""

  @auth.require(auth.READ, 'isolate/management')
  def get(self):
    self.response.write(render_template('restricted.html', {
        'xsrf_token': self.generate_xsrf_token(),
        'map_reduce_jobs': [
            {'id': job_id, 'name': job_def['name']}
            for job_id, job_def in map_reduce_jobs.MAP_REDUCE_JOBS.iteritems()
        ],
    }))


class RestrictedGoogleStorageConfig(auth.AuthenticatingHandler):
  """View and modify Google Storage config entries."""

  @auth.require(auth.READ, 'isolate/management')
  def get(self):
    settings = config.settings()
    self.response.write(render_template('gs_config.html', {
        'gs_bucket': settings.gs_bucket,
        'gs_client_id_email': settings.gs_client_id_email,
        'gs_private_key': settings.gs_private_key,
        'xsrf_token': self.generate_xsrf_token(),
    }))

  @auth.require(auth.UPDATE, 'isolate/management')
  def post(self):
    settings = config.settings()
    settings.gs_bucket = self.request.get('gs_bucket')
    settings.gs_client_id_email = self.request.get('gs_client_id_email')
    settings.gs_private_key = self.request.get('gs_private_key')
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    try:
      # Ensure key is correct, it's easy to make a mistake when creating it.
      gcs.URLSigner.load_private_key(settings.gs_private_key)
    except Exception as exc:
      self.response.write('Bad private key: %s' % exc)
      return
    # Store the settings.
    settings.put()
    self.response.write('Done!')


class InternalEreporter2Mail(webapp2.RequestHandler):
  """Handler class to generate and email an exception report."""
  def get(self):
    """Sends email(s) containing the errors logged."""
    # Must be run from a cron job.
    if 'true' != self.request.headers.get('X-AppEngine-Cron'):
      self.abort(403, 'Must be a cron request.')

    # Do not use self.request.host_url because it will be http:// and will point
    # to the backend, with an host format that breaks the SSL certificate.
    host_url = 'https://%s.appspot.com' % app_identity.get_application_id()
    request_id_url = host_url + '/restricted/ereporter2/request/'
    report_url = host_url + '/restricted/ereporter2/report'
    recipients = self.request.get(
        'recipients', config.settings().monitoring_recipients)
    result = ereporter2.generate_and_email_report(
        utils.get_module_version_list(None, False),
        should_ignore_error_record,
        recipients,
        request_id_url,
        report_url,
        ereporter2.REPORT_TITLE_TEMPLATE,
        ereporter2.REPORT_CONTENT_TEMPLATE,
        {})
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    if result:
      self.response.write('Success.')
    else:
      # Do not HTTP 500 since we do not want it to be retried.
      self.response.write('Failed.')


class RestrictedEreporter2Report(auth.AuthenticatingHandler):
  """Returns all the recent errors as a web page."""

  @auth.require(auth.READ, 'isolate/management')
  def get(self):
    """Reports the errors logged and ignored.

    Arguments:
      start: epoch time to start looking at. Defaults to the messages since the
             last email.
      end: epoch time to stop looking at. Defaults to now.
      modules: comma separated modules to look at.
      tainted: 0 or 1, specifying if desiring tainted versions. Defaults to 0.
    """
    request_id_url = '/restricted/ereporter2/request/'
    end = int(float(self.request.get('end', 0)) or time.time())
    start = int(
        float(self.request.get('start', 0)) or
        ereporter2.get_default_start_time() or 0)
    modules = self.request.get('modules')
    if modules:
      modules = modules.split(',')
    tainted = bool(int(self.request.get('tainted', '0')))
    module_versions = utils.get_module_version_list(modules, tainted)
    report, ignored = ereporter2.generate_report(
        start, end, module_versions, should_ignore_error_record)
    env = ereporter2.get_template_env(start, end, module_versions)
    content = ereporter2.report_to_html(
        report, ignored,
        ereporter2.REPORT_HEADER_TEMPLATE,
        ereporter2.REPORT_CONTENT_TEMPLATE,
        request_id_url, env)
    out = render_template('ereporter2_report.html', {'content': content})
    self.response.write(out)


class RestrictedEreporter2Request(auth.AuthenticatingHandler):
  """Dumps information about single logged request."""

  @auth.require(auth.READ, 'isolate/management')
  def get(self, request_id):
    # TODO(maruel): Add UI.
    data = ereporter2.log_request_id_to_dict(request_id)
    if not data:
      self.abort(404, detail='Request id was not found.')
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.write(utils.encode_to_json(data))


### Mapreduce related handlers


class RestrictedLaunchMapReduceJob(auth.AuthenticatingHandler):
  """Enqueues a task to start a map reduce job on the backend module.

  A tree of map reduce jobs inherits module and version of a handler that
  launched it. All UI handlers are executes by 'default' module. So to run a
  map reduce on a backend module one needs to pass a request to a task running
  on backend module.
  """

  @auth.require(auth.UPDATE, 'isolate/management')
  def post(self):
    job_id = self.request.get('job_id')
    assert job_id in map_reduce_jobs.MAP_REDUCE_JOBS
    # Do not use 'backend' module when running from dev appserver. Mapreduce
    # generates URLs that are incompatible with dev appserver URL routing when
    # using custom modules.
    success = enqueue_task(
        url='/internal/taskqueue/mapreduce/launch/%s' % job_id,
        queue_name=map_reduce_jobs.MAP_REDUCE_TASK_QUEUE,
        use_dedicated_module=not utils.is_local_dev_server())
    # New tasks should show up on the status page.
    if success:
      self.redirect('/internal/mapreduce/status')
    else:
      self.abort(500, 'Failed to launch the job')


class InternalLaunchMapReduceJobWorkerHandler(webapp2.RequestHandler):
  """Called via task queue or cron to start a map reduce job."""
  def post(self, job_id):
    if not self.request.headers.get('X-AppEngine-QueueName'):
      self.abort(405, detail='Only internal task queue tasks can do this')
    map_reduce_jobs.launch_job(job_id)


### Non-restricted handlers


class ProtocolHandler(auth.AuthenticatingHandler):
  """Base class for request handlers that implement isolate protocol."""

  # Isolate protocol uses 'token' instead of 'xsrf_token'.
  xsrf_token_request_param = 'token'

  def send_json(self, body, http_code=200):
    """Serializes |body| into JSON and sends it as a response."""
    self.response.set_status(http_code)
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.write(utils.encode_to_json(body))

  def send_error(self, message, http_code=400):
    """Sends a error message and aborts the request, logs the error."""
    logging.error(message)
    self.abort(http_code, detail=message)

  def send_data(self, data, filename=None, offset=0):
    """Sends binary data as a response.

    If |offset| is zero, returns an entire |data| and sets HTTP code to 200.
    If |offset| is non-zero, returns a subrange of |data| with HTTP code
    set to 206 and 'Content-Range' header.
    If |offset| is outside of acceptable range, returns HTTP code 416.
    """
    # Bad offset? Return 416.
    if offset < 0 or offset >= len(data):
      return self.send_error(
          'Unacceptable offset.\nRequested offset is %d while file '
          'size is %d' % (offset, len(data)), http_code=416)

    # Common headers that are set regardless of |offset| value.
    if filename:
      self.response.headers['Content-Disposition'] = (
          'attachment; filename="%s"' % filename)
    self.response.headers['Content-Type'] = 'application/octet-stream'
    self.response.headers['Cache-Control'] = 'public, max-age=43200'

    if not offset:
      # Returning an entire file.
      self.response.set_status(200)
      self.response.out.write(data)
    else:
      # Returning a partial content, set Content-Range header.
      self.response.set_status(206)
      self.response.headers['Content-Range'] = (
          'bytes %d-%d/%d' % (offset, len(data) - 1, len(data)))
      self.response.out.write(data[offset:])

  @property
  def client_protocol_version(self):
    """Returns protocol version client provided during handshake, or None if not
    known.

    Valid only for POST or PUT requests for now.
    """
    # See HandshakeHandler, its where xsrf_token_data is generated.
    return self.xsrf_token_data.get('v')


class HandshakeHandler(ProtocolHandler):
  """Returns access token, version and capabilities of the server.

  Request body is a JSON dict:
    {
      "client_app_version": "0.2",
      "fetcher": true,
      "protocol_version": "1.0",
      "pusher": true,
    }

  Response body is a JSON dict:
    {
      "access_token": "......",
      "protocol_version": "1.0",
      "server_app_version": "138-193f1f3",
    }
    or
    {
      "error": "Some user friendly error text",
      "protocol_version": "1.0",
      "server_app_version": "138-193f1f3",
    }
  """

  # This handler is called to get XSRF token, there's nothing to enforce yet.
  xsrf_token_enforce_on = ()

  @auth.require(auth.READ, 'isolate/namespaces/')
  def post(self):
    """Responds with access token and server version."""
    try:
      request = json.loads(self.request.body)
      client_protocol = str(request['protocol_version'])
      client_app_version = str(request['client_app_version'])
      pusher = request.get('pusher', True)
      fetcher = request.get('fetcher', True)
    except (ValueError, KeyError) as exc:
      return self.send_error(
          'Invalid body of /handshake call.\nError: %s.' % exc)

    # This access token will be used to validate each subsequent request.
    access_token = self.generate_xsrf_token({'v': client_protocol})

    # Log details of the handshake to the server log.
    logging_info = {
        'Access Id': auth.get_current_identity().to_bytes(),
        'Client app version': client_app_version,
        'Client is fetcher': fetcher,
        'Client is pusher': pusher,
        'Client protocol version': client_protocol,
        'Token': access_token,
    }
    logging.info(
        '\n'.join('%s: %s' % (k, logging_info[k])
        for k in sorted(logging_info)))

    # Send back the response.
    self.send_json({
        'access_token': access_token,
        'protocol_version': ISOLATE_PROTOCOL_VERSION,
        'server_app_version': config.get_app_version(),
    })


class PreUploadContentHandler(ProtocolHandler):
  """Checks for entries existence, generates upload URLs.

  Request body is a JSON list:
  [
      {
          "h": <hex digest>,
          "i": <1 for isolated file, 0 for rest of them>
          "s": <int entry size>,
      },
      ...
  ]

  Response is a JSON list of the same length where each item is either:
    * If an entry is missing: a list with two URLs - URL to upload a file to,
      and URL to call when upload is done (can be null).
    * If entry is already present: null.

  For instance:
  [
      ["<upload url>", "<finalize url>"],
      null,
      null,
      ["<upload url>", null],
      null,
      ...
  ]
  """

  # Default expiration time for signed links.
  DEFAULT_LINK_EXPIRATION = datetime.timedelta(hours=4)

  # Info about a requested entry:
  #   digest: hex string with digest
  #   size: uncompressed item size
  #   is_isolated: True if it's *.isolated file
  EntryInfo = collections.namedtuple(
      'EntryInfo', ['digest', 'size', 'is_isolated'])

  _gs_url_signer = None

  @staticmethod
  def parse_request(body, namespace):
    """Parses a request body into a list of EntryInfo objects."""
    hex_digest_size = get_hash_algo(namespace).digest_size * 2
    try:
      out = [
        PreUploadContentHandler.EntryInfo(
            str(m['h']), int(m['s']), bool(m['i']))
        for m in json.loads(body)
      ]
      for i in out:
        check_hash(i.digest, hex_digest_size)
      return out
    except (ValueError, KeyError, TypeError) as exc:
      raise ValueError('Bad body: %s' % exc)

  @staticmethod
  def check_entry_infos(entries, namespace):
    """Generator that checks for EntryInfo entries existence.

    Yields pairs (EntryInfo object, True if such entry exists in Datastore).
    """
    # Kick off all queries in parallel. Build mapping Future -> digest.
    futures = {}
    for entry_info in entries:
      key = entry_key(namespace, entry_info.digest)
      futures[key.get_async(use_cache=False)] = entry_info

    # Pick first one that finishes and yield it, rinse, repeat.
    while futures:
      future = ndb.Future.wait_any(futures)
      # TODO(maruel): For items that were present, make sure
      # future.get_result().compressed_size == entry_info.size.
      yield futures.pop(future), bool(future.get_result())

  @staticmethod
  def tag_entries(entries, namespace):
    """Enqueues a task to update the timestamp for given entries."""
    url = '/internal/taskqueue/tag/%s/%s' % (
        namespace, utils.datetime_to_timestamp(utcnow()))
    payload = ''.join(binascii.unhexlify(e.digest) for e in entries)
    return enqueue_task(url, 'tag', payload=payload)

  @staticmethod
  def should_push_to_gs(entry_info):
    """True to direct client to upload given EntryInfo directly to GS."""
    # Relatively small *.isolated files go through app engine to cache them.
    if entry_info.is_isolated and entry_info.size <= MAX_MEMCACHE_ISOLATED:
      return False
    # All other large enough files go through GS.
    return entry_info.size >= MIN_SIZE_FOR_DIRECT_GS

  @property
  def gs_url_signer(self):
    """On demand instance of CloudStorageURLSigner object."""
    if not self._gs_url_signer:
      settings = config.settings()
      self._gs_url_signer = gcs.URLSigner(
          settings.gs_bucket,
          settings.gs_client_id_email,
          settings.gs_private_key)
    return self._gs_url_signer

  def generate_store_url(self, entry_info, namespace, http_verb, uploaded_to_gs,
                         expiration):
    """Generates a signed URL to /content-gs/store method.

    Arguments:
      entry_info: A EntryInfo instance.
    """
    # Data that goes into request parameters and signature.
    expiration_ts = str(int(time.time() + expiration.total_seconds()))
    item_size = str(entry_info.size)
    is_isolated = str(int(entry_info.is_isolated))
    uploaded_to_gs = str(int(uploaded_to_gs))

    # Generate signature.
    sig = StoreContentHandler.generate_signature(
        config.settings().global_secret, http_verb, expiration_ts, namespace,
        entry_info.digest, item_size, is_isolated, uploaded_to_gs)

    # Bare full URL to /content-gs/store endpoint.
    url_base = self.uri_for(
        'store-gs', namespace=namespace, hash_key=entry_info.digest, _full=True)

    # Construct url with query parameters, reuse auth token.
    params = {
        'g': uploaded_to_gs,
        'i': is_isolated,
        's': item_size,
        'sig': sig,
        'token': self.request.get('token'),
        'x': expiration_ts,
    }
    return '%s?%s' % (url_base, urllib.urlencode(params))

  def generate_push_urls(self, entry_info, namespace):
    """Generates a pair of URLs to be used by clients to upload an item.

    The GS filename is exactly ContentEntry.key.id().

    URL's being generated are 'upload URL' and 'finalize URL'. Client uploads
    an item to upload URL (via PUT request) and then POST status of the upload
    to a finalize URL.

    Finalize URL may be optional (it's None in that case).
    """
    if self.should_push_to_gs(entry_info):
      # Store larger stuff in Google Storage.
      key = entry_key(namespace, entry_info.digest)
      upload_url = self.gs_url_signer.get_upload_url(
          filename=key.id(),
          content_type='application/octet-stream',
          expiration=self.DEFAULT_LINK_EXPIRATION)
      finalize_url = self.generate_store_url(
          entry_info, namespace,
          http_verb='POST',
          uploaded_to_gs=True,
          expiration=self.DEFAULT_LINK_EXPIRATION)
    else:
      # Store smallish entries and *.isolated in Datastore directly.
      upload_url = self.generate_store_url(
          entry_info, namespace,
          http_verb='PUT',
          uploaded_to_gs=False,
          expiration=self.DEFAULT_LINK_EXPIRATION)
      finalize_url = None
    return upload_url, finalize_url

  @auth.require(auth.UPDATE, 'isolate/namespaces/{namespace}')
  def post(self, namespace):
    """Reads body with items to upload and replies with URLs to upload to."""
    if not re.match(r'^%s$' % NAMESPACE_RE, namespace):
      self.send_error(
          'Invalid namespace; allowed keys must pass regexp "%s"' %
          NAMESPACE_RE)

    # Parse a body into list of EntryInfo objects.
    try:
      entries = self.parse_request(self.request.body, namespace)
    except ValueError as err:
      return self.send_error(
          'Bad /pre-upload request.\n(%s)\n%s' % (err, self.request.body[:200]))

    # Generate push_urls for missing entries.
    push_urls = {}
    existing = []
    for entry_info, exists in self.check_entry_infos(entries, namespace):
      if exists:
        existing.append(entry_info)
      else:
        push_urls[entry_info.digest] = self.generate_push_urls(
            entry_info, namespace)

    # Send back the response.
    self.send_json([push_urls.get(entry_info.digest) for entry_info in entries])

    # Log stats, enqueue tagging task that updates last access time.
    stats.add_entry(stats.LOOKUP, len(entries), len(existing))
    if existing:
      # Ignore errors in a call below. They happen when task queue service has
      # a bad time and doesn't accept tagging tasks. We don't want isolate
      # server's reliability to depend on task queue service health. An ignored
      # error here means there's a chance some entry might be deleted sooner
      # than it should.
      self.tag_entries(existing, namespace)


class RetrieveContentHandler(ProtocolHandler):
  """The handlers for retrieving contents by its SHA-1 hash |hash_key|.

  Can produce 5 types of responses:
    * HTTP 200: the content is in response body as octet-stream.
    * HTTP 206: partial content is in response body.
    * HTTP 302: http redirect to a file with the content.
    * HTTP 404: content is not available, response body is a error message.
    * HTTP 416: requested byte range can not be satisfied.
  """

  @auth.require(auth.READ, 'isolate/namespaces/{namespace}')
  def get(self, namespace, hash_key):  #pylint: disable=W0221
    # Parse 'Range' header if it's present to extract initial offset.
    # Only support single continuous range from some |offset| to the end.
    offset = 0
    range_header = self.request.headers.get('range')
    if range_header:
      match = re.match(r'bytes=(\d+)-', range_header)
      if not match:
        return self.send_error(
            'Unsupported byte range.\n\'%s\'.' % range_header, http_code=416)
      offset = int(match.group(1))

    memcache_entry = memcache.get(hash_key, namespace='table_%s' % namespace)
    if memcache_entry is not None:
      self.send_data(memcache_entry, filename=hash_key, offset=offset)
      stats.add_entry(stats.RETURN, len(memcache_entry) - offset, 'memcache')
      return

    entry = entry_key(namespace, hash_key).get()
    if not entry:
      return self.send_error('Unable to retrieve the entry.', http_code=404)

    if entry.content is not None:
      self.send_data(entry.content, filename=hash_key, offset=offset)
      stats.add_entry(stats.RETURN, len(entry.content) - offset, 'inline')
      return

    # Generate signed download URL.
    settings = config.settings()
    # TODO(maruel): The GS object may not exist anymore. Handle this.
    signer = gcs.URLSigner(settings.gs_bucket,
        settings.gs_client_id_email, settings.gs_private_key)
    # The entry key is the GS filepath.
    signed_url = signer.get_download_url(entry.key.id())

    # Redirect client to this URL. If 'Range' header is used, client will
    # correctly pass it to Google Storage to fetch only subrange of file,
    # so update stats accordingly.
    self.redirect(signed_url)
    stats.add_entry(
        stats.RETURN, entry.compressed_size - offset, 'GS; %s' % entry.key.id())


class StoreContentHandler(ProtocolHandler):
  """Creates ContentEntry Datastore entity for some uploaded file.

  Clients usually do not call this handler explicitly. Signed URL to it
  is returned in /pre-upload call.

  This handler is called in two ways:
    * As a POST request to finalize a file already uploaded to GS. Request
      body is empty in that case.
    * As a PUT request to upload an actual data and create ContentEntry in one
      call. Request body contains octet-stream with entry's data.

  In either case query parameters define details of new content entry:
    g - 1 if it was previously uploaded to GS.
    i - 1 if it its *.isolated file.
    s - size of the uncompressed file.
    x - URL signature expiration timestamp.
    sig - signature of request parameters, to verify they are not tampered with.

  Can produce 3 types of responses:
    * HTTP 200: success, entry is created or existed before, response body is
      a json dict with information about new entry.
    * HTTP 400: fatal error, retrying request won't fix it, response body is
      a error message.
    * HTTP 503: transient error, request should be retried by client, response
      body is a error message.

  In case of HTTP 200, body is a JSON dict:
  {
      'entry': {<details about the entry>}
  }
  """

  @staticmethod
  def generate_signature(secret_key, http_verb, expiration_ts, namespace,
                         hash_key, item_size, is_isolated, uploaded_to_gs):
    """Generates HMAC-SHA1 signature for given set of parameters.

    Used by PreUploadContentHandler to sign store URLs and by
    StoreContentHandler to validate them.

    All arguments should be in form of strings.
    """
    data_to_sign = '\n'.join([
        http_verb,
        expiration_ts,
        namespace,
        hash_key,
        item_size,
        is_isolated,
        uploaded_to_gs,
    ])
    mac = hmac.new(secret_key, digestmod=hashlib.sha1)
    mac.update(data_to_sign)
    return mac.hexdigest()

  @auth.require(auth.UPDATE, 'isolate/namespaces/{namespace}')
  def post(self, namespace, hash_key):
    """POST is used when finalizing upload to GS."""
    return self.handle(namespace, hash_key)

  @auth.require(auth.UPDATE, 'isolate/namespaces/{namespace}')
  def put(self, namespace, hash_key):
    """PUT is used when uploading directly to datastore via this handler."""
    return self.handle(namespace, hash_key)

  def handle(self, namespace, hash_key):
    """Handles this request."""
    # Extract relevant request parameters.
    expiration_ts = self.request.get('x')
    item_size = self.request.get('s')
    is_isolated = self.request.get('i')
    uploaded_to_gs = self.request.get('g')
    signature = self.request.get('sig')

    # Build correct signature.
    expected_sig = self.generate_signature(
        config.settings().global_secret, self.request.method, expiration_ts,
        namespace, hash_key, item_size, is_isolated, uploaded_to_gs)

    # Verify signature is correct.
    if not utils.constant_time_equals(signature, expected_sig):
      return self.send_error('Incorrect signature.')

    # Convert parameters from strings back to something useful.
    # It can't fail since matching signature means it was us who generated
    # this strings in a first place.
    expiration_ts = int(expiration_ts)
    item_size = int(item_size)
    is_isolated = bool(int(is_isolated))
    uploaded_to_gs = bool(int(uploaded_to_gs))

    # Verify signature is not yet expired.
    if time.time() > expiration_ts:
      return self.send_error('Expired signature.')

    if uploaded_to_gs:
      # GS upload finalization uses empty POST body.
      assert self.request.method == 'POST'
      if self.request.headers.get('content-length'):
        return self.send_error('Expecting empty POST.')
      content = None
    else:
      # Datastore upload uses PUT.
      assert self.request.method == 'PUT'
      if self.request.headers.get('content-length'):
        content = self.request.body
      else:
        content = ''

    # Info about corresponding GS entry (if it exists).
    gs_bucket = config.settings().gs_bucket
    key = entry_key(namespace, hash_key)

    # Verify the data while at it since it's already in memory but before
    # storing it in memcache and datastore.
    if content is not None:
      # Verify advertised hash matches the data.
      try:
        hex_digest, expanded_size = hash_content(content, namespace)
        if hex_digest != hash_key:
          raise ValueError(
              'Hash and data do not match, '
              '%d bytes (%d bytes expanded)' % (len(content), expanded_size))
        if expanded_size != item_size:
          raise ValueError(
              'Advertised data length (%d) and actual data length (%d) '
              'do not match' % (item_size, expanded_size))
      except ValueError as err:
        return self.send_error('Inline verification failed.\n%s' % err)
      # Successfully verified!
      compressed_size = len(content)
      needs_verification = False
    else:
      # Fetch size of the stored file.
      file_info = gcs.get_file_info(gs_bucket, key.id())
      if not file_info:
        # TODO(maruel): Do not fail yet. If the request got up to here, the file
        # is likely there but the service may have trouble fetching the metadata
        # from GS.
        return self.send_error(
            'File should be in Google Storage.\nFile: \'%s\' Size: %d.' %
            (key.id(), item_size))
      compressed_size = file_info.size
      needs_verification = True

    # Data is here and it's too large for DS, so put it in GS. It is likely
    # between MIN_SIZE_FOR_GS <= len(content) < MIN_SIZE_FOR_DIRECT_GS
    if content is not None and len(content) >= MIN_SIZE_FOR_GS:
      if not gcs.write_file(gs_bucket, key.id(), [content]):
        # Returns 503 so the client automatically retries.
        return self.send_error(
            'Unable to save the content to GS.', http_code=503)
      # It's now in GS.
      uploaded_to_gs = True

    # Can create entity now, everything appears to be legit.
    entry = create_entry(key)
    if not entry:
      self.send_json({'entry': {}})
      stats.add_entry(stats.DUPE, compressed_size, 'inline')
      return

    # If it's not in GS then put it inline.
    if not uploaded_to_gs:
      assert content is not None and len(content) < MIN_SIZE_FOR_GS
      entry.content = content

    # Start saving Datastore entry.
    entry.is_isolated = is_isolated
    entry.compressed_size = compressed_size
    entry.expanded_size = -1 if needs_verification else item_size
    entry.is_verified = not needs_verification

    futures = [entry.put_async()]

    # Start saving *.isolated into memcache iff its content is available and
    # it's not in Datastore: there's no point in saving inline blobs in memcache
    # because ndb already memcaches them.
    if (content is not None and entry.content is None and
        entry.is_isolated and entry.compressed_size <= MAX_MEMCACHE_ISOLATED):
      futures.append(save_in_memcache(namespace, hash_key, content, async=True))

    # Log stats.
    where = 'GS; ' + 'inline' if entry.content is not None else entry.key.id()

    # Verification task will be accessing the entity, so ensure it exists.
    ndb.Future.wait_all(futures)

    # Trigger a verification task for files in the GS.
    if needs_verification:
      url = '/internal/taskqueue/verify/%s/%s' % (namespace, hash_key)
      if not enqueue_task(url, 'verify'):
        # TODO(vadimsh): Don't fail whole request here, because several RPCs are
        # required to roll it back and there isn't much time left
        # (after DeadlineExceededError is already caught) to perform them.
        # AppEngine gives less then a second to handle DeadlineExceededError.
        # It's unreliable. We need some other mechanism that can detect
        # unverified entities and launch verification tasks, for instance
        # a datastore index on 'is_verified' boolean field and a cron task that
        # verifies all unverified entities.
        logging.error('Unable to add task to verify uploaded content.')

    # TODO(vadimsh): Fill in details about the entry, such as expiration time.
    self.send_json({'entry': {}})
    stats.add_entry(stats.STORE, entry.compressed_size, where)


###


class RootHandler(webapp2.RequestHandler):
  """Tells the user to RTM."""
  def get(self):
    self.response.write(render_template('root.html'))


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    config.warmup()
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def CreateApplication(debug=False):
  """Creates the url router.

  The basic layouts is as follow:
  - /internal/.* requires being an instance administrator.
  - /restricted/.* requires being an instance administrator.
  - /content/.* has the public HTTP API.
  - /stats/.* has statistics.
  """
  # Routes added to WSGIApplication only a dev mode.
  dev_routes = []
  if utils.is_local_dev_server():
    dev_routes.extend(gcs.URLSigner.switch_to_dev_mode())

  # Supported authentication mechanisms.
  auth.configure([
    auth.oauth_authentication,
    auth.cookie_authentication,
    auth.service_to_service_authentication,
    acl.whitelisted_ip_authentication,
  ])

  # Customize auth UI to show that it's running on isolate server.
  auth_ui.configure_ui(
      app_name='Isolate Server',
      app_version=config.get_app_version(),
      app_revision_url=config.get_app_revision_url())

  # Routes with Auth REST API and Auth UI.
  auth_routes = []
  auth_routes.extend(auth_ui.get_rest_api_routes())
  auth_routes.extend(auth_ui.get_ui_routes())

  # Namespace can be letters, numbers, '-', '.' and '_'.
  namespace = r'/<namespace:%s>' % NAMESPACE_RE
  # Do not enforce a length limit to support different hashing algorithm. This
  # should represent a valid hex value.
  hashkey = r'/<hash_key:[a-f0-9]{4,}>'
  # This means a complete key is required.
  namespace_key = namespace + hashkey

  return webapp2.WSGIApplication(dev_routes + auth_routes + [
      # Triggers a taskqueue.
      webapp2.Route(
          r'/internal/cron/cleanup/trigger/<name:[a-z_]+>',
          InternalCleanupTriggerHandler),
      # TODO(maruel): Remove me.
      webapp2.Route(
          r'/restricted/cleanup/trigger/<name:[a-z_]+>',
          InternalCleanupTriggerHandler),

      # Cleanup tasks.
      webapp2.Route(
          r'/internal/taskqueue/cleanup/old',
          InternalCleanupOldEntriesWorkerHandler),
      webapp2.Route(
          r'/internal/taskqueue/cleanup/obliterate',
          InternalObliterateWorkerHandler),
      webapp2.Route(
          r'/internal/taskqueue/cleanup/trim_lost',
          InternalCleanupTrimLostWorkerHandler),

      # Tasks triggered by other request handlers.
      webapp2.Route(
          r'/internal/taskqueue/tag%s/<timestamp:\d+>' % namespace,
          InternalTagWorkerHandler),
      webapp2.Route(
          r'/internal/taskqueue/verify%s' % namespace_key,
          InternalVerifyWorkerHandler),

     webapp2.Route(
          r'/internal/cron/ereporter2/mail', InternalEreporter2Mail),
     # TODO(maruel): Remove me.
     webapp2.Route(
          r'/restricted/cron/ereporter2/mail', InternalEreporter2Mail),
     webapp2.Route(
          r'/restricted/ereporter2/report',
          RestrictedEreporter2Report),
     webapp2.Route(
          r'/restricted/ereporter2/request/<request_id:[0-9a-fA-F]+>',
          RestrictedEreporter2Request),


      # Stats
      webapp2.Route(
          r'/internal/cron/stats/update',
          stats.InternalStatsUpdateHandler),
     # TODO(maruel): Remove me.
      webapp2.Route(
          r'/restricted/stats/update',
          stats.InternalStatsUpdateHandler),

      # Administrative urls.
      webapp2.Route(
          r'/restricted', RestrictedAdminUIHandler),
      webapp2.Route(
          r'/restricted/whitelistip', acl.RestrictedWhitelistIPHandler),
      webapp2.Route(
          r'/restricted/gs_config', RestrictedGoogleStorageConfig),

      # Mapreduce related urls.
      webapp2.Route(
          r'/restricted/launch_map_reduce',
          RestrictedLaunchMapReduceJob),
      webapp2.Route(
          r'/internal/taskqueue/mapreduce/launch/<job_id:[^\/]+>',
          InternalLaunchMapReduceJobWorkerHandler),

      # The public API:
      webapp2.Route(
          r'/content-gs/handshake',
          HandshakeHandler),
      webapp2.Route(
          r'/content-gs/pre-upload/<namespace:.*>',
          PreUploadContentHandler),
      webapp2.Route(
          r'/content-gs/retrieve%s' % namespace_key,
          RetrieveContentHandler),
      webapp2.Route(
          r'/content-gs/store%s' % namespace_key,
          StoreContentHandler,
          name='store-gs'),

      # Public stats.
      webapp2.Route(r'/stats/json', stats.StatsJsonHandler),
      webapp2.Route(r'/stats', stats.StatsHandler),

      # AppEngine-specific url:
      webapp2.Route(r'/_ah/warmup', WarmupHandler),

      # Must be last.
      webapp2.Route(r'/', RootHandler),
  ],
  debug=debug)
