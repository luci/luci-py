# Copyright 2012 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""This module defines Isolate Server backend url handlers."""

import binascii
import logging
import time
import zlib

import webapp2
from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

import config
import gcs
import mapreduce_jobs
import model
import stats
import template
from components import decorators
from components import utils


# The maximum number of items to delete at a time.
ITEMS_TO_DELETE_ASYNC = 100


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
      request,
      model.get_hash_algo(namespace).digest_size,
      model.MAX_KEYS_PER_DB_OPS)


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
      futures.extend(delete(to_delete) or [])
      to_delete = []

    # That's a lot of on-going operations which could take a significant amount
    # of memory. Wait a little on the oldest operations.
    # TODO(maruel): Profile memory usage to see if a few thousands of on-going
    # RPC objects is a problem in practice.
    while len(futures) > 10 * ITEMS_TO_DELETE_ASYNC:
      futures.pop(0).wait()

  if to_delete:
    logging.info('Deleting %s entries', len(to_delete))
    futures.extend(delete(to_delete) or [])

  ndb.Future.wait_all(futures)
  return deleted_count


### Restricted handlers


class InternalCleanupOldEntriesWorkerHandler(webapp2.RequestHandler):
  """Removes the old data from the datastore.

  Only a task queue task can use this handler.
  """
  # pylint: disable=R0201
  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      runtime.DeadlineExceededError)
  @decorators.require_taskqueue('cleanup')
  def post(self):
    q = model.ContentEntry.query(
        model.ContentEntry.expiration_ts < utils.utcnow()
        ).iter(keys_only=True)
    total = incremental_delete(q, delete=model.delete_entry_and_gs_entry)
    logging.info('Deleting %s expired entries', total)


class InternalObliterateWorkerHandler(webapp2.RequestHandler):
  """Deletes all the stuff."""
  # pylint: disable=R0201
  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      runtime.DeadlineExceededError)
  @decorators.require_taskqueue('cleanup')
  def post(self):
    logging.info('Deleting ContentEntry')
    incremental_delete(
        model.ContentEntry.query().iter(keys_only=True),
        ndb.delete_multi_async)

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
  # pylint: disable=R0201
  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      runtime.DeadlineExceededError)
  @decorators.require_taskqueue('cleanup')
  def post(self):
    """Enumerates all GS files and delete those that do not have an associated
    ContentEntry.
    """
    gs_bucket = config.settings().gs_bucket

    def filter_missing():
      futures = {}
      cutoff = time.time() - 60*60
      for filepath, filestats in gcs.list_files(gs_bucket):
        # If the file was uploaded in the last hour, ignore it.
        if filestats.st_ctime >= cutoff:
          continue

        # This must match the logic in model.entry_key(). Since this request
        # will in practice touch every item, do not use memcache since it'll
        # mess it up by loading every items in it.
        # TODO(maruel): Batch requests to use get_multi_async() similar to
        # datastore_utils.page_queries().
        future = model.entry_key_from_id(filepath).get_async(
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
  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      runtime.DeadlineExceededError)
  @decorators.require_cronjob
  def get(self, name):
    if name in ('obliterate', 'old', 'orphaned', 'trim_lost'):
      url = '/internal/taskqueue/cleanup/' + name
      # The push task queue name must be unique over a ~7 days period so use
      # the date at second precision, there's no point in triggering each of
      # time more than once a second anyway.
      now = utils.utcnow().strftime('%Y-%m-%d_%I-%M-%S')
      if utils.enqueue_task(url, 'cleanup', name=name + '_' + now):
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
  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      runtime.DeadlineExceededError)
  @decorators.require_taskqueue('tag')
  def post(self, namespace, timestamp):
    digests = []
    now = utils.timestamp_to_datetime(long(timestamp))
    expiration = config.settings().default_expiration
    try:
      digests = payload_to_hashes(self, namespace)
      # Requests all the entities at once.
      futures = ndb.get_multi_async(
          model.entry_key(namespace, binascii.hexlify(d)) for d in digests)

      to_save = []
      while futures:
        # Return opportunistically the first entity that can be retrieved.
        future = ndb.Future.wait_any(futures)
        futures.remove(future)
        item = future.get_result()
        if item and item.next_tag_ts < now:
          # Update the timestamp. Add a bit of pseudo randomness.
          item.expiration_ts, item.next_tag_ts = model.expiration_jitter(
              now, expiration)
          to_save.append(item)
      if to_save:
        ndb.put_multi(to_save)
      logging.info(
          'Timestamped %d entries out of %s', len(to_save), len(digests))
    except Exception as e:
      logging.error('Failed to stamp entries: %s\n%d entries', e, len(digests))
      raise


class InternalVerifyWorkerHandler(webapp2.RequestHandler):
  """Verify the SHA-1 matches for an object stored in Cloud Storage."""

  @staticmethod
  def purge_entry(entry, message, *args):
    """Logs error message, deletes |entry| from datastore and GS."""
    logging.error(
        'Verification failed for %s: %s', entry.key.id(), message % args)
    model.delete_entry_and_gs_entry([entry.key])

  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      gcs.TransientError,
      runtime.DeadlineExceededError)
  @decorators.require_taskqueue('verify')
  def post(self, namespace, hash_key):
    entry = model.entry_key(namespace, hash_key).get()
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
        entry.compressed_size <= model.MAX_MEMCACHE_ISOLATED and
        entry.is_isolated)
    expanded_size = 0
    digest = model.get_hash_algo(namespace)
    data = None

    try:
      # Start a loop where it reads the data in block.
      stream = gcs.read_file(gs_bucket, entry.key.id())
      if save_to_memcache:
        # Wraps stream with a generator that accumulates the data.
        stream = Accumulator(stream)

      for data in model.expand_content(namespace, stream):
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

    except gcs.NotFoundError as e:
      # Somebody deleted a file between get_file_info and read_file calls.
      self.purge_entry(entry, 'File was unexpectedly deleted')
      return
    except (gcs.ForbiddenError, gcs.AuthorizationError) as e:
      # Misconfiguration in Google Storage ACLs. Don't delete an entry, it may
      # be fine. Maybe ACL problems would be fixed before the next retry.
      logging.warning(
          'CloudStorage auth issues (%s): %s', e.__class__.__name__, e)
      # Abort so the job is retried automatically.
      return self.abort(500)
    except (gcs.FatalError, zlib.error, IOError) as e:
      # ForbiddenError and AuthorizationError inherit FatalError, so this except
      # block should be last.
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
      model.save_in_memcache(namespace, hash_key, ''.join(stream.accumulated))
    future.wait()


class InternalStatsUpdateHandler(webapp2.RequestHandler):
  """Called every few minutes to update statistics."""
  @decorators.require_cronjob
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    minutes = stats.generate_stats()
    if minutes is not None:
      msg = 'Processed %d minutes' % minutes
      logging.info(msg)
      self.response.write(msg)


### Mapreduce related handlers


class InternalLaunchMapReduceJobWorkerHandler(webapp2.RequestHandler):
  """Called via task queue or cron to start a map reduce job."""
  @decorators.require_taskqueue(mapreduce_jobs.MAPREDUCE_TASK_QUEUE)
  def post(self, job_id):  # pylint: disable=R0201
    mapreduce_jobs.launch_job(job_id)


###


def get_routes():
  """Returns the routes to be executed on the backend."""
  # Namespace can be letters, numbers, '-', '.' and '_'.
  namespace = r'/<namespace:%s>' % model.NAMESPACE_RE
  # Do not enforce a length limit to support different hashing algorithm. This
  # should represent a valid hex value.
  hashkey = r'/<hash_key:[a-f0-9]{4,}>'
  # This means a complete key is required.
  namespace_key = namespace + hashkey

  return [
    # Triggers a taskqueue.
    webapp2.Route(
        r'/internal/cron/cleanup/trigger/<name:[a-z_]+>',
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

    # Stats
    webapp2.Route(
        r'/internal/cron/stats/update', InternalStatsUpdateHandler),

    # Mapreduce related urls.
    webapp2.Route(
        r'/internal/taskqueue/mapreduce/launch/<job_id:[^\/]+>',
        InternalLaunchMapReduceJobWorkerHandler),
  ]


def create_application(debug):
  """Creates the url router for the backend.

  The backend only implements urls under /internal/.
  """
  # Necessary due to email sent by cron job.
  template.bootstrap()
  return webapp2.WSGIApplication(get_routes(), debug=debug)
