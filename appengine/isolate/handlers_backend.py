# Copyright 2012 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""This module defines Isolate Server backend url handlers."""

import binascii
import datetime
import json
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
import model
import stats
import template
from components import decorators
from components import utils


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


def _split_payload(request, chunk_size, max_chunks):
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

  return [content[i * chunk_size:(i + 1) * chunk_size] for i in range(count)]


def _payload_to_hashes(request, namespace):
  """Converts a raw payload into hashes as bytes."""
  h = model.get_hash(namespace)
  return _split_payload(request, h.digest_size, model.MAX_KEYS_PER_DB_OPS)


def _throttle_futures(futures, limit):
  finished = []
  if len(futures) <= limit:
    return futures, finished
  while len(futures) > limit:
    ndb.Future.wait_any(futures)
    tmp = []
    for f in futures:
      if f.done():
        f.get_result()
        finished.append(f)
      else:
        tmp.append(f)
    futures = tmp
  return futures, finished


def _incremental_delete(query, delete_async):
  """Applies |delete| to objects in a query asynchrously.

  This function is itself synchronous. It runs the query for at most 9 minutes
  to try to have enough time to finish the deletion. Swallows most frequent
  exceptions.

  Arguments:
  - query: iterator of items to process.
  - delete_async: callback that accepts an object to delete and returns a
        ndb.Future.

  Returns the number of objects found.
  """
  start = time.time()
  try:
    count = 0
    deleted_count = 0
    futures = []
    for item in query:
      count += 1
      if not (count % 1000):
        logging.debug('Found %d items', count)
      futures.append(delete_async(item))
      deleted_count += 1

      # Throttle.
      futures, _ = _throttle_futures(futures, 25)

      # Time to stop querying.
      if (time.time() - start) > 9*60:
        break

    for f in futures:
      f.get_result()
  except (
      datastore_errors.BadRequestError,
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      runtime.DeadlineExceededError) as e:
    logging.info('Silencing exception %s', e)

  # Try to do the best it can.
  for f in futures:
    try:
      f.get_result()
    except (
        datastore_errors.BadRequestError,
        datastore_errors.InternalError,
        datastore_errors.Timeout,
        datastore_errors.TransactionFailedError,
        runtime.DeadlineExceededError) as e:
      logging.info('Silencing exception as last chance; %s', e)

  return deleted_count


def _yield_orphan_gcs_files(gs_bucket):
  """Iterates over the whole GCS bucket for unreferenced files.

  Finds files in GCS that are not referred to by a ContentEntry.

  Only return files at least 1 day old to reduce the risk of failure.

  Yields:
    path of unreferenced files in the bucket
  """
  good = 0
  orphaned = 0
  size_good = 0
  size_orphaned = 0
  # pylint: disable=too-many-nested-blocks
  try:
    futures = {}
    cutoff = time.time() - 24*60*60
    # https://cloud.google.com/appengine/docs/standard/python/googlecloudstorageclient/gcsfilestat_class
    for filepath, filestats in gcs.list_files(gs_bucket):
      # If the file was uploaded in the last hour, ignore it.
      if filestats.st_ctime >= cutoff:
        continue

      # This must match the logic in model.get_entry_key(). Since this request
      # will touch every ContentEntry, do not use memcache since it'll
      # overflow it up by loading every items in it.
      try:
        # TODO(maruel): Handle non-ascii files, in practice they cannot be
        # digests so they must be deleted anyway.
        key = model.entry_key_from_id(str(filepath))
      except AssertionError:
        # It's not even a valid entry.
        orphaned += 1
        size_orphaned += filestats.st_size
        yield filepath
        continue

      futures[key.get_async(use_memcache=False)] = (filepath, filestats)

      if len(futures) > 100:
        ndb.Future.wait_any(futures)
        tmp = {}
        # pylint: disable=redefined-outer-name
        for f, (filepath, filestats) in futures.items():
          if f.done():
            if not f.get_result():
              # Orphaned, delete.
              orphaned += 1
              size_orphaned += filestats.st_size
              yield filepath
            else:
              good += 1
              size_good += filestats.st_size
          else:
            tmp[f] = (filepath, filestats)
        futures = tmp

    while futures:
      ndb.Future.wait_any(futures)
      tmp = {}
      for f, (filepath, filestats) in futures.items():
        if f.done():
          if not f.get_result():
            # Orphaned, delete.
            orphaned += 1
            size_orphaned += filestats.st_size
            yield filepath
          else:
            good += 1
            size_good += filestats.st_size
        else:
          # Not done yet.
          tmp[f] = (filepath, filestats)
      futures = tmp
  finally:
    size_good_tb = size_good / 1024. / 1024. / 1024. / 1024.
    size_orphaned_gb = size_orphaned / 1024. / 1024. / 1024.
    logging.info(
        'Found:\n'
        '- %d good GCS files; %d bytes (%.1fTiB)\n'
        '- %d orphaned files; %d bytes (%.1fGiB)',
        good, size_good, size_good_tb,
        orphaned, size_orphaned, size_orphaned_gb)


### Cron handlers


class CronCleanupExpiredHandler(webapp2.RequestHandler):
  """Triggers taskqueues to delete 500 items at a time."""
  @decorators.require_cronjob
  def get(self):
    # On the production instance, there's so many expired items that running the
    # query serially takes more time than the rate of items that expires (!)
    #
    # So instead of running the query to fetch all the ContentEntry keys:
    # - Find the lower and upper bounds of expiration_ts.
    # - Trigger N query tasks, one per hour:
    #   - In each query task, run the query shard for ContentEntry entities with
    #     expiration_ts set on this hour.
    #   - For each 500 ContentEntry keys found:
    #     - Trigger a task that will delete these and their associated GCS file,
    #       if any.

    # Do not run for more than 9 minutes. Exceeding 10min hard limit causes 500.
    # Normally this cron job should complete within a few milliseconds.
    time_to_stop = time.time() + 9*60
    now = utils.utcnow()

    # Get the very oldest item. The reason for keys_only=True is that we suspect
    # (to be verified) that the datastore will accept more inconsistency, which
    # increases the likelihood of not timing out.
    q = model.ContentEntry.query(
        default_options=ndb.QueryOptions(
            keys_only=True, read_policy=ndb.EVENTUAL_CONSISTENCY)).order(
                model.ContentEntry.expiration_ts)
    entity = None
    try:
      key = q.get()
      entity = key.get()
    except datastore_errors.Timeout:
      # This happens on prod instance with a huge table. We're talking range of
      # 6 billions entities. This is because the query above is exact, not an
      # estimation.
      logging.warning('Query timed out; guessing instead')
      days_interval = 1
      for i in range(300, -1, -days_interval):
        # It was observed that limiting the range on both sides helps with the
        # chances of the query succeeding, instead of raising a Timeout.
        q = model.ContentEntry.query(
            model.ContentEntry.expiration_ts < now - datetime.timedelta(days=i),
            model.ContentEntry.expiration_ts >=
            now - datetime.timedelta(days=i + days_interval))
        try:
          # Don't order() here otherwise the query will likely time out. Don't
          # bother with keys_only=True since the lack of order() should suffice.
          # Just find a goddam expired entity, that's sufficient for our needs.
          entity = q.get()
        except datastore_errors.Timeout:
          logging.warning('Still no dice, got a timeout with %d days ago', i)
        if entity:
          logging.info('Found something %d days ago', i)
          break
    if not entity:
      logging.debug('No oldest found')
      return
    if entity.expiration_ts >= now:
      logging.info('%s >= %s skipping', entity.expiration_ts, now)
      return
    logging.debug('Oldest: %s', entity.expiration_ts)

    # As a crude way to parallelise the query, shard subqueries to be bounded
    # per hour. Normally there should be one hour (or two when crossing hour)
    # but not much more. When in backlogged mode, there could be many many
    # hours.
    oldest = entity.expiration_ts
    current = oldest
    triggered = 0
    hours = 0
    while current < now:
      if time.time() >= time_to_stop:
        # The cron job ran for too long. There's a lot of backlog. Not a big
        # deal, it will be triggered again soon.
        break
      if hours == 40:
        # Limit the number of parallel queries at a time, we don't want to blow
        # up quota.
        break

      hours += 1
      end = current + datetime.timedelta(hours=1)
      if end > now:
        end = now
      data = {'start': current, 'end': end}
      if utils.enqueue_task(
          '/internal/taskqueue/cleanup/query_expired',
          'cleanup-query-expired', payload=utils.encode_to_json(data)):
        triggered += 1
      else:
        logging.warning('Failed to trigger task for %s', data)
      current = end
    logging.info('Triggered %d tasks for %d hour(s) starting %s up to %s',
                 triggered, hours, oldest, now)


class CronCleanupOrphanHandler(webapp2.RequestHandler):
  """Triggers a taskqueue."""
  @decorators.require_cronjob
  def get(self):
    if not utils.enqueue_task(
        '/internal/taskqueue/cleanup/orphan',
        'cleanup-orphan'):
      logging.warning('Failed to trigger task')


class CronStatsUpdateHandler(webapp2.RequestHandler):
  """Called every few minutes to update statistics."""
  @decorators.require_cronjob
  def get(self):
    minutes = stats.cron_generate_stats()
    if minutes is not None:
      logging.info('Processed %d minutes', minutes)


class CronStatsSendToBQHandler(webapp2.RequestHandler):
  """Called every few minutes to send statistics to BigQuery."""
  @decorators.require_cronjob
  def get(self):
    stats.cron_send_to_bq()


### Task queue handlers


class TaskCleanupQueryExpiredHandler(webapp2.RequestHandler):
  """Runs a query for one day worth of expired ContentEntry."""
  # pylint: disable=no-self-use
  @decorators.require_taskqueue('cleanup-query-expired')
  def post(self):
    # Do not run for more than 9 minutes. Exceeding 10min hard limit causes 500.
    time_to_stop = time.time() + 9*60

    data = json.loads(self.request.body)
    start = utils.parse_datetime(data['start'])
    end = utils.parse_datetime(data['end'])
    logging.info('Deleting between %s and %s', start, end)

    triggered = 0
    total = 0
    q = model.ContentEntry.query(
        model.ContentEntry.expiration_ts >= start,
        model.ContentEntry.expiration_ts < end)
    cursor = None
    more = True
    while more and time.time() < time_to_stop:
      # Since this query dooes not fetch the ContentEntry entities themselves,
      # we cannot easily compute the size of the data deleted.
      keys, cursor, more = q.fetch_page(
          500, start_cursor=cursor, keys_only=True)
      if not keys:
        break
      total += len(keys)
      data = utils.encode_to_json([k.string_id() for k in keys])
      if utils.enqueue_task(
          '/internal/taskqueue/cleanup/expired',
          'cleanup-expired', payload=data):
        triggered += 1
      else:
        logging.warning('Failed to trigger task')
    logging.info('Triggered %d tasks for %d entries', triggered, total)


class TaskCleanupExpiredHandler(webapp2.RequestHandler):
  """Removes the old expired data from the datastore."""
  # pylint: disable=no-self-use
  @decorators.require_taskqueue('cleanup-expired')
  def post(self):
    keys = [
      model.entry_key_from_id(str(l)) for l in json.loads(self.request.body)
    ]
    total = _incremental_delete(keys, model.delete_entry_and_gs_entry_async)
    logging.info('Deleted %d expired entries', total)


class TaskCleanupOrphanHandler(webapp2.RequestHandler):
  """Removes the GS files that are not referenced anymore.

  It can happen for example when a ContentEntry is deleted without the file
  properly deleted.

  Only a task queue task can use this handler.
  """
  # pylint: disable=no-self-use
  @decorators.require_taskqueue('cleanup-orphan')
  def post(self):
    """Enumerates all GS files and delete those that do not have an associated
    ContentEntry.
    """
    gs_bucket = config.settings().gs_bucket
    logging.debug('Operating on GCS bucket: %s', gs_bucket)
    total = _incremental_delete(
        _yield_orphan_gcs_files(gs_bucket),
        lambda f: gcs.delete_file_async(gs_bucket, f, True))
    logging.info('Deleted %d lost GS files', total)
    # TODO(maruel): Find all the empty directories that are old and remove them.
    # We need to safe guard against the race condition where a user would upload
    # to this directory.


class TaskTagWorkerHandler(webapp2.RequestHandler):
  """Tags hot ContentEntry entities that were tested for presence.

  Updates .expiration_ts and .next_tag_ts in ContentEntry to note that a client
  tagged them.

  This makes sure they are not evicted from the LRU cache too fast.
  """
  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      runtime.DeadlineExceededError)
  @decorators.require_taskqueue('tag')
  def post(self, namespace, timestamp):
    saved = 0
    digests = []
    now = utils.timestamp_to_datetime(long(timestamp))
    expiration = config.settings().default_expiration
    try:
      digests = _payload_to_hashes(self, namespace)
      # Requests all the entities at once.
      fetch_futures = ndb.get_multi_async(
          model.get_entry_key(namespace, binascii.hexlify(d)) for d in digests)

      save_futures = []
      while fetch_futures:
        # Return opportunistically the first entity that can be retrieved.
        fetch_futures, done = _throttle_futures(
            fetch_futures, len(fetch_futures)-1)
        for f in done:
          item = f.get_result()
          if item and item.next_tag_ts < now:
            # Update the timestamp. Add a bit of pseudo randomness.
            item.expiration_ts, item.next_tag_ts = model.expiration_jitter(
                now, expiration)
            save_futures.append(item.put_async())
            saved += 1
        save_futures, _ = _throttle_futures(save_futures, 100)

      for f in save_futures:
        f.get_result()
      logging.info(
          'Timestamped %d entries out of %d', saved, len(digests))
    except Exception as e:
      logging.error('Failed to stamp entries: %s\n%d entries', e, len(digests))
      raise


class TaskVerifyWorkerHandler(webapp2.RequestHandler):
  """Verify the SHA-1 matches for an object stored in Cloud Storage."""

  @staticmethod
  def purge_entry(entry, message, *args):
    """Logs error message, deletes |entry| from datastore and GS."""
    logging.error(
        'Verification failed for %s: %s', entry.key.id(), message % args)
    # pylint is confused that ndb.tasklet returns a ndb.Future.
    # pylint: disable=no-member
    model.delete_entry_and_gs_entry_async(entry.key).get_result()

  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError,
      gcs.TransientError,
      runtime.DeadlineExceededError)
  @decorators.require_taskqueue('verify')
  def post(self, namespace, hash_key):
    original_request = self.request.get('req')
    entry = model.get_entry_key(namespace, hash_key).get()
    if not entry:
      logging.error('Failed to find entity\n%s', original_request)
      return
    if entry.is_verified:
      logging.warning('Was already verified\n%s', original_request)
      return
    if entry.content is not None:
      logging.error(
          'Should not be called with inline content\n%s', original_request)
      return

    # Get GS file size.
    gs_bucket = config.settings().gs_bucket
    gs_file_info = gcs.get_file_info(gs_bucket, entry.key.id())

    # It's None if file is missing.
    if not gs_file_info:
      # According to the docs, GS is read-after-write consistent, so a file is
      # missing only if it wasn't stored at all or it was deleted, in any case
      # it's not a valid ContentEntry.
      self.purge_entry(entry, 'No such GS file\n%s', original_request)
      return

    # Expected stored length and actual length should match.
    if gs_file_info.size != entry.compressed_size:
      self.purge_entry(entry,
          'Bad GS file: expected size is %d, actual size is %d\n%s',
          entry.compressed_size, gs_file_info.size,
          original_request)
      return

    save_to_memcache = (
        entry.compressed_size <= model.MAX_MEMCACHE_ISOLATED and
        entry.is_isolated)

    try:
      # crbug.com/916644: Verify 2 times to cope with possible data flakiness.
      verified = False
      for i in range(2):
        digest = model.get_hash(namespace)
        expanded_size = 0

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
        if digest.hexdigest() == hash_key:
          verified = True
          break
        logging.warning(
            'SHA-1 do not match data in %d-th iteration: got %s, want %s',
            i + 1, digest.hexdigest(), hash_key)

      if not verified:
        self.purge_entry(
            entry, 'SHA-1 do not match data\n'
            '%d bytes, %d bytes expanded, expected %d bytes\n%s',
            entry.compressed_size, expanded_size, entry.expanded_size,
            original_request)

    except gcs.NotFoundError as e:
      # Somebody deleted a file between get_file_info and read_file calls.
      self.purge_entry(
          entry, 'File was unexpectedly deleted\n%s', original_request)
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
          'Failed to read the file (%s): %s\n%s',
          e.__class__.__name__, e, original_request)
      return

    # Verified. Data matches the hash.
    entry.expanded_size = expanded_size
    entry.is_verified = True
    future = entry.put_async()
    logging.info(
        '%d bytes (%d bytes expanded) verified\n%s',
        entry.compressed_size, expanded_size, original_request)
    if save_to_memcache:
      model.save_in_memcache(namespace, hash_key, ''.join(stream.accumulated))
    future.wait()
    return


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
    # Cron jobs.
    webapp2.Route(
        r'/internal/cron/cleanup/trigger/expired',
        CronCleanupExpiredHandler),
    webapp2.Route(
        r'/internal/cron/cleanup/trigger/orphan',
        CronCleanupOrphanHandler),

    # Cleanup tasks.
    webapp2.Route(
        r'/internal/taskqueue/cleanup/query_expired',
        TaskCleanupQueryExpiredHandler),
    webapp2.Route(
        r'/internal/taskqueue/cleanup/expired',
        TaskCleanupExpiredHandler),
    webapp2.Route(
        r'/internal/taskqueue/cleanup/orphan',
        TaskCleanupOrphanHandler),

    # Tasks triggered by other request handlers.
    webapp2.Route(
        r'/internal/taskqueue/tag%s/<timestamp:\d+>' % namespace,
        TaskTagWorkerHandler),
    webapp2.Route(
        r'/internal/taskqueue/verify%s' % namespace_key,
        TaskVerifyWorkerHandler),

    # Stats
    webapp2.Route(
        r'/internal/cron/stats/update', CronStatsUpdateHandler),
    webapp2.Route(
        r'/internal/cron/stats/send_to_bq', CronStatsSendToBQHandler),
  ]


def create_application(debug):
  """Creates the url router for the backend.

  The backend only implements urls under /internal/.
  """
  # Necessary due to email sent by cron job.
  template.bootstrap()
  return webapp2.WSGIApplication(get_routes(), debug=debug)
