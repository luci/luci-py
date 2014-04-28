# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Framework to handle bookkeeping of statistics generation at the minute level.

It implements all the code to update and store results coherently despise
potential datastore inconsistencies, in a transaction-less and efficient manner.

This framework doesn't gather data by itself. Data harvesting, the actual
measurements saved and presentations must be supplied by the user.
"""

import calendar
import collections
import datetime
import logging

from google.appengine.api import datastore_errors
from google.appengine.api import logservice
from google.appengine.ext import ndb
from google.appengine.runtime import DeadlineExceededError

from components import utils


# Logs prefix.
PREFIX = 'Stats: '


# Number of minutes to ignore because they are too fresh. This is done so that
# eventual log consistency doesn't have to be managed explicitly. On the dev
# server, there's no eventual inconsistency so process up to the last minute.
TOO_RECENT = 5 if not utils.is_local_dev_server() else 1


# One handled HTTP request and the associated statistics if any.
StatsEntry = collections.namedtuple('StatsEntry', ('request', 'entries'))


class StatisticsFramework(object):
  # Maximum number of days to look back to generate stats when starting fresh.
  # It will always start looking at 00:00 on the given day in UTC time.
  MAX_BACKTRACK = 5
  # Process at maximum 2 hours at a time.
  MAX_MINUTES_PER_PROCESS = 120

  def __init__(self, root_key_id, snapshot_cls, generate_snapshot):
    """Creates an instance to do bookkeeping of statistics.

    Arguments:
    - root_key_id: Root key id of the entity to use for transaction. It must be
                   unique to the instance and application.
    - snapshot_cls: Snapshot class that contains all the data. It must have an
                    accumulate() member function to sum the values from one
                    instance into another. It is important that all properties
                    have sensible default value.
    - generate_snapshot: Function taking (start_time, end_time) as epoch and
                         returning a snapshot_cls instance for this time frame.

    ndb access to self.root_key is using both local cache and memcache but
    access to stats_day_cls, stats_hour_cls and stats_minute_cls does not use
    ndb's memcache.
    """
    # All these members should be considered constants. This class is
    # thread-safe since it is never mutated.
    self.root_key_id = root_key_id
    self.snapshot_cls = snapshot_cls
    self._generate_snapshot = generate_snapshot

    # Generate the model classes. The factories are members so they can be
    # overriden if necessary.
    self.stats_root_cls = self._generate_stats_root_cls()
    self.root_key = ndb.Key(self.stats_root_cls, self.root_key_id)
    self.stats_day_cls = self._generate_stats_day_cls(self.snapshot_cls)
    self.stats_hour_cls = self._generate_stats_hour_cls(self.snapshot_cls)
    self.stats_minute_cls = self._generate_stats_minute_cls(self.snapshot_cls)

  def process_next_chunk(self, up_to):
    """Processes as much minutes starting at a specific time.

    This class should be called from a non-synchronized cron job, so it will
    rarely have more than one instance running at a time. Every entity is self
    contained so it explicitly handles datastore inconsistency.

    Arguments:
    - up_to: number of minutes to buffer between 'now' and the last minute to
             process. Will usually be in the range of 1 to 10.

    Returns the number of self.stats_minute_cls generated, e.g. the number of
    minutes processed successfully by self_generate_snapshot.
    """
    count = 0
    original_minute = None
    try:
      now = _utcnow()
      original_minute = self._get_next_minute_to_process(now)
      next_minute = original_minute
      while now - next_minute >= datetime.timedelta(minutes=up_to):
        self._process_one_minute(next_minute)
        self._set_last_processed_time(next_minute)
        count += 1
        if self.MAX_MINUTES_PER_PROCESS == count:
          break
        next_minute = next_minute + datetime.timedelta(minutes=1)
        now = _utcnow()
      return count
    except DeadlineExceededError:
      msg = (
          'Got DeadlineExceededError while processing stats.\n'
          'Processing started at %s; tried to get up to %smins from now; '
          'Processed %dmins') % (
          original_minute, up_to, count)
      if not count:
        logging.error(msg)
      else:
        logging.warning(msg)
      raise

  def day_key(self, day):
    """Returns the complete entity key for a specific day stats.

    The key is to a self.stats_day_cls instance.

    Argument:
      - day is a datetime.date instance.
    """
    assert day.__class__ is datetime.date
    return ndb.Key(
        self.stats_root_cls, self.root_key_id,
        self.stats_day_cls, str(day))

  def hour_key(self, hour):
    """Returns the complete entity key for a specific hour stats.

    Argument:
      - hour is a datetime.datetime.
    """
    assert isinstance(hour, datetime.datetime)
    return ndb.Key(
        self.stats_root_cls, self.root_key_id,
        self.stats_day_cls, str(hour.date()),
        self.stats_hour_cls, '%02d' % hour.hour)

  def minute_key(self, minute):
    """Returns the complete entity key for a specific minute stats.

    Argument:
      - minute is a datetime.date instance.
    """
    assert isinstance(minute, datetime.datetime)
    return ndb.Key(
        self.stats_root_cls, self.root_key_id,
        self.stats_day_cls, str(minute.date()),
        self.stats_hour_cls, '%02d' % minute.hour,
        self.stats_minute_cls, '%02d' % minute.minute)

  ### Protected code.

  @staticmethod
  def _generate_stats_root_cls():
    class StatsRoot(ndb.Model):
      """Used as a base class for transaction coherency.

      It will be updated once every X minutes when the cron job runs to gather
      new data.
      """
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)
      timestamp = ndb.DateTimeProperty(indexed=False)

    return StatsRoot

  @staticmethod
  def _generate_stats_day_cls(snapshot_cls):
    class StatsDay(ndb.Model):
      """Statistics for the whole day.

      The Key format is YYYY-MM-DD with 0 prefixes so the key sort naturally.
      Ancestor is self.stats_root_cls with key id self.root_key_id.

      This entity is updated every time a new self.stats_hour_cls is sealed,
      so ~1 update per hour.
      """
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)
      modified = ndb.DateTimeProperty(indexed=False, auto_now_add=True)

      # Statistics for the day.
      values_compressed = ndb.LocalStructuredProperty(
          snapshot_cls, compressed=True, name='values_c')
      # This is needed for backward compatibility
      values_uncompressed = ndb.LocalStructuredProperty(
          snapshot_cls, name='values')

      # Hours that have been summed. A complete day will be set to (1<<24)-1,
      # e.g.  0xFFFFFF, e.g. 24 bits or 6x4 bits.
      hours_bitmap = ndb.IntegerProperty(default=0)

      # Used for queries.
      SEALED_BITMAP = 0xFFFFFF

      @property
      def values(self):
        return self.values_compressed or self.values_uncompressed

      def to_dict(self):
        return {
          'key': self.to_date(),
          'values': self.values.to_dict(),
        }

      def to_date(self):
        """Returns the datetime.date instance for this instance."""
        year, month, day = self.key.id().split('-', 2)
        return datetime.date(int(year), int(month), int(day))

      def _pre_put_hook(self):
        if bool(self.values_compressed) == bool(self.values_uncompressed):
          raise datastore_errors.BadValueError('Invalid object')

    return StatsDay

  @staticmethod
  def _generate_stats_hour_cls(snapshot_cls):
    class StatsHour(ndb.Model):
      """Statistics for a single hour.

      The Key format is HH with 0 prefix so the key sort naturally. Ancestor is
      self.stats_day_cls.

      This entity is updated every time a new self.stats_minute_cls is
      generated under a transaction, so ~1 transaction per minute.
      """
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)
      # Statistics for the hour.
      values_compressed = ndb.LocalStructuredProperty(
          snapshot_cls, compressed=True, name='values_c')
      # This is needed for backward compatibility
      values_uncompressed = ndb.LocalStructuredProperty(
          snapshot_cls, name='values')

      # Minutes that have been summed. A complete hour will be set to (1<<60)-1,
      # e.g. 0xFFFFFFFFFFFFFFF, e.g. 60 bits or 15x4 bits.
      minutes_bitmap = ndb.IntegerProperty(indexed=False, default=0)

      # Used for queries.
      SEALED_BITMAP = 0xFFFFFFFFFFFFFFF

      @property
      def values(self):
        return self.values_compressed or self.values_uncompressed

      def to_dict(self):
        return {
          'key': self.to_datetime(),
          'values': self.values.to_dict(),
        }

      def to_datetime(self):
        """Returns the datetime.datetime instance for this instance."""
        key = self.key
        year, month, day = key.parent().id().split('-', 2)
        return datetime.datetime(
            int(year), int(month), int(day), int(key.id()))

      def _pre_put_hook(self):
        if bool(self.values_compressed) == bool(self.values_uncompressed):
          raise datastore_errors.BadValueError('Invalid object')

    return StatsHour

  @staticmethod
  def _generate_stats_minute_cls(snapshot_cls):
    class StatsMinute(ndb.Model):
      """Statistics for a single minute.

      The Key format is MM with 0 prefix so the key sort naturally. Ancestor is
      self.stats_hour_cls.

      This entity is written once and never modified so it is sealed by
      definition.
      """
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)
      # Statistics for one minute.
      values_compressed = ndb.LocalStructuredProperty(
          snapshot_cls, compressed=True, name='values_c')
      # This is needed for backward compatibility
      values_uncompressed = ndb.LocalStructuredProperty(
          snapshot_cls, name='values')

      @property
      def values(self):
        return self.values_compressed or self.values_uncompressed

      def to_dict(self):
        return {
          'key': self.to_datetime(),
          'values': self.values.to_dict(),
        }

      def to_datetime(self):
        """Returns the datetime.datetime instance for this instance."""
        key = self.key
        year, month, day = key.parent().parent().id().split('-', 2)
        hour = key.parent().id()
        return datetime.datetime(
            int(year), int(month), int(day), int(hour), int(key.id()))

      def _pre_put_hook(self):
        if bool(self.values_compressed) == bool(self.values_uncompressed):
          raise datastore_errors.BadValueError('Invalid object')

    return StatsMinute

  def _set_last_processed_time(self, moment):
    """Saves the last minute processed.

    Occasionally used in tests to bound the search for statistics.
    """
    assert moment.second == 0, moment
    assert moment.microsecond == 0, moment
    root = self.root_key.get()
    if not root:
      # This is bad, someone deleted the root entity.
      root = self.stats_root_cls(id=self.root_key_id)
    root.timestamp = moment
    root.put()

  def _get_next_minute_to_process(self, now):
    """Returns a datetime.datetime representing the last minute that was last
    sealed.

    It ensures the entities self.stats_day_cls and self.stats_hour_cls
    exist for the minute that is going to be processed.

    It doesn't look at self.stats_minute_cls so the entity for the minute could
    exist.
    """
    root = self.root_key.get()
    if root and root.timestamp:
      # Returns the minute right after.
      # Zap seconds and microseconds from root.timestamp to be much more strict
      # about the processing.
      timestamp = datetime.datetime(*root.timestamp.timetuple()[:5], second=0)
      minute_after = timestamp + datetime.timedelta(minutes=1)
      if minute_after.date() != timestamp.date():
        # That was 23:59. Make sure day for 00:00 exists.
        self.stats_day_cls.get_or_insert(
            str(minute_after.date()), parent=self.root_key,
            values_compressed=self.snapshot_cls())

      if minute_after.hour != timestamp.hour:
        # That was NN:59.
        self.stats_hour_cls.get_or_insert(
            '%02d' % minute_after.hour,
            parent=self.day_key(minute_after.date()),
            values_compressed=self.snapshot_cls())
      return minute_after
    return self._guess_earlier_minute_to_process(now)

  def _guess_earlier_minute_to_process(self, now):
    """Searches backward in time for the last sealed day."""
    logging.info(
        '%s.timestamp is missing or invalid; searching for last sealed day',
        self.root_key)
    # Use a loop to find it. In practice it would require an index but it's
    # better to not have the index and make this code slower. We do it by
    # counting all the entities and fetching the last one.
    q = self.stats_day_cls.query(ancestor=self.root_key)
    # TODO(maruel): It should search for recent unsealed day, that is still
    # reachable from the logs, e.g. the logs for that day hasn't been expurged
    # yet.
    q.filter(
        self.stats_day_cls.hours_bitmap == self.stats_day_cls.SEALED_BITMAP)
    # Can't use .order(-self.stats_day_cls.key) without an index. So use a cheap
    # tricky with offset=count-1.
    # TODO(maruel): that this won't work on very large history, likely after 3
    # years worth of history. Fix this accordingly by 2016.
    count = q.count()
    if not count:
      logging.info('Failed to find sealed day')
      # Maybe there's an non-sealed day. Get the latest one.
      q = self.stats_day_cls.query(ancestor=self.root_key)
      q.order(-self.stats_day_cls.key)
      day = q.get()
      if not day:
        logging.info('Failed to find a day at all')
        # We are bootstrapping as there is no entity at all. Use ~5 days ago at
        # midnight to regenerate stats. It will be resource intensive.
        today = now.date()
        key_id = str(today - datetime.timedelta(days=self.MAX_BACKTRACK))
        day = self.stats_day_cls.get_or_insert(
            key_id, parent=self.root_key, values_compressed=self.snapshot_cls())
        logging.info('Selected/created: %s', key_id)
      else:
        logging.info('Selected: %s', day.key)
    else:
      last_sealed_day = q.get(offset=count-1)
      assert last_sealed_day
      # Take the next unsealed day. Note that if there's a non-sealed, sealed,
      # sealed sequence of self.stats_day_cls, the non-sealed entity will be
      # skipped.
      # TODO(maruel): Fix this.
      key_id = str(last_sealed_day.to_date() + datetime.timedelta(days=1))
      day = self.stats_day_cls.get_or_insert(
          key_id, parent=self.root_key, values_compressed=self.snapshot_cls())
      logging.info('Selected: %s', key_id)

    # TODO(maruel): Should we trust it all the time or do an explicit query? For
    # now, trust the bitmap.
    hour_bit = _lowest_missing_bit(day.hours_bitmap)
    assert hour_bit < 24, (hour_bit, day.to_date())

    hour = self.stats_hour_cls.get_or_insert(
        '%02d' % hour_bit, parent=day.key,
        values_compressed=self.snapshot_cls())
    minute_bit = _lowest_missing_bit(hour.minutes_bitmap)
    assert minute_bit < 60, minute_bit
    date = day.to_date()
    result = datetime.datetime(
        date.year, date.month, date.day, hour_bit, minute_bit)
    logging.info('Using: %s', result)
    return result

  def _process_one_minute(self, moment):
    """Generates exactly one self.stats_minute_cls.

    Always process logs in exactly 1 minute chunks. It is small so it won't take
    too long even under relatively high QPS.

    In theory a transaction should be used when saving the aggregated statistics
    in self.stats_hour_cls and self.stats_day_cls. In practice it is not
    necessary because:
    - The caller uses a lock to guard against concurrent calls.
    - Even if it were to become inconsistent or have 2 cron jobs run
      simultaneously, hours_bit|minutes_bit will stay internally consistent with
      the associated values snapshot in it in the respective
      self.stats_day_cls and self.stats_hour_cls entities.
    """
    minute_key_id = '%02d' % moment.minute

    # Fetch the entities. Do not use ndb's memcache but use in-process local
    # cache.
    opts = ndb.ContextOptions(use_memcache=False)
    future_day = self.stats_day_cls.get_or_insert_async(
        str(moment.date()), parent=self.root_key,
        values_compressed=self.snapshot_cls(),
        context_options=opts)
    future_hour = self.stats_hour_cls.get_or_insert_async(
        '%02d' % moment.hour, parent=self.day_key(moment.date()),
        values_compressed=self.snapshot_cls(),
        context_options=opts)
    future_minute = self.stats_minute_cls.get_by_id_async(
        minute_key_id, parent=self.hour_key(moment), use_memcache=False)

    day = future_day.get_result()
    hour = future_hour.get_result()
    # Normally 'minute' should be None.
    minute = future_minute.get_result()
    futures = []

    if not minute:
      # Call the harvesting function.
      end = moment + datetime.timedelta(minutes=1)
      minute_values = self._generate_snapshot(
          calendar.timegm(moment.timetuple()), calendar.timegm(end.timetuple()))

      minute = self.stats_minute_cls(
          id=minute_key_id, parent=hour.key, values_compressed=minute_values)
      futures.append(minute.put_async(use_memcache=False))
    else:
      minute_values = minute.values

    minute_bit = (1 << moment.minute)
    minute_bit_is_set = bool(hour.minutes_bitmap & minute_bit)
    if not minute_bit_is_set:
      hour.values.accumulate(minute_values)
      hour.minutes_bitmap |= minute_bit
      futures.append(hour.put_async(use_memcache=False))
      if hour.minutes_bitmap == self.stats_hour_cls.SEALED_BITMAP:
        logging.info(
            '%s Hour is sealed: %s %s:00',
            self.root_key_id, day.key.id(), hour.key.id())

    # Adds data for the past hour back into day.
    if hour.minutes_bitmap == self.stats_hour_cls.SEALED_BITMAP:
      hour_bit = (1 << moment.hour)
      hour_bit_is_set = bool(day.hours_bitmap & hour_bit)
      if not hour_bit_is_set:
        day.values.accumulate(hour.values)
        day.hours_bitmap |= hour_bit
        futures.append(day.put_async(use_memcache=False))
        if day.hours_bitmap == self.stats_day_cls.SEALED_BITMAP:
          logging.info(
              '%s Day is sealed: %s', self.root_key_id, day.key.id())

    if futures:
      ndb.Future.wait_all(futures)


### Private stuff.


def _lowest_missing_bit(bitmap):
  """For a bitmap, returns the lowest missing bit.

  Do not check the sign bit. If all bits are set, return the sign bit. It's the
  caller to handle this case.
  """
  for i in xrange(64):
    if not (bitmap & (1 << i)):
      return i
  return 64


def _utcnow():
  """To be mocked in tests."""
  return datetime.datetime.utcnow()


def _yield_logs(start_time, end_time):
  """Yields logservice.RequestLogs for the requested time interval.

  Meant to be mocked in tests.
  """
  # If module_versions is not specified, it will default to the current version
  # on current module, which is not what we want.
  # TODO(maruel): Keep request.offset and use it to resume the query by using it
  # instead of using start_time/end_time.
  module_versions = utils.get_module_version_list(None, True)
  for request in logservice.fetch(
      start_time=start_time - 1 if start_time else start_time,
      end_time=end_time + 1 if end_time else end_time,
      include_app_logs=True,
      module_versions=module_versions):
    yield request


### Public API.


def add_entry(message):
  """Adds an entry for the current request.

  Meant to be mocked in tests.
  """
  logging.debug(PREFIX + message)


def accumulate(lhs, rhs):
  """Adds the values from rhs into lhs.

  Both must be an ndb.Model. lhs is modified. rhs is not.

  rhs._properties not in lhs._properties are lost.
  lhs._properties not in rhs._properties are untouched.

  This function has specific handling for ndb.LocalStructuredProperty, it
  refuses any instance with a default value. THIS IS A TRAP. The default object
  instance will be aliased one way or another. It's just not worth the risk.
  """
  assert isinstance(lhs, ndb.Model), lhs
  assert isinstance(rhs, ndb.Model), rhs

  # Access to a protected member NNN of a client class
  # pylint: disable=W0212
  for key in set(lhs._properties).intersection(rhs._properties):
    if hasattr(lhs, key) and hasattr(rhs, key):
      assert not lhs._properties[key]._repeated
      default = lhs._properties[key]._default
      lhs_value = getattr(lhs, key, default)
      rhs_value = getattr(rhs, key, default)
      if hasattr(lhs_value, 'accumulate'):
        # Do not use ndb.LocalStructuredProperty(MyClass, default=MyClass())
        # since any object created without specifying a object for this property
        # will get the exact instance provided as the default argument, it's
        # dangerous aliasing. See the unit test for a way to set a default value
        # that is safe.
        assert default is None, key
        assert callable(lhs_value.accumulate), key
        lhs_value.accumulate(rhs_value)
      else:
        setattr(lhs, key, lhs_value + rhs_value)


def yield_entries(start_time, end_time):
  """Yields StatsEntry in this time interval.

  Look at requests that *ended* between [start_time, end_time[. Ignore the start
  time of the request. This is because the parameters start_time and end_time of
  logserver.fetch() filters on the completion time of the request.
  """
  offset = len(PREFIX)
  for request in _yield_logs(start_time, end_time):
    if not request.finished or not request.end_time:
      continue
    if start_time and request.end_time < start_time:
      continue
    if end_time and request.end_time >= end_time:
      continue

    # Gathers all the entries added via add_entry().
    entries = [
      l.message[offset:] for l in request.app_logs
      if l.level <= logservice.LOG_LEVEL_INFO and l.message.startswith(PREFIX)
    ]
    yield StatsEntry(request, entries)


def filterout(futures):
  """Filters out inexistent entities."""
  intermediary = (i.get_result() for i in futures)
  return [i for i in intermediary if i]


def get_days_async(handler, now, num_days):
  if not num_days:
    return []
  now = now or _utcnow()
  today = now.date()
  gen = (
    handler.day_key(today - datetime.timedelta(days=i))
    for i in xrange(num_days)
  )
  return ndb.get_multi_async(gen, use_cache=False, use_memcache=False)


def get_days(handler, now, num_days):
  return filterout(get_days_async(handler, now, num_days))


def get_hours_async(handler, now, num_hours):
  if not num_hours:
    return []
  now = now or _utcnow()
  gen = (
      handler.hour_key(now - datetime.timedelta(hours=i))
      for i in xrange(num_hours))
  return ndb.get_multi_async(gen, use_cache=False, use_memcache=False)


def get_hours(handler, now, num_hours):
  return filterout(get_hours_async(handler, now, num_hours))


def get_minutes_async(handler, now, num_minutes):
  if not num_minutes:
    return []
  now = now or _utcnow()
  gen = (
      handler.minute_key(now - datetime.timedelta(minutes=i))
      for i in xrange(num_minutes))
  return ndb.get_multi_async(gen, use_cache=False, use_memcache=False)


def get_minutes(handler, now, num_minutes):
  return filterout(get_minutes_async(handler, now, num_minutes))


def generate_stats_data(num_days, num_hours, num_minutes, now, stats_handler):
  """Returns a dict for data requested in the query.

  The values are returned in reverse chronological order.
  TODO(maruel): Does it make sense?

  Disables ndb's local cache and memcache usage.
  """
  # Fire up all the datastore requests.
  future_days = get_days_async(stats_handler, now, num_days)
  future_hours = get_hours_async(stats_handler, now, num_hours)
  future_minutes = get_minutes_async(stats_handler, now, num_minutes)
  return {
    'days': filterout(future_days),
    'hours': filterout(future_hours),
    'minutes': filterout(future_minutes),
    'now': (now or _utcnow()).strftime(utils.DATETIME_FORMAT),
  }


def generate_stats_data_from_request(request, stats_handler):
  """Returns a dict for data requested in the query."""
  DEFAULT_DAYS = 5
  DEFAULT_HOURS = 48
  DEFAULT_MINUTES = 120
  MIN_VALUE = 0
  MAX_DAYS = 367
  MAX_HOURS = 480
  MAX_MINUTES = 480

  days = utils.get_request_as_int(
      request, 'days', DEFAULT_DAYS, MIN_VALUE, MAX_DAYS)
  hours = utils.get_request_as_int(
      request, 'hours', DEFAULT_HOURS, MIN_VALUE, MAX_HOURS)
  minutes = utils.get_request_as_int(
      request, 'minutes', DEFAULT_MINUTES, MIN_VALUE, MAX_MINUTES)
  now = utils.get_request_as_datetime(request, 'now')
  return generate_stats_data(days, hours, minutes, now, stats_handler)
