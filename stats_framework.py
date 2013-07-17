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
import datetime
import logging

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.api import memcache
from google.appengine.ext import ndb
# pylint: enable=E0611,F0401


# Formatting a datetime instance to the minute.
TIME_FORMAT = '%Y-%m-%d %H:%M'


class StatisticsFramework(object):
  # Default lock timeout is one hour when calling generate_snapshot.
  LOCK_TIMEOUT = 3600

  # Maximum number of days to look back to generate stats when starting fresh.
  # It will always start looking at 00:00 on the given day in UTC time.
  MAX_BACKTRACK = 5

  def __init__(self, root_key_id, snapshot_cls, generate_snapshot):
    """Creates an instance to do bookkeeping of statistics.

    Warning: this class will do a datastore operation upon creation to ensure
    the root entity exists.

    Arguments:
    - root_key_id: Root key id of the entity to use for transaction. It must be
                   unique to the instance and application.
    - snapshot_cls: Snapshot class that contains all the data. It must have an
                    accumulate() member function to sum the values from one
                    instance into another. It is important that all properties
                    have sensible default value.
    - generate_snapshot: Function taking (start_time, end_time) as epoch and
                         returning a snapshot_cls instance for this time frame.
    """
    # All these members should be considered constants. This class is
    # thread-safe since it is never mutated.
    self.root_key_id = root_key_id
    self.snapshot_cls = snapshot_cls
    self._generate_snapshot = generate_snapshot

    # Generate the model classes.
    self.root_stats_cls = self._generate_root_stats_cls()
    self.daily_stats_cls = self._generate_daily_stats_cls()
    self.hourly_stats_cls = self._generate_hourly_stats_cls()
    self.minute_stats_cls = self._generate_minute_stats_cls()

    self.root_key = ndb.Key(self.root_stats_cls, self.root_key_id)

    # The root entity is used for transactions so make sure it exists.
    self.root_stats_cls.get_or_insert(self.root_key_id)

    self.memcache_namespace = 'stats_%s' % self.root_key_id

  def process_next_chunk(self, up_to, get_now=None):
    """Processes as much minutes starting at a specific time.

    This class should be called from a non-synchronized cron job, so it's more
    or less guaranteed to only have one instance running at a time. In any case,
    it keeps its own internal locking to protect itself against misuse.
    Explicitly handles datastore inconsistency.

    Arguments:
    - up_to: number of minutes to buffer between 'now' and the last minute to
             process. Will usually be in the range of 1 to 10.
    - get_now: optional argument that returns a 'now' value. Mostly to be used
               in test to hard code the value of 'now'.

    Returns the number of self.minute_stats_cls generated, e.g. the number of
    minutes processed successfully by self_generate_snapshot.
    """
    if not memcache.add(
        'lock', 1, time=self.LOCK_TIMEOUT, namespace=self.memcache_namespace):
      return 0

    get_now = get_now or datetime.datetime.utcnow
    # At this point, the 'lock' is owned.
    try:
      now = get_now()
      next_minute = self._get_next_minute_to_process(now)
      count = 0
      while now - next_minute >= datetime.timedelta(minutes=up_to):
        # Keep the lock alive.
        memcache.set(
            'lock', 1,
            time=self.LOCK_TIMEOUT,
            namespace=self.memcache_namespace)
        self._process_one_minute(next_minute)
        self._set_last_processed_time(next_minute)
        count += 1
        next_minute = next_minute + datetime.timedelta(minutes=1)
        now = get_now()
      return count
    finally:
      # Make sure to release the lock. At worst, the lock will be held for
      # self.LOCK_TIMEOUT.
      memcache.delete('lock', namespace=self.memcache_namespace)

  def day_key(self, day):
    """Returns the complete entity key for a specific day stats.

    The key is to a self.daily_stats_cls instance.

    Argument:
      - day is a datetime.date instance.
    """
    assert isinstance(day, datetime.date)
    return ndb.Key(
        self.root_stats_cls, self.root_key_id,
        self.daily_stats_cls, str(day))

  def hour_key(self, hour):
    """Returns the complete entity key for a specific hour stats.

    Argument:
      - hour is a datetime.datetime.
    """
    assert isinstance(hour, datetime.datetime)
    return ndb.Key(
        self.root_stats_cls, self.root_key_id,
        self.daily_stats_cls, str(hour.date()),
        self.hourly_stats_cls, '%02d' % hour.hour)

  def minute_key(self, minute):
    """Returns the complete entity key for a specific minute stats.

    Argument:
      - minute is a datetime.date instance.
    """
    assert isinstance(minute, datetime.datetime)
    return ndb.Key(
        self.root_stats_cls, self.root_key_id,
        self.daily_stats_cls, str(minute.date()),
        self.hourly_stats_cls, '%02d' % minute.hour,
        self.minute_stats_cls, '%02d' % minute.minute)

  ### Protected code.

  @staticmethod
  def _generate_root_stats_cls():
    class RootStats(ndb.Model):
      """Used as a base class for transaction coherency."""
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)

    return RootStats

  def _generate_daily_stats_cls(self):
    class DailyStats(ndb.Model):
      """Statistics for the whole day.

      The Key format is YYYY-MM-DD with 0 prefixes so the key sort naturally.
      Ancestor is self.root_stats_cls with key id self.root_key_id.

      This entity is updated every time a new self.hourly_stats_cls is sealed,
      so ~1 update per hour.
      """
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)
      modified = ndb.DateTimeProperty(indexed=False, auto_now_add=True)

      # Statistics for the day.
      values = ndb.LocalStructuredProperty(
          self.snapshot_cls, default=self.snapshot_cls())

      # Hours that have been summed. A complete day will be set to (1<<24)-1,
      # e.g.  0xFFFFFF, e.g. 24 bits or 6x4 bits.
      hours_bitmap = ndb.IntegerProperty(default=0)

      # Used for queries.
      SEALED_BITMAP = 0xFFFFFF

      def to_date(self):
        """Returns the datetime.date instance for this instance."""
        year, month, day = self.key.id().split('-', 2)
        return datetime.date(int(year), int(month), int(day))

    return DailyStats

  def _generate_hourly_stats_cls(self):
    class HourlyStats(ndb.Model):
      """Statistics for a single hour.

      The Key format is HH with 0 prefix so the key sort naturally. Ancestor is
      self.daily_stats_cls.

      This entity is updated every time a new self.minute_stats_cls is
      generated under a transaction, so ~1 transaction per minute.
      """
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)
      values = ndb.LocalStructuredProperty(
          self.snapshot_cls, default=self.snapshot_cls())

      # Minutes that have been summed. A complete hour will be set to (1<<60)-1,
      # e.g. 0xFFFFFFFFFFFFFFF, e.g. 60 bits or 15x4 bits.
      minutes_bitmap = ndb.IntegerProperty(indexed=False, default=0)

      # Used for queries.
      SEALED_BITMAP = 0xFFFFFFFFFFFFFFF

    return HourlyStats

  def _generate_minute_stats_cls(self):
    class MinuteStats(ndb.Model):
      """Statistics for a single minute.

      The Key format is MM with 0 prefix so the key sort naturally. Ancestor is
      self.hourly_stats_cls.

      This entity is written once and never modified so it is sealed by
      definition.
      """
      created = ndb.DateTimeProperty(indexed=False, auto_now=True)
      values = ndb.LocalStructuredProperty(
          self.snapshot_cls, default=self.snapshot_cls())

    return MinuteStats

  def _set_last_processed_time(self, moment):
    """Saves to memcache the last minute processed."""
    t = (moment.year, moment.month, moment.day, moment.hour, moment.minute)
    memcache.set('last_processed', t, namespace=self.memcache_namespace)

  def _get_next_minute_to_process(self, now):
    """Returns a datetime.datetime representing the last minute that was last
    sealed.

    It ensures the entities self.daily_stats_cls and self.hourly_stats_cls
    exist for the minute that is going to be processed.

    It doesn't look at self.minute_stats_cls so the entity for the minute could
    exist.
    """
    value = memcache.get('last_processed', namespace=self.memcache_namespace)
    if value:
      assert isinstance(value, tuple) and len(value) == 5, value
      # Returns the minute right after.
      last_processed = datetime.datetime(*value)
      minute_after = last_processed + datetime.timedelta(minutes=1)

      if minute_after.date() != last_processed.date():
        # That was 23:59. Make sure day for 00:00 exists.
        self.daily_stats_cls.get_or_insert(
            str(minute_after.date()), parent=self.root_key)

      if minute_after.hour != last_processed.hour:
        # That was NN:59.
        self.hourly_stats_cls.get_or_insert(
            '%02d' % minute_after.hour,
            parent=self.day_key(minute_after.date()))
      return minute_after

    # Search backward in time for the last sealed day. It can return nothing if
    # stats are just getting started.
    q = self.daily_stats_cls.query(ancestor=self.root_key)
    q.filter(
        self.daily_stats_cls.hours_bitmap ==
        self.daily_stats_cls.SEALED_BITMAP)
    q.order(-self.daily_stats_cls.key)
    last_sealed_day = q.get()
    if not last_sealed_day:
      # Maybe there's an non-sealed day. Get the latest one.
      q = self.daily_stats_cls.query(ancestor=self.root_key)
      q.order(-self.daily_stats_cls.key)
      day = q.get()
      if not day:
        # Too bad. Use ~5 days ago at midnight to regenerate stats. It will be
        # resource intensive.
        today = now.date()
        key_id = str(today - datetime.timedelta(days=self.MAX_BACKTRACK))
        day = self.daily_stats_cls.get_or_insert(key_id, parent=self.root_key)
    else:
      # Take the next unsealed day. Note that if there's a sealed, non-sealed,
      # sealed sequence of self.daily_stats_cls, the non-sealed entity will be
      # skipped.
      day_after = last_sealed_day.to_date() + datetime.timedelta(days=1)
      day = self.daily_stats_cls.get_or_insert(str(day_after))

    # TODO(maruel): Should we trust it all the time or do an explicit query? For
    # now, trust the bitmap.
    hour_bit = _lowest_missing_bit(day.hours_bitmap)
    assert hour_bit < 24, hour_bit
    hour = self.hourly_stats_cls.get_or_insert(
        '%02d' % hour_bit, parent=day.key)
    minute_bit = _lowest_missing_bit(hour.minutes_bitmap)
    assert minute_bit < 60, minute_bit
    date = day.to_date()
    return datetime.datetime(
        date.year, date.month, date.day, hour_bit, minute_bit)

  def _process_one_minute(self, moment):
    """Generates exactly one self.minute_stats_cls.

    Always process logs in exactly 1 minute chunks. It is small so it won't take
    too long even under relatively high QPS.

    In theory a transaction should be used when saving the aggregated statistics
    in self.hourly_stats_cls and self.daily_stats_cls. In practice it is not
    necessary because:
    - The caller uses a lock to guard against concurrent calls.
    - Even if it were to become inconsistent or have 2 cron jobs run
      simultaneously, hours_bit|minutes_bit will stay internally consistent with
      the associated values snapshot in it in the respective
      self.daily_stats_cls and self.hourly_stats_cls entities.
    """
    minute_key_id = '%02d' % moment.minute

    # Fetch the entities.
    future_daily = self.daily_stats_cls.get_or_insert_async(
        str(moment.date()), parent=self.root_key)
    future_hourly = self.hourly_stats_cls.get_or_insert_async(
        '%02d' % moment.hour, parent=self.day_key(moment.date()))
    future_minute = self.minute_stats_cls.get_by_id_async(
        minute_key_id, parent=self.hour_key(moment))

    daily = future_daily.get_result()
    hourly = future_hourly.get_result()
    # Normally 'minute' should be None.
    minute = future_minute.get_result()
    futures = []

    if not minute:
      # Call the harvesting function.
      end = moment + datetime.timedelta(minutes=1)
      minute_values = self._generate_snapshot(
          calendar.timegm(moment.timetuple()), calendar.timegm(end.timetuple()))

      minute = self.minute_stats_cls(
          id=minute_key_id, parent=hourly.key, values=minute_values)
      futures.append(minute.put_async())
    else:
      minute_values = minute.values

    minute_bit = (1 << moment.minute)
    minute_bit_is_set = bool(hourly.minutes_bitmap & minute_bit)
    if not minute_bit_is_set:
      hourly.values.accumulate(minute_values)
      hourly.minutes_bitmap |= minute_bit
      futures.append(hourly.put_async())
      if hourly.minutes_bitmap == self.hourly_stats_cls.SEALED_BITMAP:
        logging.info(
            '%s Hour is sealed: %s-%s',
            self.root_key_id, daily.key.id(), hourly.key.id())

    # Adds data for the past hour back into daily.
    if hourly.minutes_bitmap == self.hourly_stats_cls.SEALED_BITMAP:
      hour_bit = (1 << moment.hour)
      hour_bit_is_set = bool(daily.hours_bitmap & hour_bit)
      if not hour_bit_is_set:
        daily.values.accumulate(hourly.values)
        daily.hours_bitmap |= hour_bit
        futures.append(daily.put_async())
        if daily.hours_bitmap == self.daily_stats_cls.SEALED_BITMAP:
          logging.info(
              '%s Day is sealed: %s', self.root_key_id, daily.key.id())

    if futures:
      ndb.Future.wait_all(futures)


def accumulate(lhs, rhs):
  """Adds the values from rhs into lhs.

  rhs._properties not in lhs._properties are lost.
  lhs._properties not in rhs._properties are untouched.
  """
  # Access to a protected member NNN of a client class
  # pylint: disable=W0212
  for key in set(lhs._properties).intersection(rhs._properties):
    if hasattr(lhs, key) and hasattr(rhs, key):
      default = lhs._properties[key]._default
      total = getattr(lhs, key, default) + getattr(rhs, key, default)
      setattr(lhs, key, total)


def _lowest_missing_bit(bitmap):
  """For a bitmap, returns the lowest missing bit.

  Do not check the sign bit. If all bits are set, return the sign bit. It's the
  caller to handle this case.
  """
  for i in xrange(64):
    if not (bitmap & (1 << i)):
      return i
  return 64
