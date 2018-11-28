# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Swarming bot management, e.g. list of known bots and their state.

    +-----------+
    |BotRoot    |
    |id=<bot_id>|
    +-----------+
        |
        +------+--------------+
        |      |              |
        |      v              v
        |  +-----------+  +-------+
        |  |BotSettings|  |BotInfo|
        |  |id=settings|  |id=info|
        |  +-----------+  +-------+
        |
        +------+-----------+----- ... ----+
        |      |           |              |
        |      v           v              v
        |  +--------+  +--------+     +--------+
        |  |BotEvent|  |BotEvent| ... |BotEvent|
        |  |id=fffff|  |if=ffffe| ... |id=00000|
        |  +--------+  +--------+     +--------+
        |
        +------+
        |      |
        |      v
        |  +-------------+
        |  |BotDimensions|                                        task_queues.py
        |  |id=1         |
        |  +-------------+
        |
        +--------------- ... -----+
        |                         |
        v                         v
    +-------------------+     +-------------------+
    |BotTaskDimensions  | ... |BotTaskDimensions  |               task_queues.py
    |id=<dimension_hash>| ... |id=<dimension_hash>|
    +-------------------+     +-------------------+

    +--------Root--------+
    |DimensionAggregation|
    |id=current          |
    +--------------------+

- BotEvent is a monotonically inserted entity that is added for each event
  happening for the bot.
- BotInfo is a 'dump-only' entity used for UI, it permits quickly show the
  state of every bots in an single query. It is basically a cache of the last
  BotEvent and additionally updated on poll. It doesn't need to be updated in a
  transaction.
- BotSettings contains bot-specific settings. It must be updated in a
  transaction and contains admin-provided settings, contrary to the other
  entities which are generated from data provided by the bot itself.
"""

import datetime
import hashlib
import logging

from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from server import config
from server import task_pack
from server import task_queues


# BotEvent entities are deleted after a while.
_OLD_BOT_EVENTS_CUT_OFF = datetime.timedelta(days=366*2)


### Models.

# There is one BotRoot entity per bot id. Multiple bots could run on a single
# host, for example with multiple phones connected to a host. In this case, the
# id is specific to each device acting as a bot.
BotRoot = datastore_utils.get_versioned_root_model('BotRoot')


class _BotCommon(ndb.Model):
  """Data that is copied from the BotEvent into BotInfo for performance."""
  # State is purely informative. It is completely free form.
  state = datastore_utils.DeterministicJsonProperty(json_type=dict)

  # IP address as seen by the HTTP handler.
  external_ip = ndb.StringProperty(indexed=False)

  # Bot identity as seen by the HTTP handler.
  authenticated_as = ndb.StringProperty(indexed=False)

  # Version of swarming_bot.zip the bot is currently running.
  version = ndb.StringProperty(default='', indexed=False)

  # Set when either:
  # - dimensions['quarantined'] or state['quarantined'] is set. This either
  #   happens via internal python error (e.g. an exception while generating
  #   dimensions) or via self-health check.
  # - dimensions['id'] is not exactly one item.
  # - invalid HTTP POST request keys.
  # - BotSettings.quarantined was set at that moment.
  # https://crbug.com/839415
  quarantined = ndb.BooleanProperty(default=False, indexed=False)

  # If set, the bot is rejecting tasks due to maintenance.
  maintenance_msg = ndb.StringProperty(indexed=False)

  # Affected by event_type == 'request_task', 'task_killed', 'task_completed',
  # 'task_error'.
  task_id = ndb.StringProperty(indexed=False)

  # Machine Provider lease ID, for bots acquired from Machine Provider.
  lease_id = ndb.StringProperty(indexed=False)

  # UTC seconds from epoch when bot will be reclaimed by Machine Provider.
  # Only one of lease_expiration_ts and leased_indefinitely must be specified.
  lease_expiration_ts = ndb.DateTimeProperty(indexed=False)

  # Whether or not the bot is leased indefinitely from Machine Provider.
  # Only one of lease_expiration_ts and leased_indefinitely must be specified.
  leased_indefinitely = ndb.BooleanProperty(indexed=False)

  # ID of the MachineType, for bots acquired from Machine Provider.
  machine_type = ndb.StringProperty(indexed=False)

  # ID of the MachineLease, for bots acquired from Machine Provider.
  machine_lease = ndb.StringProperty(indexed=False)

  # Dimensions are used for task selection. They are encoded as a list of
  # key:value. Keep in mind that the same key can be used multiple times. The
  # list must be sorted. It is indexed to enable searching for bots.
  dimensions_flat = ndb.StringProperty(repeated=True)

  @property
  def dimensions(self):
    """Returns a dict representation of self.dimensions_flat."""
    out = {}
    for i in self.dimensions_flat:
      k, v = i.split(':', 1)
      out.setdefault(k, []).append(v)
    return out

  @property
  def task(self):
    if not self.task_id:
      return None
    return task_pack.unpack_run_result_key(self.task_id)

  def to_dict(self, exclude=None):
    exclude = ['dimensions_flat'] + (exclude or [])
    out = super(_BotCommon, self).to_dict(exclude=exclude)
    out['dimensions'] = self.dimensions
    return out

  def _pre_put_hook(self):
    super(_BotCommon, self)._pre_put_hook()
    self.dimensions_flat.sort()
    if self.lease_expiration_ts and self.leased_indefinitely:
      raise datastore_errors.BadValueError(
        'lease_expiration_ts and leased_indefinitely both set:\n%s' % self)


class BotInfo(_BotCommon):
  """This entity declare the knowledge about a bot that successfully connected.

  Parent is BotRoot. Key id is 'info'.

  This entity is a cache of the last BotEvent and is additionally updated on
  poll, which does not create a BotEvent.
  """
  # One of:
  NOT_IN_MAINTENANCE = 1<<9 # 512
  IN_MAINTENANCE = 1<<8     # 256
  # One of:
  ALIVE = 1<<7 # 128
  DEAD = 1<<6  # 64
  # One of:
  NOT_MACHINE_PROVIDER = 1<<5 # 32
  MACHINE_PROVIDER = 1<<4     # 16
  # One of:
  HEALTHY = 1<<3     # 8
  QUARANTINED = 1<<2 # 4
  # One of:
  IDLE = 1<<1 # 2
  BUSY = 1<<0 # 1

  # First time this bot was seen.
  first_seen_ts = ndb.DateTimeProperty(auto_now_add=True, indexed=False)

  # Last time the bot pinged and this entity was updated
  last_seen_ts = ndb.DateTimeProperty()

  # Must only be set when self.task_id is set.
  task_name = ndb.StringProperty(indexed=False)

  # Avoid having huge amounts of indices to query by quarantined/idle.
  composite = ndb.IntegerProperty(repeated=True)

  def _calc_composite(self):
    """Returns the value for BotInfo.composite, which permits quick searches."""
    timeout = config.settings().bot_death_timeout_secs
    is_dead = (utils.utcnow() - self.last_seen_ts).total_seconds() >= timeout
    return [
      self.IN_MAINTENANCE if self.maintenance_msg else self.NOT_IN_MAINTENANCE,
      self.DEAD if is_dead else self.ALIVE,
      self.MACHINE_PROVIDER if self.machine_type else self.NOT_MACHINE_PROVIDER,
      self.QUARANTINED if self.quarantined else self.HEALTHY,
      self.BUSY if self.task_id else self.IDLE
    ]

  @property
  def id(self):
    return self.key.parent().string_id()

  @property
  def is_dead(self):
    # Only valid after it's stored.
    return self.DEAD in self.composite

  def to_dict(self, exclude=None):
    out = super(BotInfo, self).to_dict(exclude=exclude)
    # Inject the bot id, since it's the entity key.
    out['id'] = self.id
    out['is_dead'] = self.is_dead
    return out

  def _pre_put_hook(self):
    super(BotInfo, self)._pre_put_hook()
    if not self.task_id:
      self.task_name = None
    self.composite = self._calc_composite()


class BotEvent(_BotCommon):
  """This entity is immutable.

  Parent is BotRoot. Key id is monotonically decreasing with
  datastore_utils.store_new_version().

  This entity is created on each bot state transition.
  """
  ALLOWED_EVENTS = {
    # Bot specific events that are outside the scope of a task:
    'bot_connected',
    'bot_error',
    'bot_leased',
    'bot_log',
    'bot_rebooting',
    'bot_shutdown',
    'bot_terminate',

    # Bot polling result:
    'request_restart',
    'request_sleep',
    'request_task',
    'request_update',

    # Task lifetime as processed by the bot:
    'task_completed',
    'task_error',
    'task_killed',
    'task_update',
  }

  # Common properties for all events (which includes everything in _BotCommon).
  ts = ndb.DateTimeProperty(auto_now_add=True)
  event_type = ndb.StringProperty(choices=ALLOWED_EVENTS)

  # event_type == 'bot_error', 'request_restart', 'bot_rebooting', etc.
  message = ndb.TextProperty()

  @property
  def previous_key(self):
    """Returns the ndb.Key to the previous event."""
    return ndb.Key(
        self.__class__, self.key.integer_id()+1, parent=self.key.parent())


class BotSettings(ndb.Model):
  """Contains all settings that are set by the administrator on the server.

  Parent is BotRoot. Key id is 'settings'.

  This entity must always be updated in a transaction.
  """
  # If set to True, no task is handed out to this bot due to the bot being in a
  # broken situation.
  quarantined = ndb.BooleanProperty()


class DimensionValues(ndb.Model):
  """Inlined into DimensionAggregation, never stored standalone."""
  dimension = ndb.StringProperty()
  values = ndb.StringProperty(repeated=True)


class DimensionAggregation(ndb.Model):
  """Has all dimensions that are currently exposed by the bots.

  There's a single root entity stored with id 'current', see KEY below.

  This entity is updated via cron job /internal/cron/aggregate_bots_dimensions
  updated every hour.
  """
  dimensions = ndb.LocalStructuredProperty(DimensionValues, repeated=True)

  ts = ndb.DateTimeProperty()

  # We only store one of these entities. Use this key to refer to any instance.
  KEY = ndb.Key('DimensionAggregation', 'current')


### Private APIs.


### Public APIs.


def get_root_key(bot_id):
  """Returns the BotRoot ndb.Key for a known bot."""
  if not bot_id:
    raise ValueError('Bad id')
  return ndb.Key(BotRoot, bot_id)


def get_info_key(bot_id):
  """Returns the BotInfo ndb.Key for a known bot."""
  return ndb.Key(BotInfo, 'info', parent=get_root_key(bot_id))


def get_events_query(bot_id, order):
  """Returns an ndb.Query for most recent events in reverse chronological order.
  """
  # Disable the in-process local cache. This is important, as there can be up to
  # a thousand entities loaded in memory, and this is a pure memory leak, as
  # there's no chance this specific instance will need these again, therefore
  # this leads to 'Exceeded soft memory limit' AppEngine errors.
  q = BotEvent.query(
      default_options=ndb.QueryOptions(use_cache=False),
      ancestor=get_root_key(bot_id))
  if order:
    q = q.order(BotEvent.key)
  return q


def get_settings_key(bot_id):
  """Returns the BotSettings ndb.Key for a known bot."""
  return ndb.Key(BotSettings, 'settings', parent=get_root_key(bot_id))


def filter_dimensions(q, dimensions):
  """Filters a ndb.Query for BotInfo based on dimensions in the request."""
  for d in dimensions:
    parts = d.split(':', 1)
    if len(parts) != 2 or any(i.strip() != i or not i for i in parts):
      raise ValueError('Invalid dimensions')
    q = q.filter(BotInfo.dimensions_flat == d)
  return q


def filter_availability(
    q, quarantined, in_maintenance, is_dead, is_busy, is_mp):
  """Filters a ndb.Query for BotInfo based on quarantined/is_dead/is_busy."""
  if quarantined is not None:
    if quarantined:
      q = q.filter(BotInfo.composite == BotInfo.QUARANTINED)
    else:
      q = q.filter(BotInfo.composite == BotInfo.HEALTHY)

  if in_maintenance is not None:
    if in_maintenance:
      q = q.filter(BotInfo.composite == BotInfo.IN_MAINTENANCE)
    else:
      q = q.filter(BotInfo.composite == BotInfo.NOT_IN_MAINTENANCE)

  if is_busy is not None:
    if is_busy:
      q = q.filter(BotInfo.composite == BotInfo.BUSY)
    else:
      q = q.filter(BotInfo.composite == BotInfo.IDLE)

  if is_dead:
    q = q.filter(BotInfo.composite == BotInfo.DEAD)
  elif is_dead is not None:
    q = q.filter(BotInfo.composite == BotInfo.ALIVE)

  if is_mp is not None:
    if is_mp:
      q = q.filter(BotInfo.composite == BotInfo.MACHINE_PROVIDER)
    else:
      q = q.filter(BotInfo.composite == BotInfo.NOT_MACHINE_PROVIDER)

  # TODO(charliea): Add filtering based on the 'maintenance' field.

  return q


def bot_event(
    event_type, bot_id, external_ip, authenticated_as, dimensions, state,
    version, quarantined, maintenance_msg, task_id, task_name, **kwargs):
  """Records when a bot has queried for work.

  The sheer fact this event is happening means the bot is alive (not dead), so
  this is good. It may be quarantined though, and in this case, it will be
  evicted from the task queues.

  If it's declaring maintenance, it will not be evicted from the task queues, as
  maintenance is supposed to be temporary and expected to complete within a
  reasonable time frame.

  Arguments:
  - event_type: event type, one of BotEvent.ALLOWED_EVENTS.
  - bot_id: bot id.
  - external_ip: IP address as seen by the HTTP handler.
  - authenticated_as: bot identity as seen by the HTTP handler.
  - dimensions: Bot's dimensions as self-reported. If not provided, keep
        previous value.
  - state: ephemeral state of the bot. It is expected to change constantly. If
        not provided, keep previous value.
  - version: swarming_bot.zip version as self-reported. Used to spot if a bot
        failed to update promptly. If not provided, keep previous value.
  - quarantined: bool to determine if the bot was declared quarantined.
  - maintenance_msg: string describing why the bot is in maintenance.
  - task_id: packed task id if relevant. Set to '' to zap the stored value.
  - task_name: task name if relevant. Zapped when task_id is zapped.
  - kwargs: optional values to add to BotEvent relevant to event_type.
  - lease_id (in kwargs): ID assigned by Machine Provider for this bot.
  - lease_expiration_ts (in kwargs): UTC seconds from epoch when Machine
        Provider lease expires. Only one of lease_expiration_ts and
        leased_indefinitely must be specified.
  - leased_indefinitely (in kwargs): Whether or not the Machine Provider
        lease is indefinite. Only one of lease_expiration_ts and
        leased_indefinitely must be specified.
  - machine_type (in kwargs): ID of the lease_management.MachineType this
        Machine Provider bot was leased for.
  - machine_lease (in kwargs): ID of the lease_management.MachineType
        corresponding to this bot.

  Returns:
    ndb.Key to BotEvent entity if one was added.
  """
  if not bot_id:
    return

  # Retrieve the previous BotInfo and update it.
  info_key = get_info_key(bot_id)
  bot_info = info_key.get()
  if not bot_info:
    bot_info = BotInfo(key=info_key)
  bot_info.last_seen_ts = utils.utcnow()
  bot_info.external_ip = external_ip
  bot_info.authenticated_as = authenticated_as
  bot_info.maintenance_msg = maintenance_msg
  if dimensions:
    bot_info.dimensions_flat = task_queues.dimensions_to_flat(dimensions)
  if state:
    bot_info.state = state
  if quarantined is not None:
    bot_info.quarantined = quarantined
  if task_id is not None:
    bot_info.task_id = task_id
  if task_name:
    bot_info.task_name = task_name
  if version is not None:
    bot_info.version = version
  if kwargs.get('lease_id') is not None:
    bot_info.lease_id = kwargs['lease_id']
  if kwargs.get('lease_expiration_ts') is not None:
    assert not kwargs.get('leased_indefinitely'), bot_id
    bot_info.lease_expiration_ts = kwargs['lease_expiration_ts']
  if kwargs.get('leased_indefinitely') is not None:
    assert not kwargs.get('lease_expiration_ts'), bot_id
    bot_info.leased_indefinitely = kwargs['leased_indefinitely']
  if kwargs.get('machine_type') is not None:
    bot_info.machine_type = kwargs['machine_type']
  if kwargs.get('machine_lease') is not None:
    bot_info.machine_lease = kwargs['machine_lease']

  if quarantined:
    # Make sure it is not in the queue since it can't reap anything.
    task_queues.cleanup_after_bot(info_key.parent())

  if event_type in ('request_sleep', 'task_update'):
    # Handle this specifically. It's not much of an even worth saving a BotEvent
    # for but it's worth updating BotInfo. The only reason BotInfo is GET is to
    # keep first_seen_ts. It's not necessary to use a transaction here since no
    # BotEvent is being added, only last_seen_ts is really updated.
    bot_info.put()
    return

  event = BotEvent(
      parent=get_root_key(bot_id),
      event_type=event_type,
      external_ip=external_ip,
      authenticated_as=authenticated_as,
      dimensions_flat=bot_info.dimensions_flat,
      quarantined=bot_info.quarantined,
      maintenance_msg=bot_info.maintenance_msg,
      state=bot_info.state,
      task_id=bot_info.task_id,
      version=bot_info.version,
      **kwargs)

  if event_type in ('task_completed', 'task_error', 'task_killed'):
    # Special case to keep the task_id in the event but not in the summary.
    bot_info.task_id = ''

  datastore_utils.store_new_version(event, BotRoot, [bot_info])
  return event.key


def has_capacity(dimensions):
  """Returns True if there's a reasonable chance for this task request
  dimensions set to be serviced by a bot alive.

  First look at the task queues, then look into the datastore to figure this
  out.
  """
  assert not ndb.in_transaction()
  # Look at the fast path.
  cap = task_queues.probably_has_capacity(dimensions)
  if cap is not None:
    return cap

  # Do a query. That's slower and it's eventually consistent.
  q = BotInfo.query()
  flat = task_queues.dimensions_to_flat(dimensions)
  for f in flat:
    q = q.filter(BotInfo.dimensions_flat == f)

  # Add it to the 'quick cache' to improve performance. This cache is kept for
  # the same duration as how long bots are considered still alive without a
  # ping. There are two uses case:
  # - there's a single bot in the fleet for these dimensions and it takes a
  #   long time rebooting. This is the case with Android with slow
  #   initialization and some baremetal bots (thanks SCSI firmware!).
  # - Machine Provider recycle the fleet simultaneously, which causes
  #   instantaneous downtime. https://crbug.com/888603
  seconds = config.settings().bot_death_timeout_secs

  if q.count(limit=1):
    logging.info('Found capacity via BotInfo: %s', flat)
    task_queues.set_has_capacity(dimensions, seconds)
    return True

  # Search a bit harder. In this case, we're looking for BotEvent which would be
  # a bot that used to exist recently.
  cutoff = utils.utcnow() - datetime.timedelta(seconds=seconds)
  q = BotEvent.query(BotEvent.ts > cutoff)
  flat = task_queues.dimensions_to_flat(dimensions)
  for f in flat:
    q = q.filter(BotEvent.dimensions_flat == f)
  if q.count(limit=1):
    logging.info('Found capacity via BotEvent: %s', flat)
    task_queues.set_has_capacity(dimensions, seconds)
    return True

  logging.warning('HAS NO CAPACITY: %s', flat)
  return False


def cron_update_bot_info():
  """Refreshes BotInfo.composite for dead bots."""
  dt = datetime.timedelta(seconds=config.settings().bot_death_timeout_secs)
  cutoff = utils.utcnow() - dt

  @ndb.tasklet
  def run(bot_key):
    bot = yield bot_key.get_async()
    if (bot and bot.last_seen_ts <= cutoff and
        (BotInfo.ALIVE in bot.composite or BotInfo.DEAD not in bot.composite)):
      # Updating it recomputes composite.
      yield bot.put_async()
      logging.info('DEAD: %s', bot.id)
      raise ndb.Return(1)
    raise ndb.Return(0)

  # The assumption here is that a cron job can churn through all the entities
  # fast enough. The number of dead bot is expected to be <10k. In practice the
  # average runtime is around 8 seconds.
  dead = 0
  seen = 0
  failed = 0
  try:
    futures = []
    for b in BotInfo.query(BotInfo.last_seen_ts <= cutoff):
      seen += 1
      if BotInfo.ALIVE in b.composite or BotInfo.DEAD not in b.composite:
        # Make sure the variable is not aliased.
        k = b.key
        # Unregister the bot from task queues since it can't reap anything.
        task_queues.cleanup_after_bot(k.parent())
        # Retry more often than the default 1. We do not want to throw too much
        # in the logs and there should be plenty of time to do the retries.
        f = datastore_utils.transaction_async(lambda: run(k), retries=5)
        futures.append(f)
        if len(futures) >= 5:
          ndb.Future.wait_any(futures)
          for i in xrange(len(futures) - 1, -1, -1):
            if futures[i].done():
              try:
                dead += futures.pop(i).get_result()
              except datastore_utils.CommitError:
                logging.warning('Failed to commit a Tx')
                failed += 1
    for f in futures:
      try:
        dead += f.get_result()
      except datastore_utils.CommitError:
        logging.warning('Failed to commit a Tx')
        failed += 1
  finally:
    logging.debug(
        'Seen %d bots, updated %d bots, failed %d tx', seen, dead, failed)
  return dead


def cron_delete_old_bot_events():
  """Deletes very old BotEvent entites."""
  start = utils.utcnow()
  # Run for 4.5 minutes and schedule the cron job every 5 minutes. Running for
  # 9.5 minutes (out of 10 allowed for a cron job) results in 'Exceeded soft
  # private memory limit of 512 MB with 512 MB' even if this loop should be
  # fairly light on memory usage.
  time_to_stop = start + datetime.timedelta(seconds=int(4.5*60))
  more = True
  cursor = None
  count = 0
  try:
    # Order is by key, so it is naturally ordered by bot, which means the
    # operations will mainly operate on one root entity at a time.
    q = BotEvent.query(default_options=ndb.QueryOptions(keys_only=True)).filter(
        BotEvent.ts <= start - _OLD_BOT_EVENTS_CUT_OFF)
    while more:
      keys, cursor, more = q.fetch_page(10, start_cursor=cursor)
      ndb.delete_multi(keys)
      count += len(keys)
      if utils.utcnow() >= time_to_stop:
        break
    return count
  except runtime.DeadlineExceededError:
    pass
  finally:
    logging.info('Deleted %d BotEvent entities', count)
