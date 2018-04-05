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

from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from server import config
from server import task_pack


# Margin of randomization of BOT_REBOOT_PERIOD_SECS. Per-bot period will be in
# range [period * (1 - margin), period * (1 + margin)).
BOT_REBOOT_PERIOD_RANDOMIZATION_MARGIN = 0.2


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
  quarantined = ndb.BooleanProperty(default=False)

  # Affected by event_type == 'request_task', 'task_killed', 'task_completed',
  # 'task_error'.
  task_id = ndb.StringProperty(indexed=False)

  # Machine Provider lease ID, for bots acquired from Machine Provider.
  lease_id = ndb.StringProperty(indexed=False)

  # UTC seconds from epoch when bot will be reclaimed by Machine Provider.
  lease_expiration_ts = ndb.DateTimeProperty(indexed=False)

  # ID of the MachineType, for bots acquired from Machine Provider.
  machine_type = ndb.StringProperty(indexed=False)

  # ID of the MachineLease, for bots acquired from Machine Provider.
  machine_lease = ndb.StringProperty(indexed=False)

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


class BotInfo(_BotCommon):
  """This entity declare the knowledge about a bot that successfully connected.

  Parent is BotRoot. Key id is 'info'.

  This entity is a cache of the last BotEvent and is additionally updated on
  poll, which does not create a BotEvent.
  """
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

  # Dimensions are used for task selection. They are encoded as a list of
  # key:value. Keep in mind that the same key can be used multiple times. The
  # list must be sorted.
  # It IS indexed to enable searching for bots.
  dimensions_flat = ndb.StringProperty(repeated=True)

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
      self.DEAD if is_dead else self.ALIVE,
      self.MACHINE_PROVIDER if self.machine_type else self.NOT_MACHINE_PROVIDER,
      self.QUARANTINED if self.quarantined else self.HEALTHY,
      self.BUSY if self.task_id else self.IDLE
    ]

  @property
  def id(self):
    return self.key.parent().string_id()

  def is_dead(self, now):
    """Returns True if the bot is dead based on timestamp now."""
    # TODO(maruel): https://crbug.com/826421 return self.DEAD in self.composite
    timeout = config.settings().bot_death_timeout_secs
    return (now - self.last_seen_ts).total_seconds() >= timeout

  def to_dict(self, exclude=None):
    out = super(BotInfo, self).to_dict(exclude=exclude)
    # Inject the bot id, since it's the entity key.
    out['id'] = self.id
    return out

  def to_dict_with_now(self, now):
    out = self.to_dict()
    # TODO(maruel): https://crbug.com/826421 Remove.
    out['is_dead'] = self.is_dead(now)
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
    'bot_connected', 'bot_error', 'bot_leased', 'bot_log', 'bot_rebooting',
    'bot_shutdown', 'bot_terminate',
    'request_restart', 'request_update', 'request_sleep', 'request_task',
    'task_completed', 'task_error', 'task_killed', 'task_update',
  }
  # Dimensions are used for task selection. They are encoded as a list of
  # key:value. Keep in mind that the same key can be used multiple times. The
  # list must be sorted.
  # It is NOT indexed because this is not needed for events.
  dimensions_flat = ndb.StringProperty(repeated=True, indexed=False)

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


def dimensions_to_flat(dimensions):
  out = []
  for k, values in dimensions.iteritems():
    for v in values:
      out.append('%s:%s' % (k, v))
  out.sort()
  return out


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
  query = BotEvent.query(ancestor=get_root_key(bot_id))
  if order:
    query = query.order(BotEvent.key)
  return query


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


def filter_availability(q, quarantined, is_dead, now, is_busy, is_mp):
  """Filters a ndb.Query for BotInfo based on quarantined/is_dead/is_busy."""
  if quarantined is not None:
    if quarantined:
      q = q.filter(BotInfo.composite == BotInfo.QUARANTINED)
    else:
      q = q.filter(BotInfo.composite == BotInfo.HEALTHY)

  if is_busy is not None:
    if is_busy:
      q = q.filter(BotInfo.composite == BotInfo.BUSY)
    else:
      q = q.filter(BotInfo.composite == BotInfo.IDLE)

  dt = datetime.timedelta(seconds=config.settings().bot_death_timeout_secs)
  cutoff = now - dt
  if is_dead:
    # TODO(maruel): https://crbug.com/826421 Use BotInfo.DEAD
    q = q.filter(BotInfo.last_seen_ts <= cutoff)
  elif is_dead is not None:
    # TODO(maruel): https://crbug.com/826421 Use BotInfo.ALIVE
    q = q.filter(BotInfo.last_seen_ts > cutoff)

  if is_mp is not None:
    if is_mp:
      q = q.filter(BotInfo.composite == BotInfo.MACHINE_PROVIDER)
    else:
      q = q.filter(BotInfo.composite == BotInfo.NOT_MACHINE_PROVIDER)

  return q


def bot_event(
    event_type, bot_id, external_ip, authenticated_as, dimensions, state,
    version, quarantined, task_id, task_name, **kwargs):
  """Records when a bot has queried for work.

  Arguments:
  - event: event type.
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
  - task_id: packed task id if relevant. Set to '' to zap the stored value.
  - task_name: task name if relevant. Zapped when task_id is zapped.
  - kwargs: optional values to add to BotEvent relevant to event_type.
  - lease_id (in kwargs): ID assigned by Machine Provider for this bot.
  - lease_expiration_ts (in kwargs): UTC seconds from epoch when Machine
        Provider lease expires.
  - machine_type (in kwargs): ID of the lease_management.MachineType this
        Machine Provider bot was leased for.
  - machine_lease (in kwargs): ID of the lease_management.MachineType
        corresponding to this bot.
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
  if dimensions:
    bot_info.dimensions_flat = dimensions_to_flat(dimensions)
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
    bot_info.lease_expiration_ts = kwargs['lease_expiration_ts']
  if kwargs.get('machine_type') is not None:
    bot_info.machine_type = kwargs['machine_type']
  if kwargs.get('machine_lease') is not None:
    bot_info.machine_lease = kwargs['machine_lease']

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
      state=bot_info.state,
      task_id=bot_info.task_id,
      version=bot_info.version,
      **kwargs)

  if event_type in ('task_completed', 'task_error', 'task_killed'):
    # Special case to keep the task_id in the event but not in the summary.
    bot_info.task_id = ''

  datastore_utils.store_new_version(event, BotRoot, [bot_info])


def get_bot_reboot_period(bot_id, state):
  """Returns how long (in sec) a bot should run before being rebooted.

  Uses state['periodic_reboot_secs'] as a baseline, deterministically
  pseudo-randomizing it it on per-bot basis, to make sure that bots do not
  reboot all at once.
  """
  periodic_reboot_secs = state.get('periodic_reboot_secs')
  if not isinstance(periodic_reboot_secs, (float, int)):
    return None

  # Seed stays constant during lifetime of a swarming_bot process, but changes
  # whenever bot is restarted. That way all bots on average restart every
  # periodic_reboot_secs.
  seed_bytes = hashlib.sha256(
      '%s%s' % (bot_id, state.get('started_ts'))).digest()[:2]
  seed = ord(seed_bytes[0]) + 256 * ord(seed_bytes[1])
  factor = 2 * (seed - 32768) / 65536.0 * BOT_REBOOT_PERIOD_RANDOMIZATION_MARGIN
  return int(periodic_reboot_secs * (1.0 + factor))


def should_restart_bot(bot_id, state):
  """Decides whether a bot needs to be restarted.

  Args:
    bot_id: ID of the bot.
    state: dict representing current bot state.

  Returns:
    Tuple (True to restart, text message explaining the reason).
  """
  # Periodically reboot bots to workaround OS level leaks (especially on Win).
  running_time = state.get('running_time', 0)
  assert isinstance(running_time, (int, float))
  period = get_bot_reboot_period(bot_id, state)
  if period and running_time > period:
    return True, 'Periodic reboot: running longer than %ds' % period
  return False, ''


def cron_update_bot_info():
  """Refreshes BotInfo.composite for dead bots."""
  dt = datetime.timedelta(seconds=config.settings().bot_death_timeout_secs)
  cutoff = utils.utcnow() - dt

  @ndb.tasklet
  def run(bot_key):
    bot = yield bot_key.get_async()
    if (bot.last_seen_ts <= cutoff and
        (BotInfo.ALIVE in bot.composite or BotInfo.DEAD not in bot.composite)):
      # Updating it recomputes composite.
      yield bot.put_async()
      raise ndb.Return(1)
    raise ndb.Return(0)

  # The assumption here is that a cron job can churn through all the entities
  # fast enough. The number of dead bot is expected to be <10k.
  nb = 0
  futures = []
  for b in BotInfo.query(BotInfo.last_seen_ts <= cutoff):
    if BotInfo.ALIVE in b.composite or BotInfo.DEAD not in b.composite:
      futures.append(datastore_utils.transaction_async(lambda: run(b.key)))
      if len(futures) >= 25:
        ndb.Future.wait_any(futures)
        for i in xrange(len(futures) - 1, -1, -1):
          if futures[i].done():
            nb += futures.pop(i).get_result()
  for f in futures:
    nb += f.get_result()
  logging.info('Updated %d bots', nb)
  return nb
