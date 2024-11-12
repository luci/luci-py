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
               |           |              |
               v           v              v
           +--------+  +--------+     +--------+
           |BotEvent|  |BotEvent| ... |BotEvent|
           |id=fffff|  |if=ffffe| ... |id=00000|
           +--------+  +--------+     +--------+

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
import functools
import logging
import time

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from proto.api import swarming_pb2  # pylint: disable=no-name-in-module
from server import config
from server import task_pack
from server import task_queues
from server.constants import OR_DIM_SEP


# BotEvent entities are deleted when they are older than the cutoff.
_OLD_BOT_EVENTS_CUT_OFF = datetime.timedelta(days=4 * 7)
# BotInfo entities are deleted when they are older than the cutoff.
_OLD_BOT_INFO_CUT_OFF = _OLD_BOT_EVENTS_CUT_OFF + datetime.timedelta(hours=4)


### Models.


class BotRoot(ndb.Model):
  """Root entity for BotEvent, BotInfo and BotSettings.

  Doesn't actually exist in the datastore. Just used as a parent for other
  entity keys.
  """


class _BotCommon(ndb.Model):
  """Common data between BotEvent and BotInfo.

  Parent is BotRoot.
  """
  # State is purely informative. It is completely free form.
  state = datastore_utils.DeterministicJsonProperty(json_type=dict)

  # The current bot session ID as reported when the bot connected.
  session_id = ndb.StringProperty(indexed=False)

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

  # Deprecated. TODO(crbug/897355): Remove.
  lease_id = ndb.StringProperty(indexed=False)
  lease_expiration_ts = ndb.DateTimeProperty(indexed=False)
  leased_indefinitely = ndb.BooleanProperty(indexed=False)
  machine_type = ndb.StringProperty(indexed=False)
  machine_lease = ndb.StringProperty(indexed=False)

  # Dimensions are used for task selection. They are encoded as a list of
  # key:value. Keep in mind that the same key can be used multiple times. The
  # list must be sorted. It is indexed to enable searching for bots.
  dimensions_flat = ndb.StringProperty(repeated=True)

  # Last time the bot pinged and this entity was updated
  last_seen_ts = ndb.DateTimeProperty(indexed=False)

  # Time the bot started polling for next task.
  # None is set during running task or hooks.
  idle_since_ts = ndb.DateTimeProperty(indexed=False)

  # When this entity can be cleaned up via Cloud Datastore TTL policy.
  expire_at = ndb.DateTimeProperty(indexed=False)

  @property
  def dimensions(self):
    """Returns a dict representation of self.dimensions_flat."""
    out = {}
    for i in self.dimensions_flat:
      k, v = i.split(':', 1)
      out.setdefault(k, []).append(v)
    return out

  @property
  def id(self):
    return self.key.parent().string_id()

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

  def to_proto(self, out):
    """Converts self to a swarming_pb2.Bot."""
    # Used by BotEvent.to_proto() and BotInfo.to_proto().
    if self.key:
      out.bot_id = self.key.parent().string_id()
    #out.session_id = ''  # https://crbug.com/786735
    for l in self.dimensions_flat:
      if l.startswith(u'pool:'):
        out.pools.append(l[len(u'pool:'):])

    if self.is_dead:
      out.status = swarming_pb2.MISSING
      out.status_msg = ''

    if self.is_idle:
      out.status = swarming_pb2.IDLE

    # https://crbug.com/757931: QUARANTINED_BY_SERVER
    # https://crbug.com/870723: OVERHEAD_BOT_INTERNAL
    # https://crbug.com/870723: HOST_REBOOTING
    # https://crbug.com/913978: RESERVED
    if self.quarantined:
      out.status = swarming_pb2.QUARANTINED_BY_BOT
      msg = (self.state or {}).get(u'quarantined')
      if msg:
        if not isinstance(msg, basestring):
          # Having {'quarantined': True} is valid for the state, convert this to
          # a string.
          msg = 'true'
        out.status_msg = msg
    elif self.maintenance_msg:
      out.status = swarming_pb2.OVERHEAD_MAINTENANCE_EXTERNAL
      out.status_msg = self.maintenance_msg
    elif self.task_id:
      out.status = swarming_pb2.BUSY

    if self.task_id:
      out.current_task_id = self.task_id
    for key, values in sorted(self.dimensions.items()):
      d = out.dimensions.add()
      d.key = key
      for value in values:
        d.values.append(value)

    # The BotInfo part.
    if self.state:
      out.info.supplemental.update(self.state)
    if self.version:
      out.info.version = self.version
    if self.authenticated_as:
      out.info.authenticated_as = self.authenticated_as
    if self.external_ip:
      out.info.external_ip = self.external_ip
    if self.is_dead and self.last_seen_ts:
      out.info.last_seen_ts.FromDatetime(self.last_seen_ts)
    # TODO(maruel): Populate bot.info.host and bot.info.devices.
    # https://crbug.com/916570

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
  NOT_IN_MAINTENANCE = 1 << 9  # 512
  IN_MAINTENANCE = 1 << 8  # 256
  # One of:
  ALIVE = 1 << 7  # 128
  DEAD = 1 << 6  # 64
  # One of:
  HEALTHY = 1 << 3  # 8
  QUARANTINED = 1 << 2  # 4
  # One of:
  IDLE = 1<<1 # 2
  BUSY = 1<<0 # 1

  # First time this bot was seen.
  first_seen_ts = ndb.DateTimeProperty(auto_now_add=True, indexed=False)

  # Must only be set when self.task_id is set.
  task_name = ndb.StringProperty(indexed=False)

  # Avoid having huge amounts of indices to query by quarantined/idle.
  composite = ndb.IntegerProperty(repeated=True)

  def calc_composite(self):
    """Returns the value for BotInfo.composite, which permits quick searches."""
    return [
        self.IN_MAINTENANCE
        if self.maintenance_msg else self.NOT_IN_MAINTENANCE,
        self.DEAD if self._should_be_dead() else self.ALIVE,
        self.QUARANTINED if self.quarantined else self.HEALTHY,
        self.IDLE if self.idle_since_ts else self.BUSY,
    ]

  def _should_be_dead(self, deadline=None):
    if not self.last_seen_ts:
      return False
    return self.last_seen_ts <= (deadline or self._deadline())

  @property
  def is_dead(self):
    assert self.composite, 'Please store first'
    return self.DEAD in self.composite

  def to_dict(self, exclude=None):
    out = super(BotInfo, self).to_dict(exclude=exclude)
    # Inject the bot id, since it's the entity key.
    out['id'] = self.id
    out['is_dead'] = self.is_dead
    return out

  #def to_proto(self, out):
  #  """Converts self to a swarming_pb2.Bot."""
  # This populates most of the data.
  # super(BotInfo, self).to_proto(out)
  # https://crbug.com/757931: QUARANTINED_BY_SERVER
  # https://crbug.com/870723: OVERHEAD_BOT_INTERNAL
  # https://crbug.com/870723: HOST_REBOOTING
  # https://crbug.com/913978: RESERVED
  # TODO(maruel): Populate bot.info.host and bot.info.devices.
  # https://crbug.com/916570

  def _pre_put_hook(self):
    super(BotInfo, self)._pre_put_hook()
    if not self.task_id:
      self.task_name = None
    self.composite = self.calc_composite()

  @classmethod
  def yield_alive_bots(cls):
    """Yields BotInfo of all alive bots."""
    q = cls.query(cls.composite == cls.ALIVE)
    cursor = None
    more = True
    while more:
      bots, cursor, more = q.fetch_page(1000, start_cursor=cursor)
      for b in bots:
        yield b

  @staticmethod
  def _deadline():
    dt = datetime.timedelta(seconds=config.settings().bot_death_timeout_secs)
    return utils.utcnow() - dt


class BotEvent(_BotCommon):
  """This entity is immutable.

  Parent is BotRoot.

  This entity is created on each bot state transition.
  """
  _MAPPING = {
      'bot_connected': swarming_pb2.BOT_NEW_SESSION,
      'bot_error': swarming_pb2.BOT_HOOK_ERROR,
      'bot_log': swarming_pb2.BOT_HOOK_LOG,
      'bot_missing': swarming_pb2.BOT_MISSING,
      'bot_rebooting': swarming_pb2.BOT_REBOOTING_HOST,
      'bot_shutdown': swarming_pb2.BOT_SHUTDOWN,
      'bot_terminate': swarming_pb2.INSTRUCT_TERMINATE_BOT,
      'request_restart': swarming_pb2.INSTRUCT_RESTART_BOT,

      # Shall only be stored when there is a significant difference in the bot
      # state versus the previous event.
      'request_sleep': swarming_pb2.INSTRUCT_IDLE,
      'request_task': swarming_pb2.INSTRUCT_START_TASK,
      'request_update': swarming_pb2.INSTRUCT_UPDATE_BOT_CODE,
      'task_completed': swarming_pb2.TASK_COMPLETED,
      'task_error': swarming_pb2.TASK_INTERNAL_FAILURE,
      'task_killed': swarming_pb2.TASK_KILLED,

      # These values are not registered in the API.
      'bot_idle': None,
      'bot_polling': None,
      'task_update': None,
  }

  ALLOWED_EVENTS = {
      # Bot specific events that are outside the scope of a task:
      'bot_connected',
      'bot_error',
      'bot_idle',
      'bot_log',
      'bot_missing',
      'bot_polling',
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
  event_type = ndb.StringProperty(choices=ALLOWED_EVENTS, indexed=False)

  # event_type == 'bot_error', 'request_restart', 'bot_rebooting', etc.
  message = ndb.TextProperty()

  @property
  def is_dead(self):
    return self.event_type == 'bot_missing'

  @property
  def is_idle(self):
    return (self.event_type in ('request_sleep', 'bot_idle')
            and not self.quarantined and not self.maintenance_msg)

  def to_proto(self, out):
    """Converts self to a swarming_pb2.BotEvent."""
    if self.ts:
      out.event_time.FromDatetime(self.ts)
    # Populates out.bot with _BotCommon.
    _BotCommon.to_proto(self, out.bot)
    e = self._MAPPING.get(self.event_type)
    if e:
      out.event = e
    if self.message:
      out.event_msg = self.message


class BotSettings(ndb.Model):
  """Contains all settings that are set by the administrator on the server.

  Parent is BotRoot. Key id is 'settings'.

  This entity must always be updated in a transaction.
  """
  # If set to True, no task is handed out to this bot due to the bot being in a
  # broken situation.
  quarantined = ndb.BooleanProperty()


### Public APIs.


def get_root_key(bot_id):
  """Returns the BotRoot ndb.Key for a known bot."""
  if not bot_id:
    raise ValueError('Bad id')
  return ndb.Key(BotRoot, bot_id)


def get_info_key(bot_id):
  """Returns the BotInfo ndb.Key for a known bot."""
  return ndb.Key(BotInfo, 'info', parent=get_root_key(bot_id))


def get_events_query(bot_id):
  """Returns an ndb.Query for most recent events in reverse chronological order.
  """
  # Disable the in-process local cache. This is important, as there can be up to
  # a thousand entities loaded in memory, and this is a pure memory leak, as
  # there's no chance this specific instance will need these again, therefore
  # this leads to 'Exceeded soft memory limit' AppEngine errors.
  q = BotEvent.query(
      default_options=ndb.QueryOptions(use_cache=False),
      ancestor=get_root_key(bot_id))
  q = q.order(-BotEvent.ts)
  return q


def get_latest_info(bot_id):
  """Returns the last known BotInfo, reconstructing it from the event history
  if necessary.

  The returned BotInfo should not be used in any transactions since it may not
  really exist.

  Returns:
    (BotInfo, exists) where `exists` is True if BotInfo exists for real and
    False if the bot was already deleted and BotInfo was reconstructed from
    the history.
  """
  # A quick check for bots that exist.
  info_key = get_info_key(bot_id)
  bot = info_key.get(use_cache=False, use_memcache=False)
  if bot:
    return bot, True

  # If there is no BotInfo, it means the bot doesn't currently exist (i.e. it
  # was deleted or never existed). Look into the event history to get its last
  # known state. If the history is empty, it means the bot never existed or was
  # deleted long time ago. If there's a history, it means the bot existed, but
  # was deleted.
  events = get_events_query(bot_id).fetch(1)
  if not events:
    return None, False

  # After the first get() and before get_events_query() a bot may have
  # handshaked, which created a BotEvent record that get_events_query() fetched.
  # Here, another get() is made to make sure the bot is still actually missing.
  # If we skip this check, we may accidentally mark a very new bot as deleted.
  #
  # See https://crbug.com/1407381 for more information.
  bot = info_key.get(use_cache=False)
  if bot:
    return bot, True

  # Reconstruct BotInfo of the deleted bot based on the latest event.
  bot = BotInfo(key=info_key,
                dimensions_flat=events[0].dimensions_flat,
                state=events[0].state,
                session_id=events[0].session_id,
                external_ip=events[0].external_ip,
                authenticated_as=events[0].authenticated_as,
                version=events[0].version,
                quarantined=events[0].quarantined,
                maintenance_msg=events[0].maintenance_msg,
                task_id=events[0].task_id,
                last_seen_ts=events[0].ts)

  # message_conversion.bot_info_to_rpc calls `is_dead` and this property
  # require `composite` to be calculated. The calculation is done in
  # _pre_put_hook usually. But the BotInfo shouldn't be stored in this
  # case, as it's already deleted.
  bot.composite = bot.calc_composite()

  return bot, False


def get_bot_pools(bot_id):
  """Returns pools the bot belongs to or should belong to once it appears.

  Uses the latest BotInfo, falling back to the last recorded BotEvent for
  deleted bots that don't have a BotInfo. If there's no such bot at all returns
  an empty list.

  Returns:
    List of pools or an empty list if the bot is unknown.
  """
  # TODO(vadimsh): Check the static config first before hitting the datastore.
  # Fallback to the datastore only if the config is empty (to support showing
  # deleted bots).

  # Note: unlike get_latest_info() here we don't care about carefully detecting
  # if a bot is dead. This allows to skip one get() when checking deleted bots.
  bot = get_info_key(bot_id).get(use_cache=False, use_memcache=False)
  if bot:
    return get_pools_from_dimensions_flat(bot.dimensions_flat)
  events = get_events_query(bot_id).fetch(1)
  if not events:
    return []
  return get_pools_from_dimensions_flat(events[0].dimensions_flat)


def get_settings_key(bot_id):
  """Returns the BotSettings ndb.Key for a known bot."""
  return ndb.Key(BotSettings, 'settings', parent=get_root_key(bot_id))


def filter_dimensions(q, dimensions):
  """Filters a ndb.Query for BotInfo based on dimensions in the request."""
  for d in dimensions:
    parts = d.split(':', 1)
    if len(parts) != 2 or any(i.strip() != i or not i for i in parts):
      raise ValueError('Invalid dimensions')
    # expand OR operator
    # e.g. 'foo:A|B' -> ['foo:A', 'foo:B']
    values = parts[1].split(OR_DIM_SEP)
    dims = ['%s:%s' % (parts[0], v) for v in values]
    q = q.filter(BotInfo.dimensions_flat.IN(dims))
  return q


def filter_availability(q, quarantined, in_maintenance, is_dead, is_busy):
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

  # TODO(charliea): Add filtering based on the 'maintenance' field.

  return q


def _apply_event_updates(bot_info, event_type, now, session_id, task_id,
                         task_name, external_ip, authenticated_as,
                         dimensions_flat, state, version, quarantined,
                         maintenance_msg, register_dimensions):
  """Mutates BotInfo based on event details passed to bot_event(...)."""
  # Bump the expiration time every time the entity is touched. Note that this
  # field is unindexed (Cloud Datastore TTL policy doesn't need an index),
  # the cost of updating it is negligible.
  bot_info.expire_at = now + _OLD_BOT_INFO_CUT_OFF

  # `bot_missing` event is created by the server (see cron_update_bot_info), not
  # a bot. So it shouldn't update fields that are reported by the bot itself.
  #
  # If the `last_seen_ts` gets updated, it would change the bot composite
  # to alive. And if it clears `maintenance_msg`, it would change the composite
  # to NOT_IN_MAINTENANCE and lose the message.
  if event_type != 'bot_missing':
    bot_info.last_seen_ts = now
    if session_id is not None:
      bot_info.session_id = session_id
    if external_ip is not None:
      bot_info.external_ip = external_ip
    if authenticated_as is not None:
      bot_info.authenticated_as = authenticated_as
    bot_info.maintenance_msg = maintenance_msg

  # Override values in BotInfo only if we have newer values.
  if state is not None:
    bot_info.state = state
  if version is not None:
    bot_info.version = version
  if quarantined is not None:
    bot_info.quarantined = quarantined
  if task_id is not None:
    bot_info.task_id = task_id
  if task_name is not None:
    bot_info.task_name = task_name

  # Remove the task from the BotInfo summary in the following cases
  # 1) When the task finishes (event_type=task_XXX)
  #    In these cases, the BotEvent shall have the task
  #    since the event still refers to it
  # 2) When the bot is polling (event_type=request_sleep|bot_idle|bot_polling)
  #    The bot has already finished the previous task.
  #    But it could have forgotten to remove the task from the BotInfo.
  #    So ensure the task is removed.
  # 3) When the bot is missing
  #    We assume it can't process assigned task anymore.
  if event_type in ('task_completed', 'task_error', 'task_killed',
                    'request_sleep', 'bot_idle', 'bot_polling', 'bot_missing'):
    bot_info.task_id = None
    bot_info.task_name = None

  # idle_since_ts is updated only when bot is idle in healthy state.
  is_idle = (event_type in ('request_sleep', 'bot_idle')
             and not bot_info.quarantined and not bot_info.maintenance_msg)
  if is_idle:
    bot_info.idle_since_ts = bot_info.idle_since_ts or now
  else:
    bot_info.idle_since_ts = None

  # Update dimensions only if they are given and `register_dimensions` is True.
  if (dimensions_flat and register_dimensions
      and bot_info.dimensions_flat != dimensions_flat):
    logging.debug('bot_event: Updating dimensions. from: %s, to: %s',
                  bot_info.dimensions_flat, dimensions_flat)
    bot_info.dimensions_flat = dimensions_flat


def _snapshot_bot_info(bot_info):
  """Captures a subset of BotInfo used to detect it we need to emit an event."""
  return (
      bot_info.calc_composite(),
      bot_info.dimensions_flat,
  )


# Events that happen very often and not worth reporting individually.
_FREQUENT_EVENTS = frozenset(
    ['request_sleep', 'task_update', 'bot_idle', 'bot_polling'])

# Events that may result in creation of new BotInfo entities (i.e. a new bot
# appearing). Most often this is just bot_connected.
_HEALTHY_BOT_EVENTS = frozenset([
    'bot_connected',
    'bot_idle',
    'bot_polling',
    'request_restart',
    'request_sleep',
    'request_task',
    'request_update',
])


def _should_store_event(event_type, before, after):
  """Decides if we should store a new BotEvent entity.

  Arguments:
    event_type: event type, one of BotEvent.ALLOWED_EVENTS.
    before: a result of _snapshot_bot_info() before any changes.
    after: a result of _snapshot_bot_info() after all changes has been applied.

  Returns:
    True or False.
  """
  return (
      # Record all "rare" events.
      event_type not in _FREQUENT_EVENTS
      # Record status changing events (crbug.com/952984, crbug.com/1040345).
      or before[0] != after[0]
      # Record changes to dimensions (crbug.com/1015365).
      or before[1] != after[1])


def _insert_bot_with_txn(root_key, bot_info, event):
  """Stores BotInfo and/or BotEvent (skipping None)."""
  entities = []

  if bot_info:
    assert bot_info.key.root() == root_key
    entities.append(bot_info)
  if event:
    assert event.key.root() == root_key
    entities.append(event)
  if not entities:
    return

  attempt = 1
  delay = 0.1
  what = 'event %s' % event.event_type if event else 'bot info'
  while True:
    try:
      logging.info('Attempt %d to insert %s (%d entities) for bot_id %s',
                   attempt, what, len(entities), root_key.id())
      attempt += 1
      if len(entities) == 1:
        entities[0].put()
      else:
        datastore_utils.transaction(lambda: ndb.put_multi(entities), retries=0)
      break
    except (datastore_utils.CommitError, datastore_errors.InternalError,
            datastore_errors.Timeout,
            datastore_errors.TransactionFailedError) as exc:
      logging.warning('_insert_bot_with_txn: error inserting %s for %s: %s',
                      what, root_key.id(), exc)
      delay = min(5.0, delay * 2.0)
      time.sleep(delay)


def bot_event(event_type,
              bot_id,
              session_id=None,
              task_id=None,
              task_name=None,
              external_ip=None,
              authenticated_as=None,
              dimensions=None,
              state=None,
              version=None,
              quarantined=None,
              maintenance_msg=None,
              event_msg=None,
              register_dimensions=False):
  """Updates the state of the bot in the datastore.

  This call usually means the bot is alive (not dead), except for `bot_missing`
  event which is created by server. The bot may be quarantined, in which case it
  will be unregistered from the task queues.

  If the bot is declaring maintenance, it will be kept in the task queues, as
  maintenance is supposed to be temporary and expected to complete within a
  reasonable time frame.

  Arguments:
    event_type: event type, one of BotEvent.ALLOWED_EVENTS. Required.
    bot_id: bot id. Required.
    session_id: the current bot session ID.
    task_id: packed task id if relevant. Set to '' to zap the stored value. If
        None, keep the previous value.
    task_name: task name if relevant. Zapped when task_id is zapped. If None,
        keep the previous value.
    external_ip: IP address as seen by the HTTP handler. If None, keep the
        previous value.
    authenticated_as: bot identity as seen by the HTTP handler. If None, keep
        the previous value.
    dimensions: bot's dimensions as self-reported. If None, keep the previous
        value.
    state: a dict with ephemeral state of the bot. It is expected to change
        constantly. If None or empty, keep the previous value.
    version: swarming_bot.zip version as self-reported. Used to spot if a bot
        failed to update promptly. If None, keep the previous value.
    quarantined: bool to determine if the bot was declared quarantined. If None,
        keep the previous value.
    maintenance_msg: string describing why the bot is in maintenance. If None
        or empty, the bot is not considered in maintenance.
    event_msg: an optional informational message to store in BotEvent. Doesn't
        affect bot state.
    register_dimensions: bool to specify whether to update dimensions stored in
        BotInfo. This affects discoverability of this bot by the scheduler. If
        `dimensions` is empty, `register_dimensions` is ignored.

  Returns:
    ndb.Key to BotEvent entity if one was added.
  """
  assert event_type in BotEvent.ALLOWED_EVENTS, event_type
  if not bot_id:
    return None

  # BotInfo and BotEvent operate with flattened dimensions.
  dimensions_flat = task_queues.bot_dimensions_to_flat(dimensions or {})

  # Retrieve the previous BotInfo to update it. Note that most events are
  # ultimately produced by a single serial bot process and there should not be
  # concurrent events from the same bot, therefore there are no transactions
  # here. In the worst case some intermediary state changes won't be properly
  # recorded.
  info_key = get_info_key(bot_id)
  bot_info = info_key.get(use_cache=False, use_memcache=False)
  store_bot_info = True
  if not bot_info:
    # Register only id and pool dimensions at the first handshake.
    bot_info = BotInfo(
        key=info_key,
        dimensions_flat=[
            d for d in dimensions_flat
            if d.startswith('id:') or d.startswith('pool:')
        ],
    )
    # Create BotInfo only if this event indicates the bot is actually alive.
    # This check exists to workaround race conditions when deleting bots. In
    # particular, cron_update_bot_info marks the bot as deleted before it
    # emits `bot_missing` event. If between these two calls, something
    # (like GCE Provider) notices the bot is marked as dead and calls DeleteBot,
    # the late `bot_missing` event ends up recreating the deleted bot. A similar
    # thing can happen when a dead bot suddenly comes back to life just to
    # report that it has failed a task and is terminating now (`bot_shutdown`
    # event).
    store_bot_info = event_type in _HEALTHY_BOT_EVENTS
    if not store_bot_info:
      logging.warning('No BotInfo(%s) when storing %s', bot_id, event_type)

  # Use the exact same timestamp in both BotInfo and BotEvent for consistency.
  now = utils.utcnow()

  # Snapshot the state before any changes, used in _should_store_event.
  state_before = _snapshot_bot_info(bot_info)

  # Mutate BotInfo in place based on the event details.
  _apply_event_updates(bot_info=bot_info,
                       event_type=event_type,
                       now=now,
                       session_id=session_id,
                       task_id=task_id,
                       task_name=task_name,
                       external_ip=external_ip,
                       authenticated_as=authenticated_as,
                       dimensions_flat=dimensions_flat,
                       state=state,
                       version=version,
                       quarantined=quarantined,
                       maintenance_msg=maintenance_msg,
                       register_dimensions=register_dimensions)

  # Snapshot the state after changes, used in _should_store_event.
  state_after = _snapshot_bot_info(bot_info)

  # If the bot is quarantined, unregister it from scheduler queues.
  if quarantined:
    task_queues.cleanup_after_bot(bot_id)

  # We always need to save BotInfo (in particular `last_seen_ts`) to keep the
  # bot alive in the datastore, but we should record BotEvent only in case
  # something interesting happens.
  if _should_store_event(event_type, state_before, state_after):
    event = BotEvent(parent=bot_info.key.parent(),
                     event_type=event_type,
                     ts=now,
                     expire_at=now + _OLD_BOT_EVENTS_CUT_OFF,
                     session_id=bot_info.session_id,
                     external_ip=bot_info.external_ip,
                     authenticated_as=bot_info.authenticated_as,
                     dimensions_flat=(dimensions_flat
                                      or bot_info.dimensions_flat),
                     quarantined=bot_info.quarantined,
                     maintenance_msg=bot_info.maintenance_msg,
                     state=bot_info.state,
                     task_id=task_id or bot_info.task_id,
                     version=bot_info.version,
                     last_seen_ts=bot_info.last_seen_ts,
                     idle_since_ts=bot_info.idle_since_ts,
                     message=event_msg)
    _insert_bot_with_txn(info_key.root(), bot_info if store_bot_info else None,
                         event)
    return event.key

  # No need to emit an event. Just update BotInfo on its own.
  if store_bot_info:
    _insert_bot_with_txn(info_key.root(), bot_info, None)
  return None


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

  # Add it to the 'quick cache' to improve performance. This cache is kept for
  # the same duration as how long bots are considered still alive without a
  # ping. Useful if there's a single bot in the fleet for these dimensions and
  # it takes a long time to reboot. This is the case with Android with slow
  # initialization and some baremetal bots (thanks SCSI firmware!).
  seconds = config.settings().bot_death_timeout_secs

  @ndb.tasklet
  def run_query(flat):
    # Do a query. That's slower and it's eventually consistent.
    q = BotInfo.query()
    for f in flat:
      q = q.filter(BotInfo.dimensions_flat == f)

    num = yield q.count_async(limit=1)
    if num:
      logging.info('Found capacity via BotInfo: %s', flat)
      raise ndb.Return(True)

    # Search a bit harder. In this case, we're looking for BotEvent which would
    # be a bot that used to exist recently.
    cutoff = utils.utcnow() - datetime.timedelta(seconds=seconds)
    q = BotEvent.query(BotEvent.ts > cutoff)
    for f in flat:
      q = q.filter(BotEvent.dimensions_flat == f)
    num = yield q.count_async(limit=1)
    if num:
      logging.info('Found capacity via BotEvent: %s', flat)
      raise ndb.Return(True)
    raise ndb.Return(False)

  futures = [
      run_query(f) for f in task_queues.expand_dimensions_to_flats(dimensions)
  ]

  ndb.tasklets.Future.wait_all(futures)
  if any(f.get_result() for f in futures):
    task_queues.set_has_capacity(dimensions, seconds)
    return True

  logging.warning('HAS NO CAPACITY: %s', dimensions)
  return False


def get_pools_from_dimensions_flat(dimensions_flat):
  """Gets pools from dimensions_flat."""
  return [
      d.replace('pool:', '') for d in dimensions_flat if d.startswith('pool:')
  ]


def cron_update_bot_info():
  """Refreshes BotInfo.composite for dead bots."""
  @ndb.tasklet
  def run(bot_key):
    bot = bot_key.get()
    if not bot:
      logging.debug('BotInfo %s deleted since query or query was stale',
                    bot_key)
      raise ndb.Return(None)
    if not bot.is_dead and bot._should_be_dead():
      # `is_dead` is updated in _pre_put_hook based on should_be_dead.
      logging.info('Changing Bot status to DEAD: %s', bot.id)
      yield bot.put_async()
      raise ndb.Return(bot)
    logging.debug('BotInfo changed since query or query was stale, %r', bot)
    raise ndb.Return(None)

  # Note: tx_result can potentially block for a significant amount of time since
  # it makes several datastore updates (including a transaction) in a blocking
  # way.
  def tx_result(future, stats):
    try:
      bot = future.get_result()
      if not bot:
        stats['stale'] += 1
        return
      stats['dead'] += 1

      # Unregister the bot from task queues since it can't reap anything.
      task_queues.cleanup_after_bot(bot.id)

      # Note: this is best effort at this point. If it fails, there'll be no
      # retry: the bot is already marked as dead.
      logging.info('Sending bot_missing event: %s', bot.id)
      bot_event('bot_missing', bot.id)
    except datastore_utils.CommitError:
      logging.warning('Failed to commit a Tx')
      stats['failed'] += 1

  # The assumption here is that a cron job can visit all alive bots fast enough.
  # The number of bots is expected to be up to ~30k. It takes about a minute to
  # process them (assuming there's negligible amount of dead bots). See also
  # cron.yaml `/internal/cron/monitoring/bots/update_bot_info` entry that
  # depends on this timing.
  cron_stats = {
      'seen': 0,
      'dead': 0,
      'failed': 0,
      'stale': 0,
  }

  # _deadline() hits the instance config cache. Do it only once here instead of
  # several thousand times inside _should_be_dead() in the loop below.
  deadline = BotInfo._deadline()

  futures = []
  logging.debug('Finding dead based on deadline %s...', deadline)
  try:
    for info in BotInfo.yield_alive_bots():
      cron_stats['seen'] += 1
      if cron_stats['seen'] % 500 == 0:
        logging.debug('Visited %d bots so far', cron_stats['seen'])

      # We visit all alive bots and check if any should be marked as dead now.
      # Note that an alternative would be to have an index on `last_seen_ts`,
      # but this index turns out to be very hot (being update on every poll).
      # See https://chromium.googlesource.com/infra/luci/luci-py/+/4e9aecba.
      if info.is_dead or not info._should_be_dead(deadline):
        continue

      # Transactionally flip the state of the bot to DEAD. Retry more often than
      # the default 1. We do not want to throw too much in the logs and there
      # should be plenty of time to do the retries.
      f = datastore_utils.transaction_async(functools.partial(run, info.key),
                                            retries=5)
      futures.append(f)

      # Limit the number of concurrent transactions to avoid OOMs.
      if len(futures) > 20:
        ndb.Future.wait_any(futures)
        pending = []
        for f in futures:
          if f.done():
            tx_result(f, cron_stats)
          else:
            pending.append(f)
        futures = pending

    # Collect all remaining futures.
    for f in futures:
      tx_result(f, cron_stats)

  finally:
    logging.debug('Seen: %d, marked as dead: %d, stale: %d, failed: %d',
                  cron_stats['seen'], cron_stats['dead'], cron_stats['stale'],
                  cron_stats['failed'])

  return cron_stats['dead']
