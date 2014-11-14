# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot management, e.g. list of known bots and their state.

          +---------+
          |BotRoot  |
          |id=bot_id|
          +---------+
               |
        +------+-------+---------------+---------+----- ... -----+
        |              |               |         |               |
        v              v               v         v               v
    +-------+    +-----------+    +--------+ +--------+     +--------+
    |BotInfo|    |BotSettings|    |BotEvent| |BotEvent| ... |BotEvent|
    |id=info|    |id=settings|    |id=fffff| |if=ffffe| ... |id=00000|
    +-------+    +-----------+    +--------+ +--------+     +--------+

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

from google.appengine.ext import ndb

from components import datastore_utils
from components import utils
from server import task_result


# The amount of time that has to pass before a machine is considered dead.
BOT_DEATH_TIMEOUT = datetime.timedelta(seconds=10*60)


# How long bot may run before being asked to reboot, sec. Do it often on canary
# to stress test the reboot mechanism (including bot startup code).
BOT_REBOOT_PERIOD_SECS = 3600 if utils.is_canary() else 12 * 3600


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
  # Dimensions are used for task selection.
  dimensions = datastore_utils.DeterministicJsonProperty(json_type=dict)

  # State is purely informative.
  state = datastore_utils.DeterministicJsonProperty(json_type=dict)

  # IP address as seen by the HTTP handler.
  external_ip = ndb.StringProperty(indexed=False)

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

  # Affected by event_type == 'request_task', 'task_completed', 'task_error'.
  task_id = ndb.StringProperty(indexed=False)

  @property
  def task(self):
    if not self.task_id:
      return None
    return task_result.unpack_run_result_key(self.task_id)


class BotInfo(_BotCommon):
  """This entity declare the knowledge about a bot that successfully connected.

  Parent is BotRoot. Key id is 'info'.

  This entity is a cache of the last BotEvent and is additionally updated on
  poll, which does not create a BotEvent.
  """
  # First time this bot was seen.
  first_seen_ts = ndb.DateTimeProperty(auto_now_add=True, indexed=False)

  # Last time the bot pinged and this entity was updated
  last_seen_ts = ndb.DateTimeProperty(auto_now=True)

  # Must only be set when self.task_id is set.
  task_name = ndb.StringProperty(indexed=False)

  @property
  def id(self):
    return self.key.parent().string_id()

  def is_dead(self, now):
    """Returns True if the bot is dead based on timestamp now."""
    return (now - self.last_seen_ts) >= BOT_DEATH_TIMEOUT

  def to_dict(self):
    out = super(BotInfo, self).to_dict()
    # Inject the bot id, since it's the entity key.
    out['id'] = self.id
    return out

  def to_dict_with_now(self, now):
    out = self.to_dict()
    out['is_dead'] = self.is_dead(now)
    return out

  def _pre_put_hook(self):
    super(BotInfo, self)._pre_put_hook()
    if not self.task_id:
      self.task_name = None


class BotEvent(_BotCommon):
  """This entity is immutable.

  Parent is BotRoot. Key id is monotonically decreasing with
  datastore_utils.store_new_version().

  This entity is created on each bot state transition.
  """
  ALLOWED_EVENTS = {
    'bot_connected', 'bot_error', 'bot_rebooting',
    'request_restart', 'request_update', 'request_sleep', 'request_task',
    'task_completed', 'task_error', 'task_update',
  }
  # Common properties for all events (which includes everything in _BotCommon).
  ts = ndb.DateTimeProperty(auto_now=True)
  event_type = ndb.StringProperty(choices=ALLOWED_EVENTS)

  # event_type == 'bot_error', 'request_restart' or 'bot_rebooting'
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
  return BotEvent.query(ancestor=get_root_key(bot_id)).order(BotEvent.key)


def get_settings_key(bot_id):
  """Returns the BotSettings ndb.Key for a known bot."""
  return ndb.Key(BotSettings, 'settings', parent=get_root_key(bot_id))


def bot_event(
    event_type, bot_id, external_ip, dimensions, state, version, quarantined,
    task_id, task_name, **kwargs):
  """Records when a bot has queried for work.

  Arguments:
  - event: event type.
  - bot_id: bot id.
  - external_ip: IP address as seen by the HTTP handler.
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
  """
  if not bot_id:
    return

  # Retrieve the previous BotInfo and update it.
  info_key = get_info_key(bot_id)
  bot_info = info_key.get() or BotInfo(key=info_key)
  bot_info.last_seen_ts = utils.utcnow()
  bot_info.external_ip = external_ip
  if dimensions:
    bot_info.dimensions = dimensions
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
      dimensions=bot_info.dimensions,
      quarantined=bot_info.quarantined,
      state=bot_info.state,
      task_id=bot_info.task_id,
      version=bot_info.version,
      **kwargs)

  if event_type in ('task_completed', 'task_error'):
    # Special case to keep the task_id in the event but not in the summary.
    bot_info.task_id = ''

  datastore_utils.store_new_version(event, BotRoot, [bot_info])


def get_bot_reboot_period(bot_id, state):
  """Returns how long (in sec) a bot should run before being rebooted.

  Uses BOT_REBOOT_PERIOD_SECS as a baseline, deterministically
  pseudo-randomizing it it on per-bot basis, to make sure that bots do not
  reboot all at once.
  """
  # Seed stays constant during lifetime of a swarming_bot process, but changes
  # whenever bot is restarted. That way all bots on average restart every
  # BOT_REBOOT_PERIOD_SECS.
  seed_bytes = hashlib.sha1(
      '%s%s' % (bot_id, state.get('started_ts'))).digest()[:2]
  seed = ord(seed_bytes[0]) + 256 * ord(seed_bytes[1])
  factor = 2 * (seed - 32768) / 65536.0 * BOT_REBOOT_PERIOD_RANDOMIZATION_MARGIN
  return int(BOT_REBOOT_PERIOD_SECS * (1.0 + factor))


def should_restart_bot(bot_id, state):
  """Decides whether a bot needs to be restarted.

  Args:
    bot_id: ID of the bot.
    state: A dictionary representing current bot state, see
        handlers_api.BotPollHandler for the list of keys.

  Returns:
    Tuple (True to restart, text message explaining the reason).
  """
  # Periodically reboot bots to workaround OS level leaks (especially on Win).
  running_time = state.get('running_time', 0)
  assert isinstance(running_time, (int, float))
  period = get_bot_reboot_period(bot_id, state)
  if running_time > period:
    return True, 'Periodic reboot: running longer than %ds' % period
  return False, ''
