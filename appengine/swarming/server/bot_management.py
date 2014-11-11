# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot management, e.g. list of known bots and their state."""

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


class Bot(ndb.Model):
  """This entity declare the knowledge about a bot that successfully connected.

  The key id is the id of the bot.

  Multiple bots could run on a single host, for example with multiple
  phones connected to a host. In this case, the id is specific to each device
  acting as a bot. So while the id must be unique, the hostname doesn't have
  to be.
  """
  # Dimensions are normally static throughout the lifetime of a bot.
  dimensions = datastore_utils.DeterministicJsonProperty(json_type=dict)

  # State is normally changing during the lifetime of a bot.
  state = datastore_utils.DeterministicJsonProperty(json_type=dict)

  # FQDN on which this bot is running on.
  hostname = ndb.StringProperty()

  # IP address as seen by the bot.
  internal_ip = ndb.StringProperty()

  # IP address as seen by the HTTP handler.
  external_ip = ndb.StringProperty()

  # First time this bot was seen.
  created_ts = ndb.DateTimeProperty(auto_now_add=True)

  # Last time the bot pinged and this entity was updated
  last_seen_ts = ndb.DateTimeProperty(auto_now=True, name='last_seen')

  # Version the bot is currently at.
  version = ndb.StringProperty(default='')

  # The current task being run on this bot.
  # TODO(maruel): Save the task_id? It'd be more space efficient, since it fits
  # a IntegerProperty.
  task = ndb.KeyProperty(kind='TaskRunResult')

  # If set to True, no task is handed out to this bot due to the bot being in a
  # broken situation.
  quarantined = ndb.BooleanProperty()

  @property
  def task_entity(self):
    """Returns the TaskRunResult currently executing."""
    # TODO(maruel): This is inefficient in View, fix.
    return self.task.get() if self.task else None

  def is_dead(self, now):
    """Returns True if the bot is dead based on timestamp now."""
    return (now - self.last_seen_ts) >= BOT_DEATH_TIMEOUT

  def to_dict(self):
    out = super(Bot, self).to_dict()
    # Inject the bot id, since it's the entity key.
    out['id'] = self.key.string_id()
    # Replace the task key with the task id.
    if self.task:
      out['task'] = task_result.pack_run_result_key(self.task)
    return out

  def to_dict_with_now(self, now):
    out = self.to_dict()
    out['is_dead'] = self.is_dead(now)
    return out


### Public APIs.


def get_bot_key(bot_id):
  """Returns the ndb.Key for a known Bot."""
  if not bot_id:
    raise ValueError('Bad id')
  # Create a root entity so writes are not done on the root entity but a child
  # entity. 'BotRoot' entity doesn't exist as an entity, it is only a root
  # entity.
  return ndb.Key('BotRoot', bot_id, Bot, bot_id)


def tag_bot_seen(
    bot_id, hostname, internal_ip, external_ip, dimensions, version,
    quarantined, state):
  """Records when a bot has queried for work.

  Arguments:
  - bot_id: ID of the bot. Usually the hostname but can be different if multiple
        swarming bot run on a single host.
  - hostname: FQDN hostname that runs the bot.
  - external_ip: IP address as seen by the HTTP handler.
  - internal_ip: IP address as seen by the bot.
  - dimensions: Bot's dimensions.
  - version: swarming_bot.zip version. Used to spot if a bot failed to update
        promptly.
  - quarantined: bool to signal if the bot should be exempted from running
        tasks. That's set when the bot itself signals it should be quarantined.
  - state: ephemeral state of the bot. It is expected to change constantly.
  """
  # The primary reason for the write is to update .last_seen_ts.
  bot = Bot(
      key=get_bot_key(bot_id),
      dimensions=dimensions or {},
      hostname=hostname,
      internal_ip=internal_ip,
      external_ip=external_ip,
      version=version,
      quarantined=quarantined,
      state=state)
  bot.put()
  return bot


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


def should_restart_bot(bot_id, _attributes, state):
  """Decides whether a bot needs to be restarted.

  Args:
    bot_id: ID of the bot.
    attributes: A dictionary representing the machine attributes.
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
