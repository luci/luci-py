# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot management.

Includes management of the swarming_bot.zip code and the list of known bots.
"""

import datetime
import os.path

from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import datastore_utils
from server import bot_archive
from server import file_chunks
from server import task_common


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# The key to use to access the start slave script file model.
START_SLAVE_SCRIPT_KEY = 'start_slave_script'


# The amount of time that has to pass before a machine is considered dead.
MACHINE_DEATH_TIMEOUT = datetime.timedelta(seconds=30*60)


# The amount of time that needs to pass before the last_seen field of
# MachineStats will update for a given machine, to prevent too many puts.
MACHINE_UPDATE_TIME = datetime.timedelta(seconds=120)


### Models.


class Bot(ndb.Model):
  """This entity declare the knowledge about a bot that successfully connected.

  The key id is the id of the bot.
  """
  dimensions = datastore_utils.DeterministicJsonProperty(json_type=dict)

  # Generally, the hostname == id, but it's not always true. For example,
  # multiple bots could run on a single machine, for example with multiple
  # phones connected to a host. In this case, hostname is the FQDN of the host
  # but the id is specific to each device acting as a bot. So while the id
  # must be unique, the hostname doesn't have to.
  hostname = ndb.StringProperty()

  # Use auto_now_add instead of auto_now to make unit testing easier.
  last_seen = ndb.DateTimeProperty(auto_now_add=True)

  def to_dict(self):
    out = super(Bot, self).to_dict()
    out['id'] = self.key.string_id()
    return out


### Private stuff.

def _get_start_slave():
  """Returns the start_slave.py content to be used in the zip.

  First fetch it from the database, if present. Pass the one in the tree
  otherwise.
  """
  content = file_chunks.RetrieveFile(START_SLAVE_SCRIPT_KEY)
  if content:
    return content
  with open(os.path.join(ROOT_DIR, 'swarm_bot', 'start_slave.py'), 'rb') as f:
    return f.read()


### Public APIs.


def store_start_slave(script):
  """Stores the given script as the new start slave script for all slave.

  Args:
    script: The contents of the new start slave script.
  """
  file_chunks.StoreFile(START_SLAVE_SCRIPT_KEY, script)

  # Clear the cached version value since it has now changed. This is *super*
  # aggressive to flush all memcache but that's the only safe way to not send
  # old code by accident.
  while not memcache.flush_all():
    pass


def get_slave_version(host):
  """Retrieves the slave version loaded on this server.

  The memcache is first checked for the version, otherwise the value
  is generated and then stored in the memcache.

  Returns:
    The hash of the current slave version.
  """
  namespace = os.environ['CURRENT_VERSION_ID']
  key = 'bot_version' + host
  bot_version = memcache.get(key, namespace=namespace)
  if bot_version:
    return bot_version

  # Need to calculate it.
  additionals = {'start_slave.py': _get_start_slave()}
  bot_dir = os.path.join(ROOT_DIR, 'swarm_bot')
  bot_version = bot_archive.get_swarming_bot_version(bot_dir, host, additionals)
  memcache.set(key, bot_version, namespace=namespace)
  return bot_version


def get_swarming_bot_zip(host):
  """Returns a zipped file of all the files a slave needs to run.

  Returns:
    A string representing the zipped file's contents.
  """
  # Get the start slave script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {'start_slave.py': _get_start_slave()}
  bot_dir = os.path.join(ROOT_DIR, 'swarm_bot')
  return bot_archive.get_swarming_bot_zip(bot_dir, host, additionals)


def get_bot_key(bot_id):
  """Returns the ndb.Key for a known Bot."""
  if not bot_id:
    raise ValueError('Bad id')
  return ndb.Key(Bot, bot_id)


def tag_bot_seen(bot_id, hostname, dimensions):
  """Records when a bot has queried for work.

  Arguments:
  - bot_id: ID of the bot. Usually the hostname but can be different if multiple
    swarming bot run on a single host.
  - hostname: FQDN hostname that runs the bot or None if unspecified. It happens
    on pings while running a job.
  - dimensions: Bot's dimensions or None if unspecified. It happens on pings
    while running a job.
  """
  key = get_bot_key(bot_id)
  bot = key.get()
  if (bot and
      bot.last_seen + MACHINE_UPDATE_TIME >= task_common.utcnow() and
      (not dimensions or bot.dimensions == dimensions) and
      (not hostname or bot.hostname == hostname)):
    return bot

  bot = bot or Bot(key=key)
  bot.last_seen = task_common.utcnow()
  if dimensions:
    bot.dimensions = dimensions
  if hostname:
    # TODO(maruel): Handle id-hostname mismatches better.
    bot.hostname = hostname
  bot.put()
  return bot
