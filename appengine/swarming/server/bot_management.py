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
from common import test_request_message
from server import bot_archive
from server import file_chunks
from server import task_common


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# The key to use to access the start slave script file model.
START_SLAVE_SCRIPT_KEY = 'start_slave_script'


# The amount of time that has to pass before a machine is considered dead.
BOT_DEATH_TIMEOUT = datetime.timedelta(seconds=30*60)


### Models.


class Bot(ndb.Model):
  """This entity declare the knowledge about a bot that successfully connected.

  The key id is the id of the bot.

  Multiple bots could run on a single host, for example with multiple
  phones connected to a host. In this case, the id is specific to each device
  acting as a bot. So while the id must be unique, the hostname doesn't have
  to be.
  """
  dimensions = datastore_utils.DeterministicJsonProperty(json_type=dict)

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
      out['task'] = task_common.pack_run_result_key(self.task)
    return out

  def to_dict_with_now(self, now):
    out = self.to_dict()
    out['is_dead'] = self.is_dead(now)
    return out


### Public APIs.


def get_bootstrap(host_url):
  """Returns the mangled version of the utility script bootstrap.py."""
  content = file_chunks.RetrieveFile('bootstrap.py')
  if not content:
    # Fallback to the one embedded in the tree.
    with open(os.path.join(ROOT_DIR, 'swarm_bot/bootstrap.py'), 'rb') as f:
      content = f.read()
  header = 'host_url = %r\n' % host_url
  return header + content


def get_start_slave():
  """Returns the start_slave.py content to be used in the zip.

  First fetch it from the database, if present. Pass the one in the tree
  otherwise.
  """
  content = file_chunks.RetrieveFile(START_SLAVE_SCRIPT_KEY)
  if content:
    return content
  with open(os.path.join(ROOT_DIR, 'swarm_bot', 'start_slave.py'), 'rb') as f:
    return f.read()


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
  additionals = {'start_slave.py': get_start_slave()}
  bot_dir = os.path.join(ROOT_DIR, 'swarm_bot')
  bot_version = bot_archive.get_swarming_bot_version(bot_dir, host, additionals)
  memcache.set(key, bot_version, namespace=namespace)
  return bot_version


def get_swarming_bot_zip(host):
  """Returns a zipped file of all the files a slave needs to run.

  Returns:
    A string representing the zipped file's contents.
  """
  namespace = os.environ['CURRENT_VERSION_ID']
  key = 'bot_version-%s' + get_slave_version(host)
  code = memcache.get(key, namespace=namespace)
  if code:
    return code

  # Get the start slave script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {'start_slave.py': get_start_slave()}
  bot_dir = os.path.join(ROOT_DIR, 'swarm_bot')
  code = bot_archive.get_swarming_bot_zip(bot_dir, host, additionals)
  memcache.set(key, code, namespace=namespace)
  return code


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
    quarantined):
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
  """
  # The primary reason for the write is to update .last_seen_ts.
  bot = Bot(
      key=get_bot_key(bot_id),
      dimensions=dimensions or {},
      hostname=hostname,
      internal_ip=internal_ip,
      external_ip=external_ip,
      version=version,
      quarantined=quarantined)
  bot.put()
  return bot


def validate_and_fix_attributes(attributes):
  """Validates and fixes the attributes of the requesting machine.

  Args:
    attributes: A dictionary representing the machine attributes.

  Raises:
    test_request_message.Error: If the request format/attributes aren't valid.

  Returns:
    A dictionary containing the fixed attributes of the machine.
  """
  # Parse given attributes.
  for key, value in attributes.iteritems():
    if key == 'dimensions':
      if not isinstance(value, dict):
        raise test_request_message.Error(
            'Invalid value for %s: %s\n%s' % (key, value, attributes))

    elif key == 'tag':
      # TODO(maruel): 'tag' is now ignored but do not abort if present so
      # upgrading can be done seamlessly. Delete this condition once all the
      # servers and bots are upgraded.
      pass

    elif key in ('id', 'ip', 'version'):
      if not isinstance(value, basestring):
        raise test_request_message.Error(
            'Invalid value for %s: %s\n%s' % (key, value, attributes))

    elif key == 'try_count':
      if not isinstance(value, int):
        raise test_request_message.Error(
            'Invalid value for %s: %s\n%s' % (key, value, attributes))
      if value < 0:
        raise test_request_message.Error(
            'Invalid value for %s: %s\n%s' % (key, value, attributes))

    else:
      raise test_request_message.Error(
          'Invalid key %s\n%s' % (key, attributes))

  if 'dimensions' not in attributes:
    raise test_request_message.Error('Missing mandatory attribute: dimensions')
  if 'id' not in attributes:
    raise test_request_message.Error('Missing mandatory attribute: id')
  attributes.setdefault('try_count', 0)


def should_restart_bot(_bot_id, _attributes, _state):
  """Decides whether a bot needs to be restarted.

  Args:
    bot_id: ID of the bot.
    attributes: A dictionary representing the machine attributes.
    state: A dictionary representing current bot state.

  Returns:
    Tuple (True to restart, text message explaining the reason).
  """
  # TODO(vadimsh): Implement.
  return False, ''
