# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot management.

Includes management of the swarming_bot.zip code and the list of known bots.
"""

import datetime
import logging
import os.path

from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import datastore_utils
from common import rpc
from common import test_request_message
from server import bot_archive
from server import file_chunks


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# The key to use to access the start slave script file model.
START_SLAVE_SCRIPT_KEY = 'start_slave_script'


# The amount of time that has to pass before a machine is considered dead.
MACHINE_DEATH_TIMEOUT = datetime.timedelta(seconds=30*60)


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

  # IP address as seen by the bot.
  internal_ip = ndb.StringProperty()

  # IP address as seen by the HTTP handler.
  external_ip = ndb.StringProperty()

  # Use auto_now_add instead of auto_now to make unit testing easier.
  last_seen = ndb.DateTimeProperty(auto_now_add=True)

  # Version the bot is currently at.
  version = ndb.StringProperty(default='')

  task = ndb.KeyProperty(kind='TaskRunResult')

  @property
  def task_entity(self):
    """Returns the TaskRunResult currently executing."""
    # TODO(maruel): This is inefficient in View, fix.
    return self.task.get() if self.task else None

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
  namespace = os.environ['CURRENT_VERSION_ID']
  key = 'bot_version-%s' + get_slave_version(host)
  code = memcache.get(key, namespace=namespace)
  if code:
    return code

  # Get the start slave script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {'start_slave.py': _get_start_slave()}
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
    bot_id, hostname, internal_ip, external_ip, dimensions, version):
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
  """
  bot = Bot(
      key=get_bot_key(bot_id),
      dimensions=dimensions or {},
      hostname=hostname,
      internal_ip=internal_ip,
      external_ip=external_ip,
      version=version)
  bot.put()
  return bot


def check_version(attributes, server_url):
  """Checks the slave version, forcing it to update if required."""
  expected_version = get_slave_version(server_url)
  if attributes.get('version', '') != expected_version:
    logging.info(
        '%s != %s, Updating slave %s',
        expected_version, attributes.get('version', 'N/A'), attributes['id'])
    url = server_url.rstrip('/') + '/get_slave_code/' + expected_version
    return {
      'commands': [rpc.BuildRPC('UpdateSlave', url)],
      # The only time a bot would have results to send here would be if it
      # failed to update.
      'result_url': server_url.rstrip('/') + '/remote_error',
      'try_count': 0,
    }
  return {}


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
  for attrib, value in attributes.items():
    if attrib == 'dimensions':
      if not isinstance(value, dict):
        raise test_request_message.Error('Invalid attrib value for dimensions')

    elif attrib == 'id':
      if not isinstance(value, basestring):
        raise test_request_message.Error('Invalid attrib value for id')

    elif attrib in ('ip', 'tag', 'version'):
      if not isinstance(value, (str, unicode)):
        raise test_request_message.Error(
            'Invalid attrib value type for ' + attrib)

    elif attrib == 'try_count':
      if not isinstance(value, int):
        raise test_request_message.Error(
            'Invalid attrib value type for try_count')
      if value < 0:
        raise test_request_message.Error(
            'Invalid negative value for try_count')

    else:
      raise test_request_message.Error(
          'Invalid attribute to machine: ' + attrib)

  if 'dimensions' not in attributes:
    raise test_request_message.Error('Missing mandatory attribute: dimensions')
  if 'id' not in attributes:
    raise test_request_message.Error('Missing mandatory attribute: id')
  attributes.setdefault('try_count', 0)
  return attributes
