# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot code. Includes bootstrap and swarming_bot.zip.

It includes everything that is AppEngine specific. The non-GAE code is in
bot_archive.py.
"""

import collections
import os.path

from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import auth
from components import datastore_utils
from server import bot_archive


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


### Models.


File = collections.namedtuple('File', ('content', 'who', 'when'))


class VersionedFile(ndb.Model):
  created_ts = ndb.DateTimeProperty(indexed=False, auto_now_add=True)
  who = auth.IdentityProperty(indexed=False)
  content = ndb.BlobProperty(compressed=True)

  ROOT_MODEL = datastore_utils.get_versioned_root_model('VersionedFileRoot')

  @classmethod
  def fetch(cls, name):
    """Returns the current version of the instance."""
    return datastore_utils.get_versioned_most_recent(
        cls, cls._gen_root_key(name))

  def store(self, name):
    """Stores a new version of the instance."""
    # Create an incomplete key.
    self.key = ndb.Key(self.__class__, None, parent=self._gen_root_key(name))
    self.who = auth.get_current_identity()
    return datastore_utils.store_new_version(self, self.ROOT_MODEL)

  @classmethod
  def _gen_root_key(cls, name):
    return ndb.Key(cls.ROOT_MODEL, name)


### Public APIs.


def get_bootstrap(host_url):
  """Returns the mangled version of the utility script bootstrap.py.

  Returns:
    File instance.
  """
  header = 'host_url = %r\n' % host_url
  obj = VersionedFile.fetch('bootstrap.py')
  if obj and obj.content:
    return File(header + obj.content, obj.who, obj.created_ts)
  # Fallback to the one embedded in the tree.
  with open(os.path.join(ROOT_DIR, 'swarming_bot/bootstrap.py'), 'rb') as f:
    return File(header + f.read(), None, None)


def store_bootstrap(content):
  """Stores a new version of bootstrap.py."""
  return VersionedFile(content=content).store('bootstrap.py')


def get_bot_config():
  """Returns the current version of bot_config.py and extra metadata.

  Returns:
    File instance.
  """
  obj = VersionedFile.fetch('bot_config.py')
  if obj:
    return File(obj.content, obj.who, obj.created_ts)

  # Fallback to the one embedded in the tree.
  path = os.path.join(ROOT_DIR, 'swarming_bot', 'bot_config.py')
  with open(path, 'rb') as f:
    return File(f.read(), None, None)


def store_bot_config(content):
  """Stores a new version of bot_config.py."""
  out = VersionedFile(content=content).store('bot_config.py')
  # Clear the cached version value since it has now changed. This is *super*
  # aggressive to flush all memcache but that's the only safe way to not send
  # old code by accident.
  while not memcache.flush_all():
    pass
  return out


def get_bot_version(host):
  """Retrieves the bot version loaded on this server.

  The memcache is first checked for the version, otherwise the value
  is generated and then stored in the memcache.

  Returns:
    The hash of the current bot version.
  """
  namespace = os.environ['CURRENT_VERSION_ID']
  key = 'bot_version' + host
  bot_version = memcache.get(key, namespace=namespace)
  if bot_version:
    return bot_version

  # Need to calculate it.
  additionals = {'bot_config.py': get_bot_config().content}
  bot_dir = os.path.join(ROOT_DIR, 'swarming_bot')
  bot_version = bot_archive.get_swarming_bot_version(bot_dir, host, additionals)
  memcache.set(key, bot_version, namespace=namespace)
  return bot_version


def get_swarming_bot_zip(host):
  """Returns a zipped file of all the files a bot needs to run.

  Returns:
    A string representing the zipped file's contents.
  """
  namespace = os.environ['CURRENT_VERSION_ID']
  key = 'bot_version-%s' + get_bot_version(host)
  code = memcache.get(key, namespace=namespace)
  if code:
    return code

  # Get the start bot script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {'bot_config.py': get_bot_config().content}
  bot_dir = os.path.join(ROOT_DIR, 'swarming_bot')
  code = bot_archive.get_swarming_bot_zip(bot_dir, host, additionals)
  memcache.set(key, code, namespace=namespace)
  return code
