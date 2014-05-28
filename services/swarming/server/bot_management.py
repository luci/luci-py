# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot code management."""

import os.path

from google.appengine.api import memcache

from server import bot_archive
from server import file_chunks


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The key to use to access the start slave script file model.
START_SLAVE_SCRIPT_KEY = 'start_slave_script'


def StoreStartSlaveScript(script):
  """Stores the given script as the new start slave script for all slave.

  Args:
    script: The contents of the new start slave script.
  """
  file_chunks.StoreFile(START_SLAVE_SCRIPT_KEY, script)

  # Clear the cached version value since it has now changed.
  memcache.delete('slave_version', namespace=os.environ['CURRENT_VERSION_ID'])


def SlaveVersion():
  """Retrieves the slave version loaded on this server.

  The memcache is first checked for the version, otherwise the value
  is generated and then stored in the memcache.

  Returns:
    The hash of the current slave version.
  """
  slave_version = memcache.get('slave_version',
                               namespace=os.environ['CURRENT_VERSION_ID'])
  if slave_version:
    return slave_version
  # Get the start slave script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {
    'start_slave.py': file_chunks.RetrieveFile(START_SLAVE_SCRIPT_KEY) or '',
  }
  slave_version = bot_archive.get_swarming_bot_version(
      os.path.join(ROOT_DIR, 'swarm_bot'), additionals)
  memcache.set('slave_version', slave_version,
               namespace=os.environ['CURRENT_VERSION_ID'])
  return slave_version


def get_swarming_bot_zip():
  """Returns a zipped file of all the files a slave needs to run.

  Returns:
    A string representing the zipped file's contents.
  """
  # Get the start slave script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {
    'start_slave.py': file_chunks.RetrieveFile(START_SLAVE_SCRIPT_KEY) or '',
  }
  return bot_archive.get_swarming_bot_zip(
      os.path.join(ROOT_DIR, 'swarm_bot'), additionals)
