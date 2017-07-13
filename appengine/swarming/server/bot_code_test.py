#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import StringIO
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb
from components import auth
from test_support import test_case

from server import bot_archive
from server import bot_code
from components import config

CLIENT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(test_env.APP_DIR)), 'client')
sys.path.insert(0, CLIENT_DIR)
from third_party.depot_tools import fix_encoding
from utils import file_path
sys.path.pop(0)


class BotManagementTest(test_case.TestCase):
  def setUp(self):
    super(BotManagementTest, self).setUp()
    self.testbed.init_user_stub()

    self.mock(
        auth, 'get_current_identity',
        lambda: auth.Identity(auth.IDENTITY_USER, 'joe@localhost'))


  def test_get_bootstrap(self):
    def get_self_config_mock(path, revision=None, store_last_good=False):
      self.assertEqual('scripts/bootstrap.py', path)
      self.assertEqual(None, revision)
      self.assertEqual(True, store_last_good)
      return None, 'foo bar'
    self.mock(config, 'get_self_config', get_self_config_mock)
    f = bot_code.get_bootstrap('localhost', 'token', None)
    expected = (
      '#!/usr/bin/env python\n'
      'host_url = \'localhost\'\n'
      'bootstrap_token = \'token\'\n'
      'foo bar')
    self.assertEqual(expected, f.content)

  def test_get_bot_config(self):
    def get_self_config_mock(path, revision=None, store_last_good=False):
      self.assertEqual('scripts/bot_config.py', path)
      self.assertEqual(None, revision)
      self.assertEqual(True, store_last_good)
      return None, 'foo bar'
    self.mock(config, 'get_self_config', get_self_config_mock)
    f = bot_code.get_bot_config(None)
    self.assertEqual('foo bar', f.content)

  def test_store_bot_config(self):
    # When a new start bot script is uploaded, we should recalculate the
    # version hash since it will have changed.
    expected = {
      'config/bot_config.py': bot_code.get_bot_config().content,
    }
    v1, additionals = bot_code.get_bot_version('http://localhost')
    self.assertEqual(expected, additionals)
    bot_code.store_bot_config('http://localhost', 'dummy_script')
    # memcache timeout is 60 seconds, fake it by just clearing.
    bot_code.memcache.flush_all()
    v2, additionals = bot_code.get_bot_version('http://localhost')
    expected = {'config/bot_config.py': 'dummy_script'}
    self.assertEqual(expected, additionals)
    v3, additionals = bot_code.get_bot_version('http://localhost:8080')
    self.assertEqual(expected, additionals)
    self.assertNotEqual(v1, v2)
    self.assertNotEqual(v1, v3)
    self.assertNotEqual(v2, v3)

  def test_get_bot_version(self):
    actual, additionals = bot_code.get_bot_version('http://localhost')
    self.assertTrue(re.match(r'^[0-9a-f]{64}$', actual), actual)
    expected = {
      'config/bot_config.py': bot_code.get_bot_config().content,
    }
    self.assertEqual(expected, additionals)

  def mock_memcache(self):
    local_mc = {'store': {}, 'reads': 0, 'writes': 0}

    @ndb.tasklet
    def mock_memcache_get(version, desc, part=None):
      value = local_mc['store'].get(bot_code.bot_key(version, desc, part))
      if value is not None:
        local_mc['reads'] += 1
      raise ndb.Return(value)

    @ndb.tasklet
    def mock_memcache_set(value, version, desc, part=None):
      local_mc['writes'] += 1
      key = bot_code.bot_key(version, desc, part)
      local_mc['store'][key] = value
      return ndb.Return(None)

    self.mock(bot_code, 'bot_memcache_set', mock_memcache_set)
    self.mock(bot_code, 'bot_memcache_get', mock_memcache_get)
    self.mock(bot_code, 'MAX_MEMCACHED_SIZE_BYTES', 100000)

    return local_mc

  def test_get_swarming_bot_zip(self):
    local_mc = self.mock_memcache()

    self.assertEqual(0, local_mc['writes'])
    zipped_code = bot_code.get_swarming_bot_zip('http://localhost')
    self.assertEqual(0, local_mc['reads'])
    self.assertNotEqual(0, local_mc['writes'])

    # Make sure that we read from memcached if we get it again
    zipped_code_copy = bot_code.get_swarming_bot_zip('http://localhost')
    self.assertEqual(local_mc['writes'], local_mc['reads'])
    # Why not assertEqual? Don't want to dump ~1MB of data if this fails.
    self.assertTrue(zipped_code == zipped_code_copy)

    # Ensure the zip is valid and all the expected files are present.
    with zipfile.ZipFile(StringIO.StringIO(zipped_code), 'r') as zip_file:
      for i in bot_archive.FILES:
        with zip_file.open(i) as f:
          content = f.read()
          if os.path.basename(i) != '__init__.py':
            self.assertTrue(content, i)

    temp_dir = tempfile.mkdtemp(prefix='swarming')
    try:
      # Try running the bot and ensure it can import the required files. (It
      # would crash if it failed to import them).
      bot_path = os.path.join(temp_dir, 'swarming_bot.zip')
      with open(bot_path, 'wb') as f:
        f.write(zipped_code)
      proc = subprocess.Popen(
          [sys.executable, bot_path, 'start_bot', '-h'],
          cwd=temp_dir,
          stdout=subprocess.PIPE,
          stderr=subprocess.STDOUT)
      out = proc.communicate()[0]
      self.assertEqual(0, proc.returncode, out)
    finally:
      file_path.rmtree(temp_dir)

  def test_get_swarming_bot_zip_is_reproducible(self):
    self.mock(time, 'time', lambda: 1500000000.0)
    local_mc = self.mock_memcache()

    zipped_code_1 = bot_code.get_swarming_bot_zip('http://localhost')

    # Time passes, memcache clears.
    self.mock(time, 'time', lambda: 1500001000.0)
    local_mc['store'].clear()

    # Some time later, the exact same zip is fetched, byte-to-byte.
    zipped_code_2 = bot_code.get_swarming_bot_zip('http://localhost')
    self.assertTrue(zipped_code_1 == zipped_code_2)

  def test_bootstrap_token(self):
    tok = bot_code.generate_bootstrap_token()
    self.assertEqual(
        {'for': 'user:joe@localhost'}, bot_code.validate_bootstrap_token(tok))


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
