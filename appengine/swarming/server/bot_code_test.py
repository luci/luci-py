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
import unittest
import zipfile

import test_env
test_env.setup_test_env()

from components import auth
from test_support import test_case

from server import bot_archive
from server import bot_code

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

  def test_store_bot_config(self):
    # When a new start bot script is uploaded, we should recalculate the
    # version hash since it will have changed.
    v1 = bot_code.get_bot_version('http://localhost')
    bot_code.store_bot_config('dummy_script')
    v2 = bot_code.get_bot_version('http://localhost')
    v3 = bot_code.get_bot_version('http://localhost:8080')
    self.assertNotEqual(v1, v2)
    self.assertNotEqual(v1, v3)
    self.assertNotEqual(v2, v3)

  def test_get_bot_version(self):
    actual = bot_code.get_bot_version('http://localhost')
    self.assertTrue(re.match(r'^[0-9a-f]{40}$', actual), actual)

  def test_get_swarming_bot_zip(self):
    zipped_code = bot_code.get_swarming_bot_zip('http://localhost')
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

  def test_bootstrap_token(self):
    tok = bot_code.generate_bootstrap_token()
    self.assertEqual(
        {'for': 'user:joe@localhost'}, bot_code.validate_bootstrap_token(tok))


if __name__ == '__main__':
  fix_encoding.fix_encoding()
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
