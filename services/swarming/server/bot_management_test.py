#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import shutil
import StringIO
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from server import bot_archive
from server import bot_management
from support import test_case


class BotManagementTest(test_case.TestCase):
  def testStoreStartSlaveScriptClearCache(self):
    # When a new start slave script is uploaded, we should recalculate the
    # version hash since it will have changed.
    old_version = bot_management.SlaveVersion()

    bot_management.StoreStartSlaveScript('dummy_script')

    self.assertNotEqual(old_version,
                        bot_management.SlaveVersion())

  def testget_swarming_bot_zip(self):
    zipped_code = bot_management.get_swarming_bot_zip()

    temp_dir = tempfile.mkdtemp(prefix='swarming')
    try:
      with zipfile.ZipFile(StringIO.StringIO(zipped_code), 'r') as zip_file:
        zip_file.extractall(temp_dir)

      for i in bot_archive.FILES:
        self.assertTrue(os.path.isfile(os.path.join(temp_dir, i)), i)

      # Try running the slave and ensure it can import the required files.
      # (It would crash if it failed to import them).
      subprocess.check_output(
          [sys.executable, os.path.join(temp_dir, 'slave_machine.py'), '-h'])
    finally:
      shutil.rmtree(temp_dir)


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
