#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Generates the swarming_bot.zip archive for local testing.
"""

import hashlib
import json
import logging
import os
import StringIO
import sys
import zipfile

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def read_config():
  config_path = os.path.join(ROOT_DIR, 'swarming_bot', 'config', 'config.json')
  with open(config_path, 'rb') as f:
    return json.load(f) or {}


def get_swarming_bot_zip():
  config = read_config()
  bot_config_path = os.path.join(
      ROOT_DIR, 'swarming_bot', 'config', 'bot_config.py')
  with open(bot_config_path, 'rb') as f:
    additionals = {'config/bot_config.py': f.read()}
  from server import bot_archive
  return bot_archive.get_swarming_bot_zip(
      os.path.join(ROOT_DIR, 'swarming_bot'), config['server'], '1',
      additionals, config['enable_ts_monitoring'])


def main():
  if len(sys.argv) > 1:
    print >> sys.stderr, (
        'This script creates a swarming_bot.zip file locally in the server '
        'directory. This script doesn\'t accept any argument.')
    return 1

  sys.path.insert(0, ROOT_DIR)
  content, _ = get_swarming_bot_zip()
  with open(os.path.join(ROOT_DIR, 'swarming_bot.zip'), 'wb') as f:
    f.write(content)
  return 0


if __name__ == '__main__':
  sys.exit(main())
