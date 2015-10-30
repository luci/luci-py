# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Android specific utility functions.

This file serves as an API to bot_config.py. bot_config.py can be replaced on
the server to allow additional server-specific functionality.
"""

import collections
import logging
import os


from adb import adb_commands_safe
from adb import high


def initialize(pub_key, priv_key):
  return high.Initialize(pub_key, priv_key)


def get_devices(bot):
  return high.GetDevices(on_error=bot.post_error if bot else None)


def close_devices(devices):
  return high.CloseDevices(devices)
