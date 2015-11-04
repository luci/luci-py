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
from adb import adb_protocol
from adb import common
from adb import high


# Master switch that can easily be temporarily increased to INFO or even DEBUG
# when needed by simply pushing a new tainted swarming server version. This
# helps quickly debugging issues. On the other hand, even INFO level is quite
# verbose so keep it at WARNING by default.
LEVEL = logging.WARNING
adb_commands_safe._LOG.setLevel(LEVEL)
adb_protocol._LOG.setLevel(LEVEL)
common._LOG.setLevel(LEVEL)
high._LOG.setLevel(LEVEL)


def initialize(pub_key, priv_key):
  return high.Initialize(pub_key, priv_key)


def get_devices(bot):
  return high.GetDevices(
      'swarming', 10000, 10000, on_error=bot.post_error if bot else None,
      as_root=True)


def close_devices(devices):
  return high.CloseDevices(devices)


def kill_adb():
  return adb_commands_safe.KillADB()
