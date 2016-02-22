# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Instance specific settings."""

from google.appengine.ext import ndb

from components.datastore_utils import config


class GlobalConfig(config.GlobalConfig):
  """Application wide settings."""
  # id to inject into pages if applicable.
  google_analytics = ndb.StringProperty(indexed=False, default='')

  # The number of seconds an old task can be deduped from.
  reusable_task_age_secs = ndb.IntegerProperty(
      indexed=False, default=7*24*60*60)

  # The amount of time that has to pass before a machine is considered dead.
  bot_death_timeout_secs = ndb.IntegerProperty(default=10*60)

  # Enable ts_mon based monitoring.
  enable_ts_monitoring = ndb.BooleanProperty(indexed=False, default=False)


def settings(fresh=False):
  """Loads GlobalConfig or a default one if not present.

  If fresh=True, a full fetch from NDB is done.
  """
  if fresh:
    GlobalConfig.clear_cache()
  return GlobalConfig.cached()
