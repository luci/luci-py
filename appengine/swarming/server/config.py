# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Instance specific settings."""

from google.appengine.ext import ndb

from components import config


class GlobalConfig(config.GlobalConfig):
  """Application wide settings."""
  # id to inject into pages if applicable.
  google_analytics = ndb.StringProperty(indexed=False)


def settings():
  """Loads GlobalConfig or create one if not present.

  Saves any default value that could be missing from the stored entity.
  """
  return GlobalConfig.cached()
