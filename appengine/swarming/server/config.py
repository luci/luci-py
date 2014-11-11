# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Instance specific settings."""

from google.appengine.ext import ndb

from components import auth
from components import datastore_utils
from components import utils


class GlobalConfig(ndb.Model):
  """Application wide settings."""
  created_ts = ndb.DateTimeProperty(indexed=False, auto_now_add=True)
  who = auth.IdentityProperty(indexed=False)

  # id to inject into pages if applicable.
  google_analytics = ndb.StringProperty(indexed=False)

  ROOT_MODEL = datastore_utils.get_versioned_root_model('GlobalConfigRoot')
  ROOT_KEY = ndb.Key(ROOT_MODEL, 1)

  @classmethod
  def fetch(cls):
    """Returns the current version of the instance."""
    return datastore_utils.get_versioned_most_recent(cls, cls.ROOT_KEY)

  def store(self, who=None):
    """Stores a new version of the instance."""
    # Create an incomplete key.
    self.key = ndb.Key(self.__class__, None, parent=self.ROOT_KEY)
    self.who = who or auth.get_current_identity()
    return datastore_utils.store_new_version(self, self.ROOT_MODEL)


@utils.cache_with_expiration(expiration_sec=60)
def settings():
  """Loads GlobalConfig or create one if not present.

  Saves any default value that could be missing from the stored entity.
  """
  config = GlobalConfig.fetch()
  if not config:
    config = GlobalConfig()
    config.store(who=auth.get_service_self_identity())
  return config
