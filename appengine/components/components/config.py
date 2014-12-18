# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Versioned singleton entity with global application configuration.

Example usage:
  from components import config

  class MyConfig(config.GlobalConfig):
    param1 = ndb.StringProperty()
    param2 = ndb.StringProperty()
    ...

  ...

  def do_stuff():
    param1 = MyConfig.cached().param1
    param2 = MyConfig.cached().param2
    ...

  def modify():
    conf = MyConfig.fetch()
    conf.modify(param1='123')


Advantages over regular ndb.Entity with predefined key:
  1. All changes are logged (see datastore_utils.store_new_version).
  2. In-memory process-wide cache.
"""

from google.appengine.ext import ndb

from components import auth
from components import datastore_utils
from components import utils


class GlobalConfig(ndb.Model):
  """Singleton entity with the global configuration of the service.

  All changes are stored in the revision log.
  """

  # When this revision of configuration was created.
  updated_ts = ndb.DateTimeProperty(indexed=False, auto_now_add=True)
  # Who created this revision of configuration.
  updated_by = auth.IdentityProperty(indexed=False)

  @classmethod
  def cached(cls):
    """Fetches config entry from local cache or datastore.

    Bootstraps it if missing. May return slightly stale data but in most cases
    doesn't do any RPCs. Should be used for read-only access to config.
    """
    # @utils.cache_with_expiration can't be used directly with 'cached' since it
    # will be applied to base class method, not a concrete implementation
    # specific to a subclass. So build new class specific fetcher on the fly on
    # a first attempt (it's not a big deal if it happens concurrently in MT
    # environment, last one wins). Same can be achieved with metaclasses, but no
    # one likes metaclasses.
    if not cls._config_fetcher:
      @utils.cache_with_expiration(expiration_sec=60)
      def config_fetcher():
        conf = cls.fetch()
        if not conf:
          conf = cls()
          conf.set_defaults()
          conf.store(updated_by=auth.get_service_self_identity())
        return conf
      cls._config_fetcher = staticmethod(config_fetcher)
    return cls._config_fetcher()

  @classmethod
  def fetch(cls):
    """Returns the current up-to-date version of the config entity.

    Always fetches it from datastore. May return None if missing.
    """
    return datastore_utils.get_versioned_most_recent(cls, cls._get_root_key())

  def store(self, updated_by=None):
    """Stores a new version of the config entity."""
    # Create an incomplete key, to be completed by 'store_new_version'.
    self.key = ndb.Key(self.__class__, None, parent=self._get_root_key())
    self.updated_by = updated_by or auth.get_current_identity()
    return datastore_utils.store_new_version(self, self._get_root_model())

  def modify(self, updated_by=None, **kwargs):
    """Applies |kwargs| dict to the entity and stores the entity if changed."""
    dirty = False
    for k, v in kwargs.iteritems():
      assert k in self._properties, k
      if getattr(self, k) != v:
        setattr(self, k, v)
        dirty = True
    if dirty:
      self.store(updated_by=updated_by)
    return dirty

  def set_defaults(self):
    """Fills in default values for empty config. Implemented by subclasses."""

  ### Private stuff.

  _config_fetcher = None

  @classmethod
  def _get_root_model(cls):
    return datastore_utils.get_versioned_root_model('%sRoot' % cls.__name__)

  @classmethod
  def _get_root_key(cls):
    return ndb.Key(cls._get_root_model(), 1)
