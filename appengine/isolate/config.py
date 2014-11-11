# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Instance specific settings."""

import os

from google.appengine.api import app_identity
from google.appengine.api import modules
from google.appengine.ext import ndb

from components import auth
from components import datastore_utils
from components import utils


class GlobalConfig(ndb.Model):
  """Application wide settings."""
  created_ts = ndb.DateTimeProperty(indexed=False, auto_now_add=True)
  who = auth.IdentityProperty(indexed=False)

  # The number of seconds a cache entry must be kept for before it is evicted.
  default_expiration = ndb.IntegerProperty(indexed=False, default=7*24*60*60)

  # This determines the number of initial letters from the ContentEntry hash
  # value to use as buckets in ContentShard. This is to even out writes across
  # multiple entity groups. The goal is to get into the range of ~1 write per
  # second per bucket.
  #
  # Each letter represent 4 bits of information, so the number of ContentShard
  # will be 16**N. Nominal values are:
  #   1: 16 buckets
  #   2: 256 buckets
  #   3: 4096 buckets
  #   4: 65536 buckets
  sharding_letters = ndb.IntegerProperty(indexed=False, default=1)

  # Secret key used to generate XSRF tokens and signatures.
  global_secret = ndb.BlobProperty()

  # The Google Cloud Storage bucket where to save the data. By default it's the
  # name of the application instance.
  gs_bucket = ndb.StringProperty(indexed=False)

  # Email address of Service account used to access Google Storage.
  gs_client_id_email = ndb.StringProperty(indexed=False, default='')

  # Secret key used to sign Google Storage URLs: base64 encoded *.der file.
  gs_private_key = ndb.StringProperty(indexed=False, default='')

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

  def _pre_put_hook(self):
    """Generates global_secret only when necessary.

    If default=os.urandom(16) was set on secret, it would fetch 16 bytes of
    random data on every process startup, which is unnecessary.
    """
    self.global_secret = self.global_secret or os.urandom(16)


@utils.cache_with_expiration(expiration_sec=60)
def settings():
  """Loads GlobalConfig or create one if not present.

  Saves any default value that could be missing from the stored entity.
  """
  config = GlobalConfig.fetch()
  if not config:
    # TODO(maruel): Remove this code once all instances are migrated.
    # Access to a protected member XXX of a client class.
    # pylint: disable=W0212
    config = ndb.Key(GlobalConfig, 'global_config').get(use_cache=False)
    if config:
      config.store(who=auth.get_service_self_identity())

  if not config:
    config = GlobalConfig(gs_bucket=app_identity.get_application_id())
    config.store(who=auth.get_service_self_identity())

  return config


def get_local_dev_server_host():
  """Returns 'hostname:port' for a default module on a local dev server."""
  assert utils.is_local_dev_server()
  return modules.get_hostname(module='default')


def warmup():
  """Precaches configuration in local memory, to be called from warmup handler.

  This call is optional. Everything works even if 'warmup' is never called.
  """
  settings()
  utils.get_task_queue_host()
  utils.get_app_version()
