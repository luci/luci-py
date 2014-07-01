# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Instance specific settings.

Use the datastore editor to change the default values.

Workflow to add a new configuration value to GlobalConfig is:
  - Add the Property to GlobalConfig with its default value set.
  - Upload your instance.
  - Flushing memcache will force a write of the default value for your new
    property as soon as a setting is looked up.

Workflow to edit a value:
  - Update the value with AppEngine's Datastore Viewer.
  - Wait a few seconds for datastore coherency.
  - Flush Memcache.
  - Wait 1 min for a local in-memory cache to expire.
"""

import os

from google.appengine.api import app_identity
from google.appengine.api import modules
from google.appengine.ext import ndb

from components import utils


class GlobalConfig(ndb.Model):
  """Application wide settings."""
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
  # Access to a protected member XXX of a client class.
  # pylint: disable=W0212
  context = ndb.ContextOptions(use_cache=False)
  config = GlobalConfig.get_or_insert('global_config', context_options=context)

  # Look in AppEngine internal guts to see if all the values were stored. If
  # not, store the missing properties in the entity in the datastore.
  missing = dict(
      (key, prop._default)
      for key, prop in GlobalConfig._properties.iteritems()
      if not prop._has_value(config))
  if missing:
    # Not all the properties are set so store the entity with the missing
    # property set to the default value. In theory it should be done in a
    # transaction but in practice it's fine.
    for key, default in missing.iteritems():
      setattr(config, key, default)
    config.put()

  if not config.gs_bucket:
    config.gs_bucket = app_identity.get_application_id()
    config.put()

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
