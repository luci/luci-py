# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Instance specific settings."""

import os

from google.appengine.api import app_identity
from google.appengine.api import modules
from google.appengine.ext import ndb

from components import utils
from components.datastore_utils import config


class GlobalConfig(config.GlobalConfig):
  """Application wide settings."""
  # The number of seconds a cache entry must be kept for before it is evicted.
  default_expiration = ndb.IntegerProperty(indexed=False, default=30*24*60*60)

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

  # id to inject into pages if applicable.
  google_analytics = ndb.StringProperty(indexed=False, default='')

  # Enable ts_mon based monitoring.
  enable_ts_monitoring = ndb.BooleanProperty(indexed=False, default=False)

  def set_defaults(self):
    self.global_secret = os.urandom(16)
    self.gs_bucket = app_identity.get_application_id()


def settings(fresh=False):
  """Loads GlobalConfig or a default one if not present.

  If fresh=True, a full fetch from NDB is done.
  """
  if fresh:
    GlobalConfig.clear_cache()
  return GlobalConfig.cached()


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
