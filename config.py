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

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.api import app_identity
from google.appengine.api import modules
from google.appengine.ext import ndb
# pylint: enable=E0611,F0401

import utils


# App engine module to run task queue tasks on.
TASK_QUEUE_MODULE = 'backend'

# Requests that can possibly produce stats log entries.
STATS_REQUEST_PATHS = ('/content/', '/content-gs/', '/restricted/content/')

# Modules that can possibly produce stats log entries.
STATS_MODULES = ('default',)



class GlobalConfig(ndb.Model):
  """Application wide settings."""
  # The number of days a cache entry must be kept for before it is evicted.
  # Note: this doesn't applies to namespaces where is_temporary is True. For
  # these, the retention is always 1 day.
  retention_days = ndb.IntegerProperty(indexed=False, default=7)

  # Secret key used to generate XSRF tokens and signatures.
  global_secret = ndb.BlobProperty()

  # The Google Cloud Storage bucket where to save the data. By default it's the
  # name of the application instance.
  gs_bucket = ndb.StringProperty(
      indexed=False, default=app_identity.get_application_id())

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

  return config


def is_local_dev_server():
  """Returns True if running on local development server."""
  return os.environ.get('SERVER_SOFTWARE', '').startswith('Development')


def get_app_version():
  """Returns currently running version (not necessary a default one)."""
  return modules.get_current_version_name()


def get_task_queue_host():
  """Returns domain name of app engine instance to run a task queue task on.

  This domain name points to a matching version of appropriate app engine
  module - <version>.<module>.isolateserver.appspot.com where:
    version: version of the module that is calling this function.
    module: app engine module to execute task on.

  That way a task enqueued from version 'A' of default module would be executed
  on same version 'A' of backend module.
  """
  return modules.get_hostname(module=TASK_QUEUE_MODULE)
