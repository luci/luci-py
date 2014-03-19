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
import re

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import modules
from google.appengine.ext import ndb

from components import utils


# App engine module to run task queue tasks on.
TASK_QUEUE_MODULE = 'backend'

# Requests that can possibly produce stats log entries.
STATS_REQUEST_PATHS = (
    # TODO(maruel): Remove once rolled out to prod.
    '/content/',
    '/content-gs/',
    # TODO(maruel): Remove once rolled out to prod.
    '/restricted/content/',
)

# Modules that can possibly produce stats log entries.
STATS_MODULES = ('default',)


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

  # Comma separated list of email addresses that will receive exception reports.
  # If not set, all admins will receive the message.
  monitoring_recipients = ndb.StringProperty(indexed=False, default='')

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


def get_module_version_list(module_list, tainted):
  """Returns a list of pairs (module name, version name) to fetch logs for.

  Arguments:
    module_list: list of modules to list, defaults to all modules.
    tainted: if False, excludes versions with '-tainted' in their name.
  """
  result = []
  if not module_list:
    # If the function it called too often, it'll raise a OverQuotaError. So
    # cache it for 10 minutes.
    module_list = memcache.get('modules_list')
    if not module_list:
      module_list = modules.get_modules()
      memcache.set('modules_list', module_list, time=10*60)

  for module in module_list:
    # If the function it called too often, it'll raise a OverQuotaError.
    # Versions is a bit more tricky since we'll loose data, since versions are
    # changed much more often than modules. So cache it for 3 minutes.
    key = 'modules_list-' + module
    version_list = memcache.get(key)
    if not version_list:
      version_list = modules.get_versions(module)
      memcache.set(key, version_list, time=3*60)
    result.extend(
        (module, v) for v in version_list if tainted or '-tainted' not in v)
  return result


def get_local_dev_server_host():
  """Returns 'hostname:port' for a default module on a local dev server."""
  assert utils.is_local_dev_server()
  return modules.get_hostname(module='default')


@utils.cache
def get_app_version():
  """Returns currently running version (not necessary a default one)."""
  return modules.get_current_version_name()


@utils.cache
def get_app_revision_url():
  """Returns URL of a git revision page for currently running app version.

  Works only for non-tainted versions uploaded with tools/update.py: app version
  should look like '162-efaec47'.

  Returns None if a version is tainted or has unexpected name.
  """
  rev = re.match(r'\d+-([a-f0-9]+)$', get_app_version())
  template = 'https://code.google.com/p/swarming/source/detail?r=%s'
  return template % rev.group(1) if rev else None


@utils.cache
def get_task_queue_host():
  """Returns domain name of app engine instance to run a task queue task on.

  This domain name points to a matching version of appropriate app engine
  module - <version>.<module>.isolateserver.appspot.com where:
    version: version of the module that is calling this function.
    module: app engine module to execute task on.

  That way a task enqueued from version 'A' of default module would be executed
  on same version 'A' of backend module.
  """
  # modules.get_hostname sometimes fails with unknown internal error.
  # Cache its result in a memcache to avoid calling it too often.
  cache_key = 'task_queue_host:%s' % get_app_version()
  value = memcache.get(cache_key)
  if not value:
    value = modules.get_hostname(module=TASK_QUEUE_MODULE)
    memcache.set(cache_key, value)
  return value


def warmup():
  """Precaches configuration in local memory, to be called from warmup handler.

  This call is optional. Everything works even if 'warmup' is never called.
  """
  settings()
  get_app_version()
  get_task_queue_host()
