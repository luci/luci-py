# Copyright 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
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
"""

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.api import app_identity
from google.appengine.ext import ndb
# pylint: enable=E0611,F0401


class GlobalConfig(ndb.Model):
  """Application wide settings."""
  # The number of days a cache entry must be kept for before it is evicted.
  # Note: this doesn't applies to namespaces where is_temporary is True. For
  # these, the retension is always 1 day.
  retension_days = ndb.IntegerProperty(indexed=False, default=7)

  # The Google Cloud Storage bucket where to save the data. By default it's the
  # name of the application instance.
  gs_bucket = ndb.StringProperty(
      indexed=False, default=app_identity.get_application_id())

  # Enable appstats and optionally cost calculation.
  #
  # Note: to take effect, the datastore cache must first be cleared by flushing
  # memcache, and only then, the instances need to be restarted. This can be
  # achieved by switching the default app version or reuploading the same code
  # to the same version, which forces all the instances to restart.
  enable_appstats = ndb.BooleanProperty(indexed=False, default=False)
  enable_appstats_cost = ndb.BooleanProperty(indexed=False, default=False)


def settings():
  return settings_async().get_result()


@ndb.tasklet
def settings_async():
  """Loads GlobalConfig or create one if not present, asynchronously.

  Saves any default value that could be missing from the stored entity.
  """
  # Access to a protected member XXX of a client class.
  # pylint: disable=W0212
  config = yield GlobalConfig.get_or_insert_async('global_config')

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
    yield config.put_async()

  raise ndb.Return(config)
