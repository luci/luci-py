# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Administration API accessible only by service admins.

Defined as Endpoints API mostly to abuse API Explorer UI and not to write our
own admin UI. Note that all methods are publicly visible (though the source code
itself is also publicly visible, so not a big deal).

Callers have to be in 'administrators' group.
"""

import logging

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from components import auth
from components import config


# This is used by endpoints indirectly.
package = 'luci-config'


class ServiceConfigStorageType(messages.Enum):
  """Type of repository where service configs are stored."""
  GITILES = 0


class GlobalConfig(config.GlobalConfig):
  """Server-wide static configuration stored in datastore.

  Typically it is set once during service setup and is never changed.
  """
  # Type of repository where service configs are stored.
  services_config_storage_type = msgprop.EnumProperty(ServiceConfigStorageType)
  # If config storage type is Gitiles, URL to the root of service configs
  # directory.
  services_config_location = ndb.StringProperty()


class GlobalConfigMessage(messages.Message):
  """GlobalConfig as a RPC message."""
  services_config_storage_type = messages.EnumField(ServiceConfigStorageType, 1)
  services_config_location = messages.StringField(2)


@auth.endpoints_api(name='admin', version='v1', title='Administration API')
class AdminApi(remote.Service):
  """Administration API accessible only by the service admins."""

  @auth.endpoints_method(
      GlobalConfigMessage, GlobalConfigMessage, name='globalConfig')
  @auth.require(auth.is_admin)
  def global_config(self, request):
    """Reads/writes global configuration."""
    conf = GlobalConfig.fetch()
    if not conf:
      conf = GlobalConfig()

    changed = conf.modify(
        services_config_storage_type=request.services_config_storage_type,
        services_config_location=request.services_config_location)

    if changed:
      logging.warning('Updated global configuration')

    return GlobalConfigMessage(
        services_config_location=conf.services_config_location,
        services_config_storage_type=conf.services_config_storage_type)
