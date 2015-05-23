# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Cloud Endpoints API for configs.

* Reads/writes config service location.
* Validates configs. TODO(nodir): implement.
"""

import logging

from components import auth
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from . import common


class ConfigSettingsMessage(messages.Message):
  """Configuration service location."""
  # Example: 'luci-config.appspot.com'
  service_hostname = messages.StringField(1)


@auth.endpoints_api(name='config', version='v1', title='Configuration service')
class ConfigApi(remote.Service):
  """Configuration service."""

  @auth.endpoints_method(ConfigSettingsMessage, ConfigSettingsMessage)
  @auth.require(auth.is_admin)
  def settings(self, request):
    """Reads/writes config service location. Accessible only by admins."""
    settings = common.ConfigSettings.fetch() or common.ConfigSettings()
    if request.service_hostname is not None:
      # Change only if service_hostname was specified.
      changed = settings.modify(service_hostname=request.service_hostname)
      if changed:
        logging.warning('Updated config settings')
    settings = common.ConfigSettings.fetch() or settings
    return ConfigSettingsMessage(
        service_hostname=settings.service_hostname,
    )
