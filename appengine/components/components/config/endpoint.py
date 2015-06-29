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
from . import validation


class ConfigSettingsMessage(messages.Message):
  """Configuration service location."""
  # Example: 'luci-config.appspot.com'
  service_hostname = messages.StringField(1)


class ValidateRequestMessage(messages.Message):
  config_set = messages.StringField(1, required=True)
  path = messages.StringField(2, required=True)
  content = messages.BytesField(3, required=True)


class ValidationMessage(messages.Message):
  class Severity(messages.Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
  text = messages.StringField(1, required=True)
  severity = messages.EnumField(Severity, 2, required=True)


class ValidateResponseMessage(messages.Message):
  messages = messages.MessageField(ValidationMessage, 1, repeated=True)


@auth.endpoints_api(name='config', version='v1', title='Configuration service')
class ConfigApi(remote.Service):
  """Configuration service."""

  @auth.endpoints_method(
      ConfigSettingsMessage, ConfigSettingsMessage,
      http_method='POST')
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

  @auth.endpoints_method(
      ValidateRequestMessage, ValidateResponseMessage, http_method='POST')
  def validate(self, request):
    """Validates a config.

    Compatible with validation protocol described in ValidationCfg message of
    /appengine/config_service/proto/service_config.proto.
    """
    ctx = validation.Context()
    validation.validate(request.config_set, request.path, request.content, ctx)

    res = ValidateResponseMessage()
    for m in ctx.result().messages:
      res.messages.append(ValidationMessage(
          severity=ValidationMessage.Severity.lookup_by_number(m.severity),
          text=m.text,
      ))
    return res
