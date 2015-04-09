# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging

from protorpc import messages
from protorpc import message_types
from protorpc import remote
import endpoints

from components import auth

import acl
import storage


# This is used by endpoints indirectly.
package = 'luci-config'


class Status(messages.Enum):
  """Status codes for ConfigApi methods."""
  # Operation completed successfully.
  SUCCESS = 1
  # Requested config was not found.
  CONFIG_NOT_FOUND = 2
  # Requested config set has unexpected format.
  INVALID_CONFIG_SET = 3


@auth.endpoints_api(name='config', version='v1', title='Configuration Service')
class ConfigApi(remote.Service):
  """API to access configurations."""

  ##############################################################################
  # endpoint: get_config

  GET_CONFIG_REQUEST_RESOURCE_CONTAINER = endpoints.ResourceContainer(
      message_types.VoidMessage,
      config_set=messages.StringField(1, required=True),
      path=messages.StringField(2, required=True),
      revision=messages.StringField(3),
      hash_only=messages.BooleanField(4),
  )

  class GetConfigResponseMessage(messages.Message):
    # Can be SUCCESS, CONFIG_NOT_FOUND and INVALID_CONFIG_SET
    status = messages.EnumField(Status, 1, required=True)
    # If SUCCESS, config revision.
    revision = messages.StringField(2)
    # If SUCCESS, hash of the config file contents.
    # Can be used in get_config_by_hash endpoint.
    content_hash = messages.StringField(3)
    # If SUCCESS and request.only_hash is not set to True, the contents of the
    # config file.
    content = messages.BytesField(4)

  @auth.endpoints_method(
      GET_CONFIG_REQUEST_RESOURCE_CONTAINER,
      GetConfigResponseMessage,
      path='config_sets/{config_set}/config/{path}')
  def get_config(self, request):
    """Gets a config file."""
    res = self.GetConfigResponseMessage()

    try:
      has_access = acl.can_read_config_set(
          request.config_set, headers=self.request_state.headers)
    except ValueError:
      res.status = Status.INVALID_CONFIG_SET
      return res

    if not has_access:
      logging.warning(
          '%s does not have access to %s',
          auth.get_current_identity().to_bytes(),
          request.config_set)
      res.status = Status.CONFIG_NOT_FOUND
      return res

    res.revision, res.content_hash = storage.get_config_hash(
        request.config_set, request.path, revision=request.revision)
    if not res.content_hash:
      res.status = Status.CONFIG_NOT_FOUND
    else:
      res.status = Status.SUCCESS
      if not request.hash_only:
        res.content = storage.get_config_by_hash(res.content_hash)
        if not res.content:
          res.status = Status.CONFIG_NOT_FOUND
          logging.warning(
              'Config hash is found, but the blob is not.\n'
              'File: "%s:%s:%s". Hash: %s', request.config_set,
              request.revision, request.path, res.content_hash)
    return res

  ##############################################################################
  # endpoint: get_config_by_hash

  GET_CONFIG_BY_HASH_REQUEST_RESOURCE_CONTAINER = endpoints.ResourceContainer(
      message_types.VoidMessage,
      content_hash=messages.StringField(1, required=True),
  )

  class GetConfigByHashResponseMessage(messages.Message):
    # Can be SUCCESS and CONFIG_NOT_FOUND
    status = messages.EnumField(Status, 1, required=True)
    # If SUCCESS, the config content.
    content = messages.BytesField(2)

  @auth.endpoints_method(
      GET_CONFIG_BY_HASH_REQUEST_RESOURCE_CONTAINER,
      GetConfigByHashResponseMessage,
      path='config/{content_hash}')
  def get_config_by_hash(self, request):
    """Gets a config file by its hash."""
    res = self.GetConfigByHashResponseMessage(
        content=storage.get_config_by_hash(request.content_hash))
    res.status = (
        Status.SUCCESS if res.content is not None
        else Status.CONFIG_NOT_FOUND)
    return res
