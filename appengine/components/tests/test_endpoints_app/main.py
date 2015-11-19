# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Pylint doesn't like endpoints.
# pylint: disable=C0322,R0201

import endpoints
from protorpc import message_types
from protorpc import messages
from protorpc import remote

from components import auth


package = 'testing_api'


class WhoResponse(messages.Message):
  identity = messages.StringField(1)
  ip = messages.StringField(2)
  host = messages.StringField(3)


class HostTokenRequest(messages.Message):
  host = messages.StringField(1)


class HostTokenRespones(messages.Message):
  host_token = messages.StringField(1)


@auth.endpoints_api(name='testing_service', version='v1')
class TestingServiceApi(remote.Service):
  @auth.endpoints_method(
      message_types.VoidMessage,
      WhoResponse,
      name='who',
      http_method='GET')
  @auth.public
  def who(self, _request):
    return WhoResponse(
        host=auth.get_peer_host(),
        identity=auth.get_current_identity().to_bytes(),
        ip=auth.ip_to_string(auth.get_peer_ip()))

  @auth.endpoints_method(
      message_types.VoidMessage,
      message_types.VoidMessage,
      name='forbidden',
      http_method='GET')
  @auth.require(lambda: False)
  def forbidden(self, _request):
    pass

  @auth.endpoints_method(
      HostTokenRequest,
      HostTokenRespones,
      name='create_host_token',
      http_method='POST')
  @auth.public
  def create_host_token(self, request):
    return HostTokenRespones(host_token=auth.create_host_token(request.host))


app = endpoints.api_server([TestingServiceApi])
