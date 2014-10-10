# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Pylint doesn't like endpoints.
# pylint: disable=C0322,R0201

import endpoints
import os
import sys

from protorpc import message_types
from protorpc import messages
from protorpc import remote

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'components', 'third_party'))

from components import auth


package = 'testing_api'


class WhoResponse(messages.Message):
  identity = messages.StringField(1)


@auth.endpoints_api(name='testing_service', version='v1')
class TestingServiceApi(remote.Service):
  @auth.endpoints_method(
      message_types.VoidMessage,
      WhoResponse,
      name='who',
      http_method='GET')
  def who(self, _request):
    return WhoResponse(identity=auth.get_current_identity().to_bytes())

  @auth.endpoints_method(
      message_types.VoidMessage,
      message_types.VoidMessage,
      name='forbidden',
      http_method='GET')
  @auth.require(lambda: False)
  def forbidden(self, _request):
    pass


app = endpoints.api_server([TestingServiceApi])
