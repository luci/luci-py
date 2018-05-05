#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import json
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from protorpc import messages
from protorpc import remote
import endpoints
import webapp2

from test_support import test_case
import endpoints_webapp2


class Msg(messages.Message):
  s = messages.StringField(1)
  s2 = messages.StringField(2)


CONTAINER = endpoints.ResourceContainer(Msg, x=messages.StringField(3))


@endpoints.api('Service', 'v1')
class EndpointsService(remote.Service):

  @endpoints.method(Msg, Msg)
  def post(self, _request):
    return Msg()

  @endpoints.method(Msg, Msg, http_method='GET')
  def get(self, _request):
    return Msg()

  @endpoints.method(CONTAINER, Msg, http_method='GET')
  def get_container(self, _request):
    return Msg()

  @endpoints.method(Msg, Msg)
  def post_403(self, _request):
    raise endpoints.ForbiddenException('access denied')


class EndpointsWebapp2TestCase(test_case.TestCase):
  def test_decode_message_post(self):
    request = webapp2.Request(
        {
          'QUERY_STRING': 's2=b',
        },
        method='POST',
        body='{"s": "a"}',
    )
    msg = endpoints_webapp2.decode_message(
        EndpointsService.post.remote, request)
    self.assertEqual(msg.s, 'a')
    self.assertEqual(msg.s2, None)  # because it is not a ResourceContainer.

  def test_decode_message_get(self):
    request = webapp2.Request(
        {
          'QUERY_STRING': 's=a',
        },
        method='GET',
        route_kwargs={'s2': 'b'},
    )
    msg = endpoints_webapp2.decode_message(EndpointsService.get.remote, request)
    self.assertEqual(msg.s, 'a')
    self.assertEqual(msg.s2, 'b')

  def test_decode_message_get_resource_container(self):
    request = webapp2.Request(
        {
          'QUERY_STRING': 's=a',
        },
        method='GET',
        route_kwargs={'s2': 'b', 'x': 'c'},
    )
    rc = endpoints_webapp2.decode_message(
        EndpointsService.get_container.remote, request)
    self.assertEqual(rc.s, 'a')
    self.assertEqual(rc.s2, 'b')
    self.assertEqual(rc.x, 'c')

  def test_handle_403(self):
    app = webapp2.WSGIApplication(
        endpoints_webapp2.api_routes(EndpointsService), debug=True)
    request = webapp2.Request.blank('/api/Service/v1/post_403')
    request.method = 'POST'
    response = request.get_response(app)
    self.assertEqual(response.status_int, 403)
    self.assertEqual(json.loads(response.body), {
      'error': {
        'message': 'access denied',
      },
    })

  def test_api_server_routes(self):
    routes = sorted([
        r.template for r in endpoints_webapp2.api_server([EndpointsService])])
    self.assertEqual(routes, [
        '/api/Service/v1/get',
        '/api/Service/v1/get',
        '/api/Service/v1/get_container',
        '/api/Service/v1/get_container',
        '/api/Service/v1/post',
        '/api/Service/v1/post',
        '/api/Service/v1/post_403',
        '/api/Service/v1/post_403',
        '/api/discovery/v1/apis',
        '/api/discovery/v1/apis/<name>/<version>/rest',
    ])

  def test_discovery_routing(self):
    app = webapp2.WSGIApplication(
        [endpoints_webapp2.discovery_service_route([EndpointsService], '/api')],
        debug=True)
    request = webapp2.Request.blank('/api/discovery/v1/apis/Service/v1/rest')
    request.method = 'GET'
    response = json.loads(request.get_response(app).body)
    self.assertEqual(response['id'], 'Service:v1')

  def test_directory_routing(self):
    app = webapp2.WSGIApplication(
        [endpoints_webapp2.directory_service_route([EndpointsService], '/api')],
        debug=True)
    request = webapp2.Request.blank('/api/discovery/v1/apis')
    request.method = 'GET'
    response = json.loads(request.get_response(app).body)
    self.assertEqual(len(response.get('items', [])), 1)
    self.assertEqual(response['items'][0]['id'], 'Service:v1')

if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
