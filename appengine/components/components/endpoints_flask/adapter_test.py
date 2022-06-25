# #!/usr/bin/env vpython
# # Copyright 2016 The LUCI Authors. All rights reserved.
# # Use of this source code is governed under the Apache License, Version 2.0
# # that can be found in the LICENSE file.

import json
import sys
import unittest

# from test_support import test_env
# test_env.setup_test_env()

from protorpc import messages
from protorpc import remote
import endpoints
import flask
import mock

from test_support import test_case
from werkzeug import datastructures
import adapter


class Msg(messages.Message):
  s = messages.StringField(1)
  s2 = messages.StringField(2)
  r = messages.StringField(4, repeated=True)


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


class EndpointsFlaskTestCase(test_case.TestCase):
  def test_decode_message_post(self):
    request = mock.MagicMock()
    request.data = json.dumps({"s": "a"})
    request.method = 'POST'
    msg = adapter.decode_message(EndpointsService.post.remote, request)
    self.assertEqual(msg.s, "a")
    self.assertEqual(msg.s2, None)  # because it is not a ResourceContainer.

  def test_decode_message_get(self):
    request = mock.MagicMock()
    request.data = json.dumps({})
    request.args = datastructures.MultiDict([("s", "a")])
    request.form = datastructures.MultiDict([("s2", "b"), ("r", "x"),
                                             ("r", "y")])
    request.values = datastructures.CombinedMultiDict(
        [request.args, request.form])
    request.method = 'GET'
    msg = adapter.decode_message(EndpointsService.get.remote, request)
    self.assertEqual(msg.s, 'a')
    self.assertEqual(msg.s2, 'b')
    self.assertEqual(msg.r, ['x', 'y'])

  def test_decode_message_get_resource_container(self):
    request = mock.MagicMock()
    request.data = json.dumps({})
    request.args = datastructures.MultiDict([("s", "a")])
    request.form = datastructures.MultiDict([("s2", "b"), ("x", "c")])
    request.values = datastructures.CombinedMultiDict(
        [request.args, request.form])
    request.method = 'GET'
    rc = adapter.decode_message(EndpointsService.get_container.remote, request)
    self.assertEqual(rc.s, 'a')
    self.assertEqual(rc.s2, 'b')
    self.assertEqual(rc.x, 'c')

  # def test_handle_403(self):
  #   app = adapter.api_server([EndpointsService], base_path='/_ah/api')
  #   with app.test_client() as client:
  #     response = client.post('/_ah/api/Service/v1/post_403')
  #   self.assertEqual(response.status, 403)
  #   self.assertEqual(json.loads(response.body), {
  #       'error': {
  #           'message': 'access denied',
  #       },
  #   })

  def test_api_routes(self):
    routes = sorted([
        route for route, _, _, _ in adapter.api_routes([EndpointsService],
                                                       base_path='/_ah/api')
    ])
    self.assertEqual(
        routes,
        [
            # Flask URLs must be unique
            '/_ah/api/Service/v1/get',
            '/_ah/api/Service/v1/get',
            '/_ah/api/Service/v1/get_container',
            '/_ah/api/Service/v1/get_container',
            '/_ah/api/Service/v1/post',
            '/_ah/api/Service/v1/post',
            '/_ah/api/Service/v1/post_403',
            '/_ah/api/Service/v1/post_403',
            '/_ah/api/discovery/v1/apis',
            '/_ah/api/discovery/v1/apis/<name>/<version>/rest',
            '/_ah/api/explorer',
            '/_ah/api/static/proxy.html',
        ])

  # def test_discovery_routing(self):
  #   app = adapter.api_server([EndpointsService], base_path='/_ah/api')
  #   with app.test_client() as client:
  #   request = webapp2.Request.blank('/api/discovery/v1/apis/Service/v1/rest')
  #   request.method = 'GET'
  #   response = json.loads(request.get_response(app).body)
  #   self.assertEqual(response['id'], 'Service:v1')


#   def test_directory_routing(self):
#     app = webapp2.WSGIApplication(
#         [adapter.directory_service_route([EndpointsService], '/api')],
#         debug=True)
#     request = webapp2.Request.blank('/api/discovery/v1/apis')
#     request.method = 'GET'
#     response = json.loads(request.get_response(app).body)
#     self.assertEqual(len(response.get('items', [])), 1)
#     self.assertEqual(response['items'][0]['id'], 'Service:v1')

#   def test_proxy_routing(self):
#     app = webapp2.WSGIApplication([adapter.explorer_proxy_route('/api')],
#                                   debug=True)
#     request = webapp2.Request.blank('/api/static/proxy.html')
#     request.method = 'GET'
#     response = request.get_response(app).body
#     self.assertIn('/api', response)

#   def test_redirect_routing(self):
#     app = webapp2.WSGIApplication([adapter.explorer_redirect_route('/api')],
#                                   debug=True)
#     request = webapp2.Request.blank('/api/explorer')
#     request.method = 'GET'
#     response = request.get_response(app)
#     self.assertEqual(response.status, '302 Moved Temporarily')

if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
