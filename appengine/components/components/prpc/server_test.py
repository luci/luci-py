#!/usr/bin/env vpython
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import httplib
import json
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

import webapp2
import webtest
import flask

from test_support import test_case

from google.protobuf import empty_pb2

from components.prpc import encoding
from components.prpc import flask_server
from components.prpc import webapp2_server
from components.prpc import server_base
from components.prpc.test import test_pb2
from components.prpc.test import test_prpc_pb2


class TestServicer(object):
  """TestServicer implements the Test service in test.proto."""

  DESCRIPTION = test_prpc_pb2.TestServiceDescription

  def __init__(self):
    self.given = None
    self.echoed = None
    self.give_callback = None

  def Give(self, request, context):
    if self.give_callback:
      self.give_callback(request, context)
    self.given = request.m
    return empty_pb2.Empty()

  def Take(self, _request, _context):
    return test_pb2.TakeResponse(k=self.given)

  def Echo(self, request, _context):
    self.echoed = request
    return test_pb2.EchoResponse(response=['hello!', str(request.r.m)])


class BadTestServicer(object):
  """BadTestServicer implements the Test service in test.proto, but poorly."""

  DESCRIPTION = test_prpc_pb2.TestServiceDescription

  def Give(self, _request, _context):
    return 5

  def Take(self, _request, _context):
    raise Exception("Look at me, I'm bad.")

  def Echo(self, request, _context):
    return None  # no respose and no status code


class PRPCServerTestCaseBase(test_case.TestCase):
  def setUp(self):
    super(PRPCServerTestCaseBase, self).setUp()
    self.allowed_origins = ['allowed.com', 'allowed-2.com']
    self.service = TestServicer()
    self.bad_app = None
    self.explicit_origins_app = None
    self.no_cors_app = None

  def make_headers(self, enc):
    return {
        'Content-Type': enc[1],
        'Accept': enc[1],
    }

  def check_headers(self, headers, prpc_code, origin=None):
    if origin is not None:
      self.assertEqual(headers['Access-Control-Allow-Origin'], origin)
      self.assertEqual(headers['Vary'], 'Origin')
      self.assertEqual(headers['Access-Control-Allow-Credentials'], 'true')
    self.assertEqual(headers['X-Content-Type-Options'], 'nosniff')
    self.assertEqual(headers['X-Prpc-Grpc-Code'], str(prpc_code.value))
    self.assertEqual(
        headers['Access-Control-Expose-Headers'],
        ('X-Prpc-Grpc-Code'),
    )

  def get_post_response(self,
                        app,
                        url,
                        data=None,
                        headers='',
                        expect_errors=False):
    raise NotImplementedError()

  def get_options_response(self, app, url, headers=''):
    raise NotImplementedError()

  def get_response_body(self, response):
    raise NotImplementedError()

  def get_response_status(self, response):
    raise NotImplementedError()

  # when adding new test case please add the function as check_XXX and add it
  # check_all_test()

  def check_echo(self, enc):
    headers = self.make_headers(enc)
    headers['Origin'] = 'example.com'
    encoder = encoding.get_encoder(enc)
    req = test_pb2.EchoRequest()
    req.r.m = 94049
    encoded_req = encoder(req)
    if enc == encoding.Encoding.JSON:
      encoded_req = encoded_req[4:]
    http_resp = self.get_post_response(
        self.app,
        '/prpc/test.Test/Echo',
        encoded_req,
        headers,
    )
    self.check_headers(
        http_resp.headers,
        server_base.StatusCode.OK,
        origin='example.com',
    )

    self.assertEqual(self.get_response_status(http_resp), httplib.OK)
    raw_resp = self.get_response_body(http_resp)
    resp = test_pb2.EchoResponse()
    decoder = encoding.get_decoder(enc)
    if enc == encoding.Encoding.JSON:
      raw_resp = raw_resp[4:]
    decoder(raw_resp, resp)

    self.assertEqual(len(resp.response), 2)
    self.assertEqual(resp.response[0], 'hello!')
    self.assertEqual(resp.response[1], '94049')

  def check_context(self):
    calls = []
    def rpc_callback(_request, context):
      calls.append({
        'peer': context.peer(),
        'is_active': context.is_active(),
        'time_remaining': context.time_remaining(),
      })
    self.service.give_callback = rpc_callback

    headers = self.make_headers(encoding.Encoding.BINARY)
    req = test_pb2.GiveRequest(m=3333)
    raw_resp = self.get_post_response(
        self.app,
        '/prpc/test.Test/Give',
        req.SerializeToString(),
        headers,
    )

    raw_resp_body = self.get_response_body(raw_resp)
    self.assertEqual(len(raw_resp_body), 0)

    self.assertEqual(calls, [
        {
            'is_active': True,
            'peer': 'ipv4:127.0.0.1',
            'time_remaining': None,
        },
    ])

  def check_servicer_persistence(self):
    """Basic test which ensures the servicer state persists."""

    headers = self.make_headers(encoding.Encoding.BINARY)
    req = test_pb2.GiveRequest(m=3333)
    resp = self.get_post_response(self.app, '/prpc/test.Test/Give',
                                  req.SerializeToString(), headers)
    raw_resp = self.get_response_body(resp)
    self.assertEqual(len(raw_resp), 0)

    req = empty_pb2.Empty()
    resp = self.get_post_response(
        self.app,
        '/prpc/test.Test/Take',
        req.SerializeToString(),
        headers,
    )
    raw_resp = self.get_response_body(resp)
    resp = test_pb2.TakeResponse()
    test_pb2.TakeResponse.ParseFromString(resp, raw_resp)
    self.assertEqual(resp.k, 3333)

  def check_echo_encodings(self):
    """Basic test which checks Echo service works with different encodings."""

    self.check_echo(encoding.Encoding.BINARY)
    self.check_echo(encoding.Encoding.JSON)
    self.check_echo(encoding.Encoding.TEXT)

  def check_with_bad_headers(self):
    """Make sure the server gives a reasonable response for bad headers."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.get_post_response(
        self.app,
        '/prpc/test.Test/Give',
        data=req.SerializeToString(),
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        expect_errors=True,
    )
    self.assertEqual(self.get_response_status(resp), httplib.BAD_REQUEST)
    self.check_headers(resp.headers, server_base.StatusCode.INVALID_ARGUMENT)

  def check_bad_service(self):
    """Make sure the server handles an unknown service."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.get_post_response(
        self.app,
        '/prpc/IDontExist/Give',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(self.get_response_status(resp), httplib.NOT_IMPLEMENTED)
    self.check_headers(resp.headers, server_base.StatusCode.UNIMPLEMENTED)

  def check_bad_method(self):
    """Make sure the server handles an unknown method."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.get_post_response(
        self.app,
        '/prpc/test.Test/IDontExist',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(self.get_response_status(resp), httplib.NOT_IMPLEMENTED)
    self.check_headers(resp.headers, server_base.StatusCode.UNIMPLEMENTED)

  def check_bad_app(self):
    """Make sure the server handles a bad servicer implementation."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.get_post_response(
        self.bad_app,
        '/prpc/test.Test/Give',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(self.get_response_status(resp),
                     httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server_base.StatusCode.INTERNAL)

    req = empty_pb2.Empty()
    resp = self.get_post_response(
        self.bad_app,
        '/prpc/test.Test/Take',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(self.get_response_status(resp),
                     httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server_base.StatusCode.INTERNAL)

    req = test_pb2.EchoRequest()
    resp = self.get_post_response(
        self.bad_app,
        '/prpc/test.Test/Echo',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(self.get_response_status(resp),
                     httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server_base.StatusCode.INTERNAL)

  def check_bad_request(self):
    """Make sure the server handles a malformed request."""

    resp = self.get_post_response(
        self.app,
        '/prpc/test.Test/Give',
        'asdfjasdhlkiqwuebweo',
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(self.get_response_status(resp), httplib.BAD_REQUEST)
    self.check_headers(resp.headers, server_base.StatusCode.INVALID_ARGUMENT)

  def check_options_no_cors(self):
    """Make sure the server can reject CORs."""
    origin = 'AnotherSite.com'
    options_headers = self.make_headers(encoding.Encoding.BINARY)
    options_headers['origin'] = origin

    no_cors_resp = self.get_options_response(self.no_cors_app,
                                             '/prpc/test.Test/Give',
                                             headers=options_headers)
    no_cors_headers = no_cors_resp.headers
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Origin'))
    self.assertIsNone(no_cors_headers.get('Very'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Credentials'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Headers'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Methods'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Max-Age'))

  def check_options_allowed_origins(self):
    """Make sure the server only allows allowlisted origins is specified."""
    # Check we allow origins found in server.allowed_origins
    allowed_origin = self.allowed_origins[0]
    options_headers = self.make_headers(encoding.Encoding.BINARY)
    options_headers['origin'] = allowed_origin

    allowed_resp = self.get_options_response(self.explicit_origins_app,
                                             '/prpc/test.Test/Give',
                                             headers=options_headers)
    allowed_headers = allowed_resp.headers
    self.assertEqual(allowed_headers['Access-Control-Allow-Origin'],
                     allowed_origin)
    self.assertEqual(allowed_headers['Vary'], 'Origin')
    self.assertEqual(allowed_headers['Access-Control-Allow-Credentials'],
                     'true')
    self.assertEqual(allowed_headers['Access-Control-Allow-Headers'],
                     'Origin, Content-Type, Accept, Authorization')
    self.assertEqual(allowed_headers['Access-Control-Allow-Methods'],
                     'OPTIONS, POST')
    self.assertEqual(allowed_headers['Access-Control-Max-Age'], '600')

    # Check we block origins not found in server.allowed_origins if
    # server.allowed_origins is not empty.
    options_headers = self.make_headers(encoding.Encoding.BINARY)
    options_headers['origin'] = 'NotOkaySite.com'

    blocked_resp = self.get_options_response(self.explicit_origins_app,
                                             '/prpc/test.Test/Give',
                                             headers=options_headers)
    blocked_headers = blocked_resp.headers
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Origin'))
    self.assertIsNone(blocked_headers.get('Very'))
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Credentials'))
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Headers'))
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Methods'))
    self.assertIsNone(blocked_headers.get('Access-Control-Max-Age'))

  def check_options_allow_cors(self):
    """Make sure the server can allow CORs."""
    origin = 'AnotherSite.com'
    options_headers = self.make_headers(encoding.Encoding.BINARY)
    options_headers['origin'] = origin

    cors_resp = self.get_options_response(self.app,
                                          '/prpc/test.Test/Give',
                                          headers=options_headers)
    cors_headers = cors_resp.headers
    self.assertEqual(cors_headers['Access-Control-Allow-Origin'], origin)
    self.assertEqual(cors_headers['Vary'], 'Origin')
    self.assertEqual(cors_headers['Access-Control-Allow-Credentials'], 'true')
    self.assertEqual(cors_headers['Access-Control-Allow-Headers'],
                     'Origin, Content-Type, Accept, Authorization')
    self.assertEqual(cors_headers['Access-Control-Allow-Methods'],
                     'OPTIONS, POST')
    self.assertEqual(cors_headers['Access-Control-Max-Age'], '600')

  def check_all_test(self):
    self.check_context()
    self.check_servicer_persistence()
    self.check_echo_encodings()
    self.check_with_bad_headers()
    self.check_bad_service()
    self.check_bad_app()
    self.check_bad_request()
    self.check_options_no_cors()
    self.check_options_allowed_origins()
    self.check_options_allow_cors()


class PRPCWebapp2ServerTestCase(PRPCServerTestCaseBase):
  def setUp(self):
    super(PRPCWebapp2ServerTestCase, self).setUp()

    s = webapp2_server.Webapp2Server()
    s.add_service(self.service)
    real_app = webapp2.WSGIApplication(s.get_routes(), debug=True)
    self.app = webtest.TestApp(
        real_app,
        extra_environ={'REMOTE_ADDR': '127.0.0.1'},
    )

    bad_s = webapp2_server.Webapp2Server()
    bad_s.add_service(BadTestServicer())
    real_bad_app = webapp2.WSGIApplication(bad_s.get_routes(), debug=True)
    self.bad_app = webtest.TestApp(
        real_bad_app,
        extra_environ={'REMOTE_ADDR': '192.192.192.192'},
    )

    explicit_origins_s = webapp2_server.Webapp2Server(
        allowed_origins=self.allowed_origins)
    explicit_origins_s.add_service(self.service)
    real_explicit_origins_app = webapp2.WSGIApplication(
        explicit_origins_s.get_routes(), debug=True)
    self.explicit_origins_app = webtest.TestApp(
        real_explicit_origins_app,
        extra_environ={'REMOTE_ADDR': '::ffff:127.0.0.1'},
    )

    no_cors_s = webapp2_server.Webapp2Server(allow_cors=False)
    no_cors_s.add_service(self.service)
    real_no_cors_app = webapp2.WSGIApplication(no_cors_s.get_routes(),
                                               debug=True)
    self.no_cors_app = webtest.TestApp(
        real_no_cors_app,
        extra_environ={'REMOTE_ADDR': '::ffff:127.0.0.1'},
    )

  def get_post_response(self,
                        app,
                        url,
                        data=None,
                        headers='',
                        expect_errors=False):
    return app.post(url, data, headers, expect_errors=expect_errors)

  def get_options_response(self, app, url, headers=''):
    return app.options(url, headers)

  def get_response_body(self, response):
    return response.body

  def get_response_status(self, response):
    return response.status_int

  def test_webapp2_server(self):
    self.check_all_test()


# TODO: add a FlaskInterceptorsTest
class InterceptorsTestCase(test_case.TestCase):
  def make_test_server_app(self, servicer, interceptors):
    s = webapp2_server.Webapp2Server()
    s.add_service(servicer)
    for interceptor in interceptors:
      s.add_interceptor(interceptor)
    app = webapp2.WSGIApplication(s.get_routes(), debug=True)
    return webtest.TestApp(app, extra_environ={'REMOTE_ADDR': 'fake-ip'})

  def call_echo(self, app, m, headers=None, return_raw_resp=False):
    headers = dict(headers or {})
    headers.update({
      'Content-Type': encoding.Encoding.JSON[1],
      'Accept': encoding.Encoding.JSON[1],
    })
    raw_resp = app.post(
        '/prpc/test.Test/Echo',
        json.dumps({'r': {'m': m}}),
        headers,
        expect_errors=True)
    if return_raw_resp:
      return raw_resp
    return json.loads(raw_resp.body[4:])

  def test_no_interceptors(self):
    s = TestServicer()
    app = self.make_test_server_app(s, [])
    resp = self.call_echo(app, 123)
    self.assertEqual(resp, {u'response': [u'hello!', u'123']}, )
    self.assertEqual(s.echoed.r.m, 123)

  def test_single_noop_interceptor(self):
    calls = []

    def interceptor(request, context, details, cont):
      calls.append((request, details))
      return cont(request, context, details)

    s = TestServicer()
    app = self.make_test_server_app(s, [interceptor])
    resp = self.call_echo(app, 123, headers={'Authorization': 'x'})
    self.assertEqual(resp, {u'response': [u'hello!', u'123']}, )
    self.assertEqual(s.echoed.r.m, 123)

    # Interceptor called and saw relevant metadata.
    self.assertEqual(len(calls), 1)
    req, details = calls[0]

    self.assertEqual(req, test_pb2.EchoRequest(r=test_pb2.GiveRequest(m=123)))
    self.assertEqual(details.method, 'test.Test.Echo')
    self.assertEqual(dict(details.invocation_metadata)['authorization'], 'x')

  def test_interceptor_replies(self):
    def interceptor(request, context, details, cont):
      return test_pb2.EchoResponse(response=['intercepted!', str(request.r.m)])

    s = TestServicer()
    app = self.make_test_server_app(s, [interceptor])
    resp = self.call_echo(app, 123)
    self.assertEqual(resp, {u'response': [u'intercepted!', u'123']}, )
    self.assertIsNone(s.echoed)

  def test_interceptor_chain(self):
    calls = []

    def make(name):
      def interceptor(request, context, details, cont):
        calls.append(name)
        return cont(request, context, details)
      return interceptor

    s = TestServicer()
    app = self.make_test_server_app(s, [make(1), make(2), make(3)])
    resp = self.call_echo(app, 123)
    self.assertEqual(resp, {u'response': [u'hello!', u'123']}, )
    self.assertEqual(s.echoed.r.m, 123)

    # Interceptors are called in correct order.
    self.assertEqual(calls, [1, 2, 3])

  def test_interceptor_exceptions(self):
    class Error(Exception):
      pass

    def outter(request, context, details, cont):
      try:
        return cont(request, context, details)
      except Error as exc:
        context.set_code(server_base.StatusCode.PERMISSION_DENIED)
        context.set_details(exc.message)

    def inner(request, context, details, cont):
      raise Error('FAIL')

    s = TestServicer()
    app = self.make_test_server_app(s, [outter, inner])
    resp = self.call_echo(app, 123, return_raw_resp=True)
    self.assertEqual(resp.status_int, 403)
    self.assertTrue('FAIL' in resp.body)


class PRPCFlaskServerTestCase(PRPCServerTestCaseBase):
  def setUp(self):
    super(PRPCFlaskServerTestCase, self).setUp()

    s = flask_server.FlaskServer()
    s.add_service(self.service)
    routes = s.get_routes()
    self.app = flask.Flask('test_app')
    self.app.config['TESTING'] = True
    for route in routes:
      self.app.add_url_rule(route[0], view_func=route[1], methods=route[2])

    bad_s = flask_server.FlaskServer()
    bad_s.add_service(BadTestServicer())
    bad_s_routes = bad_s.get_routes()
    self.bad_app = flask.Flask('test_bad_app')
    self.bad_app.config['TESTING'] = True
    for route in bad_s_routes:
      self.bad_app.add_url_rule(route[0], view_func=route[1], methods=route[2])

    explicit_origins_s = flask_server.FlaskServer(
        allowed_origins=self.allowed_origins)
    explicit_origins_s.add_service(self.service)
    explicit_origins_s_routes = explicit_origins_s.get_routes()
    self.explicit_origins_app = flask.Flask('test_explicit_origins_app')
    self.explicit_origins_app.config['TESTING'] = True
    for route in explicit_origins_s_routes:
      self.explicit_origins_app.add_url_rule(route[0],
                                             view_func=route[1],
                                             methods=route[2])

    no_cors_s = flask_server.FlaskServer(allow_cors=False)
    no_cors_s.add_service(self.service)
    no_cors_s_routes = no_cors_s.get_routes()
    self.no_cors_app = flask.Flask('test_no_cors_app')
    self.no_cors_app.config['TESTING'] = True
    for route in no_cors_s_routes:
      self.no_cors_app.add_url_rule(route[0],
                                    view_func=route[1],
                                    methods=route[2])

  def get_options_response(self, app, url, headers=''):
    return app.test_client().options(url, headers=headers)

  def get_post_response(self,
                        app,
                        url,
                        data=None,
                        headers='',
                        expect_errors=False):
    return app.test_client().post(
        url,
        data=data,
        headers=headers,
    )

  def get_response_body(self, response):
    return response.data

  def get_response_status(self, response):
    return response.status_code

  def test_flask_server(self):
    self.check_all_test()


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
