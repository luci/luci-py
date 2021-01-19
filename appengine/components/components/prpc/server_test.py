#!/usr/bin/env vpython
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# pylint: disable=unused-argument

import httplib
import json
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

import webapp2
import webtest

from test_support import test_case

from google.protobuf import empty_pb2

from components.prpc import encoding
from components.prpc import server
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


class PRPCServerTestCase(test_case.TestCase):
  def setUp(self):
    super(PRPCServerTestCase, self).setUp()
    s = server.Server()
    self.service = TestServicer()
    s.add_service(self.service)
    real_app = webapp2.WSGIApplication(s.get_routes(), debug=True)
    self.app = webtest.TestApp(
        real_app,
        extra_environ={'REMOTE_ADDR': '::ffff:127.0.0.1'},
    )
    bad_s = server.Server()
    bad_s.add_service(BadTestServicer())
    real_bad_app = webapp2.WSGIApplication(bad_s.get_routes(), debug=True)
    self.bad_app = webtest.TestApp(
        real_bad_app,
        extra_environ={'REMOTE_ADDR': '192.192.192.192'},
    )

    self.allowed_origins = ['allowed.com', 'allowed-2.com']

    explicit_origins_s = server.Server(allowed_origins=self.allowed_origins)
    explicit_origins_s.add_service(self.service)
    real_explicit_origins_app = webapp2.WSGIApplication(
        explicit_origins_s.get_routes(), debug=True)
    self.explicit_origins_app = webtest.TestApp(
        real_explicit_origins_app,
        extra_environ={'REMOTE_ADDR': '::ffff:127.0.0.1'},
    )

    no_cors_s = server.Server(allow_cors=False)
    no_cors_s.add_service(self.service)
    real_no_cors_app = webapp2.WSGIApplication(
        no_cors_s.get_routes(), debug=True)
    self.no_cors_app = webtest.TestApp(
        real_no_cors_app,
        extra_environ={'REMOTE_ADDR': '::ffff:127.0.0.1'},
    )

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

  def check_echo(self, enc):
    headers = self.make_headers(enc)
    headers['Origin'] = 'example.com'
    encoder = encoding.get_encoder(enc)
    req = test_pb2.EchoRequest()
    req.r.m = 94049
    encoded_req = encoder(req)
    if enc == encoding.Encoding.JSON:
      encoded_req = encoded_req[4:]
    http_resp = self.app.post(
        '/prpc/test.Test/Echo',
        encoded_req,
        headers,
    )
    self.check_headers(
        http_resp.headers,
        server.StatusCode.OK,
        origin='example.com',
    )
    self.assertEqual(http_resp.status_int, httplib.OK)
    raw_resp = http_resp.body
    resp = test_pb2.EchoResponse()
    decoder = encoding.get_decoder(enc)
    if enc == encoding.Encoding.JSON:
      raw_resp = raw_resp[4:]
    decoder(raw_resp, resp)

    self.assertEqual(len(resp.response), 2)
    self.assertEqual(resp.response[0], 'hello!')
    self.assertEqual(resp.response[1], '94049')

  def test_context(self):
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
    raw_resp = self.app.post(
        '/prpc/test.Test/Give',
        req.SerializeToString(),
        headers,
    ).body
    self.assertEqual(len(raw_resp), 0)

    self.assertEqual(calls, [
      {
        'is_active': True,
        'peer': 'ipv6:[::ffff:127.0.0.1]',
        'time_remaining': None,
      },
    ])

  def test_servicer_persistence(self):
    """Basic test which ensures the servicer state persists."""

    headers = self.make_headers(encoding.Encoding.BINARY)
    req = test_pb2.GiveRequest(m=3333)
    raw_resp = self.app.post(
        '/prpc/test.Test/Give',
        req.SerializeToString(),
        headers,
    ).body
    self.assertEqual(len(raw_resp), 0)

    req = empty_pb2.Empty()
    raw_resp = self.app.post(
        '/prpc/test.Test/Take',
        req.SerializeToString(),
        headers,
    ).body
    resp = test_pb2.TakeResponse()
    test_pb2.TakeResponse.ParseFromString(resp, raw_resp)
    self.assertEqual(resp.k, 3333)

  def test_echo_encodings(self):
    """Basic test which checks Echo service works with different encodings."""

    self.check_echo(encoding.Encoding.BINARY)
    self.check_echo(encoding.Encoding.JSON)
    self.check_echo(encoding.Encoding.TEXT)

  def test_bad_headers(self):
    """Make sure the server gives a reasonable response for bad headers."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.app.post(
        '/prpc/test.Test/Give',
        req.SerializeToString(),
        {},
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.BAD_REQUEST)
    self.check_headers(resp.headers, server.StatusCode.INVALID_ARGUMENT)

  def test_bad_service(self):
    """Make sure the server handles an unknown service."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.app.post(
        '/prpc/IDontExist/Give',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.NOT_IMPLEMENTED)
    self.check_headers(resp.headers, server.StatusCode.UNIMPLEMENTED)

  def test_bad_method(self):
    """Make sure the server handles an unknown method."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.app.post(
        '/prpc/test.Test/IDontExist',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.NOT_IMPLEMENTED)
    self.check_headers(resp.headers, server.StatusCode.UNIMPLEMENTED)

  def test_bad_app(self):
    """Make sure the server handles a bad servicer implementation."""

    req = test_pb2.GiveRequest(m=825800)
    resp = self.bad_app.post(
        '/prpc/test.Test/Give',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server.StatusCode.INTERNAL)

    req = empty_pb2.Empty()
    resp = self.bad_app.post(
        '/prpc/test.Test/Take',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server.StatusCode.INTERNAL)

    req = test_pb2.EchoRequest()
    resp = self.bad_app.post(
        '/prpc/test.Test/Echo',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server.StatusCode.INTERNAL)

  def test_bad_request(self):
    """Make sure the server handles a malformed request."""

    resp = self.app.post(
        '/prpc/test.Test/Give',
        'asdfjasdhlkiqwuebweo',
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.BAD_REQUEST)
    self.check_headers(resp.headers, server.StatusCode.INVALID_ARGUMENT)

  def test_options_no_cors(self):
    """Make sure the server can reject CORs."""
    origin = 'AnotherSite.com'
    options_headers = self.make_headers(encoding.Encoding.BINARY)
    options_headers['origin'] = origin

    no_cors_resp = self.no_cors_app.options('/prpc/test.Test/Give',
                                            options_headers)
    no_cors_headers = no_cors_resp.headers
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Origin'))
    self.assertIsNone(no_cors_headers.get('Very'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Credentials'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Headers'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Allow-Methods'))
    self.assertIsNone(no_cors_headers.get('Access-Control-Max-Age'))

  def test_options_allowed_origins(self):
    """Make sure the server only allows allowlisted origins is specified."""
    # Check we allow origins found in server.allowed_origins
    allowed_origin = self.allowed_origins[0]
    options_headers = self.make_headers(encoding.Encoding.BINARY)
    options_headers['origin'] = allowed_origin

    allowed_resp = self.explicit_origins_app.options('/prpc/test.Test/Give',
                                                     options_headers)
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

    blocked_resp = self.explicit_origins_app.options('/prpc/test.Test/Give',
                                                     options_headers)
    blocked_headers = blocked_resp.headers
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Origin'))
    self.assertIsNone(blocked_headers.get('Very'))
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Credentials'))
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Headers'))
    self.assertIsNone(blocked_headers.get('Access-Control-Allow-Methods'))
    self.assertIsNone(blocked_headers.get('Access-Control-Max-Age'))

  def test_options_allow_cors(self):
    """Make sure the server can allow CORs."""
    origin = 'AnotherSite.com'
    options_headers = self.make_headers(encoding.Encoding.BINARY)
    options_headers['origin'] = origin

    cors_resp = self.app.options('/prpc/test.Test/Give', options_headers)
    cors_headers = cors_resp.headers
    self.assertEqual(cors_headers['Access-Control-Allow-Origin'], origin)
    self.assertEqual(cors_headers['Vary'], 'Origin')
    self.assertEqual(cors_headers['Access-Control-Allow-Credentials'], 'true')
    self.assertEqual(cors_headers['Access-Control-Allow-Headers'],
                     'Origin, Content-Type, Accept, Authorization')
    self.assertEqual(cors_headers['Access-Control-Allow-Methods'],
                     'OPTIONS, POST')
    self.assertEqual(cors_headers['Access-Control-Max-Age'], '600')


class InterceptorsTestCase(test_case.TestCase):
  def make_test_server_app(self, servicer, interceptors):
    s = server.Server()
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
        context.set_code(server.StatusCode.PERMISSION_DENIED)
        context.set_details(exc.message)

    def inner(request, context, details, cont):
      raise Error('FAIL')

    s = TestServicer()
    app = self.make_test_server_app(s, [outter, inner])
    resp = self.call_echo(app, 123, return_raw_resp=True)
    self.assertEqual(resp.status_int, 403)
    self.assertTrue('FAIL' in resp.body)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
