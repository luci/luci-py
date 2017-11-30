#!/usr/bin/env python
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import httplib
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


  def Give(self, request, _context):
    self.given = request.m
    return empty_pb2.Empty()


  def Take(self, _request, _context):
    return test_pb2.TakeResponse(k=self.given)


  def Echo(self, request, _context):
    return test_pb2.EchoResponse(response=['hello!', str(request.r.m)])


class BadTestServicer(object):
  """BadTestServicer implements the Test service in test.proto, but poorly."""

  DESCRIPTION = test_prpc_pb2.TestServiceDescription


  def Give(self, _request, _context):
    return 5


  def Take(self, _request, _context):
    raise Exception("Look at me, I'm bad.")


  def Echo(self, request, _context):
    return request


class PRPCServerTestCase(test_case.TestCase):
  def setUp(self):
    super(PRPCServerTestCase, self).setUp()
    s = server.Server()
    s.add_service(TestServicer())
    real_app = webapp2.WSGIApplication(s.get_routes(), debug=True)
    self.app = webtest.TestApp(
        real_app,
        extra_environ={'REMOTE_ADDR': 'fake-ip'},
    )
    bad_s = server.Server()
    bad_s.add_service(BadTestServicer())
    real_bad_app = webapp2.WSGIApplication(bad_s.get_routes(), debug=True)
    self.bad_app = webtest.TestApp(
        real_bad_app,
        extra_environ={'REMOTE_ADDR': 'fake-ip-2'},
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
    self.assertEqual(headers['X-Prpc-Grpc-Code'], str(prpc_code[0]))
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
        '/prpc/Test/Echo',
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

  def test_servicer_persistence(self):
    """Basic test which ensures the servicer state persists."""

    headers = self.make_headers(encoding.Encoding.BINARY)
    req = test_pb2.GiveRequest(m=3333)
    raw_resp = self.app.post(
        '/prpc/Test/Give',
        req.SerializeToString(),
        headers,
    ).body
    self.assertEqual(len(raw_resp), 0)

    req = empty_pb2.Empty()
    raw_resp = self.app.post(
        '/prpc/Test/Take',
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
        '/prpc/Test/Give',
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
        '/prpc/Test/IDontExist',
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
        '/prpc/Test/Give',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server.StatusCode.INTERNAL)

    req = empty_pb2.Empty()
    resp = self.bad_app.post(
        '/prpc/Test/Take',
        req.SerializeToString(),
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.INTERNAL_SERVER_ERROR)
    self.check_headers(resp.headers, server.StatusCode.INTERNAL)

  def test_bad_request(self):
    """Make sure the server handles a malformed request."""

    resp = self.app.post(
        '/prpc/Test/Give',
        'asdfjasdhlkiqwuebweo',
        self.make_headers(encoding.Encoding.BINARY),
        expect_errors=True,
    )
    self.assertEqual(resp.status_int, httplib.BAD_REQUEST)
    self.check_headers(resp.headers, server.StatusCode.INVALID_ARGUMENT)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
