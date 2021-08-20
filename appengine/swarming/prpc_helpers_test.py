#!/usr/bin/env vpython
# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Tests for SwarmingPRPCService."""
import sys
import unittest

import swarming_test_env
swarming_test_env.setup_test_env()

from components.prpc import codes
from components.prpc import context

import prpc_helpers
import handlers_exceptions

from test_support import test_case


class TestableService(prpc_helpers.SwarmingPRPCService):

  @prpc_helpers.PRPCMethod
  def SuccessHandler(self, request, _ctx):
    return request

  @prpc_helpers.PRPCMethod
  def BadHandler(self, _request, _ctx):
    raise handlers_exceptions.BadRequestException(
        'Bad request: no `chicken` found')

  @prpc_helpers.PRPCMethod
  def UnrecognizedErrorHandler(self, _request, _ctx):
    raise ValueError('whoops')


class SwarmingPRPCServiceTest(test_case.TestCase):

  def setUp(self):
    super(SwarmingPRPCServiceTest, self).setUp()
    self.service = TestableService()

  def test_run(self):
    self.assertEqual('fake_req',
                     self.service.SuccessHandler('fake_req', 'fake_ctx'))

  def test_run_recognized(self):
    ctx = context.ServicerContext()
    self.service.BadHandler('fake_req', ctx)

    self.assertEqual(codes.StatusCode.INVALID_ARGUMENT, ctx._code)
    self.assertEqual('Bad request: no `chicken` found', ctx._details)

  def test_run_unrecognized(self):
    ctx = context.ServicerContext()
    with self.assertRaisesRegexp(ValueError, 'whoops'):
      self.service.UnrecognizedErrorHandler('fake_req', ctx)

    self.assertEqual(codes.StatusCode.INTERNAL, ctx._code)
    self.assertEqual('Potential programming error.', ctx._details)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
