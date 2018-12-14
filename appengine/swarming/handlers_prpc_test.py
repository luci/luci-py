#!/usr/bin/env python
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import os
import sys
import unittest

import test_env_handlers

import webapp2
import webtest

from google.appengine.ext import ndb

from components import utils
from components.prpc import encoding

from proto import swarming_pb2  # pylint: disable=no-name-in-module
from server import task_queues

import handlers_bot
import handlers_prpc


def _decode(raw, dst):
  # Skip escaping characters.
  assert raw[:5] == ')]}\'\n', raw[:5]
  return encoding.get_decoder(encoding.Encoding.JSON)(raw[5:], dst)


def _encode(d):
  # Skip escaping characters.
  raw = encoding.get_encoder(encoding.Encoding.JSON)(d)
  assert raw[:5] == ')]}\'\n', raw[:5]
  return raw[5:]


class PRPCTest(test_env_handlers.AppTestBase):
  """Tests the pRPC handlers."""
  def setUp(self):
    super(PRPCTest, self).setUp()
    # handlers_bot is necessary to run fake tasks.
    routes = handlers_prpc.get_routes() + handlers_bot.get_routes()
    self.app = webtest.TestApp(
        webapp2.WSGIApplication(routes, debug=True),
        extra_environ={
          'REMOTE_ADDR': self.source_ip,
          'SERVER_SOFTWARE': os.environ['SERVER_SOFTWARE'],
        },
    )
    self._headers = {
      'Content-Type': encoding.Encoding.JSON[1],
      'Accept': encoding.Encoding.JSON[1],
    }
    self._enqueue_task_orig = self.mock(
        utils, 'enqueue_task', self._enqueue_task)
    self.now = datetime.datetime(2010, 1, 2, 3, 4, 5)
    self.mock_now(self.now)
    self.mock_default_pool_acl([])

  @ndb.non_transactional
  def _enqueue_task(self, url, queue_name, **kwargs):
    if queue_name == 'rebuild-task-cache':
      # Call directly into it.
      self.assertEqual(True, task_queues.rebuild_task_cache(kwargs['payload']))
      return True
    if queue_name == 'pubsub':
      return True
    self.fail(url)
    return False

  def test_botevents(self):
    msg = swarming_pb2.BotEventsRequest()
    raw_resp = self.app.post(
        '/prpc/swarming.BotAPI/Events', _encode(msg), self._headers,
        expect_errors=True)
    self.assertEqual(raw_resp.status, '501 Not Implemented')
    self.assertEqual(raw_resp.body, ')]}\'\n{}')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
