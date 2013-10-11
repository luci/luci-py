#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import os
import sys
import unittest

import test_env

test_env.setup_test_env()

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.ext import testbed
# pylint: enable=E0611,F0401

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import ereporter2
from third_party import auto_stub


class Ereporter2Test(auto_stub.TestCase):
  def setUp(self):
    super(Ereporter2Test, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()
    super(Ereporter2Test, self).tearDown()

  @staticmethod
  def _ErrorRecord(**kwargs):
    default_values = {
        'request_id': 'a',
        'start_time': None,
        'exception_time': None,
        'latency': 0,
        'mcycles': 0,
        'ip': '0.0.1.0',
        'nickname': None,
        'referrer': None,
        'user_agent': 'Comodore64',
        'host': 'localhost',
        'resource': '/foo',
        'method': 'GET',
        'task_queue_name': None,
        'was_loading_request': False,
        'version': 'v1',
        'module': 'default',
        'handler_module': 'main.app',
        'gae_version': '1.9.0',
        'instance': '123',
        'status': 200,
        'message': 'Failed',
    }
    default_values.update(kwargs)
    return ereporter2.ErrorRecord(**default_values)

  def test_render(self):
    data = [
      self._ErrorRecord(),
    ]
    emailed = []
    def send_email(*args):
      emailed.append(args)
      return True
    self.mock(ereporter2, '_extract_exceptions_from_logs', lambda *_: data)
    self.mock(ereporter2, 'email_html', send_email)
    result = ereporter2.generate_and_email_report(
        'joe@example.com', None, 'http://foo/bar/')
    self.assertEqual(
        'Processed 1 items, ignored 0, sent to joe@example.com:\nFailed@v1',
        result)
    expected = [
      (
        'joe@example.com',
        'Exceptions on "isolateserver-dev"',
        '<html><head><title>Exceptions on "isolateserver-dev".</title></head>\n'
        '<body><h3>1 occurrences of 1 errors across 1 versions.</h3>\n'
        '\n'
        '<span style="font-size:130%">Failed@v1</span><br>\n'
        'main.app<br>\n'
        'GET localhost/foo (HTTP 200)<br>\n'
        '<pre>Failed</pre>\n'
        '1 occurrences: <a href="http://foo/bar/a">Entry</a> <p>\n'
        '<br>\n'
        u'</body>'
      ),
    ]
    self.assertEqual(expected, emailed)

  def test_ignored(self):
    messages = [
      (
        '\nTraceback (most recent call last):\n'
        '  File \"appengine/runtime/wsgi.py\", line 239, in Handle\n'
        '    handler = _config_handle.add_wsgi_middleware(self._LoadHandler())'
            '\n'
        '  File \"appengine/ext/ndb/utils.py\", line 28, in wrapping\n'
        '    def wrapping_wrapper(wrapper):\n'
        'DeadlineExceededError'),
      (
        '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
        'appengine/_internal/django/template/__init__.py:729: UserWarning: '
        'api_milliseconds does not return a meaningful value\n'
        '  current = current()'),
    ]
    for message in messages:
      report = self._ErrorRecord(message=message)
      self.assertEqual(None, report.get_signature())

  def test_exception_frame_skipped(self):
    message = (
      '\'error\' is undefined\n'
      'Traceback (most recent call last):\n'
      '  File \"third_party/webapp2-2.5/webapp2.py\", line 1535, in __call__\n'
      '    rv = self.handle_exception(request, response, e)\n'
      '  File \"tp/jinja2-2.6/jinja2/environment.py\", line 894, in render\n'
      '    return self.environment.handle_exception(exc_info, True)\n'
      '  File \"<template>\", line 6, in top-level template code\n'
      '  File \"tp/jinja2-2.6/jinja2/environment.py\", line 372, in getattr\n'
      '    return getattr(obj, attribute)\n'
      'UndefinedError: \'error\' is undefined')
    report = self._ErrorRecord(message=message)
    self.assertEqual(
        'UndefinedError@environment.py:372@v1',
        report.get_signature())

  def test_exception_syntaxerror(self):
    message = (
      '\nTraceback (most recent call last):\n'
      '  File \"ereporter2.py\", line 74\n'
      '    class ErrorReportingInfo(ndb.Model):\n'
      '        ^\n'
      'SyntaxError: invalid syntax')
    report = self._ErrorRecord(message=message)
    self.assertEqual('SyntaxError@ereporter2.py:74@v1', report.get_signature())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
