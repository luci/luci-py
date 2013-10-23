#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import sys
import unittest

import test_env
test_env.setup_test_env()

# The app engine headers are located locally, so don't worry about not finding
# them.
# pylint: disable=E0611,F0401
from google.appengine.ext import testbed
# pylint: enable=E0611,F0401

import ereporter2
from depot_tools import auto_stub


class ErrorRecordStub(object):
  """Intentionally thin stub to test should_ignore_error_record()."""
  def __init__(self, message, exception_type):
    self.message = message
    self.exception_type = exception_type


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

  def test_signatures(self):
    messages = [
      (
        ('\nTraceback (most recent call last):\n'
        '  File \"appengine/runtime/wsgi.py\", line 239, in Handle\n'
        '    handler = _config_handle.add_wsgi_middleware(self._LoadHandler())'
            '\n'
        '  File \"appengine/ext/ndb/utils.py\", line 28, in wrapping\n'
        '    def wrapping_wrapper(wrapper):\n'
        'DeadlineExceededError'),
        'DeadlineExceededError@utils.py:28',
        'DeadlineExceededError',
        True
      ),
      (
        ('/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
        'appengine/_internal/django/template/__init__.py:729: UserWarning: '
        'api_milliseconds does not return a meaningful value\n'
        '  current = current()'),
        '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
            'appengine/_internal/django/template/__init__.py:729: UserWarning: '
            'api_milliseconds does not return a meaningful value',
        None,
        True,
      ),
      (
        ('\'error\' is undefined\n'
        'Traceback (most recent call last):\n'
        '  File \"tp/webapp2-2.5/webapp2.py\", line 1535, in __call__\n'
        '    rv = self.handle_exception(request, response, e)\n'
        '  File \"tp/jinja2-2.6/jinja2/environment.py\", line 894, in render\n'
        '    return self.environment.handle_exception(exc_info, True)\n'
        '  File \"<template>\", line 6, in top-level template code\n'
        '  File \"tp/jinja2-2.6/jinja2/environment.py\", line 372, in getattr\n'
        '    return getattr(obj, attribute)\n'
        'UndefinedError: \'error\' is undefined'),
        'UndefinedError@environment.py:372',
        'UndefinedError',
        False,
      ),
      (
        ('\nTraceback (most recent call last):\n'
        '  File \"ereporter2.py\", line 74\n'
        '    class ErrorReportingInfo(ndb.Model):\n'
        '        ^\n'
        'SyntaxError: invalid syntax'),
        'SyntaxError@ereporter2.py:74',
        'SyntaxError',
        False,
      ),
    ]

    for (message, expected_signature, excepted_exception,
         expected_ignored) in messages:
      signature, exception_type = ereporter2.signature_from_message(message)
      self.assertEqual(expected_signature, signature)
      self.assertEqual(excepted_exception, exception_type)
      result = ereporter2.should_ignore_error_record(
          ErrorRecordStub(message, exception_type))
      self.assertEqual(expected_ignored, result, message)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
