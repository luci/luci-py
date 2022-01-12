#!/usr/bin/env vpython
# -*- coding: utf-8 -*-
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

from __future__ import print_function

import logging
import os
import StringIO
import sys
import traceback

# Mutates sys.path.
import test_env

# third_party/
from depot_tools import auto_stub

import net_utils

import auth
import swarming
from utils import logging_utils


def main(args):
  """Bypasses swarming.main()'s exception handling.

  It gets in the way when debugging test failures.
  """
  dispatcher = swarming.subcommand.CommandDispatcher('swarming')
  return dispatcher.execute(swarming.OptionParserSwarming(), args)


class Common:
  # pylint: disable=no-member

  def setUp(self):
    self.mock(auth, 'ensure_logged_in', lambda _: None)
    self.mock(sys, 'stdout', StringIO.StringIO())
    self.mock(sys, 'stderr', StringIO.StringIO())
    self.mock(logging_utils, 'prepare_logging', lambda *args: None)
    self.mock(logging_utils, 'set_console_level', lambda *args: None)

  def tearDown(self):
    if not self.has_failed():
      self._check_output('', '')

  maxDiff = None

  def _check_output(self, out, err):
    self.assertMultiLineEqual(out, sys.stdout.getvalue())
    self.assertMultiLineEqual(err, sys.stderr.getvalue())

    # Flush their content by mocking them again.
    self.mock(sys, 'stdout', StringIO.StringIO())
    self.mock(sys, 'stderr', StringIO.StringIO())

  def main_safe(self, args):
    """Bypasses swarming.main()'s exception handling.

    It gets in the way when debugging test failures.
    """
    # pylint: disable=bare-except
    try:
      return main(args)
    except:
      data = '%s\nSTDOUT:\n%s\nSTDERR:\n%s' % (
          traceback.format_exc(), sys.stdout.getvalue(), sys.stderr.getvalue())
      self.fail(data)


class NetTestCase(net_utils.TestCase, Common):
  # These test fail when running in parallel
  # Need to run in sequential_test_runner.py as an executable
  no_run = 1
  """Base class that defines the url_open mock."""

  def setUp(self):
    net_utils.TestCase.setUp(self)
    Common.setUp(self)


class TestMain(NetTestCase):
  # Tests calling main().
  def test_query_base(self):
    self.expected_requests([
        (
            'https://localhost:1/_ah/api/swarming/v1/bot/botid/tasks?limit=200',
            {},
            {
                'yo': 'dawg'
            },
        ),
    ])
    ret = self.main_safe([
        'query',
        '--swarming',
        'https://localhost:1',
        'bot/botid/tasks',
    ])
    self._check_output('{\n  "yo": "dawg"\n}\n', '')
    self.assertEqual(0, ret)

  def test_query_cursor(self):
    self.expected_requests([
        (
            'https://localhost:1/_ah/api/swarming/v1/bot/botid/tasks?'
            'foo=bar&limit=2',
            {},
            {
                'cursor': '%',
                'extra': False,
                'items': ['A'],
            },
        ),
        (
            'https://localhost:1/_ah/api/swarming/v1/bot/botid/tasks?'
            'foo=bar&cursor=%25&limit=1',
            {},
            {
                'cursor': None,
                'items': ['B'],
                'ignored': True,
            },
        ),
    ])
    ret = self.main_safe([
        'query',
        '--swarming',
        'https://localhost:1',
        'bot/botid/tasks?foo=bar',
        '--limit',
        '2',
    ])
    expected = ('{\n'
                '  "extra": false, \n'
                '  "items": [\n'
                '    "A", \n'
                '    "B"\n'
                '  ]\n'
                '}\n')
    self._check_output(expected, '')
    self.assertEqual(0, ret)


if __name__ == '__main__':
  for env_var_to_remove in ('ISOLATE_SERVER', 'SWARMING_TASK_ID',
                            'SWARMING_SERVER'):
    os.environ.pop(env_var_to_remove, None)
  test_env.main()
