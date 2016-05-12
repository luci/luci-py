#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from components.config import validation
from test_support import test_case

from proto import config_pb2
from server import config


# pylint: disable=W0212,W0612


class ConfigTest(test_case.TestCase):
  def test_validate_isolate_settings(self):
    def validate(**cfg):
      cfg = config_pb2.IsolateSettings(**cfg)
      ctx = validation.Context.raise_on_error()
      config.validate_isolate_settings(cfg, ctx)

    with self.assertRaises(ValueError):
      # No namespace.
      validate(default_server='https://isolateserver.appspot.com')
    with self.assertRaises(ValueError):
      # Not a URL.
      validate(
          default_server='isolateserver.appspot.com',
          default_namespace='abc'
      )
    validate(
        default_server='https://isolateserver.appspot.com',
        default_namespace='default-gzip'
    )
    validate()

  def test_validate_settings(self):
    def validate(**cfg):
      cfg = config_pb2.SettingsCfg(**cfg)
      ctx = validation.Context.raise_on_error()
      config.validate_settings(cfg, ctx)

    with self.assertRaises(ValueError):
      validate(bot_death_timeout_secs=-1)
    with self.assertRaises(ValueError):
      validate(bot_death_timeout_secs=config.SECONDS_IN_YEAR + 1)
    with self.assertRaises(ValueError):
      validate(reusable_task_age_secs=-1)
    with self.assertRaises(ValueError):
      validate(reusable_task_age_secs=config.SECONDS_IN_YEAR + 1)
    validate()


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
