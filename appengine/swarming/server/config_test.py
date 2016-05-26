#!/usr/bin/env python
# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

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
  def validator_test(self, validator, cfg, messages):
    ctx = validation.Context()
    validator(cfg, ctx)
    self.assertEquals(ctx.result().messages, [
      validation.Message(severity=logging.ERROR, text=m)
      for m in messages
    ])

  def test_validate_isolate_settings(self):
    self.validator_test(
        config.validate_isolate_settings,
        config_pb2.IsolateSettings(
            default_server='https://isolateserver.appspot.com'),
        [
          ('either specify both default_server and default_namespace or '
           'none'),
        ])

    self.validator_test(
        config.validate_isolate_settings,
        config_pb2.IsolateSettings(
            default_server='isolateserver.appspot.com',
            default_namespace='abc',
        ),
        [
          'default_server must start with "https://" or "http://"',
        ])

    self.validator_test(
        config.validate_isolate_settings,
        config_pb2.IsolateSettings(
          default_server='https://isolateserver.appspot.com',
          default_namespace='abc',
        ),
        [])

    self.validator_test(
        config.validate_isolate_settings,
        config_pb2.IsolateSettings(),
        [])

  def test_validate_cipd_settings(self):
    self.validator_test(
        config.validate_cipd_settings,
        config_pb2.CipdSettings(),
        [
          'default_server is not set',
          'default_client_package: invalid package_name ""',
          'default_client_package: invalid version ""',
        ])

    self.validator_test(
        config.validate_cipd_settings,
        config_pb2.CipdSettings(
            default_server='chrome-infra-packages.appspot.com',
            default_client_package=config_pb2.CipdPackage(
                package_name='infra/tools/cipd/windows-i386',
                version='git_revision:deadbeef'),
            ),
        [
          'default_server must start with "https://" or "http://"',
        ])

    self.validator_test(
        config.validate_cipd_settings,
        config_pb2.CipdSettings(
            default_server='https://chrome-infra-packages.appspot.com',
            default_client_package=config_pb2.CipdPackage(
                package_name='infra/tools/cipd/${platform}',
                version='git_revision:deadbeef'),
            ),
        [])

  def test_validate_settings(self):
    self.validator_test(
        config.validate_settings,
        config_pb2.SettingsCfg(
            bot_death_timeout_secs=-1,
            reusable_task_age_secs=-1),
      [
        'bot_death_timeout_secs cannot be negative',
        'reusable_task_age_secs cannot be negative',
      ])

    self.validator_test(
        config.validate_settings,
        config_pb2.SettingsCfg(
            bot_death_timeout_secs=config.SECONDS_IN_YEAR + 1,
            reusable_task_age_secs=config.SECONDS_IN_YEAR + 1),
      [
        'bot_death_timeout_secs cannot be more than a year',
        'reusable_task_age_secs cannot be more than a year',
      ])

    self.validator_test(config.validate_settings, config_pb2.SettingsCfg(), [])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
