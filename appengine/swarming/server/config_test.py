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
        config._validate_isolate_settings,
        config_pb2.IsolateSettings(
            default_server='https://isolateserver.appspot.com'),
        [
          'either specify both default_server and default_namespace or '
            'none',
        ])

    self.validator_test(
        config._validate_isolate_settings,
        config_pb2.IsolateSettings(
            default_server='isolateserver.appspot.com',
            default_namespace='abc',
        ),
        [
          'default_server must start with "https://" or "http://localhost"',
        ])

    self.validator_test(
        config._validate_isolate_settings,
        config_pb2.IsolateSettings(
          default_server='https://isolateserver.appspot.com',
          default_namespace='abc',
        ),
        [])

    self.validator_test(
        config._validate_isolate_settings,
        config_pb2.IsolateSettings(),
        [])

  def test_validate_cipd_settings(self):
    self.validator_test(
        config._validate_cipd_settings,
        config_pb2.CipdSettings(),
        [
          'default_server is not set',
          'default_client_package: invalid package_name ""',
          'default_client_package: invalid version ""',
        ])

    self.validator_test(
        config._validate_cipd_settings,
        config_pb2.CipdSettings(
            default_server='chrome-infra-packages.appspot.com',
            default_client_package=config_pb2.CipdPackage(
                package_name='infra/tools/cipd/windows-i386',
                version='git_revision:deadbeef'),
            ),
        [
          'default_server must start with "https://" or "http://localhost"',
        ])

    self.validator_test(
        config._validate_cipd_settings,
        config_pb2.CipdSettings(
            default_server='https://chrome-infra-packages.appspot.com',
            default_client_package=config_pb2.CipdPackage(
                package_name='infra/tools/cipd/${platform}',
                version='git_revision:deadbeef'),
            ),
        [])

  def test_validate_dimension_acls(self):
    entry = config_pb2.DimensionACLs.Entry

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[]),
        [])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[
          entry(dimension=['pool:default'], usable_by='all'),
          entry(dimension=['stuff:*'], usable_by='all'),
        ]),
        [])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[entry()]),
        [
          'entry #1: at least one dimension is required',
          'entry #1: "usable_by" is required',
        ])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[
          entry(dimension=['not_kv_pair'], usable_by='all'),
        ]),
        [u'entry #1: dimension "not_kv_pair": not a valid dimension'])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[
          entry(dimension=['@@@@:abc'], usable_by='all'),
        ]),
        [u'entry #1: dimension "@@@@:abc": not a valid dimension'])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[
          entry(dimension=['pool:*'], usable_by='all'),
          entry(dimension=['pool:*'], usable_by='all'),
          entry(dimension=['pool:*'], usable_by='all'),
        ]),
        ['entry #1: dimension "pool:*": was specified multiple times'])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[
          entry(dimension=['pool:abc'], usable_by='all'),
          entry(dimension=['pool:abc'], usable_by='all'),
        ]),
        ['entry #2: dimension "pool:abc": was already specified'])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[
          entry(dimension=['pool:*'], usable_by='all'),
          entry(dimension=['pool:abc'], usable_by='all'),
        ]),
        ['entry #2: dimension "pool:abc": was already specified via "pool:*"'])

    self.validator_test(
        config._validate_dimension_acls,
        config_pb2.DimensionACLs(entry=[
          entry(dimension=['pool:abc'], usable_by='@@@badgroup@@@'),
        ]),
        ['entry #1: "usable_by" specifies invalid group name "@@@badgroup@@@"'])


  def test_validate_settings(self):
    self.validator_test(
        config._validate_settings,
        config_pb2.SettingsCfg(
            bot_death_timeout_secs=-1,
            reusable_task_age_secs=-1),
      [
        'bot_death_timeout_secs cannot be negative',
        'reusable_task_age_secs cannot be negative',
      ])

    self.validator_test(
        config._validate_settings,
        config_pb2.SettingsCfg(
            bot_death_timeout_secs=config._SECONDS_IN_YEAR + 1,
            reusable_task_age_secs=config._SECONDS_IN_YEAR + 1),
      [
        'bot_death_timeout_secs cannot be more than a year',
        'reusable_task_age_secs cannot be more than a year',
      ])

    self.validator_test(config._validate_settings, config_pb2.SettingsCfg(), [])

    self.validator_test(
        config._validate_settings,
        config_pb2.SettingsCfg(
            mp=config_pb2.MachineProviderSettings(server='http://url')),
      [
        'mp.server must start with "https://" or "http://localhost"',
      ])

    self.validator_test(
        config._validate_settings,
        config_pb2.SettingsCfg(
            mp=config_pb2.MachineProviderSettings(server='url')),
      [
        'mp.server must start with "https://" or "http://localhost"',
      ])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
