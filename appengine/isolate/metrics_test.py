#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

import isolate_test_env as test_env
test_env.setup_test_env()

from components import auth

from test_support import test_case

import config
import metrics

from proto import config_pb2


class TestMetrics(test_case.TestCase):
  """Test cases for metrics.py."""

  def test_file_size(self):
    fields = {
        'client_email': 'anonymous:anonymous',
        'client_name': 'bots',
        'download_source': 'GCS',
    }
    cfg = config_pb2.SettingsCfg(client_monitoring_config=[
        config_pb2.ClientMonitoringConfig(
            ip_whitelist='bots',
            label='bots',
        ),
    ])
    self.mock(config, '_get_settings_with_defaults', lambda: ('revision', cfg))

    # Client is whitelisted for monitoring.
    self.mock(auth, 'is_in_ip_whitelist', lambda *args, **kwargs: False)
    metrics.file_size(123)
    self.assertIsNone(metrics._bytes_requested.get(fields=fields))

    # Client is not whitelisted for monitoring.
    self.mock(auth, 'is_in_ip_whitelist', lambda *args, **kwargs: True)
    metrics.file_size(123)
    self.assertEqual(metrics._bytes_requested.get(fields=fields), 123)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
