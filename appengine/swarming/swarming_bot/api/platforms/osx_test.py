#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import mock
import sys
import unittest

import test_env_platforms
test_env_platforms.setup_test_env()

from utils import tools

if sys.platform == 'darwin':
  import osx


@unittest.skipUnless(sys.platform == 'darwin',
                     'Tests only run under darwin platform')
class TestOsx(unittest.TestCase):

  def mock_physical_disks_list(self, disks_data):
    content = []
    content.append("""
      <plist>
      <dict>
          <key>WholeDisks</key>
          <array>
    """)
    for disk_name, _ in disks_data.items():
      content.append('<string>%s</string>' % disk_name)
    content.append("""
        </array>
    </dict>
    </plist>
    """)
    return '\n'.join(content)

  def mock_disk_info(self, disk_data):
    content = []
    content.append("""
    <plist>
    <dict>
    """)
    for key_name, value in disk_data.items():
      content.append('<key>%s</key>' % key_name)
      content.append(value)
    content.append("""
    </dict>
    </plist>
    """)
    return '\n'.join(content)

  def clear_get_physical_disks_info_cache(self):
    tools.clear_cache(osx._get_physical_disks_info)

  def get_ssd(self):
    self.clear_get_physical_disks_info_cache()
    tools.clear_cache(osx.get_ssd)
    return osx.get_ssd()

  def get_disks_model(self):
    self.clear_get_physical_disks_info_cache()
    tools.clear_cache(osx.get_disks_model)
    return osx.get_disks_model()

  @mock.patch('subprocess.check_output')
  def test_get_ssd(self, mock_subprocess):
    disks_data = {
        'disk0': {
            'SolidState': '<true/>',
        },
        'disk1': {
            'SolidState': '<false/>',
        },
        'disk2': {
            'SolidState': '<true/>',
        },
    }
    side_effect = [self.mock_physical_disks_list(disks_data)]
    for _, disk_data in disks_data.items():
      side_effect.append(self.mock_disk_info(disk_data))
    mock_subprocess.side_effect = side_effect

    ssd = self.get_ssd()
    self.assertEqual((u'disk0', u'disk2'), ssd)

  @mock.patch('subprocess.check_output')
  def test_get_disks_model(self, mock_subprocess):
    disks_data = {
        'disk0': {
            'MediaName': '<string>APPLE SSD AP0257M</string>',
        },
        'disk1': {
            'MediaName': '<string>APPLE SSD AP0256M</string>',
        },
    }
    side_effect = [self.mock_physical_disks_list(disks_data)]
    for _, disk_data in disks_data.items():
      side_effect.append(self.mock_disk_info(disk_data))
    mock_subprocess.side_effect = side_effect

    disks_model = self.get_disks_model()
    self.assertEqual((u'APPLE SSD AP0256M', u'APPLE SSD AP0257M'), disks_model)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
