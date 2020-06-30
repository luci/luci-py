#!/usr/bin/env vpython3
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import mock
import sys
import textwrap
import unittest

import six

import test_env_platforms
test_env_platforms.setup_test_env()

from utils import tools

if sys.platform == 'darwin':
  import osx


@unittest.skipUnless(sys.platform == 'darwin',
                     'Tests only run under darwin platform')
class TestOsx(unittest.TestCase):

  def setUp(self):
    super(TestOsx, self).setUp()
    self.subprocess_patcher = mock.patch('subprocess.check_output')
    self.mock_subprocess = self.subprocess_patcher.start()

  def tearDown(self):
    super(TestOsx, self).tearDown()
    self.subprocess_patcher.stop()
    tools.clear_cache_all()

  def mock_physical_disks_list(self, disks_data):
    content = []
    content.append(
        textwrap.dedent("""\
      <plist>
      <dict>
          <key>WholeDisks</key>
          <array>
    """))
    for disk_name, _ in disks_data.items():
      content.append('<string>%s</string>' % disk_name)
    content.append(
        textwrap.dedent("""\
        </array>
    </dict>
    </plist>
    """))
    return '\n'.join(content).encode()

  def mock_disk_info(self, disk_data):
    content = []
    content.append(textwrap.dedent("""\
    <plist>
    <dict>
    """))
    for key_name, value in disk_data.items():
      content.append('<key>%s</key>' % key_name)
      content.append(value)
    content.append(textwrap.dedent("""\
    </dict>
    </plist>
    """))
    return '\n'.join(content).encode()

  def test_get_ssd(self):
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
    self.mock_subprocess.side_effect = side_effect

    ssd = osx.get_ssd()
    self.assertEqual((u'disk0', u'disk2'), ssd)

  def test_get_disks_model(self):
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
    self.mock_subprocess.side_effect = side_effect

    disks_model = osx.get_disks_model()
    self.assertEqual((u'APPLE SSD AP0256M', u'APPLE SSD AP0257M'), disks_model)

  def test_get_gpu_radeon_rx560_egpu(self):
    # Copied from actual output of 'system_profiler SPDisplaysDataType -xml' on
    # a Macmini8,1 with a Sonnet eGFX Breakaway Puck Radeon RX 560 attached to
    # Thunderbolt. Trimmed out irrelevant elements.
    plist = textwrap.dedent("""\
      <plist>
      <array>
        <dict>
          <key>_items</key>
          <array>
            <dict>
              <key>spdisplays_device-id</key>
              <string>0x3e9b</string>
              <key>spdisplays_vendor</key>
              <string>Intel</string>
              <key>sppci_device_type</key>
              <string>spdisplays_gpu</string>
              <key>sppci_model</key>
              <string>Intel UHD Graphics 630</string>
            </dict>
            <dict>
              <key>spdisplays_device-id</key>
              <string>0x67ef</string>
              <key>spdisplays_gpu_removable</key>
              <string>spdisplays_yes</string>
              <key>spdisplays_vendor</key>
              <string>sppci_vendor_amd</string>
              <key>sppci_device_type</key>
              <string>spdisplays_egpu</string>
              <key>sppci_model</key>
              <string>Radeon RX 560</string>
            </dict>
          </array>
        </dict>
      </array>
      </plist>""").encode()

    self.mock_subprocess.side_effect = [plist]

    gpus = osx.get_gpu()
    self.assertEqual(([u'1002', u'1002:67ef', u'8086', u'8086:3e9b'],
                      [u'AMD Radeon RX 560', u'Intel UHD Graphics 630']), gpus)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
