#!/usr/bin/env vpython3
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os
import sys
import textwrap
import unittest

# TODO(github.com/wolever/parameterized/issues/91)
# use parameterized after the bug is resolved.
from nose2.tools import params
import mock
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
    tools.clear_cache_all()
    self.mock_check_output = mock.patch('subprocess.check_output').start()
    self.mock_listdir = mock.patch('os.listdir').start()
    self.mock_path_exists = mock.patch('os.path.exists').start()

  def tearDown(self):
    super(TestOsx, self).tearDown()
    mock.patch.stopall()
    tools.clear_cache_all()

  def test_get_xcode_state(self):
    self.mock_check_output.return_value = textwrap.dedent("""\
      Xcode 11.5
      Build version 11E608c
    """).encode()
    self.mock_listdir.side_effect = [
        # os.listdir('/Applications')
        ['Google Chrome.app', 'Safari.app', 'Xcode.app'],
        # pylint: disable=line-too-long
        # os.listdir('/Applications/Xcode.app/Contents/Developer/Platforms/iPhoneOS.platform/DeviceSupport')
        ['1.0', '2.0'],
    ]

    state = osx.get_xcode_state()
    version = state['/Applications/Xcode.app']['version']
    build_version = state['/Applications/Xcode.app']['build version']
    device_support = state['/Applications/Xcode.app']['device support']
    self.assertEqual(version, '11.5')
    self.assertEqual(build_version, '11E608c')
    self.assertEqual(device_support, ['1.0', '2.0'])

  def test_get_xcode_versions(self):
    self.mock_check_output.side_effect = [
        textwrap.dedent("""\
        Xcode 11.5
        Build version abcd
      """).encode(),
        textwrap.dedent("""\
        Xcode 11.4
        Build version efgh
      """).encode(),
    ]
    self.mock_listdir.side_effect = [
        # os.listdir('/Applications')
        ['Xcode11.5.app', 'Xcode11.4.app'],
    ]
    self.mock_path_exists.side_effect = [True, False, True, False]
    versions = osx.get_xcode_versions()
    self.assertEqual(versions, ['11.4', '11.5'])

  def test_get_current_xcode_version(self):
    self.mock_check_output.return_value = textwrap.dedent("""\
      Xcode 11.5
      Build version 11E608c
    """).encode()

    version = osx.get_current_xcode_version()
    self.assertEqual(version, ('11.5', '11E608c'))

  def test_get_ios_device_ids(self):
    self.mock_check_output.return_value = b'1234abcd\n'
    self.assertEqual(osx.get_ios_device_ids(), ['1234abcd'])

  def test_get_ios_version(self):
    self.mock_check_output.return_value = b'13.5\n'
    self.assertEqual(osx.get_ios_version('1234abcd'), '13.5')

  def test_get_ios_device_type(self):
    self.mock_check_output.return_value = b'iPhone12,1\n'
    self.assertEqual(osx.get_ios_device_type('1234abcd'), 'iPhone12,1')

  def test_get_hardware_model_string(self):
    self.mock_check_output.return_value = b'MacBookPro15,1\n'
    hw_model = osx.get_hardware_model_string()
    self.assertEqual(hw_model, 'MacBookPro15,1')

  def test_get_os_version_number(self):
    with mock.patch('platform.mac_ver') as mock_mac_ver:
      mock_mac_ver.return_value = ('10.15.5', ('', '', ''), 'x86_64')
      os_version = osx.get_os_version_number()
    self.assertEqual(os_version, '10.15.5')

  def test_get_audio(self):
    plist = textwrap.dedent("""\
      <plist> <array>
        <dict>
          <key>_items</key>
          <array>
            <dict>
              <key>_items</key>
              <array>
                <dict>
                  <key>_name</key>
                  <string>MacBook Pro Microphone</string>
                  <key>coreaudio_default_audio_input_device</key>
                  <string>spaudio_yes</string>
                  <key>coreaudio_device_input</key>
                  <integer>1</integer>
                  <key>coreaudio_device_manufacturer</key>
                  <string>Apple Inc.</string>
                  <key>coreaudio_device_srate</key>
                  <real>48000</real>
                  <key>coreaudio_device_transport</key>
                  <string>coreaudio_device_type_builtin</string>
                  <key>coreaudio_input_source</key>
                  <string>MacBook Pro Microphone</string>
                </dict>
                <dict>
                  <key>_name</key>
                  <string>MacBook Pro Speakers</string>
                  <key>_properties</key>
                  <string>coreaudio_default_audio_system_device</string>
                  <key>coreaudio_default_audio_output_device</key>
                  <string>spaudio_yes</string>
                  <key>coreaudio_default_audio_system_device</key>
                  <string>spaudio_yes</string>
                  <key>coreaudio_device_manufacturer</key>
                  <string>Apple Inc.</string>
                  <key>coreaudio_device_output</key>
                  <integer>2</integer>
                  <key>coreaudio_device_srate</key>
                  <real>48000</real>
                  <key>coreaudio_device_transport</key>
                  <string>coreaudio_device_type_builtin</string>
                  <key>coreaudio_output_source</key>
                  <string>MacBook Pro Speakers</string>
                </dict>
              </array>
              <key>_name</key>
              <string>coreaudio_device</string>
            </dict>
          </array>
        </dict>
      </array>
      </plist>""").encode()
    self.mock_check_output.return_value = plist
    self.assertEqual(osx.get_audio(), ['MacBook Pro Speakers'])

  def test_get_audio_no_devices(self):
    plist = textwrap.dedent("""\
      <plist> <array>
        <dict>
          <key>_items</key>
          <array>
            <dict>
              <key>_items</key>
              <array>
              </array>
              <key>_name</key>
              <string>coreaudio_device</string>
            </dict>
          </array>
        </dict>
      </array>
      </plist>""").encode()
    self.mock_check_output.return_value = plist
    self.assertEqual(osx.get_audio(), [])

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

    self.mock_check_output.side_effect = [plist]

    gpus = osx.get_gpu()
    self.assertEqual(([u'1002', u'1002:67ef', u'8086', u'8086:3e9b'
                      ], [u'AMD Radeon RX 560', u'Intel UHD Graphics 630']),
                     gpus)

  def test_get_cpuinfo(self):
    self.mock_check_output.return_value = textwrap.dedent("""\
        machdep.cpu.max_basic: 22
        machdep.cpu.max_ext: 2147483656
        machdep.cpu.vendor: GenuineIntel
        machdep.cpu.brand_string: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
        machdep.cpu.family: 6
        machdep.cpu.model: 158
        machdep.cpu.extmodel: 9
        machdep.cpu.extfamily: 0
        machdep.cpu.stepping: 10
        machdep.cpu.feature_bits: 9221959987971750911
        machdep.cpu.leaf7_feature_bits: 43804591 1073741824
        machdep.cpu.leaf7_feature_bits_edx: 2617254912
        machdep.cpu.extfeature_bits: 1241984796928
        machdep.cpu.signature: 591594
        machdep.cpu.brand: 0
        machdep.cpu.features: FPU VME DE PSE TSC MSR PAE MCE CX8 APIC SEP MTRR PGE MCA CMOV PAT PSE36 CLFSH DS ACPI MMX FXSR SSE SSE2 SS HTT TM PBE SSE3 PCLMULQDQ DTES64 MON DSCPL VMX EST TM2 SSSE3 FMA CX16 TPR PDCM SSE4.1 SSE4.2 x2APIC MOVBE POPCNT AES PCID XSAVE OSXSAVE SEGLIM64 TSCTMR AVX1.0 RDRAND F16C
        machdep.cpu.leaf7_features: RDWRFSGS TSC_THREAD_OFFSET SGX BMI1 AVX2 SMEP BMI2 ERMS INVPCID FPU_CSDS MPX RDSEED ADX SMAP CLFSOPT IPT SGXLC MDCLEAR TSXFA IBRS STIBP L1DF SSBD
        machdep.cpu.extfeatures: SYSCALL XD 1GBPAGE EM64T LAHF LZCNT PREFETCHW RDTSCP TSCI
        machdep.cpu.logical_per_package: 16
        machdep.cpu.cores_per_package: 8
        machdep.cpu.microcode_version: 202
        machdep.cpu.processor_flag: 5
        machdep.cpu.mwait.linesize_min: 64
        machdep.cpu.mwait.linesize_max: 64
        machdep.cpu.mwait.extensions: 3
        machdep.cpu.mwait.sub_Cstates: 286531872
        machdep.cpu.thermal.sensor: 1
        machdep.cpu.thermal.dynamic_acceleration: 1
        machdep.cpu.thermal.invariant_APIC_timer: 1
        machdep.cpu.thermal.thresholds: 2
        machdep.cpu.thermal.ACNT_MCNT: 1
        machdep.cpu.thermal.core_power_limits: 1
        machdep.cpu.thermal.fine_grain_clock_mod: 1
        machdep.cpu.thermal.package_thermal_intr: 1
        machdep.cpu.thermal.hardware_feedback: 0
        machdep.cpu.thermal.energy_policy: 1
        machdep.cpu.xsave.extended_state: 31 832 1088 0
        machdep.cpu.xsave.extended_state1: 15 832 256 0
        machdep.cpu.arch_perf.version: 4
        machdep.cpu.arch_perf.number: 4
        machdep.cpu.arch_perf.width: 48
        machdep.cpu.arch_perf.events_number: 7
        machdep.cpu.arch_perf.events: 0
        machdep.cpu.arch_perf.fixed_number: 3
        machdep.cpu.arch_perf.fixed_width: 48
        machdep.cpu.cache.linesize: 64
        machdep.cpu.cache.L2_associativity: 4
        machdep.cpu.cache.size: 256
        machdep.cpu.tlb.inst.large: 8
        machdep.cpu.tlb.data.small: 64
        machdep.cpu.tlb.data.small_level1: 64
        machdep.cpu.address_bits.physical: 39
        machdep.cpu.address_bits.virtual: 48
    """).encode()

    expected = {
        u'vendor': u'GenuineIntel',
        u'model': [6, 158, 10, 202],
        u'flags': [
            u'acpi', u'aes', u'apic', u'avx1.0', u'clfsh', u'cmov', u'cx16',
            u'cx8', u'de', u'ds', u'dscpl', u'dtes64', u'est', u'f16c', u'fma',
            u'fpu', u'fxsr', u'htt', u'mca', u'mce', u'mmx', u'mon', u'movbe',
            u'msr', u'mtrr', u'osxsave', u'pae', u'pat', u'pbe', u'pcid',
            u'pclmulqdq', u'pdcm', u'pge', u'popcnt', u'pse', u'pse36',
            u'rdrand', u'seglim64', u'sep', u'ss', u'sse', u'sse2', u'sse3',
            u'sse4.1', u'sse4.2', u'ssse3', u'tm', u'tm2', u'tpr', u'tsc',
            u'tsctmr', u'vme', u'vmx', u'x2apic', u'xsave'
        ],
        u'name': u'Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz'
    }
    self.assertEqual(osx.get_cpuinfo(), expected)

  def test_get_temperatures(self):
    temp1 = osx.get_temperatures()

    def assertOpenBetween(val, lower, upper):
      self.assertGreater(val, lower)
      self.assertLess(val, upper)

    assertOpenBetween(temp1['cpu'], 0, 100)

    # use sensor cache.
    temp2 = osx.get_temperatures()
    assertOpenBetween(temp2['cpu'], 0, 100)

  @params(
      # Retina 10.12.4 or later
      (
          """\
          <plist>
            <array>
              <dict>
                <key>_items</key>
                <array>
                  <dict>
                    <key>spdisplays_ndrvs</key>
                    <array>
                      <dict>
                        <key>_name</key>
                        <string>Display 10.12.4 and later</string>
                        <key>spdisplays_display_type</key>
                        <string>spdisplays_built-in_retinaLCD</string>
                        <key>spdisplays_pixelresolution</key>
                        <string>spdisplays_2880x1800Retina</string>
                      </dict>
                    </array>
                  </dict>
                </array>
              </dict>
            </array>
          </plist>""",
          '1',
      ),
      # Retina 10.12.3 and earlier
      (
          """\
          <plist>
            <array>
              <dict>
                <key>_items</key>
                <array>
                  <dict>
                    <key>spdisplays_ndrvs</key>
                    <array>
                      <dict>
                        <key>_name</key>
                        <string>Display 10.12.4 and later</string>
                        <key>spdisplays_retina</key>
                        <string>spdisplays_yes</string>
                      </dict>
                    </array>
                  </dict>
                </array>
              </dict>
            </array>
          </plist>""",
          '1',
      ),
      # None retina
      (
          """\
          <plist>
            <array>
              <dict>
                <key>_items</key>
                <array>
                  <dict>
                    <key>spdisplays_ndrvs</key>
                    <array>
                      <dict>
                        <key>_name</key>
                        <string>Non redina display</string>
                      </dict>
                    </array>
                  </dict>
                </array>
              </dict>
            </array>
          </plist>""",
          '0',
      ))
  def test_get_monitor_hidpi(self, plist, expected):
    self.mock_check_output.return_value = textwrap.dedent(plist).encode()
    self.assertEqual(osx.get_monitor_hidpi(), expected)

  def test_get_physical_ram(self):
    self.assertGreater(osx.get_physical_ram(), 0)

  def test_get_uptime(self):
    self.assertGreater(osx.get_uptime(), 0)

  @unittest.skip('TODO(crbug.com/1101705): install pyobjc')
  def test_is_locked(self):
    self.assertIsNotNone(osx.is_locked())

  @unittest.skip('TODO(crbug.com/1101705): install pyobjc')
  def test_is_beta(self):
    self.assertIsNotNone(osx.is_beta())

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
    self.mock_check_output.side_effect = side_effect

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
    self.mock_check_output.side_effect = side_effect

    disks_model = osx.get_disks_model()
    self.assertEqual((u'APPLE SSD AP0256M', u'APPLE SSD AP0257M'), disks_model)

  def test_generate_launchd_plist(self):
    plist_data = osx.generate_launchd_plist(['echo', 'hi'], os.getcwd(),
                                            'org.swarm.bot.plist')
    plist = osx._read_plist(plist_data.encode())
    self.assertEqual(plist['Label'], 'org.swarm.bot.plist')
    self.assertEqual(plist['Program'], 'echo')
    self.assertEqual(plist['ProgramArguments'], ['echo', 'hi'])


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
