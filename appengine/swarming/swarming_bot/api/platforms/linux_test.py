#!/usr/bin/env vpython3
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import io
import logging
import os
import sys
import textwrap
import unittest

import mock

import test_env_platforms
test_env_platforms.setup_test_env()

from depot_tools import auto_stub
from utils import tools

if sys.platform == 'linux':
  import linux


# pylint: disable=line-too-long
EXYNOS_CPU_INFO = r"""
Processor : ARMv7 Processor rev 4 (v7l)
processor : 0
BogoMIPS  : 1694.10

processor : 1
BogoMIPS  : 1694.10

Features  : swp half thumb fastmult vfp edsp thumbee neon vfpv3 tls vfpv4 idiva idivt
CPU implementer : 0x41
CPU architecture: 7
CPU variant : 0x0
CPU part  : 0xc0f
CPU revision  : 4

Hardware  : SAMSUNG EXYNOS5 (Flattened Device Tree)
Revision  : 0000
Serial    : 0000000000000000
"""


CAVIUM_CPU_INFO = r"""
processor : 0
BogoMIPS  : 200.00
Features  : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics
CPU implementer : 0x43
CPU architecture: 8
CPU variant : 0x1
CPU part  : 0x0a1
CPU revision  : 1

processor : 1
BogoMIPS  : 200.00
Features  : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics
CPU implementer : 0x43
CPU architecture: 8
CPU variant : 0x1
CPU part  : 0x0a1
CPU revision  : 1
"""


MIPS64_CPU_INFO = r"""
chrome-bot@build23-b3:/tmp/1$ cat /proc/cpuinfo
system type             : Unsupported Board (CN6120p1.1-1000-NSP)
machine                 : Unknown
processor               : 0
cpu model               : Cavium Octeon II V0.1
BogoMIPS                : 2000.00
wait instruction        : yes
microsecond timers      : yes
tlb_entries             : 128
extra interrupt vector  : yes
hardware watchpoint     : yes, count: 2, address/irw mask: [0x0ffc, 0x0ffb]
isa                     : mips2 mips3 mips4 mips5 mips64r2
ASEs implemented        :
shadow register sets    : 1
kscratch registers      : 3
package                 : 0
core                    : 0
VCED exceptions         : not available
VCEI exceptions         : not available

processor               : 1
cpu model               : Cavium Octeon II V0.1
BogoMIPS                : 2000.00
wait instruction        : yes
microsecond timers      : yes
tlb_entries             : 128
extra interrupt vector  : yes
hardware watchpoint     : yes, count: 2, address/irw mask: [0x0ffc, 0x0ffb]
isa                     : mips2 mips3 mips4 mips5 mips64r2
ASEs implemented        :
shadow register sets    : 1
kscratch registers      : 3
package                 : 0
core                    : 1
VCED exceptions         : not available
VCEI exceptions         : not available
"""

PPC64_CPU_INFO = r"""
processor  : 0
cpu        : POWER10 (architected), altivec supported
clock      : 2750.000000MHz
revision   : 2.0 (pvr 0080 0200)

processor  : 1
cpu        : POWER10 (architected), altivec supported
clock      : 2750.000000MHz
revision   : 2.0 (pvr 0080 0200)

processor  : 2
cpu        : POWER10 (architected), altivec supported
clock      : 2750.000000MHz
revision   : 2.0 (pvr 0080 0200)

processor  : 3
cpu        : POWER10 (architected), altivec supported
clock      : 2750.000000MHz
revision   : 2.0 (pvr 0080 0200)

processor  : 4
cpu        : POWER10 (architected), altivec supported
clock      : 2750.000000MHz
revision   : 2.0 (pvr 0080 0200)

timebase   : 512000000
platform   : pSeries
model      : IBM,9105-42A
machine    : CHRP IBM,9105-42A
MMU        : Radix
"""


S390X_CPU_INFO = r"""
vendor_id       : IBM/S390
# processors    : 8
bogomips per cpu: 3241.00
max thread id   : 0
features        : esan3 zarch stfle msa ldisp eimm dfp edat etf3eh highgprs te vx vxd vxe gs vxe2 vxp sort dflt sie
facilities      : 0 1 2 3 4 6 7 8 9 10 12 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 30 31 32 33 34 35 36 37 38 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 57 58 59 60 61 69 70 71 72 73 74 75 76 77 80 81 82 128 129 130 131 133 134 135 146 147 148 150 151 152 155 156 168
cache0          : level=1 type=Data scope=Private size=128K line_size=256 associativity=8
cache1          : level=1 type=Instruction scope=Private size=128K line_size=256 associativity=8
cache2          : level=2 type=Data scope=Private size=4096K line_size=256 associativity=8
cache3          : level=2 type=Instruction scope=Private size=4096K line_size=256 associativity=8
cache4          : level=3 type=Unified scope=Shared size=262144K line_size=256 associativity=32
cache5          : level=4 type=Unified scope=Shared size=983040K line_size=256 associativity=60
processor 0: version = FF,  identification = 0E18E8,  machine = 8561
processor 1: version = FF,  identification = 0E18E8,  machine = 8561
processor 2: version = FF,  identification = 0E18E8,  machine = 8561
processor 3: version = FF,  identification = 0E18E8,  machine = 8561
processor 4: version = FF,  identification = 0E18E8,  machine = 8561
processor 5: version = FF,  identification = 0E18E8,  machine = 8561
processor 6: version = FF,  identification = 0E18E8,  machine = 8561
processor 7: version = FF,  identification = 0E18E8,  machine = 8561

cpu number      : 0
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 1
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 2
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 3
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 4
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 5
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 5
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 6
cpu MHz dynamic : 5200
cpu MHz static  : 5200

cpu number      : 7
cpu MHz dynamic : 5200
cpu MHz static  : 5200
"""
@unittest.skipUnless(sys.platform == 'linux', 'linux only test')
class TestCPUInfo(auto_stub.TestCase):

  def setUp(self):
    super(TestCPUInfo, self).setUp()
    tools.clear_cache_all()

  def tearDown(self):
    super(TestCPUInfo, self).tearDown()
    tools.clear_cache_all()

  def get_cpuinfo(self, text):
    self.mock(linux, '_read_cpuinfo', lambda: text)
    return linux.get_cpuinfo()

  def test_get_cpuinfo_empty(self):
    self.assertEqual({}, self.get_cpuinfo(''))

  def test_get_cpuinfo_empty(self):
    self.assertEqual({'vendor': 'N/A'}, self.get_cpuinfo('foo: bar'))

  def test_get_cpuinfo_exynos(self):
    self.assertEqual(
        {
            'flags': [
                'edsp',
                'fastmult',
                'half',
                'idiva',
                'idivt',
                'neon',
                'swp',
                'thumb',
                'thumbee',
                'tls',
                'vfp',
                'vfpv3',
                'vfpv4',
            ],
            'model': (0, 3087, 4),
            'name':
            'SAMSUNG EXYNOS5',
            'revision':
            '0000',
            'serial':
            '',
            'vendor':
            'ARMv7 Processor rev 4 (v7l)',
        }, self.get_cpuinfo(EXYNOS_CPU_INFO))

  def test_get_cpuinfo_cavium(self):
    self.assertEqual(
        {
            'flags': [
                'aes',
                'asimd',
                'atomics',
                'crc32',
                'evtstrm',
                'fp',
                'pmull',
                'sha1',
                'sha2',
            ],
            'model': (1, 161, 1),
            'vendor':
            'N/A',
        }, self.get_cpuinfo(CAVIUM_CPU_INFO))

  def test_get_cpuinfo_mips(self):
    self.assertEqual(
        {
            'flags': ['mips2', 'mips3', 'mips4', 'mips5', 'mips64r2'],
            'name': 'Cavium Octeon II V0.1',
        }, self.get_cpuinfo(MIPS64_CPU_INFO))

  def test_get_cpuinfo_ppc64(self):
    self.assertEqual({
        'name': 'POWER10',
    }, self.get_cpuinfo(PPC64_CPU_INFO))

  def test_get_cpuinfo_s390x(self):
    self.assertEqual({
        'name': 'S390',
    }, self.get_cpuinfo(S390X_CPU_INFO))

  def test_get_num_processors(self):
    self.assertTrue(linux.get_num_processors() != 0)


K8S_CGROUP = """
8:freezer:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
7:blkio:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
6:net_cls:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
5:memory:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
4:cpu,cpuacct:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
3:cpuset:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
2:devices:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
1:name=systemd:/k8s.io/baa2c4c148cc83e36b7f14fe9145c58b742c82b244f77e32cda50cf8f26a27a5
"""

NO_K8S_CGROUP = """
11:blkio:/init.scope
10:devices:/init.scope
9:memory:/init.scope
8:perf_event:/
7:cpuset:/
6:net_cls,net_prio:/
5:freezer:/
4:rdma:/
3:cpu,cpuacct:/init.scope
2:pids:/init.scope
1:name=systemd:/init.scope
0::/init.scope
"""


@unittest.skipUnless(sys.platform == 'linux', 'linux only test')
class TestDocker(auto_stub.TestCase):

  def setUp(self):
    super(TestDocker, self).setUp()
    tools.clear_cache_all()

  def tearDown(self):
    super(TestDocker, self).tearDown()
    tools.clear_cache_all()

  def get_inside_docker(self, text):
    self.mock(linux, '_read_cgroup', lambda: text)
    return linux.get_inside_docker()

  def test_get_inside_docker_k8s(self):
    self.assertEqual('stock', self.get_inside_docker(K8S_CGROUP))

  def test_get_inside_docker_no_k8s(self):
    self.assertEqual(None, self.get_inside_docker(NO_K8S_CGROUP))


@unittest.skipUnless(sys.platform == 'linux', 'linux only test')
class TestLinux(auto_stub.TestCase):

  def setUp(self):
    super(TestLinux, self).setUp()
    tools.clear_cache_all()
    self.mock_check_output = mock.patch('subprocess.check_output').start()

  def tearDown(self):
    super(TestLinux, self).tearDown()
    mock.patch.stopall()
    tools.clear_cache_all()

  def test_get_kernel(self):
    release = '1.2.3-4-generic'

    class mock_uname():
      def __init__(self):
        self.release = release

    self.mock(os, 'uname', mock_uname)
    self.assertEqual(linux.get_kernel(), release)

  def test_get_ssd(self):
    self.mock_check_output.return_value = textwrap.dedent("""\
      NAME    ROTA
      nvme0n1    0
    """)
    self.assertEqual(linux.get_ssd(), ('nvme0n1',))

  def test_get_gpu_nvidia(self):
    # pylint: disable=line-too-long
    self.mock_check_output.return_value = textwrap.dedent("""\
      18:00.0 "VGA compatible controller [0300]" "NVIDIA Corporation [10de]" "GP107GL [Quadro P1000] [1cb1]" -ra1 "NVIDIA Corporation [10de]" "GP107GL [Quadro P1000] [11bc]"
    """).encode()
    with mock.patch('builtins.open', mock.mock_open(read_data='440.82')):
      self.assertEqual(linux.get_gpu(),
                       (['10de', '10de:1cb1', '10de:1cb1-440.82'
                        ], ['Nvidia GP107GL [Quadro P1000] 440.82']))

  def test_get_gpu_intel(self):
    # pylint: disable=line-too-long
    self.mock_check_output.side_effect = (
        # lscpi -mm -nn
        '00:02.0 "VGA compatible controller [0300]" "Intel Corporation [8086]" "Device [9bc5]" -r05 "Dell [1028]" "Device [09a4]"'
        .encode('utf-8'),
        # dpkg -s libgl1-mesa-dri
        'Version: 23.2.1-1ubuntu3.1~22.04.2'.encode('utf-8'),
    )
    self.assertEqual(linux.get_gpu(),
                     (['8086', '8086:9bc5', '8086:9bc5-23.2.1'
                       ], ['Intel Comet Lake S UHD Graphics 630 23.2.1']))

  def test_get_gpu_amd(self):
    # pylint: disable=line-too-long
    self.mock_check_output.side_effect = (
        # lscpi -mm -nn
        '03:00.0 "VGA compatible controller [0300]" "Advanced Micro Devices, Inc. [AMD/ATI] [1002]" "Navi 14 [Radeon RX 5500/5500M / Pro 5500M] [7340]" -rc5 "Gigabyte Technology Co., Ltd [1458]" "Navi 14 [Radeon RX 5500/5500M / Pro 5500M] [2319]"'
        .encode('utf-8'),
        # dpkg -s libgl1-mesa-dri
        'Version: 23.2.1-1ubuntu3.1~22.04.2'.encode('utf-8'),
    )
    self.assertEqual(linux.get_gpu(),
                     (['1002', '1002:7340', '1002:7340-23.2.1'
                       ], ['AMD Radeon RX 5500 XT 23.2.1']))

  def test_get_mesa_version(self):
    self.mock_check_output.return_value = textwrap.dedent("""\
      Version: 1.2.3
    """).encode()
    self.assertEqual(linux._get_mesa_version(), '1.2.3')

  def test_get_device_tree_compatible(self):
    with mock.patch('builtins.open') as mock_open:
      mock_open.return_value = io.BytesIO(b'foo,bar')
      self.assertEqual(linux.get_device_tree_compatible(),
                       sorted(['foo', 'bar']))

  def test_is_display_attached_true(self):
    # Real output from a Linux machine with a display attached.
    self.mock_check_output.return_value = """\
Screen 0: minimum 320 x 200, current 1280 x 1024, maximum 16384 x 16384
DisplayPort-0 disconnected (normal left inverted right x axis y axis)
DisplayPort-1 disconnected (normal left inverted right x axis y axis)
DisplayPort-2 disconnected (normal left inverted right x axis y axis)
HDMI-A-0 connected primary 1280x1024+0+0 (normal left inverted right x axis y axis) 450mm x 360mm
   1280x1024     60.02*+  75.02
   1280x800      60.02
   1152x864      75.00    59.97
   1280x720      60.02
   1024x768      85.00    75.03    70.07    60.00
   800x600       85.06    72.19    75.00    60.32    56.25
   640x480       85.01    75.00    72.81    60.00    59.94
   720x400       70.08
"""
    self.assertTrue(linux.is_display_attached())

  def test_is_display_attached_false(self):
    # Real output from a Linux machine with a display attached, but not
    # functioning properly.
    self.mock_check_output.return_value = """\
Screen 0: minimum 320 x 200, current 1024 x 768, maximum 16384 x 16384
DisplayPort-0 disconnected primary (normal left inverted right x axis y axis)
DisplayPort-1 disconnected (normal left inverted right x axis y axis)
DisplayPort-2 disconnected (normal left inverted right x axis y axis)
HDMI-A-0 disconnected (normal left inverted right x axis y axis)
"""
    self.assertFalse(linux.is_display_attached())

  def test_is_display_attached_unknown(self):
    self.mock_check_output.side_effect = OSError('Executable not found')
    with self.assertLogs(level='ERROR') as logging_manager:
      self.assertIsNone(linux.is_display_attached())
    self.assertIn('ERROR:root:is_display_attached(): Executable not found',
                  logging_manager.output)

  def test_get_display_resolution_success(self):
    # Real output from a Linux machine with a 2560x144 display attached.
    self.mock_check_output.return_value = """\
Screen 0: minimum 320 x 200, current 4000 x 2560, maximum 16384 x 16384
DisplayPort-0 disconnected (normal left inverted right x axis y axis)
DisplayPort-1 disconnected (normal left inverted right x axis y axis)
DisplayPort-2 connected primary 2560x1440+0+518 (normal left inverted right x axis y axis) 597mm x 336mm
   2560x1440     59.95*+
   1920x1200     59.88
   1920x1080     60.00    50.00    59.94    49.95
   1600x1200     60.00
   1680x1050     59.95
   1600x900      60.00
   1280x1024     60.02
   1440x900      59.89
   1280x800      59.95
   1280x720      60.00    50.00    59.94
   1024x768      60.00
   800x600       60.32
   720x576       50.00
   720x480       60.00    59.94
   640x480       60.00    59.94
"""
    horizontal, vertical = linux.get_display_resolution()
    self.assertEqual(horizontal, 2560)
    self.assertEqual(vertical, 1440)

  def test_get_display_resolution_unknown(self):
    self.mock_check_output.return_value = ''
    self.assertIsNone(linux.get_display_resolution())

  def test_get_display_resolution_command_error(self):
    self.mock_check_output.side_effect = OSError('Executable not found')
    with self.assertLogs(level='ERROR') as logging_manager:
      self.assertIsNone(linux.get_display_resolution())
    self.assertIn('ERROR:root:get_display_resolution(): Executable not found',
                  logging_manager.output)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
