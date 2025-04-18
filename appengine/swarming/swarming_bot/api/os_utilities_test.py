#!/usr/bin/env vpython3
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import math
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

import mock

import test_env_api
test_env_api.setup_test_env()

from api import platforms
from depot_tools import auto_stub
from utils import file_path

# Disable caching before importing os_utilities.
from utils import tools
tools.cached = lambda func: func

import os_utilities


class TestOsUtilities(auto_stub.TestCase):

  def test_get_os_name(self):
    expected = ('Debian', 'Linux', 'Mac', 'Raspbian', 'Ubuntu', 'Windows')
    self.assertIn(os_utilities.get_os_name(), expected)

  def test_get_os_name_cases(self):
    cases = [
        ('openbsd7', 'openbsd'),
        ('netbsd9', 'netbsd'),
        ('sunos5', 'Solaris'),
    ]
    for pform, result in cases:
      self.mock(sys, "platform", pform)
      self.assertEqual(os_utilities.get_os_name(), result)

  def test_get_cpu_type(self):
    cases = [
        ('x86_64', 'amd64', 'x86'),
        ('amd64', 'amd64', 'x86'),
        ('i686', 'i686', 'x86'),
        ('i86pc', None, 'x86'),
        ('aarch64', 'arm64', 'arm64'),
        ('mips64', None, 'mips'),
        ('arm64', None, 'arm64'),
    ]
    for machine, win_cpu_type, expected in cases:
      self.mock(platform, 'machine', lambda: machine)
      if sys.platform == 'win32':
        self.mock(platforms.win, 'get_cpu_type_with_wmi', lambda: win_cpu_type)
      self.assertEqual(os_utilities.get_cpu_type(), expected)

  def test_get_cipd_architecture(self):
    cases = [
        ('x86', '64', 'amd64'),
        ('x86', '32', '386'),
        ('arm64', '64', 'arm64'),
        ('armv7l', '32', 'armv6l'),
        ('powerpc64', '64', 'ppc64'),
        ('evbarm', '64', 'arm64'),
        ('evbarm', '32', 'armv6l'),
        ('riscv', '64', 'riscv64'),
        ('loongarch64', '64', 'loong64'),
        ('s390x', '64', 's390x'),
    ]
    for cpu_type, bitness, expected in cases:
      self.mock(os_utilities, 'get_cpu_type', lambda: cpu_type)
      self.mock(os_utilities, 'get_cpu_bitness', lambda: bitness)
      self.assertEqual(os_utilities.get_cipd_architecture(), expected)

  def test_get_cipd_architecture_netbsd(self):
    cases = [
        ('amd64', 'x86_64', 'amd64'),
        ('evbarm', 'aarch64', 'arm64'),
        ('evbarm', 'earmv6hf', 'armv6l'),
        ('evbarm', 'earmv7hf', 'armv7l'),
    ]
    for machine, processor, expected in cases:
      self.mock(sys, 'platform', 'netbsd10')
      self.mock(platform, 'machine', lambda: machine)
      self.mock(platform, 'processor', lambda: processor)
      self.assertEqual(os_utilities.get_cipd_architecture(), expected)

  def test_get_os_values(self):
    cases = [
        ('openbsd7', '7.2', ['openbsd', 'openbsd-7', 'openbsd-7.2']),
        ('netbsd9', '9.3_STABLE',
         ['netbsd', 'netbsd-9', 'netbsd-9.3', 'netbsd-9.3_STABLE']),
        ('sunos5', '5.11', ['Solaris', 'Solaris-5', 'Solaris-5.11']),
    ]
    for pform, rel, result in cases:
      self.mock(sys, "platform", pform)
      self.mock(platform, "release", lambda: rel)
      self.assertEqual(os_utilities.get_os_values(), result)

  @unittest.skipUnless(sys.platform == 'linux', 'this is only for linux')
  def test_get_os_values_linux(self):
    with mock.patch(
        'platforms.linux.get_os_version_number', lambda: '16.04.6'), mock.patch(
            '%s.get_os_name' % os_utilities.__name__, lambda: 'Ubuntu'):
      self.assertEqual(
          os_utilities.get_os_values(),
          ['Linux', 'Ubuntu', 'Ubuntu-16', 'Ubuntu-16.04', 'Ubuntu-16.04.6'])

  @unittest.skipUnless(sys.platform == 'darwin', 'this is only for Mac')
  def test_get_os_values_mac(self):
    with mock.patch(
        'platforms.osx.get_os_version_number',
        return_value='10.15.5'), mock.patch(
            'platforms.osx.get_os_build_version', return_value='19F101'):
      self.assertEqual(
          os_utilities.get_os_values(),
          ['Mac', 'Mac-10', 'Mac-10.15', 'Mac-10.15.5', 'Mac-10.15.5-19F101'])

  def test_get_cpu_bitness(self):
    expected = ('32', '64')
    self.assertIn(os_utilities.get_cpu_bitness(), expected)

  def test_get_cpu_dimensions(self):
    values = os_utilities.get_cpu_dimensions()
    self.assertGreater(len(values), 1)

  def test_get_cpu_dimensions_mips(self):
    self.mock(sys, 'platform', 'linux')
    self.mock(platform, 'machine', lambda: 'mips64')
    self.mock(os_utilities,
              'get_cpuinfo', lambda: {'name': 'Cavium Octeon II V0.1'})
    self.mock(sys, 'maxsize', 2**31 - 1)
    self.assertEqual(os_utilities.get_cpu_dimensions(),
                     ['mips', 'mips-32', 'mips-32-Cavium_Octeon_II_V0.1'])

  def test_get_cpu_dimensions_aix_ppc64(self):
    self.mock(sys, 'platform', 'aix')
    self.mock(sys, 'maxsize', 2**63 - 1)
    # A possible machine ID reported by python on aix.
    self.mock(platform, 'machine', lambda: '00FAC25F4B00')
    self.mock(os_utilities, 'get_cpuinfo', lambda: {'name': 'POWER8'})
    self.assertEqual(os_utilities.get_cpu_dimensions(),
                     ['ppc64', 'ppc64-64', 'ppc64-64-POWER8'])

  def test_get_cpu_dimensions_ppc64(self):
    self.mock(sys, 'platform', 'linux')
    self.mock(platform, 'machine', lambda: 'ppc64')
    self.mock(os_utilities, 'get_cpuinfo', lambda: {'name': 'POWER8'})
    self.mock(sys, 'maxsize', 2**63 - 1)
    self.assertEqual(os_utilities.get_cpu_dimensions(),
                     ['ppc64', 'ppc64-64', 'ppc64-64-POWER8'])

  def test_get_cpu_dimensions_ppc64le(self):
    self.mock(sys, 'platform', 'linux')
    self.mock(platform, 'machine', lambda: 'ppc64le')
    self.mock(os_utilities, 'get_cpuinfo', lambda: {'name': 'POWER10'})
    self.mock(sys, 'maxsize', 2**63 - 1)
    self.assertEqual(os_utilities.get_cpu_dimensions(),
                     ['ppc64le', 'ppc64le-64', 'ppc64le-64-POWER10'])

  def test_parse_intel_model(self):
    examples = [
        ('Intel(R) Core(TM) i5-5200U CPU @ 2.20GHz', 'i5-5200U'),
        ('Intel(R) Core(TM) i7-2635QM CPU @ 2.00GHz', 'i7-2635QM'),
        ('Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz', 'i7-4578U'),
        ('Intel(R) Core(TM)2 Duo CPU     P8600  @ 2.40GHz', 'P8600'),
        ('Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz', 'i7-4870HQ'),
        ('Intel(R) Core(TM) i7-6700T CPU @ 2.80GHz', 'i7-6700T'),
        ('Intel(R) Pentium(R) CPU  N3710  @ 1.60GHz', 'N3710'),
        ('Intel(R) Xeon(R) CPU E3-1220 V2 @ 3.10GHz', 'E3-1220 V2'),
        ('Intel(R) Xeon(R) CPU E3-1230 v3 @ 3.30GHz', 'E3-1230 v3'),
        ('Intel(R) Xeon(R) CPU E5-2670 0 @ 2.60GHz', 'E5-2670'),
        ('Intel(R) Xeon(R) CPU E5-2697 v2 @ 2.70GHz', 'E5-2697 v2'),
        # As generated by platforms.gce.get_cpuinfo():
        ('Intel(R) Xeon(R) CPU Sandy Bridge GCE', 'Sandy Bridge GCE'),
        ('Intel(R) Xeon(R) CPU @ 2.30GHz', None),
    ]
    for i, expected in examples:
      actual = os_utilities._parse_intel_model(i)
      self.assertEqual(expected, actual)

  def test_get_python_versions_py3(self):
    versions = os_utilities.get_python_versions()
    self.assertEqual(len(versions), 3)
    self.assertEqual(versions[0], '3')
    self.assertEqual(versions[1], '3.11')
    # we don't know which micro version we use in test.
    self.assertRegex(versions[2], '3.11.[0-9]+')

  def test_get_ip(self):
    ip = os_utilities.get_ip()
    self.assertNotEqual('127.0.0.1', ip)
    ipv4 = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
    ipv6 = r'^%s$' % ':'.join([r'[0-9a-f]{1,4}'] * 8)
    self.assertTrue(re.match(ipv4, ip) or re.match(ipv6, ip), ip)

  def test_get_num_processors(self):
    self.assertGreater(os_utilities.get_num_processors(), 0)

  def test_get_physical_ram(self):
    self.assertGreater(os_utilities.get_physical_ram(), 0)

  def test_get_disks_info(self):
    info = os_utilities.get_disks_info()
    self.assertGreater(len(info), 0)
    root_path = 'c:\\' if sys.platform == 'win32' else '/'
    root = info[root_path]
    # Round the same way.
    free_disk = round(
        float(file_path.get_free_space(root_path)) / 1024. / 1024., 1)
    delta = math.fabs(free_disk - root['free_mb'])
    # Check that they are mostly equal. There can be some gitter as there is
    # disk I/O during the two calls.
    self.assertLess(delta, 2., (delta, free_disk, root['free_mb']))

  def test_get_gpu(self):
    actual = os_utilities.get_gpu()
    self.assertTrue(actual is None or actual)

  def test_get_dimensions(self):
    dimensions = os_utilities.get_dimensions()
    for key, values in dimensions.items():
      self.assertIsInstance(key, str)
      self.assertIsInstance(values, list)
      for value in values:
        self.assertIsInstance(value, str)
    actual = set(dimensions)
    # Only set when the process is running in a properly configured GUI context.
    actual.discard('locale')
    # Only set on machines with SSD.
    actual.discard('ssd')
    # There are cases where this dimension is not set.
    actual.discard('machine_type')
    # Only set on ARM Linux machines.
    actual.discard('device_tree_compatible')
    # Only set on bare metal Linux machines.
    actual.discard('cpu_governor')
    # Only set on Windows machines.
    actual.discard('visual_studio_version')
    # Only set on Windows machines.
    actual.discard('windows_client_version')

    expected = {
        'cipd_platform', 'cores', 'cpu', 'gce', 'gpu', 'id', 'inside_docker',
        'os', 'pool', 'python'
    }
    if platforms.is_gce():
      expected.add('gcp')
      expected.add('image')
      expected.add('zone')
    if sys.platform == 'darwin':
      expected.add('mac_model')
      expected.add('display_attached')
      # Bot may not have HiDPI and Xcode preinstalled
      actual.discard('hidpi')
      actual.discard('xcode_version')
      actual.discard('device')  # iOS devices
    if sys.platform == 'linux':
      expected.add('kernel')
      expected.add('kvm')
      expected.add('display_attached')
    if sys.platform == 'win32':
      expected.add('integrity')
      expected.add('display_attached')
    self.assertEqual(expected, actual)

  def test_override_id_via_env(self):
    mock_env = os.environ.copy()
    mock_env['SWARMING_BOT_ID'] = 'customid'
    self.mock(os, 'environ', mock_env)
    dimensions = os_utilities.get_dimensions()
    self.assertIsInstance(dimensions['id'], list)
    self.assertEqual(len(dimensions['id']), 1)
    self.assertIsInstance(dimensions['id'][0], str)
    self.assertEqual(dimensions['id'][0], 'customid')

  def test_get_state(self):
    actual = os_utilities.get_state()
    actual.pop('networks', None)
    actual.pop('reboot_required', None)
    actual.pop('temp', None)
    expected = {
        'audio',
        'cost_usd_hour',
        'cpu_name',
        'cwd',
        'disks',
        'display_resolution',
        'env',
        'gpu',
        'ip',
        'hostname',
        'nb_files_in_temp',
        'pid',
        'python',
        'ram',
        'running_time',
        'ssd',
        'started_ts',
        'uptime',
        'user',
    }
    if sys.platform in ('cygwin', 'win32'):
      expected.add('cygwin')
      expected.add('active_displays')
    if sys.platform == 'darwin':
      expected.add('xcode')
    if 'quarantined' in actual:
      self.fail(actual['quarantined'])
    self.assertEqual(expected, set(actual))

  def test_get_hostname_gce_docker(self):
    self.mock(platforms, 'is_gce', lambda: True)
    self.mock(os.path, 'isfile', lambda _: True)
    self.mock(socket, 'getfqdn', lambda: 'dockerhost')
    self.assertEqual(os_utilities.get_hostname(), 'dockerhost')

  def test_get_hostname_gce_nodocker(self):
    self.mock(platforms, 'is_gce', lambda: True)
    self.mock(os.path, 'isfile', lambda _: False)
    manual_mock = not hasattr(platforms, 'gce')
    if manual_mock:
      # On macOS.
      class Mock(object):
        def get_metadata(self):
          return None
      platforms.gce = Mock()
    try:
      self.mock(platforms.gce, 'get_metadata',
                lambda: {'instance': {'hostname': 'gcehost'}})
      self.assertEqual(os_utilities.get_hostname(), 'gcehost')
    finally:
      if manual_mock:
        del platforms.gce

  def test_get_hostname_nogce(self):
    self.mock(platforms, 'is_gce', lambda: False)
    self.mock(os.path, 'isfile', lambda _: False)
    self.mock(socket, 'getfqdn', lambda: 'somehost')
    self.assertEqual(os_utilities.get_hostname(), 'somehost')

  def test_get_hostname_macos(self):
    self.mock(platforms, 'is_gce', lambda: False)
    self.mock(os.path, 'isfile', lambda _: False)
    self.mock(socket, 'getfqdn', lambda: 'somehost.in-addr.arpa')
    self.mock(socket, 'gethostname', lambda: 'somehost')
    self.assertEqual(os_utilities.get_hostname(), 'somehost')

  def test_setup_auto_startup_win(self):
    # TODO(maruel): Figure out a way to test properly.
    pass

  def test_setup_auto_startup_osx(self):
    # TODO(maruel): Figure out a way to test properly.
    pass

  def test_host_reboot(self):
    class Foo(Exception):
      pass

    def raise_exception(x):
      raise x

    self.mock(subprocess, 'check_call', lambda _: None)
    self.mock(time, 'sleep', lambda _: raise_exception(Foo()))
    self.mock(logging, 'error', lambda *_: None)
    with self.assertRaises(Foo):
      os_utilities.host_reboot()

  def test_host_reboot_and_return(self):
    self.mock(subprocess, 'check_call', lambda _: None)
    self.assertIs(True, os_utilities.host_reboot_and_return())

  def test_host_reboot_and_return_with_message(self):
    self.mock(subprocess, 'check_call', lambda _: None)
    self.assertIs(True, os_utilities.host_reboot_and_return(message='Boo'))

  def test_host_reboot_with_timeout(self):
    self.mock(subprocess, 'check_call', lambda _: None)
    self.mock(logging, 'error', lambda *_: None)

    now = [0]
    def mock_sleep(dt):
      now[0] += dt
    self.mock(time, 'sleep', mock_sleep)
    self.mock(time, 'time', lambda: now[0])

    self.assertFalse(os_utilities.host_reboot(timeout=60))
    self.assertEqual(time.time(), 60)

  def test_get_os_version_parts(self):
    version = '17.5.1'
    os_name = 'iOS'
    expected = ['iOS-17', 'iOS-17.5', 'iOS-17.5.1']
    self.assertEqual(os_utilities.get_os_version_parts(version, os_name),
                     expected)

    version = '15.2'
    os_name = 'Mac'
    expected = ['Mac-15', 'Mac-15.2']
    self.assertEqual(os_utilities.get_os_version_parts(version, os_name),
                     expected)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
