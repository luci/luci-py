#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import os
import re
import subprocess
import sys
import time
import unittest

import test_env_api
test_env_api.setup_test_env()

from depot_tools import auto_stub

import os_utilities


class TestOsUtilities(auto_stub.TestCase):
  def test_get_os_version(self):
    version = os_utilities.get_os_version_number()
    self.assertTrue(version)
    self.assertTrue(re.match(r'^\d+\.\d+$', version), version)

  def test_get_os_version_names(self):
    name = os_utilities.get_os_version_name()
    if sys.platform == 'win32':
      self.assertTrue(isinstance(name, str), name)
    else:
      self.assertEqual(None, name)

  def test_get_os_name(self):
    expected = (u'Linux', u'Mac', u'Raspbian', u'Ubuntu', u'Windows')
    self.assertIn(os_utilities.get_os_name(), expected)

  def test_get_cpu_type(self):
    actual = os_utilities.get_cpu_type()
    if actual == u'x86':
      return
    self.assertTrue(actual.startswith(u'arm'), actual)

  def test_get_cpu_bitness(self):
    expected = (u'32', u'64')
    self.assertIn(os_utilities.get_cpu_bitness(), expected)

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
    self.assertGreater(len(os_utilities.get_disks_info()), 0)

  def test_get_gpu(self):
    actual = os_utilities.get_gpu()
    self.assertTrue(actual is None or actual)

  def test_get_dimensions(self):
    actual = set(os_utilities.get_dimensions())
    # Only set on GCE.
    actual.discard(u'machine_type')
    actual.discard(u'zone')
    # Only set on Mac.
    actual.discard(u'hidpi')
    expected = {u'cores', u'cpu', u'gpu', u'id', u'os', u'pool'}
    self.assertEqual(expected, actual)

  def test_get_state(self):
    actual = os_utilities.get_state()
    actual.pop('temp', None)
    expected = {
      u'audio', u'cost_usd_hour', u'cpuinfo', u'cwd', u'disks', u'gpu', u'ip',
      u'hostname', u'locale', u'nb_files_in_temp', u'pid', u'ram',
      u'running_time', u'started_ts', u'uptime', u'user',
    }
    if sys.platform in ('cygwin', 'win32'):
      expected.add(u'cygwin')
    if sys.platform == 'darwin':
      expected.add(u'model')
    if sys.platform == 'win32':
      expected.add(u'integrity')
    self.assertEqual(expected, set(actual))

  def test_setup_auto_startup_win(self):
    # TODO(maruel): Figure out a way to test properly.
    pass

  def test_setup_auto_startup_osx(self):
    # TODO(maruel): Figure out a way to test properly.
    pass

  def test_restart(self):
    class Foo(Exception):
      pass

    def raise_exception(x):
      raise x

    self.mock(subprocess, 'check_call', lambda _: None)
    self.mock(time, 'sleep', lambda _: raise_exception(Foo()))
    self.mock(logging, 'error', lambda *_: None)
    with self.assertRaises(Foo):
      os_utilities.restart()

  def test_restart_and_return(self):
    self.mock(subprocess, 'check_call', lambda _: None)
    self.assertIs(True, os_utilities.restart_and_return())

  def test_restart_and_return_with_message(self):
    self.mock(subprocess, 'check_call', lambda _: None)
    self.assertIs(True, os_utilities.restart_and_return(message='Boo'))

  def test_restart_with_timeout(self):
    self.mock(subprocess, 'check_call', lambda _: None)
    self.mock(logging, 'error', lambda *_: None)

    now = [0]
    def mock_sleep(dt):
      now[0] += dt
    self.mock(time, 'sleep', mock_sleep)
    self.mock(time, 'time', lambda: now[0])

    self.assertFalse(os_utilities.restart(timeout=60))
    self.assertEqual(time.time(), 60)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  unittest.main()
