#!/usr/bin/env vpython3
# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os
import signal
import subprocess
import sys
import unittest

import six

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@unittest.skipIf(
    sys.platform == 'win32',
    'TODO(crbug.com/1017545): signal object has no attribute SIGUSR1')
class Test(unittest.TestCase):

  def _run(self, cmd, sig, stdin):
    p = subprocess.Popen([sys.executable, '-u', '-c', cmd],
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         cwd=THIS_DIR)
    p.stdout.read(1)
    os.kill(p.pid, sig)
    # Wait for some output before calling communicate(), otherwise there's a
    # race condition with SIGUSR2.
    if stdin:
      e = p.stderr.read(1)
    else:
      e = b''
    out, err = p.communicate(input=stdin)
    return out, e + err

  def test_SIGUSR1(self):
    # The simple case
    cmd = ('import signal_trace,sys,time; signal_trace.register(); '
           'sys.stdout.write("1"); sys.stdout.flush(); time.sleep(60)')
    out, err = self._run(cmd, signal.SIGUSR1, None)
    self.assertEqual(b'', out)
    self.assertEqual(
        b'ERROR:root:\n'
        b'** SIGUSR1 received **\n'
        b'MainThread:\n'
        b'  File "<string>", line 1, in <module>\n'
        b'** SIGUSR1 end **\n', err)

  def test_SIGUSR1_threads(self):
    # The multithreaded case.
    cmd = ('import signal_trace,sys,time,threading; signal_trace.register(); '
           't=threading.Thread(target=time.sleep, args=(60,), name="Awesome"); '
           't.daemon=True; t.start(); '
           'sys.stdout.write("1"); sys.stdout.flush(); time.sleep(60)')
    out, err = self._run(cmd, signal.SIGUSR1, None)
    self.assertEqual(b'', out)
    self.assertTrue(
        err.startswith(b'ERROR:root:\n** SIGUSR1 received **\nAwesome:\n  '),
        repr(err))
    self.assertTrue(err.endswith(b'\n** SIGUSR1 end **\n'), repr(err))
    self.assertIn(b'MainThread:', err.splitlines())

  @unittest.skipIf(six.PY3, "crbug.com/1010816")
  def test_SIGUSR2(self):
    cmd = ('import signal_trace,sys,time; signal_trace.register(); '
           'sys.stdout.write("1"); sys.stdout.flush(); time.sleep(60)')
    out, err = self._run(cmd, signal.SIGUSR2, b'exit()\n')
    self.assertEqual(b'>>> ', out)
    self.assertTrue(
        err.startswith(
            b'Signal received : entering python shell.\nTraceback:\n'),
        repr(err))


if __name__ == '__main__':
  os.chdir(THIS_DIR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
