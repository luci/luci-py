#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Integration test for the Swarming server."""

import Queue
import optparse
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CHECKOUT_DIR = os.path.dirname(os.path.dirname(APP_DIR))
CLIENT_DIR = os.path.join(CHECKOUT_DIR, 'client')
ISOLATE_SCRIPT = os.path.join(CLIENT_DIR, 'isolate.py')
SWARMING_SCRIPT = os.path.join(CLIENT_DIR, 'swarming.py')


def gen_isolate(isolate, script):
  """Archives a script to `isolate` server."""
  tmp = tempfile.mkdtemp(prefix='swarming_smoke')
  try:
    with open(os.path.join(tmp, 'script.py'), 'wb') as f:
      f.write(script)
    path = os.path.join(tmp, 'script.isolate')
    with open(path, 'wb') as f:
      f.write(
          "{'variables': {\n"
          "'command': ['python', 'script.py'], 'files': ['script.py']\n"
          "}}")
    isolated = os.path.join(tmp, 'script.isolated')
    out = subprocess.check_output(
        [ISOLATE_SCRIPT, 'archive', '-I', isolate, '-i', path, '-s', isolated])
    return out.split(' ', 1)[0]
  finally:
    shutil.rmtree(tmp)


def capture(cmd, **kwargs):
  """Captures output and return exit code."""
  proc = subprocess.Popen(
      cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
  out = proc.communicate()[0]
  return out, proc.returncode


def test_normal(swarming, isolate, extra_flags):
  """Runs a normal task that succeeds."""
  h = gen_isolate(isolate, 'print(\'SUCCESS\')')
  subprocess.check_output(
      [SWARMING_SCRIPT, 'run', '-S', swarming, '-I', isolate, h] + extra_flags)
  return 'SUCCESS'


def test_expiration(swarming, isolate, extra_flags):
  """Schedule a task that cannot be scheduled and expire."""
  h = gen_isolate(isolate, 'print(\'SUCCESS\')')
  start = time.time()
  out, exitcode = capture(
      [
        SWARMING_SCRIPT, 'run', '-S', swarming, '-I', isolate, h,
        '--expiration', '30', '-d', 'invalid', 'always',
      ] + extra_flags)
  duration = time.time() - start
  if exitcode != 1:
    return 'Unexpected exit code: %d' % exitcode
  # TODO(maruel): Shouldn't take more than a minute or so.
  if duration < 30 or duration > 120:
    return 'Unexpected expiration timeout: %d\n%s' % (duration, out)
  return 'SUCCESS'


def test_io_timeout(swarming, isolate, extra_flags):
  """Runs a task that triggers IO timeout."""
  h = gen_isolate(
      isolate,
      'import time\n'
      'print(\'SUCCESS\')\n'
      'time.sleep(40)\n'
      'print(\'FAILURE\')')
  start = time.time()
  out, exitcode = capture(
      [
        SWARMING_SCRIPT, 'run', '-S', swarming, '-I', isolate, h,
        '--io-timeout', '30',
      ] + extra_flags)
  duration = time.time() - start
  if exitcode != 1:
    return 'Unexpected exit code: %d\n%s' % (exitcode, out)
  if duration < 30:
    return 'Unexpected fast execution: %d' % duration
  return 'SUCCESS'


def test_hard_timeout(swarming, isolate, extra_flags):
  """Runs a task that triggers hard timeout."""
  h = gen_isolate(
      isolate,
      'import time\n'
      'for i in xrange(6):'
      '  print(\'.\')\n'
      '  time.sleep(10)\n')
  start = time.time()
  out, exitcode = capture(
      [
        SWARMING_SCRIPT, 'run', '-S', swarming, '-I', isolate, h,
        '--hard-timeout', '30',
      ] + extra_flags)
  duration = time.time() - start
  if exitcode != 1:
    return 'Unexpected exit code: %d\n%s' % (exitcode, out)
  if duration < 30:
    return 'Unexpected fast execution: %d' % duration
  return 'SUCCESS'


def get_all_tests():
  m = sys.modules[__name__]
  return {k[5:]: getattr(m, k) for k in dir(m) if k.startswith('test_')}


def run_test(results, swarming, isolate, extra_flags, name, test_case):
  start = time.time()
  try:
    result = test_case(swarming, isolate, extra_flags)
  except Exception as e:
    result = e
  results.put((name, result, time.time() - start))


def main():
  parser = optparse.OptionParser()
  parser.add_option('-S', '--swarming', help='Swarming server')
  parser.add_option('-I', '--isolate-server', help='Isolate server')
  parser.add_option('-d', '--dimensions', nargs=2, default=[], action='append')
  options, args = parser.parse_args()

  if args:
    parser.error('Unsupported args: %s' % args)
  if not options.swarming:
    parser.error('--swarming required')
  if not options.isolate_server:
    parser.error('--isolate-server required')

  if not os.path.isfile(SWARMING_SCRIPT):
    parser.error('Invalid checkout, %s does not exist' % SWARMING_SCRIPT)

  extra_flags = ['--priority', '5', '--tags', 'smoke_test:1']
  for k, v in options.dimensions or [('os', 'Linux')]:
    extra_flags.extend(('-d', k, v))

  # Run all the tests in parallel.
  tests = get_all_tests()

  results = Queue.Queue(maxsize=len(tests))

  for name, fn in sorted(tests.iteritems()):
    t = threading.Thread(
        target=run_test, name=name,
        args=(results, options.swarming, options.isolate_server, extra_flags,
              name, fn))
    t.start()

  print('%d tests started' % len(tests))
  maxlen = max(len(name) for name in tests)
  for i in xrange(len(tests)):
    name, result, duration = results.get()
    print('[%d/%d] %-*s: %4.1fs: %s' %
        (i, len(tests), maxlen, name, duration, result))

  return 0


if __name__ == '__main__':
  sys.exit(main())
