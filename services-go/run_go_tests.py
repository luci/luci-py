#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs all go test found in every subdirectories."""

import optparse
import os
import signal
import subprocess
import sys
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def find_all_testable_packages(root_dir):
  """Finds all the directories containing go tests, excluding third_party."""
  directories = set()
  for root, dirs, files in os.walk(root_dir):
    if any(f.endswith('_test.go') for f in files):
      directories.add(root)
    for i in dirs[:]:
      if i.startswith('.') or i == 'third_party':
        dirs.remove(i)
  return directories


def precompile(cmd, directory):
  """Prebuilds the dependencies of the tests to get rid of annoying messages.

  It slows the whole thing down a tad but not significantly.
  """
  try:
    with open(os.devnull, 'wb') as f:
      p = subprocess.Popen(
          cmd + ['-i'], cwd=directory, stdout=f, stderr=subprocess.STDOUT)
  except OSError as e:
    pass
  p.wait()


def timed_call(cmd, cwd, piped):
  """Calls a subprocess and returns the time it took.

  Take extra precautions to kill grand-children processes.
  """
  start = time.time()
  kwargs = {}
  if piped:
    kwargs = dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  p = subprocess.Popen(cmd, cwd=cwd, preexec_fn=os.setsid, **kwargs)
  out = p.communicate()[0]
  # Kill any grand-child processes that may be left hanging around.
  try:
    os.killpg(p.pid, signal.SIGKILL)
  except OSError:
    pass
  p.wait()
  duration = time.time() - start
  return p.returncode, duration, out


def run_test_directory(cmd, verbose, directory, align, offset):
  """Runs a single test directory.

  In Go, all the *_test.go in a directory collectively make up the unit test for
  this package. So a "directory" is a "test".
  """
  if verbose:
    # In this case, do not capture output, it is directly output to stdout.
    sys.stdout.write('%-*s ...\n' % (align, directory[offset:]))
    sys.stdout.flush()
    returncode, duration, _ = timed_call(cmd, directory, False)
    if returncode:
      sys.stdout.write('\n... FAILED  %3.1fs\n' % duration)
    else:
      sys.stdout.write('... SUCCESS %3.1fs\n\n' % duration)
  else:
    # Capture the output but only print it in the case the test failed.
    sys.stdout.write('%-*s ... ' % (align, directory[offset:]))
    sys.stdout.flush()
    returncode, duration, out = timed_call(cmd, directory, True)
    if returncode:
      sys.stdout.write('FAILED  %3.1fs\n' % duration)
      sys.stdout.flush()
      sys.stdout.write(out)
      sys.stdout.write('\n')
    else:
      sys.stdout.write('SUCCESS %3.1fs\n' % duration)
  sys.stdout.flush()
  return returncode


def run_tests(cmd, verbose):
  """Runs all the Go tests in all the subdirectories of ROOT_DIR."""
  directories = find_all_testable_packages(ROOT_DIR)
  # offset and align are used to align test names in run_test_directory(), so
  # all test names are properly aligned.
  offset = len(ROOT_DIR) + 1
  align = max(map(len, directories)) - offset
  for directory in sorted(directories):
    precompile(cmd, directory)

  # TODO(maruel): Run them concurrently.
  for directory in sorted(directories):
    returncode = run_test_directory(cmd, verbose, directory, align, offset)
    if returncode:
      # Quit early, life is short.
      return returncode

  return 0


def main():
  parser = optparse.OptionParser(description=sys.modules[__name__].__doc__)
  parser.add_option('--gae', action='store_true')
  parser.add_option('-v', '--verbose', action='store_true')
  options, args = parser.parse_args()

  if options.gae:
    # TODO(maruel): Google AppEngine's 'goapp test' fails in some specific
    # circumstances when -v is not provided. Remove '-v' once this is working
    # again without hanging. http://b/12315827.
    cmd = [
        os.path.join(ROOT_DIR, 'tools', 'goapp.py'),
        'test',
        '-v',
    ]
  else:
    cmd = ['go', 'test']
    if options.verbose:
      cmd.append('-v')
  return run_tests(cmd, options.verbose or options.gae)


if __name__ == '__main__':
  sys.exit(main())
