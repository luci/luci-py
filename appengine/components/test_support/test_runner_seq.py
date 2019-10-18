#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import subprocess


def run_tests(test_files, python3=False):
  """Run tests sequentially"""
  run_cnt = 0
  skipped_tests = []
  failed_tests = []
  for test_file in test_files:
    _exit_code, skipped = _run_test(test_file, python3=python3)
    if skipped:
      skipped_tests.append(test_file)
      continue

    if _exit_code:
      failed_tests.append(test_file)

    run_cnt += 1

  print('\n-------------------------------------------------------------------')
  print('Ran %d test files, Skipped %d test files' %
        (run_cnt, len(skipped_tests)))

  if len(skipped_tests) > 0:
    print('\nSkipped tests:')
    for t in skipped_tests:
      print(' - %s' % t)

  if len(failed_tests) > 0:
    print('\nFailed tests:')
    for t in failed_tests:
      print(' - %s' % t)
    print('\nFAILED')
    return 1

  print('\nOK')
  return 0


def _run_test(test_file, python3=False):
  if python3 and not _has_py3_shebang(test_file):
    print('Skipping test in python3: %s' % test_file)
    return 0, True

  # vpython
  vpython = 'vpython'
  if python3:
    vpython += '3'

  cmd = [vpython, test_file, '-v']

  print('Running test script: %r' % cmd)
  return subprocess.call(cmd), False


def _has_py3_shebang(path):
  with open(path, 'r') as f:
    maybe_shebang = f.readline()
  return maybe_shebang.startswith('#!') and 'python3' in maybe_shebang
