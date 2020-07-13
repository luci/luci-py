#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import subprocess
import sys


def run_tests(test_cmds, python3=False):
  """Run tests sequentially"""
  run_cnt = 0
  skipped_cmds = []
  failed_cmds = []
  for cmd in test_cmds:
    _exit_code, skipped = _run_test(cmd, python3=python3)
    if skipped:
      skipped_cmds.append(cmd)
      continue

    if _exit_code:
      failed_cmds.append(cmd)

    run_cnt += 1

  print('\n-------------------------------------------------------------------')
  print('Ran %d test files, Skipped %d test files' %
        (run_cnt, len(skipped_cmds)))

  if len(skipped_cmds) > 0:
    print('\nSkipped tests:')
    for t in skipped_cmds:
      print(' - %s' % t)

  if len(failed_cmds) > 0:
    print('\nFailed tests:')
    for t in failed_cmds:
      print(' - %s' % t)
    print('\nFAILED')
    return 1

  print('\nOK')
  return 0


def _run_test(cmd, python3=False):
  if python3 and not _has_py3_shebang(cmd[0]):
    print('Skipping test in python3: %s' % cmd)
    return 0, True

  # vpython
  vpython = 'vpython'
  if python3:
    vpython += '3'

  cmd = [vpython] + cmd
  shell = False
  if sys.platform == 'win32':
    shell = True

  print('Running test script: %r' % cmd)
  return subprocess.call(cmd, shell=shell), False


def _has_py3_shebang(path):
  with open(path, 'r') as f:
    maybe_shebang = f.readline()
  return maybe_shebang.startswith('#!') and 'python3' in maybe_shebang
