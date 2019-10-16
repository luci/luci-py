#!/usr/bin/env python
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# TODO(jwata) move test steps to luci_py recipe

import os
import subprocess
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def run_unittests(attrs, python3):
  # vpython
  vpython = 'vpython'
  if python3:
    vpython += '3'

  # test.py
  testpy = os.path.join(ROOT_DIR, 'test.py')

  # filtering attributes
  if not attrs:
    attrs = ['!no_run']
  else:
    attrs.append('!no_run')
  attrs_opt = ','.join(attrs)

  cmd = [vpython, testpy, '-v', '-A', attrs_opt]

  if python3:
    # enable py3filter nose2 plugin
    cmd.append('--python3')

  print('Running test script: %r' % cmd)
  return subprocess.call(cmd)


def main():
  exit_code = 0

  for py3 in [False, True]:
    exit_code = run_unittests(['!run_later'], py3) or exit_code
    exit_code = run_unittests(['run_later'], py3) or exit_code

  return exit_code


if __name__ == '__main__':
  sys.exit(main())
