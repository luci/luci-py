#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import argparse
import logging
import os
import sys
import unittest

import six
from nose2 import discover

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
LUCI_DIR = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
PLUGINS_DIR = os.path.join(THIS_DIR, 'nose2_plugins')
CLIENT_THIRD_PARTY_DIR = os.path.join(LUCI_DIR, 'client', 'third_party')


def run_tests(python3=False, plugins=None):
  """Discover unittests and run them using nose2"""
  hook_args()

  # plugins
  if plugins is None:
    plugins = []
  if python3:
    plugins.append('py3filter')

  # fix_encoding
  sys.path.insert(0, CLIENT_THIRD_PARTY_DIR)
  from depot_tools import fix_encoding
  fix_encoding.fix_encoding()

  # add nose2 plugin dir to path
  sys.path.insert(0, PLUGINS_DIR)
  result = discover(plugins=plugins, exit=False).result
  if not result.wasSuccessful():
    return 1
  return 0


def hook_args():
  parser = argparse.ArgumentParser(add_help=False)
  parser.add_argument('-v', '--verbose', action='store_true')
  parser.add_argument('--log-level')
  args, _ = parser.parse_known_args()

  if args.verbose:
    unittest.TestCase.maxDiff = None

  if not args.log_level:
    # override default log level
    logging.basicConfig(level=logging.CRITICAL)


if __name__ == '__main__':
  sys.exit(run_tests(python3=six.PY3))
