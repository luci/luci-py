#!/usr/bin/env vpython
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os
import sys

import six
from nose2 import discover

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
LUCI_DIR = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
PLUGINS_DIR = os.path.join(THIS_DIR, 'nose2_plugins')
CLIENT_THIRD_PARTY_DIR = os.path.join(LUCI_DIR, 'client', 'third_party')


def run_tests(python3=False):
  """Discover unittests and run them using nose2"""
  # override default log level
  if not _has_arg(sys.argv, '--log-level'):
    logging.basicConfig(level=logging.CRITICAL)

  plugins = []
  if python3:
    plugins.append('py3filter')

  # fix_encoding
  sys.path.insert(0, CLIENT_THIRD_PARTY_DIR)
  from depot_tools import fix_encoding
  fix_encoding.fix_encoding()

  # add nose2 plugin dir to path
  sys.path.insert(0, PLUGINS_DIR)
  discover(plugins=plugins)


def _has_arg(argv, arg):
  return any(arg in a for a in argv)


if __name__ == '__main__':
  run_tests(python3=six.PY3)
