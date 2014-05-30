#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs either local_test_runner.py, slave_machine.py or start_slave.py."""

__version__ = '0.1'

import os
import sys

import logging_utils


def main():
  if len(sys.argv) < 2:
    print >> sys.stderr, 'Expected to be run as a bot (too few arguments).'
    return 1

  # Always make the current working directory the directory containing this
  # file. It simplifies assumptions.
  import zipped_archive
  root_dir = os.path.dirname(
      os.path.abspath(zipped_archive.get_main_script_path()))
  os.chdir(root_dir)

  # sys.argv[0] is the zip file itself.
  print('Running: %s' % sys.argv)
  cmd = sys.argv[1]
  rest = sys.argv[2:]

  if cmd == 'local_test_runner':
    logging_utils.prepare_logging('local_test_runner.log')
    import local_test_runner
    return local_test_runner.main(rest)

  if cmd == 'start_bot':
    logging_utils.prepare_logging('swarming_bot.log')
    import slave_machine
    return slave_machine.main(rest)

  if cmd == 'start_slave':
    logging_utils.prepare_logging('start_slave.log')
    # start_slave.py needs helps to know what the base directory is.
    # TODO(maruel): Remove.
    import start_slave
    start_slave.BASE_DIR = root_dir
    return start_slave.main(rest)

  print >> sys.stderr, 'Unknown script %s' % cmd
  return 1


if __name__ == '__main__':
  sys.exit(main())
