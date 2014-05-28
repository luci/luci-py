#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs either local_test_runner.py, slave_machine.py or start_slave.py."""

__version__ = '0.1'

import sys


def main():
  if len(sys.argv) < 2:
    print >> sys.stderr, 'Expected to be run as a bot (too few arguments).'
    return 1

  # sys.argv[0] is the zip file itself.
  print('Running: %s' % sys.argv)
  cmd = sys.argv[1]
  rest = sys.argv[2:]

  if cmd == 'local_test_runner':
    import local_test_runner
    local_test_runner.PrepareLogging()
    return local_test_runner.main(rest)

  if cmd == 'start_bot':
    import slave_machine
    return slave_machine.main(rest)

  if cmd == 'start_slave':
    import start_slave
    return start_slave.main(rest)

  print >> sys.stderr, 'Unknown script %s' % cmd
  return 1


if __name__ == '__main__':
  sys.exit(main())
