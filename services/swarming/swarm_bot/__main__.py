#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs either local_test_runner.py, slave_machine.py or start_slave.py.

The imports are done late so if an ImportError occurs, it is localized to this
command only.
"""

__version__ = '0.2'

import logging
import os
import shutil
import subprocess
import sys
import zipfile

import logging_utils
import zipped_archive

# This file can only be run as a zip.
THIS_FILE = os.path.abspath(zipped_archive.get_main_script_path())


# TODO(maruel): Use depot_tools/subcommand.py. The goal here is to have all the
# sub commands packed into the single .zip file as a swiss army knife (think
# busybox but worse).


def CMDlocal_test_runner(args):
  """Internal command to run a swarming task."""
  logging_utils.prepare_logging('local_test_runner.log')
  import local_test_runner
  return local_test_runner.main(args)


def CMDstart_bot(args):
  """Starts the swarming bot."""
  logging_utils.prepare_logging('swarming_bot.log')
  logging_utils.set_console_level(logging.DEBUG)
  logging.info(
      'importing slave_machine: %s, %s',
      THIS_FILE, zipped_archive.generate_version())
  import slave_machine
  result = slave_machine.main(args)
  logging.info('slave_machine exit code: %d', result)
  return result


def CMDstart_slave(_args):
  """Ill named command that actually sets up the bot."""
  logging_utils.prepare_logging('start_slave.log')
  logging_utils.set_console_level(logging.DEBUG)

  # User provided start_slave.py
  logging.info(
      'importing start_slave: %s, %s',
      THIS_FILE, zipped_archive.generate_version())
  import start_slave
  if not start_slave.setup_bot():
    # The code asked to exit.
    return 0

  logging.info('Starting the bot: %s', THIS_FILE)
  result = subprocess.call([sys.executable, THIS_FILE, 'start_bot'])
  logging.info('Bot exit code: %d', result)
  return result


def CMDrestart(_args):
  """Utility subcommand that hides the difference between each OS to reboot
  the host."""
  logging_utils.prepare_logging(None)
  import os_utilities
  # This function doesn't return.
  os_utilities.restart()
  # Should never reach here.
  return 1


def CMDversion(_args):
  """Prints the version of this file and the hash of the code."""
  logging_utils.prepare_logging(None)
  print __version__
  print zipped_archive.generate_version()
  return 0


def main():
  # Always make the current working directory the directory containing this
  # file. It simplifies assumptions.
  os.chdir(os.path.dirname(THIS_FILE))

  if os.path.basename(THIS_FILE) == 'swarming_bot.zip':
    # Self-replicate itself right away as swarming_bot.1.zip and restart as it.
    print >> sys.stderr, 'Self replicating.'
    if os.path.isfile('swarming_bot.1.zip'):
      os.remove('swarming_bot.1.zip')
    shutil.copyfile('swarming_bot.zip', 'swarming_bot.1.zip')
    cmd = [sys.executable, 'swarming_bot.1.zip'] + sys.argv[1:]
    return subprocess.call(cmd)

  # sys.argv[0] is the zip file itself.
  cmd = 'start_slave'
  args = []
  if len(sys.argv) > 1:
    cmd = sys.argv[1]
    args = sys.argv[2:]

  fn = getattr(sys.modules[__name__], 'CMD%s' % cmd, None)
  if fn:
    try:
      return fn(args)
    except ImportError:
      logging.exception('Failed to run %s', cmd)
      with zipfile.ZipFile(THIS_FILE, 'r') as f:
        logging.error('Files in %s:\n%s', THIS_FILE, f.namelist())
      return 1

  print >> sys.stderr, 'Unknown command %s' % cmd
  return 1


if __name__ == '__main__':
  sys.exit(main())
