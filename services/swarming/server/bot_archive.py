#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates the archive for the bot.

The hash of the archive is used to define the current version of the swarm bot
code.
"""

import hashlib
import json
import logging
import os
import StringIO
import sys
import zipfile


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# List of files needed by the swarm bot. In practice, all files in common/ and
# swarm_bot/ that are not unit tests are archived.
FILES = (
    '__main__.py',
    'common/__init__.py',
    'common/rpc.py',
    'common/swarm_constants.py',
    'common/test_request_message.py',
    'local_test_runner.py',
    'logging_utils.py',
    'os_utilities.py',
    'slave_machine.py',
    'url_helper.py',
    'zipped_archive.py',
)


def yield_swarming_bot_files(root_dir, host, additionals):
  """Yields all the files to map as tuple(filename, content).

  config.json is injected with json data about the server.
  """
  items = {i: None for i in FILES}
  items.update(additionals)
  items['config.json'] = json.dumps({'server': host.rstrip('/')})
  for item, content in sorted(items.iteritems()):
    if content is not None:
      yield item, content
    else:
      with open(os.path.join(root_dir, item), 'rb') as f:
        yield item, f.read()


def get_swarming_bot_zip(root_dir, host, additionals):
  """Returns a zipped file of all the files a slave needs to run.

  Arguments:
    root_dir: directory swarm_bot.
    additionals: dict(filepath: content) of additional items to put into the zip
        file, in addition to FILES and MAPPED. In practice, it's going to be a
        custom start_slave.py.
  Returns:
    A string representing the zipped file's contents.
  """
  zip_memory_file = StringIO.StringIO()
  with zipfile.ZipFile(zip_memory_file, 'w') as zip_file:
    for item, content in yield_swarming_bot_files(root_dir, host, additionals):
      zip_file.writestr(item, content)

  data = zip_memory_file.getvalue()
  logging.info('get_swarming_bot_zip(%s) is %d bytes', additionals, len(data))
  return data


def get_swarming_bot_version(root_dir, host, additionals):
  """Returns the SHA1 hash of the slave code, representing the version.

  Arguments:
    root_dir: directory swarm_bot.
    additionals: See get_swarming_bot_zip's doc.

  Returns:
    The SHA1 hash of the slave code.
  """
  result = hashlib.sha1()
  try:
    for item, content in yield_swarming_bot_files(root_dir, host, additionals):
      result.update(item)
      result.update('\x00')
      result.update(content)
      result.update('\x00')
  except IOError:
    logging.warning('Missing expected file. Hash will be invalid.')
  out = result.hexdigest()
  logging.info('get_swarming_bot_version(%s) = %s', sorted(additionals), out)
  return out


def main():
  if len(sys.argv) > 1:
    print >> sys.stderr, (
        'This script creates a swarming_bot.zip file locally in the server '
        'directory. This script doesn\'t accept any argument.')
    return 1

  with open(os.path.join(ROOT_DIR, 'swarm_bot', 'config.json'), 'rb') as f:
    config = json.load(f) or {}
  expected = ['server']
  actual = sorted(config)
  if expected != actual:
    print >> sys.stderr, 'Only expected keys \'%s\', got \'%s\'' % (
    ','.join(expected), ','.join(actual))
  swarm_bot_dir = os.path.join(ROOT_DIR, 'swarm_bot')

  zip_file = os.path.join(ROOT_DIR, 'swarming_bot.zip')
  with open(os.path.join(swarm_bot_dir, 'start_slave.py'), 'rb') as f:
    additionals = {'start_slave.py': f.read()}
  with open(zip_file, 'wb') as f:
    f.write(get_swarming_bot_zip(swarm_bot_dir, config['server'], additionals))
  return 0


if __name__ == '__main__':
  sys.exit(main())
