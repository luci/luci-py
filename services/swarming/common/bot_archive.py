# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates the archive for the bot.

The hash of the archive is used to define the current version of the swarm bot
code.
"""

import hashlib
import logging
import os
import StringIO
import zipfile

# List of files needed by the swarm bot. In practice, all files in common/ and
# swarm_bot/ that are not unit tests are archived.
FILES = (
    'common/__init__.py',
    'common/bot_archive.py',
    'common/rpc.py',
    'common/swarm_constants.py',
    'common/test_request_message.py',
    'local_test_runner.py',
    'slave_machine.py',
    'url_helper.py',
)


def SlaveCodeZipped(root_dir, additionals):
  """Returns a zipped file of all the files a slave needs to run.

  Arguments:
    root_dir: directory swarm_bot.
    additionals: dict(filepath: content) of additional items to put into the zip
                 file, in addition to FILES and MAPPED. In practice, it's going
                 to be a custom start_slave.py.
  Returns:
    A string representing the zipped file's contents.
  """
  zip_memory_file = StringIO.StringIO()
  with zipfile.ZipFile(zip_memory_file, 'w') as zip_file:
    for item in FILES:
      zip_file.write(os.path.join(root_dir, item), item)
    for filepath, content in additionals.iteritems():
      zip_file.writestr(filepath, content)

  data = zip_memory_file.getvalue()
  logging.info('SlaveCodeZipped(%s) is %d bytes', additionals, len(data))
  return data


def GenerateSlaveVersion(root_dir, additionals):
  """Returns the SHA1 hash of the slave code, representing the version.

  When run on a slave, this is the currently running version. When run on
  the server it is the version that all slaves should run.

  Arguments:
    root_dir: directory swarm_bot.
    additionals: See SlaveCodeZipped's doc.

  Returns:
    The SHA1 hash of the slave code.
  """
  result = hashlib.sha1()
  for filepath in FILES:
    try:
      with open(os.path.join(root_dir, filepath), 'rb') as f:
        result.update(f.read())
    except IOError:
      logging.warning(
          'Missing expected file %s. Hash will be invalid.', filepath)
  for _, content in sorted(additionals.iteritems()):
    result.update(content)
  d = result.hexdigest()
  logging.info('GenerateSlaveVersion(%s) = %s', additionals.keys(), d)
  return d
