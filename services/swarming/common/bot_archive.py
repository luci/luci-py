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

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# List of files needed by the swarm bot. In practice, all files in common/ and
# swarm_bot/ that are not unit tests are archived.
FILES = (
    'common/__init__.py',
    'common/bot_archive.py',
    'common/rpc.py',
    'common/swarm_constants.py',
    'common/test_request_message.py',
    'common/url_helper.py',
    'swarm_bot/local_test_runner.py',
)

# List of files that are mapped in a different place.
# TODO(maruel): It will be better to stop doing this. This can be achieved by
# using a symlink in swarm_bot/common that points back to ../common, so that
# swarm_bot itself is archived instead of archiving select items from '..'.
MAPPED = {
    'swarm_bot/slave_machine.py': 'slave_machine.py',
}

def SlaveCodeZipped(additionals):
  """Returns a zipped file of all the files a slave needs to run.

  Arguments:
    additionals: dict(filepath: content) of additional items to put into the zip
                 file, in addition to FILES and MAPPED. In practice, it's going
                 to be a custom start_slave.py.
  Returns:
    A string representing the zipped file's contents.
  """
  zip_memory_file = StringIO.StringIO()
  with zipfile.ZipFile(zip_memory_file, 'w') as zip_file:
    for item in FILES:
      zip_file.write(os.path.join(ROOT_DIR, item), item)
    for src, dst in sorted(MAPPED.iteritems()):
      zip_file.write(os.path.join(ROOT_DIR, src), dst)
    for filepath, content in additionals.iteritems():
      zip_file.writestr(filepath, content)

  data = zip_memory_file.getvalue()
  logging.info('SlaveCodeZipped(%s) is %d bytes', additionals, len(data))
  return data


def GenerateSlaveVersion(additionals, moved):
  """Returns the SHA1 hash of the slave code, representing the version.

  When run on a slave, this is the currently running version. When run on
  the server it is the version that all slaves should run.

  Arguments:
    moved: On the server, the files are in their original location. For an
           unknown historical reason, slave_machine.py is moved on the slaves.
           Due to this, this function needs to know if the function call is done
           on a pristine or modified tree.

  Returns:
    The SHA1 hash of the slave code.
  """
  # TODO(maruel): Remove 'moved' along with 'MAPPED'.
  result = hashlib.sha1()
  def process(filepath):
    try:
      with open(os.path.join(ROOT_DIR, filepath), 'rb') as f:
        result.update(f.read())
    except IOError:
      logging.warning(
          'Missing expected file %s. Hash will be invalid.', filepath)

  for item in FILES:
    process(item)
  for src, dst in sorted(MAPPED.iteritems()):
    process(dst if moved else src)
  for _, content in sorted(additionals.iteritems()):
    result.update(content)
  d = result.hexdigest()
  logging.info('GenerateSlaveVersion(%s) = %s', additionals, d)
  return d
