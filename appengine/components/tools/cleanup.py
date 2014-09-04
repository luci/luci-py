#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Removes old versions of GAE application."""

import atexit
import optparse
import os
import sys
import tempfile

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, 'third_party'))

from support import gae_sdk_utils


HEADER = (
    '# Remove lines that correspond to versions\n'
    '# you\'d like to delete from \'%s\'.\n')


def get_editor():
  """Executable to run to edit a text file."""
  return os.environ.get(
      'EDITOR', 'notepad.exe' if sys.platform == 'win32' else 'vi')


def main(args, app_dir=None):
  parser = optparse.OptionParser(
      usage='usage: %prog [options]',
      description=sys.modules[__name__].__doc__)

  gae_sdk_utils.app_sdk_options(parser, app_dir)
  options, args = parser.parse_args(args)

  # 'appcfg.py delete_version' is buggy when used with OAuth2 authentication, so
  # use cookie-based auth.
  app = gae_sdk_utils.process_sdk_options(
      parser, options, app_dir, use_oauth=False)

  # List all deployed versions, dump them to a temp file to be edited.
  versions = app.get_uploaded_versions()
  fd, path = tempfile.mkstemp()
  atexit.register(lambda: os.remove(path))
  with os.fdopen(fd, 'w') as f:
    f.write(HEADER % app.app_id + '\n'.join(versions) + '\n')

  # Let user remove versions that are no longer needed.
  exit_code = os.system('%s %s' % (get_editor(), path))
  if exit_code:
    print('Aborted.')
    return exit_code

  # Read back the file that now contains only versions to keep.
  keep = []
  with open(path, 'r') as f:
    for line in f.readlines():
      line = line.strip()
      if not line or line.startswith('#'):
        continue
      if line not in versions:
        print >> sys.stderr, 'Unknown version: %s' % line
        return 1
      if line not in keep:
        keep.append(line)

  # Calculate a list of versions to remove.
  remove = [v for v in versions if v not in keep]
  if not remove:
    print('Nothing to do.')
    return 0

  # Deleting a version is a destructive operation, confirm.
  if not gae_sdk_utils.confirm('Delete the following versions?', app, remove):
    print('Aborted.')
    return 1

  # Do it.
  for version in remove:
    print 'Deleting %s...' % version
    app.delete_version(version)

  return 0


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
