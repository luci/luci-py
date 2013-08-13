#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Updates an isolateserver instance."""

import logging
import optparse
import os
import subprocess
import sys

import find_gae_sdk

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def git(cmd):
  return subprocess.check_output(['git'] + cmd, cwd=APP_DIR)


def calculate_version(tag):
  """Returns a tag for a git checkout."""
  describe = git(['describe', 'HEAD']).rstrip()
  if not describe.startswith('baserev-'):
    print >> sys.stderr, 'Make sure to run git fetch --tags'
    sys.exit(1)

  # describe has a format similar to 'baserev-124-g33c3dca'.
  _, pseudo_revision, commit = describe.split('-', 2)
  commit = commit[1:]

  remote = 'origin/master'
  mergebase = git(['merge-base', 'HEAD', remote]).rstrip()
  if not commit.startswith(mergebase):
    pristine = False
    # Using a local commit hash is not useful, use the real base commit instead.
    # Trim it to 7 characters like 'git describe' does.
    commit = mergebase[:7]
  else:
    pristine = not git(['diff', mergebase])

  version = '%s-%s' % (pseudo_revision, commit)
  if not pristine:
    version += '-tainted'
  if tag:
    version += '-' + tag
  return version


def main():
  parser = optparse.OptionParser(description=sys.modules[__name__].__doc__)
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-A', '--app-id', help='Defaults to name in app.yaml')
  parser.add_option(
      '-t', '--tag', help='Tag to attach to a tainted version')
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  options, args = parser.parse_args()
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)

  if args:
    parser.error('Unknown arguments, %s' % args)
  options.sdk_path = options.sdk_path or find_gae_sdk.find_gae_sdk(APP_DIR)
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  version = calculate_version(options.tag)
  cmd = [
      sys.executable,
      os.path.join(options.sdk_path, 'appcfg.py'),
      'update',
      '--oauth2',
      '--noauth_local_webserver',
      '--version', version,
      APP_DIR,
  ]

  if options.app_id:
    cmd.extend(('--application', options.app_id))

  if options.verbose:
    cmd.append('--verbose')

  print('Uploading version %s' % version)
  return subprocess.call(cmd, cwd=APP_DIR)


if __name__ == '__main__':
  sys.exit(main())
