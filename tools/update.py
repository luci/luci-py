#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Updates an AppEngine instance with the version derived from the current git
checkout state.
"""

import logging
import optparse
import subprocess
import sys

import app_config
import find_gae_sdk


def git(cmd, cwd):
  return subprocess.check_output(['git'] + cmd, cwd=cwd)


def get_pseudo_revision(app_dir, remote, baserev_tag_name):
  """Returns the pseudo revision number and commit hash describing
  the base upstream commit this branch is based on.

  The base upstream commit hash is determined by 'git merge-base'. See the man
  page for more information.

  The pseudo revision is calculated by the number of commits separating the base
  upstream commit from a predetermined tag created with 'git tag'. The proper
  setup is to create a git tag of the earliest commit (the root commit) with:

    git tag -a baserev <earliest commit>

  where 'baserev' matches |baserev_tag_name|. Do not forget to push the tag
  upstream! The earliest commit should be a root commit, e.g. a commit with no
  parent. A git tree can have multiple root commits when git repositories are
  merged together so the developer should select the one that makes the most
  sense. The list of all root commits can be retrieved with:

    git rev-list --parents HEAD | egrep "^[a-f0-9]{40}$"

  Then this function uses 'git describe' which will implicitly use the tag
  created above to count the number of commits separating the tag from the merge
  base commit. This is the pseudo revision number.

  Returns:
    tuple of:
    - pseudo revision number as a int
    - upstream commit hash this branch is based of.
  """
  mergebase = git(['merge-base', 'HEAD', remote], cwd=app_dir).rstrip()
  describe = git(['describe', mergebase], cwd=app_dir).rstrip()
  if not describe.startswith(baserev_tag_name + '-'):
    print >> sys.stderr, 'Make sure to run git fetch --tags'
    sys.exit(1)

  # describe has a format similar to 'baserev-124-g33c3dca'.
  _, pseudo_revision, commit = describe.split('-', 2)
  assert commit.startswith('g'), commit
  pseudo_revision = int(pseudo_revision)
  assert mergebase.startswith(commit[1:]), (mergebase, commit)
  return pseudo_revision, mergebase


def calculate_version(app_dir, tag):
  """Returns a tag for a git checkout.

  Uses the pseudo revision number from the upstream commit this branch is based
  on, the abbreviated commit hash. Adds -tainted if the code is not
  pristine and optionally adds a tag to further describe it.
  """
  pseudo_revision, mergebase = get_pseudo_revision(
      app_dir, 'origin/master', 'baserev')
  head = git(['rev-parse', 'HEAD'], cwd=app_dir).rstrip()

  logging.info('head: %s, mergebase: %s', head, mergebase)
  if head != mergebase:
    pristine = False
  else:
    # Look for local uncommitted diff.
    pristine = not (
        git(['diff', mergebase], cwd=app_dir) or
        git(['diff', '--cached', mergebase], cwd=app_dir))
    logging.info('was diff clear? %s', pristine)

  # Trim it to 7 characters like 'git describe' does. 40 characters is
  # overwhelming!
  version = '%s-%s' % (pseudo_revision, mergebase[:7])
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
      '-m', '--module', action='append', help='Module yaml to update')
  parser.add_option(
      '-t', '--tag', help='Tag to attach to a tainted version')
  parser.add_option(
      '-s', '--sdk-path',
      help='Path to AppEngine SDK. Will try to find by itself.')
  options, args = parser.parse_args()
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)

  if args:
    parser.error('Unknown arguments, %s' % args)
  options.sdk_path = options.sdk_path or find_gae_sdk.find_gae_sdk()
  if not options.sdk_path:
    parser.error('Failed to find the AppEngine SDK. Pass --sdk-path argument.')

  find_gae_sdk.setup_gae_sdk(options.sdk_path)
  options.app_id = (
      options.app_id or find_gae_sdk.default_app_id(app_config.APP_DIR))

  version = calculate_version(app_config.APP_DIR, options.tag)
  modules = options.module or app_config.MODULES

  # 'appcfg.py update <list of modules>' does not update the rest of app engine
  # app like 'appcfg.py update <app dir>' does. It updates only modules. So do
  # index, queues, etc. updates manually afterwards.
  commands = (
      ['update'] + modules,
      ['update_indexes', '.'],
      ['update_queues', '.'],
      ['update_cron', '.'],
  )

  for cmd in commands:
    ret = find_gae_sdk.appcfg(app_config.APP_DIR, cmd, options.sdk_path,
      options.app_id, version, options.verbose)
    if ret:
      print('Command \'%s\' failed with code %d' % (' '.join(cmd), ret))
      return ret

  print(' https://%s-dot-%s.appspot.com' % (version, options.app_id))
  print(' https://appengine.google.com/deployment?app_id=s~' + options.app_id)

  return 0


if __name__ == '__main__':
  sys.exit(main())
