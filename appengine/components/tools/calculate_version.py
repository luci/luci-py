#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Given current git checkout state return version string to use for an app."""

import getpass
import logging
import optparse
import os
import subprocess
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT_DIR, '..', 'third_party_local'))

from depot_tools import git_number
from depot_tools import git_common


def git(cmd, cwd):
  return subprocess.check_output(['git'] + cmd, cwd=cwd)


def get_pseudo_revision(root, remote):
  """Returns the pseudo revision number and commit hash describing
  the base upstream commit this branch is based on.

  The base upstream commit hash is determined by 'git merge-base'. See the man
  page for more information.

  The pseudo revision is calculated by the number of commits separating the base
  upstream commit from the rootest commit.  The earliest commit should be a root
  commit, e.g. a commit with no parent. A git tree can have multiple root
  commits when git repositories are merged together. The oldest one will be
  selected. The list of all root commits can be retrieved with:

    git rev-list --parents HEAD | egrep "^[a-f0-9]{40}$"

  Returns:
    tuple of:
    - pseudo revision number as a int
    - upstream commit hash this branch is based of.
  """
  mergebase = git(['merge-base', 'HEAD', remote], cwd=root).rstrip()
  targets = git_common.parse_commitrefs(mergebase)
  git_number.load_generation_numbers(targets)
  git_number.finalize(targets)
  return git_number.get_num(targets[0]), mergebase


def is_pristine(root, mergebase):
  """Returns True if the tree is pristine relating to mergebase."""
  head = git(['rev-parse', 'HEAD'], cwd=root).rstrip()
  logging.info('head: %s, mergebase: %s', head, mergebase)

  if head != mergebase:
    return False

  # Look for local uncommitted diff.
  return not (
      git(['diff', '--ignore-submodules=none', mergebase], cwd=root) or
      git(['diff', '--ignore-submodules', '--cached', mergebase], cwd=root))


def calculate_version(root, tag):
  """Returns a tag for a git checkout.

  Uses the pseudo revision number from the upstream commit this branch is based
  on, the abbreviated commit hash. Adds -tainted-<username> if the code is not
  pristine and optionally adds a tag to further describe it.
  """
  pseudo_revision, mergebase = get_pseudo_revision(root, 'origin/master')
  pristine = is_pristine(root, mergebase)
  # Trim it to 7 characters like 'git describe' does. 40 characters is
  # overwhelming!
  version = '%s-%s' % (pseudo_revision, mergebase[:7])
  if not pristine:
    version += '-tainted-%s' % getpass.getuser()
  if tag:
    version += '-' + tag
  return version


def checkout_root(cwd):
  """Returns the root of the checkout."""
  return git(['rev-parse', '--show-toplevel'], cwd).rstrip()


def main():
  parser = optparse.OptionParser(description=sys.modules[__name__].__doc__)
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option(
      '-t', '--tag', help='Tag to attach to a tainted version')
  options, args = parser.parse_args()
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)

  if args:
    parser.error('Unknown arguments, %s' % args)

  root = checkout_root(os.getcwd())
  logging.info('Checkout root is %s', root)
  print calculate_version(root, options.tag)
  return 0


if __name__ == '__main__':
  sys.exit(main())
