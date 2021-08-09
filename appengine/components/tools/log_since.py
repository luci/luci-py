#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Prints a short log from HEAD (or [end]) to a pseudo revision number."""

from __future__ import print_function

__version__ = '1.0.1'

import json
import optparse
import subprocess
import sys

import calculate_version  # pylint: disable=W0403


def get_logs(root, pseudo_revision, mergebase, start, end):
  start_ref = '%s~%d' % (mergebase, pseudo_revision - start)
  end_ref = mergebase
  if end is not None:
    end_ref += '~%d' % (pseudo_revision - end)
  refspec = '%s..%s' % (start_ref, end_ref)
  cmd = [
      'git', '--no-pager', 'log', refspec, '--date=short', '--format=%ad %ae %s'
  ]
  nb_commits = (end or pseudo_revision) - start
  try:
    log = subprocess.check_output(cmd, cwd=root)
  except subprocess.CalledProcessError:
    print(
        '\nFailed to retrieve the log of last %d commits.' % nb_commits,
        file=sys.stderr)
    return 1
  maxlen = 0
  lines = []
  for l in log.rstrip().splitlines():
    parts = l.split(' ', 2)
    parts[1] = parts[1].split('@', 1)[0]
    maxlen = max(maxlen, len(parts[1]))
    lines.append(parts)
  out = '\n'.join(
    '%s %-*s %s' % (parts[0], maxlen, parts[1], parts[2])
    for parts in lines)
  return out, refspec


def get_revision_from_project(project):
  out = subprocess.check_output([
      'gcloud', '--project', project, 'app', 'versions', 'list',
      '--service=default', '--format=json', '--filter', 'TRAFFIC_SPLIT=1'
  ])
  service_info = json.loads(out)
  return int(service_info[0]["id"].split('-')[0])


def main():
  root = calculate_version.checkout_root('.')
  pseudo_revision, mergebase = calculate_version.get_head_pseudo_revision(
      root, 'origin/main')
  is_pristine = calculate_version.is_pristine(root, mergebase)

  parser = optparse.OptionParser(
      usage='%prog [options] <start> [end]',
      version=__version__,
      description=sys.modules[__name__].__doc__)
  parser.add_option(
      '-f', '--force', action='store_true',
      help='Run even if not pristine checkout, e.g. HEAD != origin/main')
  parser.add_option(
      '-F', '--files', action='store_true', help='List all modified files')
  parser.add_option('--project', help='Project ID used to take active revision')
  options, args = parser.parse_args()

  print(
      'Current version: %s @ %s\n' % (pseudo_revision, mergebase),
      file=sys.stderr)

  if not args and not options.project:
    parser.error(
        'Specify --project or the pseudo-revision number of the last push.')

  if options.project:
    start = get_revision_from_project(options.project)
  else:
    start = int(args[0])
  end = None
  if len(args) == 2:
    end = int(args[1])
  if len(args) > 2:
    parser.error('Too many arguments.')

  if start >= pseudo_revision:
    parser.error(
        '%d >= %d, you specified \'start\' that was not committed yet?'
        % (start, pseudo_revision))
  if end is not None:
    if start >= end:
      parser.error('%d >= %d, did you reverse start and end?' % (start, end))
    if end > pseudo_revision:
      parser.error(
          '%d >= %d, you specified \'end\' that was not committed yet?'
          % (end, pseudo_revision))
  nb_commits = (end or pseudo_revision) - start

  if not is_pristine:
    if not options.force:
      parser.error(
          'Make sure to sync to what was committed and uploaded first.')
    print(
        'Warning: --force was specified, continuing even if not pristine.\n',
        file=sys.stderr)

  out, refspec = get_logs(root, pseudo_revision, mergebase[:12], start, end)
  remote = subprocess.check_output(['git', 'remote', 'get-url', 'origin'],
                                   cwd=root).strip()
  print(remote + '/+log/' + refspec)
  print('')
  print(out)

  if options.files:
    print('')
    cmd = ['git', '--no-pager', 'diff', refspec, '--stat', '-C', '-C']
    try:
      subprocess.check_call(cmd, cwd=root)
    except subprocess.CalledProcessError:
      print(
          '\nFailed to list files of last %d commits.' % nb_commits,
          file=sys.stderr)
      return 1

  return 0


if __name__ == '__main__':
  sys.exit(main())
