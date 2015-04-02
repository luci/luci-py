# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Gitiles functions for GAE environment."""

import base64
import collections
import datetime
import re
import urlparse

from components import gerrit


GitilesUrl = collections.namedtuple(
    'GitilesUrl',
    [
      'hostname',
      'project',
      'treeish',
      'path',
    ])
Contribution = collections.namedtuple(
    'Contribution', ['name', 'email', 'time'])
Commit = collections.namedtuple(
    'Commit', ['sha', 'tree', 'parents', 'author', 'committer', 'message'])
TreeEntry = collections.namedtuple(
    'TreeEntry',
    [
      # Content hash.
      'id',
      # Entry name, e.g. filename or directory name.
      'name',
      # Object type, e.g. "blob" or "tree".
      'type',
      # For files, numeric file mode.
      'mode',
    ])
Tree = collections.namedtuple('Tree', ['id', 'entries'])
Log = collections.namedtuple('Log', ['commits'])

RGX_URL_PATH = re.compile('/([^\+]+)(\+/(.*))?')


def parse_gitiles_url(url):
  """Parses a Gitiles-formatted url.

  If /a authentication prefix is present in |url|, it is omitted.
  """
  parsed = urlparse.urlparse(url)
  path_match = RGX_URL_PATH.match(parsed.path)
  if not path_match:
    raise ValueError('Invalid Gitiles repo url: %s' % url)

  project = path_match.group(1)
  if project.startswith('a/'):
    project = project[len('a/'):]
  project = project.strip('/')

  treeish_and_path = path_match.group(3) or '/'
  if not treeish_and_path.endswith('/'):
    treeish_and_path += '/'
  sep = '/./' if '/./' in treeish_and_path else '/'
  treeish, path = treeish_and_path.split(sep, 1)
  assert not treeish.startswith('/')
  path = '/' + path.strip('/')

  return GitilesUrl(
      hostname=parsed.netloc,
      project=project,
      treeish=treeish,
      path=path)


def unparse_gitiles_url(gitiles_url):
  """Converts GitilesUrl to str."""
  assert gitiles_url
  assert gitiles_url.hostname
  assert gitiles_url.project

  result = 'https://{hostname}/{project}'.format(**gitiles_url._asdict())

  if gitiles_url.treeish or gitiles_url.path:
    result += '/+/%s' % (gitiles_url.treeish or 'master').strip('/')
  if gitiles_url.path:
    result += '/./%s' % gitiles_url.path.strip('/')
  return result


def parse_time(tm):
  """Converts time in Gitiles-specific format to datetime."""
  tm_parts = tm.split()
  # Time stamps from gitiles sometimes have a UTC offset (e.g., -0800), and
  # sometimes not.  time.strptime() cannot parse UTC offsets, so if one is
  # present, strip it out and parse manually.
  timezone = None
  if len(tm_parts) == 6:
    tm = ' '.join(tm_parts[:-1])
    timezone = tm_parts[-1]
  dt = datetime.datetime.strptime(tm, "%a %b %d %H:%M:%S %Y")
  if timezone:
    m = re.match(r'([+-])(\d\d):?(\d\d)?', timezone)
    assert m, 'Could not parse time zone information from "%s"' % timezone
    timezone_delta = datetime.timedelta(
        hours=int(m.group(2)), minutes=int(m.group(3) or '0'))
    if m.group(1) == '-':
      dt += timezone_delta
    else:
      dt -= timezone_delta
  return dt


def _parse_commit(data):
  def parse_contribution(data):
    time = data.get('time')
    if time is not None:  # pragma: no branch
      time = parse_time(time)
    return Contribution(
        name=data.get('name'),
        email=data.get('email'),
        time=time)

  return Commit(
      sha=data['commit'],
      tree=data.get('tree'),
      parents=data.get('parents'),
      author=parse_contribution(data.get('author')),
      committer=parse_contribution(data.get('committer')),
      message=data.get('message'))


def get_commit(hostname, project, treeish):
  """Gets a single Git commit.

  Returns:
    Commit object, or None if the commit was not found.
  """
  data = gerrit.fetch_json(hostname, '%s/+/%s' % (project, treeish))
  if data is None:
    return None
  return _parse_commit(data)


def get_tree(hostname, project, treeish, path):
  """Gets a tree object.

  Returns:
    Tree object, or None if the tree was not found.
  """
  assert project
  assert treeish
  assert path
  data = gerrit.fetch_json(hostname, '%s/+/%s/.%s' % (project, treeish, path))
  if data is None:
    return None

  return Tree(
      id=data['id'],
      entries=[
        TreeEntry(
            id=e['id'],
            name=e['name'],
            type=e['type'],
            mode=e['mode'],
        )
        for e in data.get('entries', [])
      ])


def get_log(hostname, project, treeish, path=None, limit=None):
  """Gets a commit log.

  Does not support paging.

  Returns:
    Log object, or None if no log available.
  """
  query_params = {}
  if limit:
    query_params['n'] = limit
  data = gerrit.fetch_json(
      hostname,
      '%s/+log/%s/.%s' % (project, treeish, path or '/'),
      query_params=query_params)
  if data is None:
    return None
  return Log(
      commits=[_parse_commit(c) for c in data.get('log', [])])


def get_file_content(hostname, project, treeish, path):
  """Gets file contents.

  Returns:
    Raw contents of the file.
  """
  assert hostname
  assert project
  assert treeish
  assert path
  assert path.startswith('/')
  data = gerrit.fetch(
      hostname,
      '%s/+/%s/.%s' % (project, treeish, path),
      accept_header='text/plain').content
  if data is None:
    return None
  return base64.b64decode(data)


def get_archive(hostname, project, treeish, dir_path=None):
  """Gets a directory as a tar.gz archive."""
  assert project
  assert treeish
  dir_path = (dir_path or '').strip('/')
  if dir_path:
    dir_path = '/./%s' % dir_path
  res = gerrit.fetch(
      hostname, '%s/+archive/%s%s.tar.gz' % (project, treeish, dir_path))
  return res.content if res else None
