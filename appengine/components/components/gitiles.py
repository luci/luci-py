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


LocationTuple = collections.namedtuple(
    'LocationTuple', ['hostname', 'project', 'treeish', 'path'])


class Location(LocationTuple):
  """Gitiles URL. Immutable.

  Contains gitiles methods, such as get_log, for convenience.
  """

  def __eq__(self, other):
    return str(self) == str(other)

  def __ne__(self, other):
    return not self.__eq__(other)

  @classmethod
  def parse(cls, url, treeishes=None):
    """Parses a Gitiles-formatted url.

    If /a authentication prefix is present in |url|, it is omitted.

    Args:
      url (str): url to parse.
      treeishes (list of str): if None (default), treats first directory after
        /+/ as treeish. Otherwise, finds longest prefix present in |treeishes|.

    Returns:
      gitiles.Location.
        treeish: if not present in the |url|, defaults to 'HEAD'.
        path: always starts with '/'. If not present in |url|, it is just '/'.
    """
    parsed = urlparse.urlparse(url)
    path_match = RGX_URL_PATH.match(parsed.path)
    if not path_match:
      raise ValueError('Invalid Gitiles repo url: %s' % url)

    hostname = parsed.netloc
    project = path_match.group(1)
    if project.startswith('a/'):
      project = project[len('a/'):]
    project = project.strip('/')

    treeish_and_path = (path_match.group(3) or '').strip('/')
    first_slash = treeish_and_path.find('/')
    if first_slash == -1:
      treeish = treeish_and_path
      path = '/'
    elif not treeishes:
      treeish = treeish_and_path[:first_slash]
      path = treeish_and_path[first_slash:]
    else:
      treeish = treeish_and_path
      treeishes = set(treeishes)
      while True:
        if treeish in treeishes:
          break
        i = treeish.rfind('/')
        assert i != 0
        if i == -1:
          break
        treeish = treeish[:i]
      path = treeish_and_path[len(treeish):]

    treeish = treeish or 'HEAD'

    path = path or ''
    if not path.startswith('/'):
      path = '/' + path

    # Check yourself.
    _validate_args(hostname, project, treeish, path, path_required=True)

    return cls(hostname, project, treeish, path)

  @classmethod
  def parse_resolve(cls, url):
    """Like parse, but supports refs with slashes.

    May send a get_refs() request.
    """
    loc = cls.parse(url)
    if loc.path and loc.path != '/':
      # If true ref name contains slash, a prefix of path might be suffix of
      # ref. Try to resolve it.
      refs = get_refs(loc.hostname, loc.project)
      if refs:
        treeishes = set(refs.keys())
        for ref in refs.iterkeys():
          for prefix in ('refs/tags/', 'refs/heads/'):
            if ref.startswith(prefix):
              treeishes.add(ref[len(prefix):])
              break
        loc = cls.parse(url, treeishes=treeishes)
    return loc

  def __str__(self):
    result = 'https://{hostname}/{project}'.format(
        hostname=self.hostname, project=self.project)
    path = (self.path or '').strip('/')

    if self.treeish or path:
      result += '/+/%s' % self.treeish_safe
    if path:
      result += '/%s' % path
    return result

  @property
  def treeish_safe(self):
    return (self.treeish or 'HEAD').strip('/')

  @property
  def path_safe(self):
    path = self.path or '/'
    if not path.startswith('/'):
      path = '/' + path
    return path

  def get_log(self, **kwargs):
    return get_log(
        self.hostname, self.project, self.treeish_safe, self.path_safe,
        **kwargs)

  def get_tree(self, **kwargs):
    return get_tree(
        self.hostname, self.project, self.treeish_safe, self.path_safe,
        **kwargs)

  def get_archive(self, **kwargs):
    return get_archive(
        self.hostname, self.project, self.treeish_safe, self.path_safe,
        **kwargs)

  def get_file_content(self, **kwargs):
    return get_file_content(
        self.hostname, self.project, self.treeish_safe, self.path_safe,
        **kwargs)


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
  _validate_args(hostname, project, treeish)
  data = gerrit.fetch_json(hostname, '%s/+/%s' % (project, treeish))
  if data is None:
    return None
  return _parse_commit(data)


def get_tree(hostname, project, treeish, path=None):
  """Gets a tree object.

  Returns:
    Tree object, or None if the tree was not found.
  """
  _validate_args(hostname, project, treeish, path)
  data = gerrit.fetch_json(hostname, '%s/+/%s%s' % (project, treeish, path))
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


def get_log(hostname, project, treeish, path=None, limit=None, **fetch_kwargs):
  """Gets a commit log.

  Does not support paging.

  Returns:
    Log object, or None if no log available.
  """
  _validate_args(hostname, project, treeish, path)
  query_params = {}
  if limit:
    query_params['n'] = limit
  path = (path or '').strip('/')
  data = gerrit.fetch_json(
      hostname,
      '%s/+log/%s/%s' % (project, treeish, path),
      params=query_params,
      **fetch_kwargs)
  if data is None:
    return None
  return Log(
      commits=[_parse_commit(c) for c in data.get('log', [])])


def get_file_content(hostname, project, treeish, path, **fetch_kwargs):
  """Gets file contents.

  Returns:
    Raw contents of the file or None if not found.
  """
  _validate_args(hostname, project, treeish, path, path_required=True)
  data = gerrit.fetch(
      hostname,
      '%s/+/%s%s' % (project, treeish, path),
      headers={'Accept': 'text/plain'},
      **fetch_kwargs)
  return base64.b64decode(data) if data is not None else None


def get_archive(hostname, project, treeish, dir_path=None, **fetch_kwargs):
  """Gets a directory as a tar.gz archive or None if not found."""
  _validate_args(hostname, project, treeish, dir_path)
  dir_path = (dir_path or '').strip('/')
  if dir_path:
    dir_path = '/%s' % dir_path
  return gerrit.fetch(
      hostname, '%s/+archive/%s%s.tar.gz' % (project, treeish, dir_path),
      **fetch_kwargs)


def get_refs(hostname, project, **fetch_kwargs):
  """Gets refs from the server.

  Returns:
    Dict (ref_name -> last_commit_sha), or None if repository was not found.
  """
  _validate_args(hostname, project)
  res = gerrit.fetch_json(hostname, '%s/+refs' % project, **fetch_kwargs)
  if res is None:
    return None
  return {k: v['value'] for k, v in res.iteritems()}


def assert_non_empty_string(value):
  assert isinstance(value, basestring)
  assert value


def _validate_args(
    hostname, project, treeish='HEAD', path=None, path_required=False):
  assert_non_empty_string(hostname)
  assert_non_empty_string(project)
  assert_non_empty_string(treeish)
  assert not treeish.startswith('/'), treeish
  assert not treeish.endswith('/'), treeish
  if path_required:
    assert path is not None
  if path is not None:
    assert_non_empty_string(path)
    assert path.startswith(path), path
