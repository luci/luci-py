# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Gerrit functions for GAE environment."""

import collections
import httplib
import json
import logging
import urllib
import urlparse

from google.appengine.api import app_identity
from google.appengine.api import urlfetch

from components import auth
from components import utils


AUTH_SCOPE = 'https://www.googleapis.com/auth/gerritcodereview'
RESPONSE_PREFIX = ")]}'"


class Error(Exception):
  """Exception class for errors commuicating with a Gerrit/Gitiles service."""

  def __init__(self, http_status, *args, **kwargs):
    super(Error, self).__init__(*args, **kwargs)
    self.http_status = http_status
    if self.http_status:  # pragma: no branch
      self.message = '(%s) %s' % (self.http_status, self.message)


Owner = collections.namedtuple('Owner', ['name', 'email', 'username'])


Revision = collections.namedtuple(
    'Revision',
    [
      # Commit sha, such as d283186300411e4d05ef0ced6c29fe77e8767a43.
      'commit',
      # Ordinal of the revision within a GerritChange, starting from 1.
      'number',
      # A ref where this commit can be fetched.
      'fetch_ref',
    ])


Change = collections.namedtuple(
    'Change',
    [
      # A "long" change id, such as
      # chromium/src~master~If1bfd2e7d0ad2c14908e5d45a513b5335d36ff01
      'id',
      # A "short" change id, such as If1bfd2e7d0ad2c14908e5d45a513b5335d36ff01
      'change_id',
      'project',
      'branch',
      'subject',
      # Owner of the Change, of type Owner.
      'owner',
      # Sha of the current revision's commit.
      'current_revision',
      # A list of Revision objects.
      'revisions',
    ])


def get_change(
    hostname, change_id, include_all_revisions=True,
    include_owner_details=False):
  """Gets a single Gerrit change by id.

  Returns Change object, or None if change was not found.
  """
  path = 'changes/%s' % change_id
  if include_owner_details:
    path += '/detail'
  if include_all_revisions:
    path += '?o=ALL_REVISIONS'
  data = fetch_json(hostname, path)
  if data is None:
    return None

  owner = None
  ownerData = data.get('owner')
  if ownerData:  # pragma: no branch
    owner = Owner(
        name=ownerData.get('name'),
        email=ownerData.get('email'),
        username=ownerData.get('username'))

  revisions = [
    Revision(
        commit=key,
        number=int(value['_number']),
        fetch_ref=value['fetch']['http']['ref'],
    ) for key, value in data.get('revisions', {}).iteritems()]
  revisions.sort(key=lambda r: r.number)

  return Change(
      id=data['id'],
      project=data.get('project'),
      branch=data.get('branch'),
      subject=data.get('subject'),
      change_id=data.get('change_id'),
      current_revision=data.get('current_revision'),
      revisions=revisions,
      owner=owner)


def set_review(
    hostname, change_id, revision, message=None, labels=None, notify=None):
  """Sets review on a revision.

  Args:
    hostname (str): Gerrit hostname.
    change_id: Gerrit change id, such as project~branch~I1234567890.
    revision: a commit sha for the patchset to review.
    message: text message.
    labels: a dict of label names and their values, such as {'Verified': 1}.
    notify: who to notify. Supported values:
      None - use default behavior, same as 'ALL'.
      'NONE': do not notify anyone.
      'OWNER': notify owner of the change_id.
      'OWNER_REVIEWERS': notify owner and OWNER_REVIEWERS.
      'ALL': notify anyone interested in the Change.
  """
  if notify is not None:
    notify = str(notify).upper()
  assert notify in (None, 'NONE', 'OWNER', 'OWNER_REVIEWERS', 'ALL')
  body = {
    'labels': labels,
    'message': message,
    'notify': notify,
  }
  body = {k:v for k, v in body.iteritems() if v is not None}

  path = 'changes/%s/revisions/%s/review' % (change_id, revision)
  fetch_json(hostname, path, method='POST', body=body)


def fetch(
    hostname, path, query_params=None, method='GET', payload=None,
    expect_status=(httplib.OK, httplib.NOT_FOUND), accept_header=None):
  """Makes a single authenticated blocking request using urlfetch.

  Raises
    auth.AuthorizationError if authentication fails.
    Error if response status is not in expect_status tuple.

  Returns parsed json contents.
  """
  if not hasattr(expect_status, '__contains__'):  # pragma: no cover
    expect_status = (expect_status,)

  assert not path.startswith('/')
  url = urlparse.urljoin('https://' + hostname, 'a/' + path)
  if query_params:
    url = '%s?%s' % (url, urllib.urlencode(query_params))
  request_headers = {
    'Content-Type': 'application/json',
    'Authorization': 'OAuth %s' % get_access_token(),
  }
  if accept_header:
    request_headers['Accept'] = accept_header

  try:
    logging.debug('%s %s' % (method, url))
    response = urlfetch.fetch(
        url, payload=payload, method=method, headers=request_headers,
        follow_redirects=False, validate_certificate=True)
  except urlfetch.Error as err:  # pragma: no cover
    raise Error(None, err.message)

  # Check if this is an authentication issue.
  auth_failed = response.status_code in (
      httplib.UNAUTHORIZED, httplib.FORBIDDEN)
  if auth_failed:
    reason = (
        'Authorization failed for %s. Status code: %s' %
        (hostname, response.status_code))
    raise auth.AuthorizationError(reason)

  if response.status_code not in expect_status:  # pragma: no cover
    raise Error(response.status_code, response.content)

  if response.status_code == httplib.NOT_FOUND:
    return None
  return response


def fetch_json(
    hostname, path, query_params=None, method='GET', body=None,
    expect_status=(httplib.OK, httplib.NOT_FOUND)):
  payload = json.dumps(body) if body else None
  res = fetch(
      hostname, path, query_params, method, payload, expect_status,
      accept_header='application/json')
  if not res or not res.content:
    return None
  if not res.content.startswith(RESPONSE_PREFIX):
    msg = ('Unexpected response format. Expected prefix %s. Received: %s' %
           (RESPONSE_PREFIX, res.content))
    raise Error(res.status_code, msg)
  content = res.content[len(RESPONSE_PREFIX):]
  return json.loads(content)


def get_access_token():  # pragma: no cover
  """Returns OAuth token to use when talking to Gitiles servers."""
  # On real GAE use app service account.
  if not utils.is_local_dev_server():
    return app_identity.get_access_token(
        ['https://www.googleapis.com/auth/gerritcodereview'])[0]
  # On dev server allow custom tokens loaded from local_dev_config. Use 'imp'
  # because dev_appserver tries to emulate app sandbox and hacks 'import' to
  # respect 'skip_files:' section in app.yaml.
  try:
    import imp
    local_dev_config = imp.load_source(
        'local_dev_config', 'local_dev_config.py')
    # Copy your chrome-internal .netrc token there.
    return local_dev_config.GITILES_OAUTH_TOKEN
  except (ImportError, IOError):
    return 'fake_token'
