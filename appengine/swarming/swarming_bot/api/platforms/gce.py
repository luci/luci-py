# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Google Compute Engine specific utility functions."""

import json
import logging
import threading
import time
import urllib2

from utils import tools


# Cache of GCE OAuth2 token.
_CACHED_OAUTH2_TOKEN_GCE = {}
_CACHED_OAUTH2_TOKEN_GCE_LOCK = threading.Lock()


def is_gce():
  """Returns True if running on Google Compute Engine."""
  return bool(get_metadata())


@tools.cached
def get_metadata():
  """Returns the GCE metadata as a dict.

  Refs:
    https://cloud.google.com/compute/docs/metadata
    https://cloud.google.com/compute/docs/machine-types

  To get the at the command line from a GCE VM, use:
    curl --silent \
      http://metadata.google.internal/computeMetadata/v1/?recursive=true \
      -H "Metadata-Flavor: Google" | python -m json.tool | less
  """
  url = 'http://metadata.google.internal/computeMetadata/v1/?recursive=true'
  headers = {'Metadata-Flavor': 'Google'}
  try:
    return json.load(
        urllib2.urlopen(urllib2.Request(url, headers=headers), timeout=5))
  except IOError as e:
    logging.info('GCE metadata not available: %s', e)
    return None


def oauth2_access_token(account='default'):
  """Returns a value of oauth2 access token."""
  # TODO(maruel): Move GCE VM authentication logic into client/utils/net.py.
  # As seen in google-api-python-client/oauth2client/gce.py
  with _CACHED_OAUTH2_TOKEN_GCE_LOCK:
    cached_tok = _CACHED_OAUTH2_TOKEN_GCE.get(account)
    # Cached and expires in more than 5 min from now.
    if cached_tok and cached_tok['expiresAt'] >= time.time() + 5*60:
      return cached_tok['accessToken']
    # Grab the token.
    url = (
        'http://metadata.google.internal/computeMetadata/v1/instance'
        '/service-accounts/%s/token' % account)
    headers = {'Metadata-Flavor': 'Google'}
    try:
      resp = json.load(
          urllib2.urlopen(urllib2.Request(url, headers=headers), timeout=20))
    except IOError as e:
      logging.error('Failed to grab GCE access token: %s', e)
      raise
    tok = {
      'accessToken': resp['access_token'],
      'expiresAt': time.time() + resp['expires_in'],
    }
    _CACHED_OAUTH2_TOKEN_GCE[account] = tok
    return tok['accessToken']


def oauth2_available_scopes(account='default'):
  """Returns a list of OAuth2 scopes granted to GCE service account."""
  metadata = get_metadata()
  if not metadata:
    return []
  accounts = metadata['instance']['serviceAccounts']
  return accounts.get(account, {}).get('scopes') or []
