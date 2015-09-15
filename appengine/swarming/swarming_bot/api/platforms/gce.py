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


### Private stuff.


# Cache of GCE OAuth2 token.
_CACHED_OAUTH2_TOKEN = {}
_CACHED_OAUTH2_TOKEN_LOCK = threading.Lock()


## Public API.


class SendMetricsFailure(Exception):
  """Signals that metrics couldn't be sent successfully."""


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
  with _CACHED_OAUTH2_TOKEN_LOCK:
    cached_tok = _CACHED_OAUTH2_TOKEN.get(account)
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
    _CACHED_OAUTH2_TOKEN[account] = tok
    return tok['accessToken']


def oauth2_available_scopes(account='default'):
  """Returns a list of OAuth2 scopes granted to GCE service account."""
  metadata = get_metadata()
  if not metadata:
    return []
  accounts = metadata['instance']['serviceAccounts']
  return accounts.get(account, {}).get('scopes') or []


@tools.cached
def get_zone():
  """Returns the zone containing the GCE VM."""
  # Format is projects/<id>/zones/<zone>
  metadata = get_metadata()
  return unicode(metadata['instance']['zone'].rsplit('/', 1)[-1])


@tools.cached
def get_machine_type():
  """Returns the GCE machine type."""
  # Format is projects/<id>/machineTypes/<machine_type>
  metadata = get_metadata()
  return unicode(metadata['instance']['machineType'].rsplit('/', 1)[-1])


@tools.cached
def get_tags():
  """Returns a list of instance tags or empty list if not GCE VM."""
  return get_metadata()['instance']['tags']


def send_metric(name, value):
  """Sets a lightweight custom metric.

  In particular, the metric has no description and it is double. To make this
  work, use "--scopes https://www.googleapis.com/auth/monitoring" when running
  "gcloud compute instances create". You can verify if the scope is enabled from
  within a GCE VM with:
    curl "http://metadata.google.internal/computeMetadata/v1/instance/\
service-accounts/default/scopes" -H "Metadata-Flavor: Google"

  Ref: https://cloud.google.com/monitoring/custom-metrics/lightweight

  To create a metric, use:
  https://developers.google.com/apis-explorer/#p/cloudmonitoring/v2beta2/cloudmonitoring.metricDescriptors.create
  It is important to set the commonLabels.
  """
  logging.info('send_metric(%s, %s)', name, value)
  assert isinstance(name, str), repr(name)
  assert isinstance(value, float), repr(value)

  metadata = get_metadata()
  project_id = metadata['project']['numericProjectId']

  url = (
    'https://www.googleapis.com/cloudmonitoring/v2beta2/projects/%s/'
    'timeseries:write') % project_id
  now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
  body = {
    'commonLabels': {
      'cloud.googleapis.com/service': 'compute.googleapis.com',
      'cloud.googleapis.com/location': get_zone(),
      'compute.googleapis.com/resource_type': 'instance',
      'compute.googleapis.com/resource_id': metadata['instance']['id'],
    },
    'timeseries': [
      {
        'timeseriesDesc': {
          'metric': 'custom.cloudmonitoring.googleapis.com/' + name,
          'project': project_id,
        },
        'point': {
          'start': now,
          'end': now,
          'doubleValue': value,
        },
      },
    ],
  }
  try:
    token = oauth2_access_token()
  except (IOError, urllib2.HTTPError) as e:
    raise SendMetricsFailure(e)

  headers = {
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json',
  }
  logging.info('%s', json.dumps(body, indent=2, sort_keys=True))
  try:
    resp = urllib2.urlopen(urllib2.Request(url, json.dumps(body), headers))
    # Result must be valid JSON. A sample response:
    #   {"kind": "cloudmonitoring#writeTimeseriesResponse"}
    logging.debug(json.load(resp))
  except (IOError, urllib2.HTTPError) as e:
    raise SendMetricsFailure(e)
