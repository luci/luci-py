# Copyright 2024 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Implements HMAC-SHA256 using a secret from the GSM."""

import hashlib
import hmac
import logging

from google.appengine.api import app_identity

from components import gsm
from components import utils


def warmup():
  """Warms up local in-memory caches, best effort."""
  try:
    get_shared_hmac_secret().access()
  except Exception:
    logging.exception('Failed to warmup up HMAC key')


def new_mac():
  """Returns an hmac object configure to use SHA256 and the shared secret."""
  key = get_shared_hmac_secret().access()
  return hmac.new(key, digestmod=hashlib.sha256)


@utils.cache
def get_shared_hmac_secret():
  """A gsm.Secret with a key used to HMAC-tag tokens."""
  if utils.is_local_dev_server():
    return _LocalDevServer()
  return gsm.Secret(
      project=app_identity.get_application_id(),
      secret='shared-hmac',
      version='current',
  )


class _LocalDevServer(object):
  """Used when running the server locally e.g. in the smoke test."""

  def access(self):
    return 'local-dev-server'
