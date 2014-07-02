# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Manages user reports.

These are DB stored reports, vs logservice based reports. This is for events
like client failure or non exceptional server failure where more details is
desired.
"""

import datetime
import logging
import platform
import traceback

from google.appengine.api import datastore_errors
from google.appengine.ext import ndb

from components import auth
from components import utils
from . import api


# Amount of time to keep error logs around.
ERROR_TIME_TO_LIVE = datetime.timedelta(days=30)


class Error(ndb.Model):
  """Represents an infrastructure error on a Swarming bot.

  The entity is immutable once created.
  """
  created_ts = ndb.DateTimeProperty(auto_now_add=True)

  # Examples includes 'bot', 'client', 'run_isolated', 'server'.
  source = ndb.StringProperty(default='unknown')

  # Examples includes 'auth', 'exception', 'task_failure'.
  category = ndb.StringProperty()

  # Identity as seen by auth module.
  identity = ndb.StringProperty()

  # Free form message for 'auth' and 'task_failure'. In case of an exception, it
  # is the exception's text.
  message = ndb.TextProperty()

  # Set if the log entry was generated via an except clause.
  exception_type = ndb.StringProperty()
  # Will be trimmed to 4kb.
  stack = ndb.TextProperty()

  # Can be the client code version or the server version.
  version = ndb.StringProperty()

  python_version = ndb.StringProperty()

  source_ip = ndb.StringProperty()

  # Only applicable for client-side reports.
  args = ndb.StringProperty(repeated=True)
  cwd = ndb.StringProperty()
  duration = ndb.FloatProperty()
  env = ndb.JsonProperty(indexed=False, json_type=dict)
  hostname = ndb.StringProperty()
  os = ndb.StringProperty()
  # The local user, orthogonal to authentication in self.identity.
  user = ndb.StringProperty()

  # Only applicable to 'server' reports.
  endpoint = ndb.StringProperty()


# Keys that can be specified by the client.
# pylint: disable=W0212
VALID_ERROR_KEYS = frozenset(Error._properties) - frozenset(
    ['created_ts', 'identity'])


def log(**kwargs):
  """Adds an error. This will indirectly notify the admins.

  Returns the entity id for the report.
  """
  try:
    identity = auth.get_current_identity().to_bytes()
  except auth.UninitializedError:
    identity = None
  try:
    # Trim all the messages to 4kb to reduce spam.
    LIMIT = 4096
    for key, value in kwargs.items():
      if key not in VALID_ERROR_KEYS:
        logging.error('Dropping unknown detail %s: %s', key, value)
        kwargs.pop(key)
      elif isinstance(value, basestring) and len(value) > LIMIT:
        value = value[:LIMIT-1] + u'\u2026'
        kwargs[key] = value

    if kwargs.get('source') == 'server':
      # Automatically use the version of the server code.
      kwargs.setdefault('version', utils.get_app_version())
      kwargs.setdefault('python_version', platform.python_version())

    error = Error(identity=identity, **kwargs)
    error.put()
    key_id = error.key.integer_id()
    logging.error('Got a %s error\n%s\n%s', error.source, key_id, error.message)
    return key_id
  except (datastore_errors.BadValueError, TypeError) as e:
    stack = api.reformat_stack(traceback.format_exc())
    # That's the error about the error.
    error = Error(
        source='server',
        category='exception',
        message='log(%s) caused: %s' % (kwargs, str(e)),
        exception_type=str(type(e)),
        stack=stack)
    error.put()
    key_id = error.key.integer_id()
    logging.error(
        'Failed to log a %s error\n%s\n%s', error.source, key_id, error.message)
    return key_id
