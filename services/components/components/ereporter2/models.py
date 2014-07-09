# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import hashlib

from google.appengine.ext import ndb


### Models.


class ErrorReportingInfo(ndb.Model):
  """Notes the last timestamp to be used to resume collecting errors."""
  KEY_ID = 'root'

  timestamp = ndb.FloatProperty()

  @classmethod
  def primary_key(cls):
    return ndb.Key(cls, cls.KEY_ID)


class ErrorReportingMonitoring(ndb.Model):
  """Represents an error that should be limited in its verbosity.

  Key name is the hash of the error name.
  """
  created_ts = ndb.DateTimeProperty(auto_now_add=True, indexed=False)

  # The error string. Limited to 500 bytes. It can be either the exception type
  # or a single line.
  error = ndb.StringProperty(indexed=False)

  # If True, this error is silenced and never reported.
  silenced = ndb.BooleanProperty(default=False, indexed=False)

  # Silence an error for a certain amount of time. This is useful to silence a
  # error for an hour or a day when a known error condition occurs. Only one of
  # |silenced| or |silenced_until| should be set.
  silenced_until = ndb.DateTimeProperty(indexed=False)

  # Minimum number of errors that must occurs before the error is reported.
  threshold = ndb.IntegerProperty(default=0, indexed=False)

  @classmethod
  def error_to_key(cls, error):
    return ndb.Key(cls, hashlib.sha1(error).hexdigest())


class Error(ndb.Model):
  """Represents an error logged either by the server itself or by a client of
  the service.

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
