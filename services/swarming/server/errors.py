# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Error management."""

import datetime

from google.appengine.ext import ndb

# Number of days to keep error logs around.
SWARM_ERROR_TIME_TO_LIVE_DAYS = 7


class SwarmError(ndb.Model):
  """Represents an infrastructure error on a Swarming bot.

  TODO(maruel): Do not directly expose the entity, add proper function to log an
  error instead.
  TODO(maruel): Integrate with ereporter2 so we hear about them.
  """
  # The name of the error.
  name = ndb.StringProperty(indexed=False)

  # A description of the error.
  message = ndb.StringProperty(indexed=False)

  # Optional details about the specific error instance.
  info = ndb.StringProperty(indexed=False)

  # The time at which this error was logged. Used to clean up old errors.
  created = ndb.DateTimeProperty(auto_now_add=True)


def _utcnow():
  """To be mocked in tests."""
  return datetime.datetime.utcnow()


def QueryOldErrors():
  """Returns keys for errors older than SWARM_ERROR_TIME_TO_LIVE_DAYS."""
  old_cutoff = (
      _utcnow() - datetime.timedelta(days=SWARM_ERROR_TIME_TO_LIVE_DAYS))
  return SwarmError.query(
      SwarmError.created < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))
