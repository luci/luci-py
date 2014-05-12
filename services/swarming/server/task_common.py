# coding: utf-8
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Task's common code and definition."""

import datetime

from google.appengine.api import datastore_errors

from components import utils


# The production server must handle up to 1000 task requests per second. The
# number of root entities must be a few orders of magnitude higher. The goal is
# to almost completely get rid of transactions conflicts. This means that the
# probability of two transactions happening on the same shard must be very low.
# This relates to number of transactions per second * seconds per transaction /
# number of shard.
#
# Intentionally starve the canary server by using only 16³=4096 root entities.
# This will cause mild transaction conflicts during load tests. On the
# production server, use 16**5 (~1 million) root entities to reduce the number
# of transaction conflict.
SHARDING_LEVEL = 3 if utils.is_canary() else 5


# Used to encode time.
UNIX_EPOCH = datetime.datetime(1970, 1, 1)


# Maximum acceptable priority value, which is effectively the lowest priority.
MAXIMUM_PRIORITY = 255


# Maximum number of shards for a single request.
MAXIMUM_SHARDS = 255


# Amount of time after which a bot is considered dead. In short, if a bot has
# not ping in the last 5 minutes while running a task, it is considered dead.
BOT_PING_TOLERANCE = datetime.timedelta(seconds=5*60)


def validate_priority(priority):
  """Throws ValueError if priority is not a valid value."""
  if 0 > priority or MAXIMUM_PRIORITY < priority:
    raise datastore_errors.BadValueError(
        'priority (%d) must be between 0 and %d (inclusive)' %
        (priority, MAXIMUM_PRIORITY))


def utcnow():
  """To be mocked in tests."""
  return datetime.datetime.utcnow()


def milliseconds_since_epoch(now):
  """Returns the number of milliseconds since unix epoch as an int."""
  now = now or utcnow()
  return int(round((now - UNIX_EPOCH).total_seconds() * 1000.))


def match_dimensions(request_dimensions, bot_dimensions):
  """Returns True if the bot dimensions satisfies the request dimensions."""
  assert isinstance(request_dimensions, dict), request_dimensions
  assert isinstance(bot_dimensions, dict), bot_dimensions
  if frozenset(request_dimensions).difference(bot_dimensions):
    return False
  for key, required in request_dimensions.iteritems():
    bot_value = bot_dimensions[key]
    if isinstance(bot_value, (list, tuple)):
      if required not in bot_value:
        return False
    elif required != bot_value:
      return False
  return True
