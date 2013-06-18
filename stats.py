# Copyright 2013 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""Generates statistics out of logs.

The first 100mb of logs read is free. It's important to keep logs concise also
for general performance concerns. Each http handler should strive to do only one
log entry at info level per request.
"""

import logging


### Public API


# Action to log.
STORE, RETURN, LOOKUP, DUPE = range(4)


def log(action, number, where):
  """Formatted statistics log entry so it can be processed for daily stats.

  The format is simple enough that it doesn't require a regexp for faster
  processing.
  """
  logging.info('%s%s; %d; %s', _PREFIX, _ACTION_NAMES[action], number, where)


### Utility


# Text to store for the corresponding actions.
_ACTION_NAMES = ['store', 'return', 'lookup', 'dupe']


# Logs prefix.
_PREFIX = 'Stats: '
