# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import functools

from components import ereporter2

# Ignore these failures, there's nothing to do.
# TODO(maruel): Store them in the db and make this runtime configurable.
_IGNORED_LINES = (
  # Probably originating from appstats.
  '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
      'appengine/_internal/django/template/__init__.py:729: UserWarning: '
      'api_milliseconds does not return a meaningful value',
)

# Ignore these exceptions.
_IGNORED_EXCEPTIONS = (
  'DeadlineExceededError',
  # These occurs during a transaction.
  'Timeout',
)

# Function that is used to determine if an error entry should be ignored.
should_ignore_error_record = functools.partial(
    ereporter2.should_ignore_error_record, _IGNORED_LINES, _IGNORED_EXCEPTIONS)
