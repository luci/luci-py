# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Configuration for ereporter2 component."""

from components import ereporter2


# Ignore these failures, there's nothing to do.
# TODO(maruel): Store them in the db and make this runtime configurable.
IGNORED_LINES = (
  # And..?
  '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
      'appengine/_internal/django/template/__init__.py:729: UserWarning: '
      'api_milliseconds does not return a meaningful value',
)

# Ignore these exceptions.
IGNORED_EXCEPTIONS = (
  'CancelledError',
  'DeadlineExceededError',
  ereporter2.SOFT_MEMORY,
)


def should_ignore_error_record(error_record):
  """Callback used by ereporter2 to filter out unimportant errors."""
  return ereporter2.should_ignore_error_record(
      IGNORED_LINES, IGNORED_EXCEPTIONS, error_record)
