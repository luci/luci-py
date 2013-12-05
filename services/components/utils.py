# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Mixed bag of small utilities."""

import datetime
import json

from google.appengine.ext import ndb


class SmartJsonEncoder(json.JSONEncoder):
  """json encoder that supports ndb.Model and datetime serialization."""
  def __init__(self, **kwargs):
    """Changes default."""
    kwargs.setdefault('separators', (',',':'))
    kwargs.setdefault('sort_keys', True)
    super(SmartJsonEncoder, self).__init__(self, **kwargs)

  def default(self, obj):  # pylint: disable=E0202
    if isinstance(obj, datetime.datetime):
      # Convert datetime objects into a string, stripping off milliseconds.
      # Only accept naive objects.
      if obj.tzinfo is not None:
        raise ValueError('Can only serialize naive datetime instance')
      return obj.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(obj, datetime.date):
      return obj.strftime('%Y-%m-%d')
    if isinstance(obj, datetime.timedelta):
      # Convert timedelta into seconds, stripping off milliseconds.
      return int(obj.total_seconds())
    if isinstance(obj, ndb.Model):
      return obj.to_dict()
    return super(SmartJsonEncoder, self).default(obj)
