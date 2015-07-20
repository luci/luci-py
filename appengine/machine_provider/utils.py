# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Miscellaneous helper functions."""

import collections


def fingerprint(proto):
  """Returns a fingerprint of a simple messages.Message instance.

  Args:
    proto: A messages.Message instance.

  Returns:
    A string which uniquely identifies proto.
  """
  components = collections.OrderedDict()

  for field in sorted(proto.all_fields(), key=lambda field: field.number):
    if proto.get_assigned_value(field.name) is not None:
      components[field.number] = proto.get_assigned_value(field.name)

  return '\0'.join(
      '%s\0%s' % (key, value) for key, value in components.iteritems()
  )
