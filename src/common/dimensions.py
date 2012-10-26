#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Implements helpers for dealing with dimensions of machines and tests."""


def MatchDimensions(requested_dimensions, machine_dimensions):
  """Identify if the machine's dimensions match the requested dimensions.

  Note that the machine could have more than what is requested but it must
  match ALL the requested dimensions.

  Args:
    requested_dimensions: A dictionary containing the requested dimensions.
    machine_dimensions: A dictionary containing the machine's dimensions.

  Returns:
    A tuple: (True if the dimensions match and False otherwise.
              A string containing any errors or warnings generated.)
  """
  logs = ''
  # An empty set of requested dimensions matches with anything...
  for (dimension_name, req_dimension_value) in requested_dimensions.items():
    # Dimension names must be strings... Our machine dimensions are unicode.
    if (not isinstance(dimension_name, (str, unicode)) or
        unicode(dimension_name) not in machine_dimensions):
      logs += '%s not in %s\n' % (dimension_name, machine_dimensions)
      return (False, logs)
    # Now that we know the dimension is there, we must confirm it has
    # all the requested values, if there are more than one.
    machine_dimension_value = machine_dimensions[unicode(dimension_name)]
    if isinstance(req_dimension_value, (list, tuple)):
      for req_dimension_value_item in req_dimension_value:
        if (not isinstance(req_dimension_value_item, (str, unicode)) or
            unicode(req_dimension_value_item) not in machine_dimension_value):
          # TODO(user): We may want to have a separate log message for not str?
          logs += '%s not in %s' % (req_dimension_value_item,
                                    machine_dimension_value)
          return (False, logs)
    else:
      if (not isinstance(req_dimension_value, (str, unicode)) or
          unicode(req_dimension_value) not in machine_dimension_value):
        logs += '%s not in %s' % (req_dimension_value, machine_dimension_value)
        return (False, logs)
  # Assumed innocent until proven guilty... :-)
  return (True, logs)
