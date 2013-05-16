#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Implements helpers for dealing with dimensions of machines and tests."""

import hashlib
import itertools
import logging

from google.appengine.api import memcache
from common import test_request_message


# The maximum number of hashes that can be in a list such that the total size
# is < 1 megabyte (otherwise we can't store it in the memcache)
MAX_HASHES_PER_MEMCACHE_ENTRY = 1000000 / len(hashlib.sha1().hexdigest())

# The maximum number of dimension to accept, since having more results in too
# many combinations of the dimensions which might cause unacceptable slowly.
MAX_DIMENSIONS_PER_MACHINE = 15


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
    if (not isinstance(dimension_name, (basestring)) or
        unicode(dimension_name) not in machine_dimensions):
      logs += '%s not in %s\n' % (dimension_name, machine_dimensions)
      return (False, logs)
    # Now that we know the dimension is there, we must confirm it has
    # all the requested values, if there are more than one.
    machine_dimension_value = machine_dimensions[unicode(dimension_name)]
    if isinstance(req_dimension_value, (list, tuple)):
      for req_dimension_value_item in req_dimension_value:
        if (not isinstance(req_dimension_value_item, (basestring)) or
            unicode(req_dimension_value_item) not in machine_dimension_value):
          # TODO(user): We may want to have a separate log message for not str?
          logs += '%s not in %s' % (req_dimension_value_item,
                                    machine_dimension_value)
          return (False, logs)
    else:
      if (not isinstance(req_dimension_value, (basestring)) or
          unicode(req_dimension_value) not in machine_dimension_value):
        logs += '%s not in %s' % (req_dimension_value, machine_dimension_value)
        return (False, logs)
  # Assumed innocent until proven guilty... :-)
  return (True, logs)


def _GenerateHashHexDigest(item):
  """Generate the hash digest for the given item.

  Args:
    item: The item to hash.

  Returns:
    The hash digest of the item.
  """
  return hashlib.sha1(test_request_message.Stringize(item)).hexdigest()


def GenerateCombinations(dimensions):
  """Generate all the possible dimensions combinations from the input.

  Examples:
    Input: {'a': 1, 'b': 2}
    Output: [{}, {'a': [1]}, {'b': [2]}, {'a': [1], 'b': [2]}]
    Input: {'a': [1, 2]}
    Output: [{}, {'a': [1]}, {'a': [2]}, {'a': [1, 2]}]

  Args:
    dimensions: A dictionary representing the dimensions values.

  Returns:
    A list of all the dimension combinations that can be generated from the
        input.
  """
  expanded_dimensions = []
  for (key, value) in dimensions.iteritems():
    if isinstance(value, list):
      expanded_dimensions.extend([key, entry] for entry in value)
    else:
      expanded_dimensions.append([key, value])

  if len(expanded_dimensions) > MAX_DIMENSIONS_PER_MACHINE:
    logging.warning('Machine has too many dimensions. Unable to generate '
                    'all combinations')
    return []

  combinations = []
  # This range can be a bit tricky to understand, we start at 0 to get the
  # combination where no dimensions are selected (This is needed because a
  # runner might be able to execute on any machine, so it would have no
  # configs). The range goes to (len(expanded_dimensions) + 1) so the final
  # iteration will be i=len(expanded_dimensions) to get the combination where
  # all the dimensions are selected.
  for i in range(0, len(expanded_dimensions) + 1):
    combinations.extend(list(itertools.combinations(expanded_dimensions, i)))

  # Collapse all the lists back to dicts, merging repeated keys together
  # (keeping all the values).
  def ConvertToDict(list_to_convert):
    converted = {}
    for (key, value) in list_to_convert:
      converted.setdefault(key, []).append(value)
    return converted

  return map(ConvertToDict, combinations)


def GenerateDimensionHash(dimensions):
  """Generate the hash for the given dimensions.

  Args:
    dimensions: The dimensions to generate a hash for.

  Returns:
    The hash for the given dimensions.
  """
  # Convert the dimensions to a standard format.
  for (key, value) in dimensions.iteritems():
    if isinstance(value, (list, tuple)) and len(value) == 1:
      dimensions[key] = value[0]
    elif isinstance(value, list):
      # With dimensions lists are really sets (but sets don't exist in
      # the json format), so we should sort them to ensure they are
      # properly compared. (i.e. 'a': [1, 2] and 'a': [2, 1] should be equal.
      dimensions[key] = sorted(value)

  return _GenerateHashHexDigest(dimensions)


def GenerateAllDimensionHashes(dimension):
  """Generate all the hashes for all the dimension subsets from the input.

  Args:
    dimension: The dimension to generate all the subsets from.

  Returns:
    A list of hashes.
  """
  full_dimension_hash = GenerateDimensionHash(dimension)
  all_dimension_hashes = memcache.get(full_dimension_hash)

  if not all_dimension_hashes:
    combinations = GenerateCombinations(dimension)

    all_dimension_hashes = map(GenerateDimensionHash, combinations)

    if len(all_dimension_hashes) > MAX_HASHES_PER_MEMCACHE_ENTRY:
      logging.warning('Dimension hash list too big to store in memcache, size '
                      '%d', len(all_dimension_hashes))
    else:
      memcache.add(full_dimension_hash, all_dimension_hashes)

  return all_dimension_hashes
