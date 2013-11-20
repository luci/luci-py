#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Dimension Generator.

Generates the dimensions for the machine it runs on. If it is given a file name
it will write out the dimensions to that file.
"""


import json
import logging
import optparse
import sys


# A mapping between sys.platform values and the corresponding swarm name
# for that platform.
PLATFORM_MAPPING = {
    'darwin': 'Mac',
    'cygwin': 'Windows',
    'linux2': 'Linux',
    'win32': 'Windows'
    }


class DimensionsGenerator(object):
  """A base class for creating dimension files."""

  def GetDimensions(self):  # pylint: disable=R0201
    """Returns a dictionary of attributes representing this machine.

    Returns:
      A dictionary of the attributes of the machine.
    """
    if sys.platform not in PLATFORM_MAPPING:
      logging.error('Running on an unknown platform, %s, unable to '
                    'generate dimensions', sys.platform)
      return {}

    return {'dimensions': {'os': PLATFORM_MAPPING[sys.platform]}}

  def WriteDimensionsToFile(self, filename):
    """Write out the dimensions to the given file.

    Args:
      filename: The name of the file to write the dimensions out to.

    Returns:
      True if the file was succesfully written to.
    """
    try:
      dimensions_file = open(filename, mode='w')
    except IOError:
      logging.error('Cannot open file %s.', filename)
      return False

    json.dump(self.GetDimensions(), dimensions_file)
    dimensions_file.close()

    return True


def main():
  parser = optparse.OptionParser(
      usage='%prog [options] filename',
      description='Automatically generates the dimensions for the current '
      'machine and stores them in the given file.')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to DEBUG. Optional. Defaults to '
                    'ERROR level.')

  (options, args) = parser.parse_args()

  if options.verbose:
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.getLogger().setLevel(logging.ERROR)

  if len(args) != 1:
    parser.error('Must specify only one filename.')

  dimensions = DimensionsGenerator()
  dimensions.WriteDimensionsToFile(args[0])


if __name__ == '__main__':
  sys.exit(main())
