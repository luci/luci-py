# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""TODO(maruel): Remove this file.
"""

import logging

from common import rpc
from common import test_request_message
from server import bot_management


# The amount of time to wait after recieving a runners last message before
# considering the runner to have run for too long. Runners that run for too
# long will be aborted automatically.
# Specified in number of seconds.
_TIMEOUT_FACTOR = 300

# The number of pings that need to be missed before a runner is considered to
# have timed out. |_TIMEOUT_FACTOR| / |this| will determine the desired delay
# between pings.
_MISSED_PINGS_BEFORE_TIMEOUT = 10

# Default Test Run Swarm filename.  This file provides parameters
# for the instance running tests.
_TEST_RUN_SWARM_FILE_NAME = 'test_run.swarm'


def CheckVersion(attributes, server_url):
  """Checks the slave version, forcing it to update if required."""
  expected_version = bot_management.SlaveVersion()
  if attributes.get('version') != expected_version:
    logging.info(
        '%s != %s, Updating slave %s',
        expected_version, attributes.get('version', 'N/A'), attributes['id'])
    return {
      'commands': [rpc.BuildRPC(
          'UpdateSlave',
        server_url.rstrip('/') + '/get_slave_code/' + expected_version),
      ],
      # The only time a slave would have results to send here would be if
      # the machine failed to update.
      'result_url': server_url.rstrip('/') + '/remote_error',
      'try_count': 0,
    }
  return {}


def ValidateAndFixAttributes(attributes):
  """Validates format and fixes the attributes of the requesting machine.

  Args:
    attributes: A dictionary representing the machine attributes.

  Raises:
    test_request_message.Error: If the request format/attributes aren't valid.

  Returns:
    A dictionary containing the fixed attributes of the machine.
  """
  # Parse given attributes.
  for attrib, value in attributes.items():
    if attrib == 'dimensions':
      # Make sure the attribute value has proper type.
      if not isinstance(value, dict):
        raise test_request_message.Error('Invalid attrib value for '
                                         'dimensions')
    elif attrib == 'id':
      # Make sure the attribute value has proper type.
      if not isinstance(value, basestring):
        raise test_request_message.Error('Invalid attrib value for id')
    elif (attrib == 'tag' or attrib == 'username' or attrib == 'password' or
          attrib == 'version'):
      # Make sure the attribute value has proper type.
      if not isinstance(value, (str, unicode)):
        raise test_request_message.Error('Invalid attrib value type for '
                                         + attrib)
    elif attrib == 'try_count':
      # Make sure try_count is a non-negative integer.
      if not isinstance(value, int):
        raise test_request_message.Error('Invalid attrib value type for '
                                         'try_count')
      if value < 0:
        raise test_request_message.Error('Invalid negative value for '
                                         'try_count')
    else:
      raise test_request_message.Error('Invalid attribute to machine: '
                                       + attrib)

  # Make sure we have 'dimensions' and 'id', the two required attribs.
  if 'dimensions' not in attributes:
    raise test_request_message.Error('Missing mandatory attribute: '
                                     'dimensions')

  if 'id' not in attributes:
    raise test_request_message.Error('Missing mandatory attribute: id')

  # Make sure attributes now has a try_count field.
  if 'try_count' not in attributes:
    attributes['try_count'] = 0

  return attributes
