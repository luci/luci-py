# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Translation layer between the old API and the new one.

Implements the public API of test_manager.py, test_request.py and
test_runner.py used in handlers.py.

Plan of attack:
- Create a translation layer to keep mostly the same API for test_management.py
  vs task_scheduler.py. It won't be 100% compatible but should be close enough
  to make the switch over easy. In the meantime, it'll be quite cheezy since the
  paradigms do not match exactly.
- Convert swarm_bot to use the new versioned API.
- Make sure all bots are deployed with new bot API.
- Delete old bot code.
- Create new client API.
- Deploy servers.
- Switch client code to use new API.
- Roll client code into chromium.
- Wait 1 month.
- Remove old client code API.

The goal is to make tests/server_smoke_test.py pass, not handlers_test.py, since
handlers_test.py edits the DB directly.

Entities translation is:
  TestRequest -> TaskRequest
  TestRunner -> TaskShardResult
The translation is not 1:1 and has patches accordingly until we get rid of the
old ones.
"""

from common import test_request_message


# Temporary, as the API is refactored while the old code is being deleted.
# pylint: disable=W0613


def convert_test_case(data):
  """Constructs a TaskProperties out of a test_request_message.TestCase.

  This code is kept for compatibility with the previous API. See make_request()
  for more details.
  """
  test_case = test_request_message.TestCase.FromJSON(data)
  # TODO(maruel): Add missing mapping and delete obsolete ones.
  assert len(test_case.configurations) == 1, test_case.configurations
  config = test_case.configurations[0]

  if test_case.tests:
    execution_timeout_secs = int(round(test_case.tests[0].hard_time_out))
    io_timeout_secs = int(round(test_case.tests[0].io_time_out))
  else:
    execution_timeout_secs = 2*60*60
    io_timeout_secs = 60*60

  # Ignore all the settings that are deprecated.
  return {
    'name': test_case.test_case_name,
    'user': test_case.requestor,
    'properties': {
      'commands': [c.action for c in test_case.tests],
      'data': test_case.data,
      'dimensions': config.dimensions,
      'env': test_case.env_vars,
      'number_shards': config.num_instances,
      'execution_timeout_secs': execution_timeout_secs,
      'io_timeout_secs': io_timeout_secs,
    },
    'priority': config.priority,
    'scheduling_expiration_secs': config.deadline_to_run,
  }
