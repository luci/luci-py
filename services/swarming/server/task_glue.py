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
"""

from common import test_request_message
from server import test_management
from server import test_request
from server import test_runner


# Global switch to use the old or new DB. Note unit tests that directly access
# the DB won't pass with the new DB.
_USE_OLD_API = True


def _convert_test_case(data):
  """Constructs a TaskProperties out of a test_request_message.TestCase.

  This code is kept for compatibility with the previous API. See new_request()
  for more details.
  """
  test_case = test_request_message.TestCase.FromJSON(data)
  # TODO(maruel): Add missing mapping and delete obsolete ones.
  assert len(test_case.configurations) == 1, test_case.configurations
  config = test_case.configurations[0]

  # Ignore all the settings that are deprecated.
  return {
    'name': test_case.test_case_name,
    'user': test_case.requestor,
    'commands': [c.action for c in test_case.tests],
    'data': test_case.data,
    'dimensions': config.dimensions,
    'env': test_case.env_vars,
    'shards': config.num_instances,
    'priority': config.priority,
    'scheduling_expiration': config.deadline_to_run,
    'execution_timeout': int(round(test_case.tests[0].hard_time_out)),
    'io_timeout': int(round(test_case.tests[0].io_time_out)),
  }


### test_manager.py public API.


def AbortRunner(runner, reason):
  if _USE_OLD_API:
    return test_management.AbortRunner(runner, reason)
  raise NotImplementedError()


def AbortStaleRunners():
  if _USE_OLD_API:
    return test_management.AbortStaleRunners()
  raise NotImplementedError()


def ExecuteTestRequest(test_case):
  if _USE_OLD_API:
    return test_management.ExecuteTestRequest(test_case)
  raise NotImplementedError()


def ExecuteRegisterRequest(attributes, server_url):
  if _USE_OLD_API:
    return test_management.ExecuteRegisterRequest(attributes, server_url)
  raise NotImplementedError()


### test_request.py public API.


def GetAllMatchingTestRequests(test_case_name):
  if _USE_OLD_API:
    return test_request.GetAllMatchingTestRequests(test_case_name)
  raise NotImplementedError()


def GetNewestMatchingTestRequests(test_case_name):
  if _USE_OLD_API:
    return test_request.GetNewestMatchingTestRequests(test_case_name)
  raise NotImplementedError()


### test_runner.py public API.


ACCEPTABLE_SORTS = test_runner.ACCEPTABLE_SORTS
TIME_BEFORE_RUNNER_HANGING_IN_MINS = (
    test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS)


def GetTestRunners(sort_by, ascending, limit, offset, sort_by_first):
  if _USE_OLD_API:
    return test_runner.GetTestRunners(
        sort_by, ascending, limit, offset, sort_by_first)
  # TODO(maruel): Once migration is complete, remove limit and offset, replace
  # with cursor.
  raise NotImplementedError()


def ApplyFilters(query, status, show_successfully_completed, test_name_filter,
                 machine_id_filter):
  if _USE_OLD_API:
    return test_runner.ApplyFilters(
        query, status, show_successfully_completed, test_name_filter,
        machine_id_filter)
  raise NotImplementedError()


def GetRunnerFromUrlSafeKey(runner_key_urlsafe):
  if _USE_OLD_API:
    return test_runner.GetRunnerFromUrlSafeKey(runner_key_urlsafe)
  raise NotImplementedError()


def PingRunner(runner_key_urlsafe, machine_id):
  if _USE_OLD_API:
    return test_runner.PingRunner(runner_key_urlsafe, machine_id)
  raise NotImplementedError()


def QueryOldRunners():
  return test_runner.QueryOldRunners()


def GetHangingRunners():
  if _USE_OLD_API:
    return test_runner.GetHangingRunners()
  raise NotImplementedError()


def DeleteRunnerFromKey(key):
  if _USE_OLD_API:
    return test_runner.DeleteRunnerFromKey(key)
  raise NotImplementedError()


def GetRunnerResults(runner_key_urlsafe):
  if _USE_OLD_API:
    return test_runner.GetRunnerResults(runner_key_urlsafe)
  raise NotImplementedError()


def UpdateTestResult(runner, machine_id, success, exit_codes, results,
                     overwrite):
  if _USE_OLD_API:
    return runner.UpdateTestResult(
        machine_id, success, exit_codes=exit_codes, results=results,
        overwrite=overwrite)
  assert not overwrite
  raise NotImplementedError()
