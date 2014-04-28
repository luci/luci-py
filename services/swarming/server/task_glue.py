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

import datetime

from common import test_request_message
from server import task_request
from server import task_result
from server import task_scheduler
from server import test_management
from server import test_request
from server import test_runner


# Global switch to use the old or new DB. Note unit tests that directly access
# the DB won't pass with the new DB.
USE_OLD_API = True


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


def AbortRunner(runner_key_urlsafe, reason):
  if USE_OLD_API:
    runner = GetRunnerFromUrlSafeKey(runner_key_urlsafe)
    if not runner or runner.started:
      return False
    test_management.AbortRunner(runner, reason)
    return True
  raise NotImplementedError()


def AbortStaleRunners():
  if USE_OLD_API:
    return test_management.AbortStaleRunners()
  raise NotImplementedError()


def ExecuteTestRequest(test_case):
  if USE_OLD_API:
    return test_management.ExecuteTestRequest(test_case)
  raise NotImplementedError()


def ExecuteRegisterRequest(attributes, server_url):
  if USE_OLD_API:
    return test_management.ExecuteRegisterRequest(attributes, server_url)
  raise NotImplementedError()


def RetryRunner(runner_key_urlsafe):
  if USE_OLD_API:
    runner = GetRunnerFromUrlSafeKey(runner_key_urlsafe)
    if not runner:
      return False
    runner.ClearRunnerRun()
    # Update the created time to make sure that retrying the runner does not
    # make it jump the queue and get executed before other runners for requests
    # added before the user pressed the retry button.
    runner.created = datetime.datetime.utcnow()
    runner.put()
    return True
  raise NotImplementedError()


### test_request.py public API.


def UrlSafe(runner_key):
  """Returns an urlsafe encoded key. What it is depends on old vs new."""
  if USE_OLD_API:
    return runner_key.urlsafe()
  return task_scheduler.pack_shard_result_key(runner_key)


def GetAllMatchingTestRequests(test_case_name):
  if USE_OLD_API:
    return test_request.GetAllMatchingTestRequests(test_case_name)
  raise NotImplementedError()


def GetNewestMatchingTestRequests(test_case_name):
  if USE_OLD_API:
    return test_request.GetNewestMatchingTestRequests(test_case_name)
  raise NotImplementedError()


### test_runner.py public API.


ACCEPTABLE_SORTS = test_runner.ACCEPTABLE_SORTS
TIME_BEFORE_RUNNER_HANGING_IN_MINS = (
    test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS)


def GetTestRunners(sort_by, ascending, limit, offset, sort_by_first):
  if USE_OLD_API:
    return test_runner.GetTestRunners(
        sort_by, ascending, limit, offset, sort_by_first)
  # TODO(maruel): Once migration is complete, remove limit and offset, replace
  # with cursor.
  raise NotImplementedError()


def ApplyFilters(query, status, show_successfully_completed, test_name_filter,
                 machine_id_filter):
  if USE_OLD_API:
    return test_runner.ApplyFilters(
        query, status, show_successfully_completed, test_name_filter,
        machine_id_filter)
  raise NotImplementedError()


def GetRunnerFromUrlSafeKey(runner_key_urlsafe):
  if USE_OLD_API:
    return test_runner.GetRunnerFromUrlSafeKey(runner_key_urlsafe)
  raise NotImplementedError()


def PingRunner(runner_key_urlsafe, machine_id):
  if USE_OLD_API:
    return test_runner.PingRunner(runner_key_urlsafe, machine_id)
  raise NotImplementedError()


def QueryOldRunners():
  return test_runner.QueryOldRunners()


def GetHangingRunners():
  if USE_OLD_API:
    return test_runner.GetHangingRunners()
  raise NotImplementedError()


def DeleteRunnerFromKey(key):
  if USE_OLD_API:
    return test_runner.DeleteRunnerFromKey(key)
  raise NotImplementedError()


def GetRunnerResults(runner_key_urlsafe):
  if USE_OLD_API:
    return test_runner.GetRunnerResults(runner_key_urlsafe)
  raise NotImplementedError()


def UpdateTestResult(runner, machine_id, success, exit_codes, results,
                     overwrite):
  if USE_OLD_API:
    return runner.UpdateTestResult(
        machine_id, success, exit_codes=exit_codes, results=results,
        overwrite=overwrite)
  assert not overwrite
  raise NotImplementedError()


### UI code that should be in jinja2 templates


def GenerateButtonWithHiddenForm(button_text, url, form_id):
  """Generate a button that when used will post to the given url.

  Args:
    button_text: The text to display on the button.
    url: The url to post to.
    form_id: The id to give the form.

  Returns:
    The html text to display the button.
  """
  button_html = '<form id="%s" method="post" action=%s>' % (form_id, url)
  button_html += (
      '<button onclick="document.getElementById(%s).submit()">%s</button>' %
      (form_id, button_text))
  button_html += '</form>'

  return button_html


def make_runner_view(runner):
  """Returns a html template friendly dict from a TestRunner."""
  # TODO(maruel): This belongs to a jinja2 template, not here.
  if USE_OLD_API:
    out = runner.to_dict()
    out['class_string'] = ''
    out['command_string'] = '&nbsp;'
    out['key_string'] = UrlSafe(runner.key)
    out['status_string'] = '&nbsp;'
    out['user'] = out['requestor']
    if runner.done:
      # TODO(maruel): All this should be done in the template instead.
      if runner.ran_successfully:
        out['status_string'] = (
            '<a title="Click to see results" href="%s?r=%s">Succeeded</a>' %
            ('/secure/get_result', out['key_string']))
      else:
        out['class_string'] = 'failed_test'
        out['command_string'] = GenerateButtonWithHiddenForm(
            'Retry',
            '/secure/retry?r=%s' % out['key_string'],
            out['key_string'])
        out['status_string'] = (
            '<a title="Click to see results" href="%s?r=%s">Failed</a>' %
            ('/secure/get_result', out['key_string']))
    elif runner.started:
      out['status_string'] = 'Running on %s' % runner.machine_id
    else:
      out['status_string'] = 'Pending'
      out['command_string'] = GenerateButtonWithHiddenForm(
          'Cancel',
          '%s?r=%s' % ('/secure/cancel', out['key_string']),
          out['key_string'])
    return out

  # Simulate the old properties until the templates are updated.
  request, result = runner
  assert isinstance(request, task_request.TaskRequest), request
  assert isinstance(result, task_result.TaskResultSummary), result
  out = {
    # TODO(maruel): out['class_string'] = 'failed_test'
    'class_string': '',
    'command_string': 'TODO',
    'created': request.created_ts,
    'ended': result.done_ts,
    'key_string': '%x' % request.key.integer_id(),
    # TODO(maruel):
    # 'machine_id': ','.join(i.bot_id for i in runner.shards if i.bot_id),
    'machine_id': 'TODO',
    'name': request.name,
    'started': min(i.started_ts for i in result.shards),
    'status_string': result.to_string(),
    'user': request.user,
  }
  return out
