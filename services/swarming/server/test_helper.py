# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Some helper test functions."""

import datetime

from google.appengine.ext import ndb

from common import test_request_message
from server import result_helper
from server import task_common
from server import task_glue
from server import task_result
from server import task_scheduler
from server import task_to_run


# The default root for all configs in a test request. The index value of the
# config is appended to the end to make the full default name.
REQUEST_MESSAGE_CONFIG_NAME_ROOT = 'c1'

# The default name for a TestRequest.
REQUEST_MESSAGE_TEST_CASE_NAME = 'tc'


def GetRequestMessage(request_name=REQUEST_MESSAGE_TEST_CASE_NAME,
                      config_name_root=REQUEST_MESSAGE_CONFIG_NAME_ROOT,
                      num_instances=1,
                      env_vars=None,
                      restart_on_failure=False,
                      platform='win-xp',
                      priority=10,
                      num_configs=1,
                      requestor=None):
  """Return a properly formatted request message text.

  Args:
    request_name: The name of the test request.
    config_name_root: The base name of each config (it will have the instance
        number appended to it).
    num_instances: The number of instance for the given config.
    env_vars: A dictionary of environment variables for the request.
    restart_on_failure: Identifies if the slave should be restarted if any
        of its tests fail.
    platform: The os to require in the test's configuration.
    priority: The priority given to this request.
    num_configs: The number of configs to have in the request message.

  Returns:
    A properly formatted request message text.
  """
  tests = [
    test_request_message.TestObject(test_name='t1', action=['ignore-me.exe']),
  ]
  configurations = [
    test_request_message.TestConfiguration(
        config_name=config_name_root + '_' + str(i),
        dimensions=dict(os=platform, cpu='Unknown', browser='Unknown'),
        num_instances=num_instances,
        priority=priority)
    for i in range(num_configs)
  ]
  request = test_request_message.TestCase(
      restart_on_failure=restart_on_failure,
      requestor=requestor,
      test_case_name=request_name,
      configurations=configurations,
      data=[('http://b.ina.ry/files2.zip', 'files2.zip')],
      env_vars=env_vars,
      tests=tests)
  return test_request_message.Stringize(request, json_readable=True)


def CreateRunner(config_name=None, machine_id=None, ran_successfully=None,
                 exit_codes=None, created=None, started=None,
                 ended=None, requestor=None, results=None):
  """Creates entities to represent a new task request and a bot running it.

  For the old DB, it's a TestRequest and TestRunner pair
  For the new DB, it's a TaskRequest and friends.

  The entity may be in a pending, running or completed state.

  Args:
    config_name: The config name for the runner.
    machine_id: The machine id for the runner. Also, if this is given the
      runner is marked as having started.
    ran_succesfully: True if the runner ran successfully.
    exit_codes: The exit codes for the runner. Also, if this is given the
      runner is marked as having finished.
    created: timedelta from now to mark this request was created in the future.
    started: timedelta from .created to when the task execution started.
    ended: timedelta from .started to when the bot completed the task.
    requestor: string representing the user name that requested the task.
    results: string that represents stdout the bot sent back.

  Returns:
    The pending runner created.
  """
  config_name = config_name or (REQUEST_MESSAGE_CONFIG_NAME_ROOT + '_0')
  request_message = GetRequestMessage(requestor=requestor)
  data = task_glue.convert_test_case(request_message)
  request, result_summary = task_scheduler.make_request(data)
  if created:
    # For some reason, pylint is being obnoxious here and generates a W0201
    # warning that cannot be disabled. See http://www.logilab.org/19607
    setattr(request, 'created_ts', datetime.datetime.utcnow() + created)
    request.put()

  # The task is reaped by a bot, so it is in a running state. Pending state is
  # not supported here.
  task_key = task_to_run.request_to_task_to_run_key(request)
  task = task_to_run.is_task_reapable(task_key, None)
  task.put()

  machine_id = machine_id or 'localhost'
  run_result = task_result.new_run_result(request, 1, machine_id)

  if ran_successfully or exit_codes:
    data = {
      'exit_codes': map(int, exit_codes.split(',')) if exit_codes else [0],
    }
    if results:
      data['outputs'] = [result_helper.StoreResults(results).key]
    # The entity needs to be saved before it can be updated, since
    # bot_update_task() accepts the key of the entity.
    task_result.put_run_result(run_result)
    if not task_scheduler.bot_update_task(run_result.key, data, machine_id):
      assert False, (
          'Expected to reap the TaskToRun that was created lines above')
    # Refresh it from the DB.

  if started:
    run_result.started_ts = run_result.created + started
  if ended:
    run_result.completed_ts = run_result.started_ts + ended

  # Mark the job as at least running.
  task_result.put_run_result(run_result)

  return result_summary, run_result


def mock_now(test, now):
  """Mocks utcnow() and ndb properties.

  In particular handles when auto_now and auto_now_add are used.

  To be used in tests only.
  """
  test.mock(task_common, 'utcnow', lambda: now)
  test.mock(ndb.DateTimeProperty, '_now', lambda _: now)
  test.mock(ndb.DateProperty, '_now', lambda _: now.date())
