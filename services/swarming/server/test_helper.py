# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Some helper test functions."""


import datetime

from common import test_request_message
from server import test_request
from server import test_runner


# The default root for all configs in a test request. The index value of the
# config is appended to the end to make the full default name.
REQUEST_MESSAGE_CONFIG_NAME_ROOT = 'c1'

# The default name for a TestRequest.
REQUEST_MESSAGE_TEST_CASE_NAME = 'tc'

DEFAULT_RESULT_URL = 'http://all.your.resul.ts/are/belong/to/us'


def _CreateRunner(request, config_name):
  """Create a basic runner.

  Args:
    request: The TestRequest that owns this runner.
    config_name: The config name for the runner.

  Returns:
    The newly created runner.
  """
  runner = test_runner.TestRunner(
      request=request.key,
      config_name=config_name,
      config_hash=request.GetConfigurationDimensionHash(config_name))
  runner.put()

  request.runner_keys.append(runner.key)
  request.put()

  return runner


def GetRequestMessage(request_name=REQUEST_MESSAGE_TEST_CASE_NAME,
                      config_name_root=REQUEST_MESSAGE_CONFIG_NAME_ROOT,
                      num_instances=1,
                      env_vars=None,
                      result_url=DEFAULT_RESULT_URL,
                      store_result='all',
                      restart_on_failure=False,
                      platform='win-xp',
                      priority=10,
                      num_configs=1):
  """Return a properly formatted request message text.

  Args:
    request_name: The name of the test request.
    config_name_root: The base name of each config (it will have the instance
        number appended to it).
    num_instances: The number of instance for the given config.
    env_vars: A dictionary of environment variables for the request.
    result_url: The result url to use.
    store_result: Identifies which Runner and Request data should stay in
        storage after the tests are done running (fail means only the failures
        are kept).
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
        data=['http://b.ina.ry/files2.zip'],
        dimensions=dict(os=platform, cpu='Unknown', browser='Unknown'),
        num_instances=num_instances,
        priority=priority,
        tests=[
          test_request_message.TestObject(
              test_name='t2', action=['ignore-me-too.exe']),
        ])
    for i in range(num_configs)
  ]
  request = test_request_message.TestCase(
      restart_on_failure=restart_on_failure,
      result_url=result_url,
      store_result=store_result,
      test_case_name=request_name,
      configurations=configurations,
      env_vars=env_vars,
      tests=tests)
  return test_request_message.Stringize(request, json_readable=True)


def CreatePendingRunner(config_name=None, machine_id=None, exit_codes=None):
  """Create a basic pending TestRunner.

  Args:
    config_name: The config name for the runner.
    machine_id: The machine id for the runner. Also, if this is given the
      runner is marked as having started.
    exit_codes: The exit codes for the runner. Also, if this is given the
      runner is marked as having finished.

  Returns:
    The pending runner created.
  """
  if not config_name:
    config_name = REQUEST_MESSAGE_CONFIG_NAME_ROOT + '_0'

  request = test_request.TestRequest(message=GetRequestMessage(),
                                     name=REQUEST_MESSAGE_TEST_CASE_NAME)
  request.put()

  runner = _CreateRunner(request, config_name)
  runner.config_instance_index = 0
  runner.num_config_instances = 1

  if machine_id:
    runner.machine_id = machine_id
    runner.started = datetime.datetime.utcnow()
    runner.ping = runner.started

  if exit_codes:
    runner.done = True
    runner.ended = datetime.datetime.utcnow()
    runner.exit_codes = exit_codes

  runner.put()

  return runner


def CreateRequest(num_instances):
  """Creates a basic request with the specified number of runners.

  Args:
    num_instances: The number of runner instances to give this request.

  Returns:
    The newly created request.
  """
  request = test_request.TestRequest(message=GetRequestMessage(),
                                     name=REQUEST_MESSAGE_TEST_CASE_NAME)
  request.put()

  for i in range(num_instances):
    runner = _CreateRunner(request, REQUEST_MESSAGE_CONFIG_NAME_ROOT + '_0')
    runner.config_instance_index = i
    runner.num_config_instances = num_instances
    runner.put()

  return request
