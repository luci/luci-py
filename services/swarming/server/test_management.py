# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Test Management.

The Test Management files contains the pipe functions to setup and run tests
through the TestRunner and TestRequest classes.
"""

import datetime
import logging
import math
import os.path
import random

from google.appengine.api import datastore_errors
from google.appengine.api import memcache
from google.appengine.ext import ndb

from common import bot_archive
from common import rpc
from common import test_request_message
from server import dimension_mapping
from server import dimensions_utils
from server import file_chunks
from server import test_request
from server import test_runner
from stats import machine_stats


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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

# Maximum value for the come_back field in a response to an idle slave machine.
# TODO(user): make this adjustable by the user.
MAX_COMEBACK_SECS = 60.0

# Maximum cap for try_count. A try_count value greater than this is clamped to
# this constant which will result in ~400M secs (>3 years).
MAX_TRY_COUNT = 30

# The odds of giving the machine a quick callback value, instead of the normal
# exponential value.
CHANCE_OF_QUICK_COMEBACK = 1.0 / 20.0

# The time to use when we want the machine to have a quick callback time.
QUICK_COMEBACK_SECS = 1.0

# The amount of time we want a machine to wait before calling back after seeing
# a server error.
COMEBACK_AFTER_SERVER_ERROR_SECS = 10.0

# Number of days to keep error logs around.
SWARM_ERROR_TIME_TO_LIVE_DAYS = 7

# The key to use to access the start slave script file model.
START_SLAVE_SCRIPT_KEY = 'start_slave_script'


class SwarmError(ndb.Model):
  """A datastore entry representing an error in Swarm."""
  # The name of the error.
  name = ndb.StringProperty(indexed=False)

  # A description of the error.
  message = ndb.StringProperty(indexed=False)

  # Optional details about the specific error instance.
  info = ndb.StringProperty(indexed=False)

  # The time at which this error was logged.  Used to clean up old errors.
  # Don't use auto_now_add so we control exactly what the time is set to
  # (since we later need to compare this value, so we need to know if it was
  # made with .now() or .utcnow()).
  created = ndb.DateTimeProperty()

  def _pre_put_hook(self):
    """Stores the creation time for this model."""
    if not self.created:
      self.created = datetime.datetime.utcnow()


def ExecuteTestRequest(request_message):
  """Attempts to execute a test request.

  Test configurations will be queued up for testing at a later time
  when a matching machine queries the server for work.

  Args:
    request_message: A string representing a test request.

  Raises:
    test_request_message.Error: If the request's message isn't valid.

  Returns:
    A dictionary containing the test_case_name field and an array of
    dictionaries containing the config_name and test_id_key fields.
  """
  logging.debug('TRM.ExecuteTestRequest msg=%s', request_message)

  # This will raise on an invalid request.
  test_case = test_request.GetTestCase(request_message)
  request = test_request.TestRequest(name=test_case.test_case_name,
                                     message=request_message)
  test_keys = {'test_case_name': test_case.test_case_name,
               'test_keys': []}

  def AddRunnerToKeys(runner):
    test_keys['test_keys'].append({
        'config_name': runner.config_name,
        'instance_index': runner.config_instance_index,
        'num_instances': runner.num_config_instances,
        'test_key': runner.key.urlsafe()})

  matching_request = test_request.GetNewestMatchingTestRequest(
      test_case.test_case_name)
  if matching_request:
    if not matching_request.GetTestCase().Equivalent(test_case):
      raise test_request_message.Error(
          'The test case name, %s, has already been used by another test case '
          'which doesn\'t have equivalent values. Please select a new test '
          'case name.' % test_case.test_case_name)

    # Check that the old request still has all it's test runners (we can't use
    # the old values if they were already deleted).
    required_runners = sum(test.min_instances
                           for test in test_case.configurations)
    if len(matching_request.runner_keys) == required_runners:
      matching_runners = ndb.get_multi(matching_request.runner_keys)

      # Reuse the old runner if none of them have failed, otherwise rerun them
      # all.
      if (all(runner and not runner.done or runner.ran_successfully
              for runner in matching_runners)):
        for runner in matching_runners:
          AddRunnerToKeys(runner)
        return test_keys

  # Only store the request if we are actually going to use it.
  request.put()

  for config in test_case.configurations:
    logging.debug('Creating runners for request=%s config=%s',
                  request.name, config.config_name)
    config_hash = request.GetConfigurationDimensionHash(config.config_name)
    # Ensure that we have a record of something with this config getting
    # created.
    dimension = dimension_mapping.DimensionMapping.get_or_insert(
        config_hash,
        dimensions=test_request_message.Stringize(config.dimensions))
    if dimension.last_seen != datetime.datetime.utcnow().date():
      # DimensionMapping automatically updates last_seen when put() is called.
      dimension.put()

    # TODO(user): deal with addition_instances later.
    assert config.min_instances > 0
    config.num_instances = config.min_instances
    for instance_index in range(config.min_instances):
      config.instance_index = instance_index
      runner = _QueueTestRequestConfig(request, config, config_hash)

      AddRunnerToKeys(runner)
      # Ensure that the request has the keys of all its runners.
      request.runner_keys.append(runner.key)

  # Save the request to save the runner keys.
  request.put()

  return test_keys


def _QueueTestRequestConfig(request, config, config_hash):
  """Queue a given request's configuration for execution.

  Args:
    request: A TestRequest object to execute.
    config: A TestConfiguration object representing the machine on which to
        run the test.
    config_hash: The config_hash for this request config.

  Returns:
    A tuple containing the id key of the test runner that was created
    and saved, as well as the test runner.
  """
  # Create a runner entity to record this request/config pair that needs
  # to be run. The runner will eventually be scheduled at a later time.
  runner = test_runner.TestRunner(
      request=request.key, requestor=request.GetTestCase().requestor,
      config_name=config.config_name, config_hash=config_hash,
      config_instance_index=config.instance_index,
      num_config_instances=config.num_instances,
      run_by = datetime.datetime.utcnow() + datetime.timedelta(
          seconds=config.deadline_to_run),
      priority=config.priority)

  runner.put()

  return runner


def _BuildTestRun(runner, server_url):
  """Build a Test Run message for the remote test script.

  Args:
    runner: A TestRunner object for this test run.
    server_url: The URL to the Swarm server so that we can set the
        result_url in the Swarm file we upload to the machines.

  Raises:
    test_request_message.Error: If the request's message isn't valid.

  Returns:
    A Test Run message for the remote test script.
  """
  request = runner.request.get().GetTestCase()
  config = runner.GetConfiguration()
  test_run = test_request_message.TestRun(
      test_run_name=request.test_case_name,
      env_vars=request.env_vars,
      instance_index=runner.config_instance_index,
      num_instances=runner.num_config_instances,
      configuration=config,
      result_url=('%s/result?r=%s&id=%s' % (server_url,
                                            runner.key.urlsafe(),
                                            runner.machine_id)),
      ping_url=('%s/runner_ping?r=%s&id=%s' % (server_url,
                                               runner.key.urlsafe(),
                                               runner.machine_id)),
      ping_delay=(_TIMEOUT_FACTOR / _MISSED_PINGS_BEFORE_TIMEOUT),
      output_destination=request.output_destination,
      cleanup=request.cleanup,
      data=(request.data + config.data),
      tests=request.tests + config.tests,
      working_dir=request.working_dir,
      encoding=request.encoding)
  test_run.ExpandVariables({
      'instance_index': runner.config_instance_index,
      'num_instances': runner.num_config_instances,
  })
  test_run.Validate()
  return test_run


def _AbortUnfullfilledRunner(runner):
  """Aborts an unfilled runner.

  An unfilled runner is one that hasn't been able to find a machine to run on
  within it's run_by deadline.

  Args:
    runner: The unfilled runner to abort.
  """
  AbortRunner(runner, reason=('Runner was unable to find a machine to '
                              'run it within %d seconds' %
                              (runner.run_by - runner.created).total_seconds()))


def AbortStaleRunners():
  """Abort any runners are taking too long to run or too long to find a match.

  If the runner is aborted because the machine timed out, it will
  automatically be retried if it hasn't been aborted more than
  MAX_AUTOMATIC_RETRY times.
  If a runner is aborted because it hasn't hasn't found any machine to run it
  in over SWARM_RUNNER_MAX_WAIT_SECS seconds, there is no automatic retry.
  """
  logging.debug('TRM.AbortStaleRunners starting')
  now = _GetCurrentTime()
  # If any active runner hasn't recieved a ping in the last _TIMEOUT_FACTOR
  # seconds then we consider it stale and abort it.
  timeout_cutoff = now - datetime.timedelta(seconds=_TIMEOUT_FACTOR)

  # Abort all currently running runners that haven't recently pinged the
  # server.
  def HandleStaleRunner(runner):
    # Get the most updated version of the runner.
    try:
      runner = ndb.transaction(runner.key.get)
    except datastore_errors.TransactionFailedError:
      # If we can't get the newest version of the runner, don't worry about
      # aborting it (since this probably means someone else is updating it).
      return

    # Ensure that the runner really has timed out and not finished.
    if not runner or runner.done or timeout_cutoff < runner.ping:
      return

    if test_runner.ShouldAutomaticallyRetryRunner(runner):
      if test_runner.AutomaticallyRetryRunner(runner):
        logging.warning('TRM.AbortStaleRunners retrying runner %s on machine '
                        '%s with key %s. Attempt %d', runner.name,
                        runner.machine_id, runner.key.urlsafe(),
                        runner.automatic_retry_count)
      else:
        logging.info('TRM.AbortStaleRunner unable to retry runner with key '
                     '%s on machine %s even though it can. Skipping for now.',
                     runner.key.urlsafe(), runner.machine_id)
    else:
      logging.error('TRM.AbortStaleRunners aborting runner %s on machine %s '
                    'with key %s', runner.name, runner.machine_id,
                    runner.key.urlsafe())
      AbortRunner(runner, reason='Runner has become stale.')

  query = test_runner.TestRunner.gql(
      'WHERE done = :1 AND ping != :2 AND ping < :3',
      False, None, timeout_cutoff)
  stale_runner_rpc = query.map_async(HandleStaleRunner)

  # Abort all runners that took longer than their own deadline to start running.
  query = test_runner.TestRunner.gql('WHERE run_by < :1 and started = :2 '
                                     'and done = :3', now, None, False)
  unfullfilled_rpc = query.map_async(_AbortUnfullfilledRunner)

  logging.debug('TRM.AbortStaleRunners done')

  ndb.Future.wait_all([stale_runner_rpc, unfullfilled_rpc])


def AbortRunner(runner, reason='Not specified.'):
  """Abort the given test runner.

  Args:
    runner: An instance of TestRunner to be aborted.
    reason: A string message indicating why the TestRunner is being aborted.
  """
  r_str = ('Tests aborted. AbortRunner() called. Reason: %s' %
           reason.encode('ascii', 'xmlcharrefreplace'))

  # The cancellation time should count as the time the runner started.
  runner.started = datetime.datetime.utcnow()
  runner.put()

  runner.UpdateTestResult(runner.machine_id, errors=r_str)


def ExecuteRegisterRequest(attributes, server_url):
  """Attempts to match the requesting machine with an existing TestRunner.

  If the machine is matched with a request, the machine is told what to do.
  Else, the machine is told to register at a later time.

  Args:
    attributes: A dictionary representing the attributes of the machine
        registering itself.
    server_url: The URL to the Swarm server so that we can set the
        result_url in the Swarm file we upload to the machines.

  Raises:
    test_request_message.Error: If the request format/attributes aren't valid.

  Returns:
    A dictionary containing the commands the machine needs to execute.
  """
  # Validate and fix machine attributes. Will throw exception on errors.
  attribs = ValidateAndFixAttributes(attributes)
  response = {}

  # Check the slave version, forcing it to update if required.
  if 'version' in attributes:
    expected_version = SlaveVersion()
    if attributes['version'] != expected_version:
      logging.info(
          '%s != %s, Forcing slave %s update',
          expected_version, attributes['version'], attributes['id'])
      response['commands'] = [rpc.BuildRPC(
          'UpdateSlave',
          server_url.rstrip('/') + '/get_slave_code')]
      response['try_count'] = 0
      # The only time a slave would have results to send here would be if
      # the machine failed to update.
      response['result_url'] = server_url.rstrip('/') + '/remote_error'

      return response
  else:
    logging.warning('%s(%s) is querying for work but it is too old to '
                    'automatically update. Please manually update the slave.',
                    attribs['id'], attributes.get('tag', None))

  dimension_hashes = dimensions_utils.GenerateAllDimensionHashes(
      attribs['dimensions'])

  machine_stats.RecordMachineQueriedForWork(
      attribs['id'], test_request_message.Stringize(attribs['dimensions']),
      attributes.get('tag', None))

  # Since the following commands are part of a GQL query, we can't use
  # explicit boolean comparison.
  unfinished_test_key = test_runner.TestRunner.query(
      test_runner.TestRunner.machine_id == attribs['id'],
      test_runner.TestRunner.done == False).get(keys_only=True)
  if unfinished_test_key:
    logging.warning('A machine is asking for a new test, but there still '
                    'seems to be an unfinished test with key, %s, running on '
                    'a machine with the same id, %s. This might just be due '
                    'to app engine being only eventually consistent',
                    unfinished_test_key.urlsafe(), attribs['id'])

  # Try assigning machine to a runner 10 times before we give up.
  # TODO(user): Tune this parameter somehow.
  assigned_runner = False
  for _ in range(10):
    # Try to find a matching test runner for the machine.
    if dimension_hashes:
      runner = _FindMatchingRunnerUsingHashes(dimension_hashes)
    else:
      runner = _FindMatchingRunnerUsingAttribs(attribs)
    # If no runner matches, no need to keep searching.
    if not runner:
      break

    # Will atomically try to assign the machine to the runner. This could
    # fail due to a race condition on the runner. If so, we loop back to
    # finding a runner.
    if test_runner.AssignRunnerToMachine(attribs['id'], runner,
                                         test_runner.AtomicAssignID):
      assigned_runner = True
      # Grab the new version of the runner.
      runner = runner.key.get()
      break

  if assigned_runner:
    # Get the commands the machine needs to execute.
    commands, result_url = _GetTestRunnerCommands(runner, server_url)
    response['commands'] = commands
    response['result_url'] = result_url
    response['try_count'] = 0
  else:
    response['try_count'] = attribs['try_count'] + 1
    # Tell machine when to come back, in seconds.
    response['come_back'] = _ComputeComebackValue(response['try_count'])

  return response


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


def _ComputeComebackValue(try_count):
  """Computes when the slave machine should return based on given try_count.

  Currently computes come_back exponentially.

  Args:
    try_count: The try_count number of the machine which is non-negative.

  Returns:
    A float, representing the seconds the slave should wait before asking
    for a new job.
  """
  # Check for negativity just to be safe.
  assert try_count >= 0

  # Limit our exponential computation to a sane amount to avoid overflow.
  try_count = min(try_count, MAX_TRY_COUNT)
  comeback_duration = min(MAX_COMEBACK_SECS, math.pow(1.5, (try_count + 1)))

  if random.random() < CHANCE_OF_QUICK_COMEBACK:
    comeback_duration = QUICK_COMEBACK_SECS

  return comeback_duration


def _FindMatchingRunnerUsingHashes(attrib_hashes):
  """Find oldest TestRunner who hasn't already been assigned a machine.

  Args:
    attrib_hashes: The list of all the configs hash that the machine
        could handle.

  Returns:
    A TestRunner object, or None if a matching runner is not found.
  """
  # We can only have 30 elements in a IN query at a time (app engine limit).
  number_of_queries = int(math.ceil(len(attrib_hashes) / 30.0))

  runner = None
  for i in range(number_of_queries):
    # We use a format argument for None, because putting None in the string
    # doesn't work.
    query_runner = test_runner.TestRunner.gql(
        'WHERE started = :1 and config_hash IN :2 ORDER BY '
        'priority_and_created LIMIT 1',
        None, attrib_hashes[i * 30:(i + 1) * 30]).get()

    # Use this runner if it is older than our currently held runner.
    if not runner:
      runner = query_runner
    elif query_runner and query_runner.created < runner.created:
      runner = query_runner

  return runner


def _FindMatchingRunnerUsingAttribs(attribs):
  """Find oldest TestRunner who hasn't already been assigned a machine.

  Args:
    attribs: The attributes defining the machine.

  Returns:
    A TestRunner object, or None if a matching runner is not found.
  """
  # TODO(user): limit the number of test runners checked to avoid querying
  # all the tasks all the time.

  # Assign test runners from earliest to latest.
  # We use a format argument for None, because putting None in the string
  # doesn't work.
  query = test_runner.TestRunner.gql('WHERE started = :1 ORDER BY '
                                     'priority_and_created',
                                     None)
  for runner in query:
    runner_dimensions = runner.GetConfiguration().dimensions
    (match, output) = dimensions_utils.MatchDimensions(runner_dimensions,
                                                       attribs['dimensions'])
    logging.info(output)
    if match:
      logging.info('matched runner %s: ' % runner.name
                   + str(runner_dimensions) + ' to machine: '
                   + str(attribs['dimensions']))
      return runner

  return None


def _GetTestRunnerCommands(runner, server_url):
  """Get the commands that need to be sent to a slave to execute the runner.

  Args:
    runner: test runner object to run.
    server_url: The URL to the Swarm server so that we can set the
        result_url in the Swarm file we upload to the machines.

  Returns:
    A tuple (commands, result_url) where commands is a list of RPC calls that
    need to be run by the remote slave machine and result_url is where it
    should post the results.
  """
  output_commands = []

  # Get test manifest and scripts.
  test_run = _BuildTestRun(runner, server_url)

  # Prepare the manifest file for downloading. The format is (directory,
  # filename, file contents).
  files_to_upload = [
      (test_run.working_dir or '', _TEST_RUN_SWARM_FILE_NAME,
       test_request_message.Stringize(test_run, json_readable=True))
  ]

  output_commands.append(rpc.BuildRPC('StoreFiles', files_to_upload))

  # Define how to run the scripts.
  swarm_file_path = _TEST_RUN_SWARM_FILE_NAME
  if test_run.working_dir:
    swarm_file_path = os.path.join(
        test_run.working_dir, _TEST_RUN_SWARM_FILE_NAME)
  command_to_execute = [
      os.path.join('local_test_runner.py'),
      '-f', swarm_file_path,
  ]

  test_case = runner.request.get().GetTestCase()
  if test_case.verbose:
    command_to_execute.append('-v')

  if test_case.restart_on_failure:
    command_to_execute.append('--restart_on_failure')

  output_commands.append(rpc.BuildRPC('RunCommands', command_to_execute))

  return (output_commands, test_run.result_url)


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.utcnow()


def QueryOldErrors():
  """Returns keys for errors older than SWARM_ERROR_TIME_TO_LIVE_DAYS."""
  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=SWARM_ERROR_TIME_TO_LIVE_DAYS))
  return SwarmError.query(
      SwarmError.created < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))


def StoreStartSlaveScript(script):
  """Stores the given script as the new start slave script for all slave.

  Args:
    script: The contents of the new start slave script.
  """
  file_chunks.StoreFile(START_SLAVE_SCRIPT_KEY, script)

  # Clear the cached version value since it has now changed.
  memcache.delete('slave_version', namespace=os.environ['CURRENT_VERSION_ID'])


def SlaveVersion():
  """Retrieves the slave version loaded on this server.

  The memcache is first checked for the version, otherwise the value
  is generated and then stored in the memcache.

  Returns:
    The hash of the current slave version.
  """
  slave_version = memcache.get('slave_version',
                               namespace=os.environ['CURRENT_VERSION_ID'])
  if slave_version:
    return slave_version
  # Get the start slave script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {
    'start_slave.py': file_chunks.RetrieveFile(START_SLAVE_SCRIPT_KEY) or '',
  }
  slave_version = bot_archive.GenerateSlaveVersion(
      os.path.join(ROOT_DIR, 'swarm_bot'), additionals)
  memcache.set('slave_version', slave_version,
               namespace=os.environ['CURRENT_VERSION_ID'])
  return slave_version


def SlaveCodeZipped():
  """Returns a zipped file of all the files a slave needs to run.

  Returns:
    A string representing the zipped file's contents.
  """
  # Get the start slave script from the database, if present. Pass an empty
  # file if the files isn't present.
  additionals = {
    'start_slave.py': file_chunks.RetrieveFile(START_SLAVE_SCRIPT_KEY) or '',
  }
  return bot_archive.SlaveCodeZipped(
      os.path.join(ROOT_DIR, 'swarm_bot'), additionals)
