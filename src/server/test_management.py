#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Test Management.

The Test Management files contains the pipe functions to setup and run tests
through the TestRunner and TestRequest classes.
"""


import datetime
import logging
import math
import os.path
import random
import StringIO
import uuid
import zipfile

from google.appengine.api import memcache
from google.appengine.ext import ndb

from common import dimensions_utils
from common import swarm_constants
from common import test_request_message
from common import version
from server import dimension_mapping
from server import test_request
from server import test_runner
from stats import machine_stats
from swarm_bot import slave_machine

# The amount of time to wait after recieving a runners last message before
# considering the runner to have run for too long. Runners that run for too
# long will be aborted automatically.
# Specified in number of seconds.
_TIMEOUT_FACTOR = 600

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

# The time (in seconds) to wait after recieving a runner before aborting it.
# This is intended to delete runners that will never run because they will
# never find a matching machine.
SWARM_RUNNER_MAX_WAIT_SECS = 24 * 60 * 60

# Number of days to keep error logs around.
SWARM_ERROR_TIME_TO_LIVE_DAYS = 7


class SwarmError(ndb.Model):
  """A datastore entry representing an error in Swarm."""
  # The name of the error.
  name = ndb.StringProperty(indexed=False)

  # A description of the error.
  message = ndb.StringProperty(indexed=False)

  # Optional details about the specific error instance.
  info = ndb.StringProperty(indexed=False)

  # The time at which this error was logged.  Used to clean up old errors.
  created = ndb.DateTimeProperty(auto_now_add=True)


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

  request = test_request.TestRequest(message=request_message)
  test_case = request.GetTestCase()  # Will raise on invalid request.
  request.name = test_case.test_case_name

  request.put()

  test_keys = {'test_case_name': test_case.test_case_name,
               'test_keys': []}

  for config in test_case.configurations:
    logging.debug('Creating runners for request=%s config=%s',
                  request.name, config.config_name)
    config_hash = request.GetConfigurationDimensionHash(config.config_name)
    # Ensure that we have a record of something with this config getting
    # created.
    dimension = dimension_mapping.DimensionMapping.get_or_insert(
        config_hash,
        dimensions=test_request_message.Stringize(config.dimensions))
    if dimension.last_seen != datetime.date.today():
      # DimensionMapping automatically updates last_seen when put() is called.
      dimension.put()

    # TODO(user): deal with addition_instances later.
    assert config.min_instances > 0
    config.num_instances = config.min_instances
    for instance_index in range(config.min_instances):
      config.instance_index = instance_index
      runner = _QueueTestRequestConfig(request, config, config_hash)

      test_keys['test_keys'].append({'config_name': config.config_name,
                                     'instance_index': instance_index,
                                     'num_instances': config.num_instances,
                                     'test_key': runner.key.urlsafe()})
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
      parent=request.key, config_name=config.config_name,
      config_hash=config_hash, config_instance_index=config.instance_index,
      num_config_instances=config.num_instances, priority=config.priority)

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
  errors = []
  assert test_run.IsValid(errors), errors
  return test_run


def AbortStaleRunners():
  """Abort any runners are taking too long to run or too long to find a match.

  If the runner is aborted because the machine timed out, it will
  automatically be retried if it hasn't been aborted more than
  MAX_AUTOMATIC_RETRY times.
  If a runner is aborted because it hasn't hasn't found any machine to run it
  in over SWARM_RUNNER_MAX_WAIT_SECS seconds, there is no automatic retry.

  Returns:
    The rpcs for all the delete queries (mainly used by tests).
  """
  logging.debug('TRM.AbortStaleRunners starting')
  now = _GetCurrentTime()
  # If any active runner hasn't recieved a ping in the last _TIMEOUT_FACTOR
  # seconds then we consider it stale and abort it.
  timeout_cutoff = now - datetime.timedelta(seconds=_TIMEOUT_FACTOR)

  # Abort all currently running runners that haven't recently pinged the
  # server.
  def HandleStaleRunner(runner):
    if test_runner.ShouldAutomaticallyRetryRunner(runner):
      if test_runner.AutomaticallyRetryRunner(runner):
        logging.warning('TRM.AbortStaleRunners retrying runner %s with key '
                        ' %s. Attempt %d', runner.name,
                        runner.key.urlsafe(), runner.automatic_retry_count)
      else:
        logging.info('TRM.AbortStaleRunner unable to retry runner with key '
                     '%s even though it can. Skipping for now.',
                     runner.urlsafe())
    else:
      logging.error('TRM.AbortStaleRunners aborting runner %s with key %s',
                    runner.name, runner.key.urlsafe())
      AbortRunner(runner, reason='Runner has become stale.')

  query = test_runner.TestRunner.gql(
      'WHERE done = :1 AND ping != :2 AND ping < :3',
      False, None, timeout_cutoff)
  stale_runner_rpc = query.map_async(HandleStaleRunner)

  # Abort all runners that haven't been able to find a machine to run them
  # in SWARM_RUNNER_MAX_WAIT_SECS seconds.
  def AbortUnfullfilledRunner(runner):
    AbortRunner(runner, reason=('Runner was unable to find a machine to '
                                'run it within %d seconds' %
                                SWARM_RUNNER_MAX_WAIT_SECS))

  timecut_off = now - datetime.timedelta(seconds=SWARM_RUNNER_MAX_WAIT_SECS)
  query = test_runner.TestRunner.gql('WHERE created < :1 and started = :2 '
                                     'and done = :3 and '
                                     'automatic_retry_count = 0', timecut_off,
                                     None, False)
  unfullfilled_rpc = query.map_async(AbortUnfullfilledRunner)

  logging.debug('TRM.AbortStaleRunners done')

  return [stale_runner_rpc, unfullfilled_rpc]


def AbortRunner(runner, reason='Not specified.'):
  """Abort the given test runner.

  Args:
    runner: An instance of TestRunner to be aborted.
    reason: A string message indicating why the TestRunner is being aborted.
  """
  r_str = ('Tests aborted. AbortRunner() called. Reason: %s' %
           reason.encode('ascii', 'xmlcharrefreplace'))
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
  response = {'id': attribs['id']}

  # Check the slave version, forcing it to update if required.
  if 'version' in attributes:
    if attributes['version'] != SlaveVersion():
      response['commands'] = [slave_machine.BuildRPC(
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

  # pylint: disable=g-explicit-bool-comparison
  unfinished_test_key = test_runner.TestRunner.query(
      test_runner.TestRunner.machine_id == attribs['id'],
      test_runner.TestRunner.done == False).get(keys_only=True)
  # pylint: enable=g-explicit-bool-comparison
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
      # Make sure the attribute value has proper type. None is a valid
      # type and is treated as if it doesn't exist.
      if value:
        try:
          value = uuid.UUID(value)
        except (ValueError, AttributeError) as e:
          logging.warning(
              'Problem with given id, generating new id.\n%s', e)
          attributes[attrib] = None
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

  # Make sure we have 'dimensions', the only required attrib.
  if 'dimensions' not in attributes:
    raise test_request_message.Error('Missing mandatory attribute: '
                                     'dimensions')

  if 'id' not in attributes or not attributes['id']:
    attributes['id'] = str(uuid.uuid4())

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
      (test_run.working_dir, _TEST_RUN_SWARM_FILE_NAME,
       test_request_message.Stringize(test_run, json_readable=True))
  ]

  # TODO(user): Use separate module for RPC related stuff rather
  # than slave_machine.
  output_commands.append(slave_machine.BuildRPC('StoreFiles',
                                                files_to_upload))

  # Define how to run the scripts.
  command_to_execute = [
      r'%s' % os.path.join(swarm_constants.TEST_RUNNER_DIR,
                           swarm_constants.TEST_RUNNER_SCRIPT),
      '-f', r'%s' % os.path.join(test_run.working_dir,
                                 _TEST_RUN_SWARM_FILE_NAME)]

  test_case = runner.request.get().GetTestCase()
  if test_case.verbose:
    command_to_execute.append('-v')

  if test_case.restart_on_failure:
    command_to_execute.append('--restart_on_failure')

  output_commands.append(slave_machine.BuildRPC('RunCommands',
                                                command_to_execute))

  return (output_commands, test_run.result_url)


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.now()


def DeleteOldErrors():
  """Cleans up errors older than a certain age.

  Returns:
    The rpc for the async delete call (mainly meant for tests).
  """
  logging.debug('DeleteOldErrors starting')
  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=SWARM_ERROR_TIME_TO_LIVE_DAYS))

  old_error_query = SwarmError.query(
      SwarmError.created < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))
  rpc = ndb.delete_multi_async(old_error_query)

  logging.debug('DeleteOldErrors done')

  return rpc


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

  slave_machine_script = os.path.join(swarm_constants.SWARM_ROOT_DIR,
                                      swarm_constants.TEST_RUNNER_DIR,
                                      swarm_constants.SLAVE_MACHINE_SCRIPT)

  slave_version = version.GenerateSwarmSlaveVersion(slave_machine_script)
  memcache.set('slave_version', slave_version,
               namespace=os.environ['CURRENT_VERSION_ID'])

  return slave_version


def SlaveCodeZipped():
  """Returns a zipped file of all the files a slave needs to run.

  Returns:
    A string representing the zipped file's contents.
  """
  zip_memory_file = StringIO.StringIO()
  with zipfile.ZipFile(zip_memory_file, 'w') as zip_file:
    slave_script = os.path.join(swarm_constants.SWARM_ROOT_DIR,
                                swarm_constants.TEST_RUNNER_DIR,
                                swarm_constants.SLAVE_MACHINE_SCRIPT)
    zip_file.write(slave_script, swarm_constants.SLAVE_MACHINE_SCRIPT)

    local_test_runner = os.path.join(swarm_constants.SWARM_ROOT_DIR,
                                     swarm_constants.TEST_RUNNER_DIR,
                                     swarm_constants.TEST_RUNNER_SCRIPT)
    zip_file.write(local_test_runner,
                   os.path.join(swarm_constants.TEST_RUNNER_DIR,
                                swarm_constants.TEST_RUNNER_SCRIPT))

    # Copy all the required helper files.
    common_dir = os.path.join(swarm_constants.SWARM_ROOT_DIR,
                              swarm_constants.COMMON_DIR)

    for common_file in swarm_constants.SWARM_BOT_COMMON_FILES:
      zip_file.write(os.path.join(common_dir, common_file),
                     os.path.join(swarm_constants.COMMON_DIR, common_file))

  return zip_memory_file.getvalue()
