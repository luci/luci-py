#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Test Request Manager.

A Test Request is a request to install and run some arbitrary data
files on a set of remote computers for testing purposes.  The Test Request
Manager accepts these requests, acquires remote machines to run them, and posts
back the results of those tests to a given URL.

Test Requests are described using strings formatted as a subset of the python
syntax to a dictionary object.  See
http://code.google.com/p/swarming/wiki/SwarmFileFormat for
complete details.

The TRM is the main component of the Test Request Server.  The TRS accepts
Test Requests through HTTP POST requests and forwards them to the TRM.  The TRS
also provides a UI for canceling Test Requests.
"""


import datetime
import logging
import math
import os.path
import random
import StringIO
import urllib
import urllib2
import urlparse
import uuid
import zipfile

from google.appengine.api import mail
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.ext import blobstore
from google.appengine.ext import db
from common import blobstore_helper
from common import dimensions_utils
from common import swarm_constants
from common import test_request_message
from common import version
from server import dimension_mapping
from server import test_request
from server import test_runner
from stats import machine_stats
from stats import runner_stats
from swarm_bot import slave_machine

# The amount of time to wait after recieving a runners last message before
# considering the runner to have run for too long. Runners that run for too
# long will be aborted automatically.
# Specified in number of seconds.
_TIMEOUT_FACTOR = 600

# The number of pings that need to be missed before a runner is considered to
# have timed out. |_TIMEOUT_FACTOR| / |this| will determine the desired delay
# between pings/
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


def OnDevAppEngine():
  """Return True if this code is running on dev app engine.

  Returns:
    True if this code is running on dev app engine.
  """
  return os.environ['SERVER_SOFTWARE'].startswith('Development')


class SwarmError(db.Model):
  """A datastore entry representing an error in Swarm."""
  # The name of the error.
  name = db.StringProperty(indexed=False)

  # A description of the error.
  message = db.StringProperty(indexed=False)

  # Optional details about the specific error instance.
  info = db.StringProperty(indexed=False)

  # The time at which this error was logged.  Used to clean up old errors.
  created = db.DateTimeProperty(auto_now_add=True)


class TestRequestManager(object):
  """The Test Request Manager."""

  def UpdateTestResult(self, runner, machine_id, success=False, exit_codes='',
                       result_blob_key=None, errors=None, overwrite=False):
    """Update the runner with results of a test run.

    Args:
      runner: a TestRunner object pointing to the test request to which to
          send the results.  This argument must not be None.
      machine_id: The machine id of the machine providing these results.
      success: a boolean indicating whether the test run succeeded or not.
      exit_codes: a string containing the array of exit codes of the test run.
      result_blob_key: a key to the blob containing the results.
      errors: a string explaining why we failed to get the actual result string.
      overwrite: a boolean indicating if we should always record this result,
          even if a result had been previously recorded.

    Returns:
      True if the results are successfully stored and the result_url is
          properly updated (if there is one).
    """
    if not runner:
      logging.error('runner argument must be given')
      # The new results won't get stored so delete them.
      if result_blob_key:
        blobstore.delete_async(result_blob_key)
      return False

    if runner.machine_id != machine_id:
      if machine_id not in runner.old_machine_ids:
        logging.warning('The machine id of the runner, %s, doesn\'t match the '
                        'machine id given, %s', runner.machine_id, machine_id)
        # The new results won't get stored so delete them.
        if result_blob_key:
          blobstore.delete_async(result_blob_key)
        return False
      # Update the old and active machines ids.
      logging.info('Received result from old machine, making it current '
                   'machine and storing results')
      if runner.machine_id:
        runner.old_machine_ids.append(runner.machine_id)
      runner.machine_id = machine_id
      runner.old_machine_ids.remove(machine_id)

    # If the runnner is marked as done, don't try to process another
    # response for it, unless overwrite is enable.
    if runner.done and not overwrite:
      stored_results = runner.GetResultString()

      # The new results won't get stored so delete them.
      new_results = None
      if result_blob_key:
        new_results = blobstore_helper.GetBlobstore(result_blob_key)
        blobstore.delete_async(result_blob_key)

      if new_results == stored_results or errors == stored_results:
        # This can happen if the server stores the results and then runs out
        # of memory, so the return code is 500, which causes the
        # local_test_runner to resend the results.
        logging.warning('The runner already contained the given result string.')
        return True
      else:
        logging.error('Got a additional response for runner=%s (key %s), '
                      'not good', runner.GetName(), runner.key())
        logging.debug('Dropped result string was:\n%s',
                      new_results or errors)
        return False

    # Clear any old result strings that are stored if we are overwriting.
    if overwrite:
      if runner.result_string_reference:
        runner.result_string_reference.delete()
        runner.result_string_reference = None
      runner.errors = None

    runner.ran_successfully = success
    runner.exit_codes = exit_codes
    runner.done = True
    runner.ended = datetime.datetime.now()

    if result_blob_key:
      assert runner.result_string_reference is None, (
          'There is a reference stored, overwriting it would leak the old '
          'element.')
      runner.result_string_reference = result_blob_key
    else:
      runner.errors = errors

    runner.put()
    logging.info('Successfully updated the results of runner %s.',
                 runner.key())

    # If the test didn't run successfully, we send an email if one was
    # requested via the test request.
    test_case = runner.request.GetTestCase()
    if not runner.ran_successfully and test_case.failure_email:
      # TODO(user): provide better info for failure. E.g., if we don't have a
      # web_request.body, we should have info like: Failed to upload files.
      self._EmailTestResults(runner, test_case.failure_email)

    # TODO(user): test result objects, and hence their test request objects,
    # are currently not deleted from the data store.  This allows someone to
    # go back to the TRS web UI and see the results of any test that has run.
    # Eventually we will want to see how to delete old requests as the apps
    # storage quota will be exceeded.
    update_successful = True
    if test_case.result_url:
      result_url_parts = urlparse.urlsplit(test_case.result_url)
      if result_url_parts[0] == 'http' or result_url_parts[0] == 'https':
        # Send the result to the requested destination.
        try:
          # Encode the result string so that it's backwards-compatible with
          # ASCII. Without this, the call to "urllib.urlencode" below can
          # throw a UnicodeEncodeError if the result string contains non-ASCII
          # characters.
          encoded_result_string = runner.GetResultString().encode('utf-8')
          urllib2.urlopen(test_case.result_url,
                          urllib.urlencode((
                              ('n', runner.request.name),
                              ('c', runner.config_name),
                              ('i', runner.config_instance_index),
                              ('m', runner.num_config_instances),
                              ('x', runner.exit_codes),
                              ('s', runner.ran_successfully),
                              ('r', encoded_result_string))))
        except urllib2.URLError:
          logging.exception('Could not send results back to sender at %s',
                            test_case.result_url)
          update_successful = False
        except urlfetch.Error:
          # The docs for urllib2.urlopen() say it only raises urllib2.URLError.
          # However, in the appengine environment urlfetch.Error may also be
          # raised. This is normal, see the "Fetching URLs in Python" section of
          # http://code.google.com/appengine/docs/python/urlfetch/overview.html
          # for more details.
          logging.exception('Could not send results back to sender at %s',
                            test_case.result_url)
          update_successful = False
      elif result_url_parts[0] == 'mailto':
        # Only send an email if we didn't send a failure email earlier to
        # prevent repeats.
        if runner.ran_successfully or not test_case.failure_email:
          self._EmailTestResults(runner, result_url_parts[1])
      else:
        logging.exception('Unknown url given as result url, %s',
                          test_case.result_url)
        update_successful = False

    runner_stats.RecordRunnerStats(runner)

    if (test_case.store_result == 'none' or
        (test_case.store_result == 'fail' and runner.ran_successfully)):
      test_runner.DeleteRunner(runner)

    return update_successful

  def _EmailTestResults(self, runner, send_to):
    """Emails the test result.

    Args:
      runner: a TestRunner object containing values relating to the the test
          run.
      send_to: the email address to send the result to. This must be a valid
          email address.
    """
    if not runner:
      logging.error('runner argument must be given')
      return

    if not mail.is_email_valid(send_to):
      logging.error('Invalid email passed to result_url, %s', send_to)
      return

    if runner.ran_successfully:
      subject = '%s succeeded.' % runner.GetName()
    else:
      subject = '%s failed.' % runner.GetName()

    message_body_parts = [
        'Test Request Name: ' + runner.request.name,
        'Configuration Name: ' + runner.config_name,
        'Configuration Instance Index: ' + str(runner.config_instance_index),
        'Number of Configurations: ' + str(runner.num_config_instances),
        'Exit Code: ' + str(runner.exit_codes),
        'Success: ' + str(runner.ran_successfully),
        'Result Output: ' + runner.GetResultString()]
    message_body = '\n'.join(message_body_parts)

    try:
      mail.send_mail(sender='Test Request Server <no_reply@google.com>',
                     to=send_to,
                     subject=subject,
                     body=message_body,
                     html='<pre>%s</pre>' % message_body)
    except Exception as e:  # pylint: disable=broad-except
      # We catch all errors thrown because mail.send_mail can throw errors
      # that it doesn't list in its description, but that are caused by our
      # inputs (such as unauthorized sender).
      logging.exception(
          'An exception was thrown when attemping to send mail\n%s', e)

  def ExecuteTestRequest(self, request_message):
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

      # TODO(user): deal with addition_instances later!!!
      assert config.min_instances > 0
      for instance_index in range(config.min_instances):
        config.instance_index = instance_index
        config.num_instances = config.min_instances
        runner = self._QueueTestRequestConfig(request, config, config_hash)

        test_keys['test_keys'].append({'config_name': config.config_name,
                                       'instance_index': instance_index,
                                       'num_instances': config.num_instances,
                                       'test_key': str(runner.key())})
    return test_keys

  def _QueueTestRequestConfig(self, request, config, config_hash):
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
        request=request, config_name=config.config_name,
        config_hash=config_hash, config_instance_index=config.instance_index,
        num_config_instances=config.num_instances)

    runner.put()

    return runner

  def _BuildTestRun(self, runner, server_url):
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
    request = runner.request.GetTestCase()
    config = runner.GetConfiguration()
    test_run = test_request_message.TestRun(
        test_run_name=request.test_case_name,
        env_vars=request.env_vars,
        instance_index=runner.config_instance_index,
        num_instances=runner.num_config_instances,
        configuration=config,
        result_url=('%s/result?r=%s&id=%s' % (server_url, str(runner.key()),
                                              runner.machine_id)),
        ping_url=('%s/runner_ping?r=%s&id=%s' % (server_url, str(runner.key()),
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

  def GetRunnerResults(self, key):
    """Returns the results of the runner specified by key.

    Args:
      key: TestRunner key representing the runner.

    Returns:
      A dictionary of the runner's results, or None if the runner not found.
    """

    try:
      runner = test_runner.TestRunner.get(key)
    except (db.BadKeyError, db.KindError, db.BadArgumentError):
      return None

    if runner:
      return self.GetResults(runner)

    return None

  def GetResults(self, runner):
    """Gets the results from the given test run.

    Args:
      runner: The instance of TestRunner to get the results from.

    Returns:
      A dictionary of the results.
    """
    return {'exit_codes': runner.exit_codes,
            'machine_id': runner.machine_id,
            'machine_tag': machine_stats.GetMachineTag(runner.machine_id),
            'config_instance_index': runner.config_instance_index,
            'num_config_instances': runner.num_config_instances,
            'output': runner.GetResultString()}

  def AbortStaleRunners(self):
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
    query = test_runner.TestRunner.gql(
        'WHERE done = :1 AND ping != :2 AND ping < :3',
        False, None, timeout_cutoff)
    for runner in query:
      if test_runner.ShouldAutomaticallyRetryRunner(runner):
        if test_runner.AutomaticallyRetryRunner(runner):
          logging.warning('TRM.AbortStaleRunners retrying runner %s with key '
                          ' %s. Attempt %d', runner.GetName(), runner.key(),
                          runner.automatic_retry_count)
        else:
          logging.info('TRM.AbortStaleRunner unable to retry runner with key '
                       '%s even though it can. Skipping for now.', runner.key())
      else:
        logging.error('TRM.AbortStaleRunners aborting runner %s with key %s',
                      runner.GetName(), runner.key())
        self.AbortRunner(runner, reason='Runner has become stale.')

    # Abort all runners that haven't been able to find a machine to run them
    # in SWARM_RUNNER_MAX_WAIT_SECS seconds.
    timecut_off = now - datetime.timedelta(seconds=SWARM_RUNNER_MAX_WAIT_SECS)
    query = test_runner.TestRunner.gql('WHERE created < :1 and started = :2 '
                                       'and done = :3 and '
                                       'automatic_retry_count = 0', timecut_off,
                                       None, False)
    for runner in query:
      self.AbortRunner(runner, reason=('Runner was unable to find a machine to '
                                       'run it within %d seconds' %
                                       SWARM_RUNNER_MAX_WAIT_SECS))

    logging.debug('TRM.AbortStaleRunners done')

  def AbortRunner(self, runner, reason='Not specified.'):
    """Abort the given test runner.

    Args:
      runner: An instance of TestRunner to be aborted.
      reason: A string message indicating why the TestRunner is being aborted.
    """
    r_str = 'Tests aborted. AbortRunner() called. Reason: %s' % reason
    self.UpdateTestResult(runner, runner.machine_id, errors=r_str)

  def ExecuteRegisterRequest(self, attributes, server_url):
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
    attribs = self.ValidateAndFixAttributes(attributes)
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

    unfinished_test_key = db.GqlQuery(
        'SELECT __key__ FROM TestRunner WHERE machine_id = :1 AND done = :2',
        attribs['id'], False).get()
    if unfinished_test_key:
      logging.warning('A machine is asking for a new test, but there still '
                      'seems to be an unfinished test with key, %s, running on '
                      'a machine with the same id, %s. This might just be due '
                      'to app engine being only eventually consisten',
                      unfinished_test_key, attribs['id'])

    # Try assigning machine to a runner 10 times before we give up.
    # TODO(user): Tune this parameter somehow.
    assigned_runner = False
    for _ in range(10):
      # Try to find a matching test runner for the machine.
      if dimension_hashes:
        runner = self._FindMatchingRunnerUsingHashes(dimension_hashes)
      else:
        runner = self._FindMatchingRunnerUsingAttribs(attribs)
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
        runner = db.get(runner.key())
        break

    if assigned_runner:
      # Get the commands the machine needs to execute.
      commands, result_url = self._GetTestRunnerCommands(runner, server_url)
      response['commands'] = commands
      response['result_url'] = result_url
      response['try_count'] = 0
    else:
      response['try_count'] = attribs['try_count'] + 1
      # Tell machine when to come back, in seconds.
      response['come_back'] = self._ComputeComebackValue(response['try_count'])

    return response

  def ValidateAndFixAttributes(self, attributes):
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

  def _ComputeComebackValue(self, try_count):
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

  def _FindMatchingRunnerUsingHashes(self, attrib_hashes):
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
          'WHERE started = :1 and config_hash IN :2 ORDER BY created LIMIT 1',
          None, attrib_hashes[i * 30:(i + 1) * 30]).get()

      # Use this runner if it is older than our currently held runner.
      if not runner:
        runner = query_runner
      elif query_runner and query_runner.created < runner.created:
        runner = query_runner

    return runner

  def _FindMatchingRunnerUsingAttribs(self, attribs):
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
    query = test_runner.TestRunner.gql('WHERE started = :1 ORDER BY created',
                                       None)
    for runner in query:
      runner_dimensions = runner.GetConfiguration().dimensions
      (match, output) = dimensions_utils.MatchDimensions(runner_dimensions,
                                                         attribs['dimensions'])
      logging.info(output)
      if match:
        logging.info('matched runner %s: ' % runner.GetName()
                     + str(runner_dimensions) + ' to machine: '
                     + str(attribs['dimensions']))
        return runner

    return None

  def _GetTestRunnerCommands(self, runner, server_url):
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
    test_run = self._BuildTestRun(runner, server_url)

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

    test_case = runner.request.GetTestCase()
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

  rpc = db.delete_async(SwarmError.gql('WHERE created < :1', old_cutoff))

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
