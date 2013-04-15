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
import urllib
import urllib2
import urlparse
import uuid

from google.appengine.api import mail
from google.appengine.api import urlfetch
from google.appengine.ext import blobstore
from google.appengine.ext import db
from common import blobstore_helper
from common import dimensions_utils
from common import test_request_message
from stats import machine_stats
from stats import runner_stats
from test_runner import slave_machine

# The amount of time to wait after recieving a runners last message before
# considering the runner to have run for too long. Runners that run for too
# long will be aborted automatically.
# Specified in number of seconds.
_TIMEOUT_FACTOR = 600

# Default Test Run Swarm filename.  This file provides parameters
# for the instance running tests.
_TEST_RUN_SWARM_FILE_NAME = 'test_run.swarm'

# Name of python script containing constants.
_SWARM_CONSTANTS_SCRIPT = 'swarm_constants.py'

# Name of python script to execute on the remote machine to run a test.
_TEST_RUNNER_SCRIPT = 'local_test_runner.py'

# Name of python script to validate swarm file format.
_TEST_REQUEST_MESSAGE_SCRIPT = 'test_request_message.py'

# Name of python script to handle url connections.
_URL_HELPER_SCRIPT = 'url_helper.py'

# Name of python script to mark folder as package.
_PYTHON_INIT_SCRIPT = '__init__.py'

# Name of directories in source tree and/or on remote machine.
_TEST_RUNNER_DIR = 'test_runner'
_COMMON_DIR = 'common'

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
COMEBACK_AFTER_SERVER_ERROR_SECS = 10

# Number of times to try a transaction before giving up. Since most likely the
# recoverable exceptions will only happen when datastore is overloaded, it
# doesn't make much sense to extensively retry many times.
MAX_TRANSACTION_RETRY_COUNT = 3

# The time (in seconds) to wait after recieving a runner before aborting it.
# This is intended to delete runners that will never run because they will
# never find a matching machine.
SWARM_RUNNER_MAX_WAIT_SECS = 24 * 60 * 60

# The maximum number of times to retry a runner that has failed for a swarm
# related reasons (like the machine timing out).
MAX_AUTOMATIC_RETRIES = 3

# Root directory of Swarm scripts.
SWARM_ROOT_DIR = os.path.join(os.path.dirname(__file__), '..')

# Number of days to keep old runners around for.
SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS = 14

# Number of days to keep error logs around.
SWARM_ERROR_TIME_TO_LIVE_DAYS = 7


def GetTestCase(request_message):
  """Returns a TestCase object representing this Test Request message.

  Args:
    request_message: The request message to convert.

  Returns:
    A TestCase object representing this Test Request.

  Raises:
    test_request_message.Error: If the request's message isn't valid.
  """
  request_object = test_request_message.TestCase()
  errors = []
  if not request_object.ParseTestRequestMessageText(request_message, errors):
    raise test_request_message.Error('\n'.join(errors))

  return request_object


def OnDevAppEngine():
  """Return True if this code is running on dev app engine.

  Returns:
    True if this code is running on dev app engine.
  """
  return os.environ['SERVER_SOFTWARE'].startswith('Development')


# MOE: start_strip
# This is removed from the public version because the public version should
# be run on python2.7 (or later), which has the builtin total_seconds() function
# for timedelta.
def TotalSeconds(time_delta):
  return ((time_delta.microseconds +
           (time_delta.seconds + time_delta.days * 24 * 3600) * 10**6) /
          10**6)
# MOE: end_strip


class TestRequest(db.Model):
  """A test request.

  Test Request objects represent one test request from one client.  The client
  can be a build machine requesting a test after a build or it could be a
  developer requesting a test from their own build.
  """
  # The message received from the caller, formatted as a Test Case as
  # specified in
  # http://code.google.com/p/swarming/wiki/SwarmFileFormat.
  message = db.TextProperty()

  # The name for this test request.
  name = db.StringProperty()

  def GetTestCase(self):
    """Returns a TestCase object representing this Test Request.

    Returns:
      A TestCase object representing this Test Request.

    Raises:
      test_request_message.Error: If the request's message isn't valid.
    """
    # NOTE: because _request_object is not declared with db.Property, it will
    # not be persisted to the data store.  This is used as a transient cache of
    # the test request message to keep from evaluating it all the time
    request_object = getattr(self, '_request_object', None)
    if not request_object:
      request_object = GetTestCase(self.message)
      self._request_object = request_object

    return request_object

  def GetConfiguration(self, config_name):
    """Gets the named configuration.

    Args:
      config_name: The name of the configuration to get.

    Returns:
      A configuration dictionary for the named configuration, or None if the
      name is not found.
    """
    for configuration in self.GetTestCase().configurations:
      if configuration.config_name == config_name:
        return configuration

    return None

  def GetConfigurationDimensionHash(self, config_name):
    """Gets the hash of the named configuration.

    Args:
      config_name: The name of the configuration to get the hash for.

    Returns:
      The hash of the configuration.
    """
    return dimensions_utils.GenerateDimensionHash(
        self.GetConfiguration(config_name).dimensions)

  def GetAllKeys(self):
    """Get all the keys representing the TestRunners owned by this instance.

    Returns:
      A list of all the keys.
    """
    # We can only access the runner if this class has been saved into the
    # database.
    if self.is_saved():
      return [runner.key() for runner in self.runners]
    else:
      return []

  def RunnerDeleted(self):
    # Delete this request if we have deleted all the runners that were created
    # because of it.
    if self.runners.count() == 0:
      self.delete()


class TestRunner(db.Model):
  """Represents one instance of a test runner.

  A test runner represents a given configuration of a given test request running
  on a given machine.
  """
  # The test being run.
  test_request = db.ReferenceProperty(TestRequest, required=True,
                                      collection_name='runners')

  # The name of the request's configuration being tested.
  config_name = db.StringProperty(indexed=False)

  # The hash of the configuration. Required in order to match machines.
  config_hash = db.StringProperty(required=True)

  # The 0 based instance index of the request's configuration being tested.
  config_instance_index = db.IntegerProperty(indexed=False)

  # The number of instances running on the same configuration as ours.
  num_config_instances = db.IntegerProperty(indexed=False)

  # The machine that is running or ran the test. This attribute is only valid
  # once a machine has been assigned to this runner.
  # TODO(user): Investigate making this a reference to the MachineStats.
  # It would require ensuring the MachineStats is created when this value
  # is set.
  machine_id = db.StringProperty()

  # A list of the old machines ids that this runner previous ran on. This is
  # useful for knowing when a ping is from an older machine.
  old_machine_ids = db.ListProperty(str, indexed=False)

  # Used to indicate if the runner has finished, either successfully or not.
  done = db.BooleanProperty(default=False)

  # The time at which this runner was created.  The runner may not have
  # started executing yet.
  created = db.DateTimeProperty(auto_now_add=True)

  # The time at which this runner was executed on a remote machine.  If the
  # runner isn't executing or ended, then the value is None and we use the
  # fact that it is None to identify if a test was started or not.
  started = db.DateTimeProperty()

  # The number of times that this runner has been retried for swarm failures
  # (such as the machine that is running it timing out).
  automatic_retry_count = db.IntegerProperty(default=0)

  # The last time a ping was recieved from the remote machine currently
  # running the test. This is used to determine if a machine has become
  # unresponse, which causes the runner to become aborted.
  ping = db.DateTimeProperty()

  # The time at which this runner ended.  This attribute is valid only when
  # the runner has ended (i.e. done == True). Until then, the value is
  # unspecified.
  ended = db.DateTimeProperty(auto_now=True)

  # True if the test run finished and succeeded.  This attribute is valid only
  # when the runner has ended. Until then, the value is unspecified.
  ran_successfully = db.BooleanProperty(indexed=False)

  # The stringized array of exit_codes for each actions of the test.
  exit_codes = db.StringProperty(indexed=False)

  # Contains any swarm specific errors that occured that caused the result
  # string to not get correct setup with the runner output (i.e. the runner
  # timed out so there was no data).
  errors = db.StringProperty(indexed=False)

  # The blobstore reference to the full output of the test.  This key valid only
  # when the runner has ended (i.e. done == True). Until then, it is None.
  result_string_reference = blobstore.BlobReferenceProperty()

  def delete(self):  # pylint: disable-msg=C6409
    # We delete the blob referenced by this model because no one
    # else will every care about it or try to reference it, so we
    # are just cleaning up the blobstore.
    if self.result_string_reference:
      self.result_string_reference.delete()

    db.Model.delete(self)

  def GetName(self):
    """Gets a name for this runner.

    Returns:
      The  name for this runner.

    Raises:
      test_request_message.Error: If the request's message isn't valid.
    """
    try:
      return '%s:%s' % (self.test_request.name, self.config_name)
    except db.ReferencePropertyResolveError as e:
      # Sometimes the test runner is unable to resolve the TestRequest
      # member.
      logging.warning(e)
      return None

  def GetConfiguration(self):
    """Gets the configuration associated with this runner.

    Returns:
      A configuration dictionary for this runner.
    """
    config = self.test_request.GetConfiguration(self.config_name)
    assert config is not None
    return config

  def GetResultString(self):
    """Get the result string for the runner.

    Returns:
      The string representing the output for this runner or an empty string
        if the result hasn't been written yet.
    """
    if self.errors:
      assert self.result_string_reference is None
      return self.errors

    if not self.result_string_reference:
      return ''

    return blobstore_helper.GetBlobstore(self.result_string_reference.key())

  def GetDimensionsString(self):
    """Get a string representing the dimensions this runner requires.

    Returns:
      The string representing the dimensions for this runner.
    """
    return test_request_message.Stringize(self.GetConfiguration().dimensions)

  def GetWaitTime(self):
    """Get the number of seconds between creation and execution start.

    Returns:
      The number of seconds the runner waited, if the runner hasn't started yet
        return 0.
    """
    if not self.started:
      return 0

    time_delta = self.started - self.created
    # MOE: start_strip
    # TODO(user): Replace with total_seconds() once python2.7 is used
    return TotalSeconds(time_delta)
    # MOE: end_strip_and_replace return time_delta.total_seconds())

  def GetMessage(self):
    """Get the message string representing this test runner.

    Returns:
      A string represent this test runner request.
    """
    message = ['Test Request Message:']
    message.append(self.test_request.message)

    message.append('')
    message.append('Test Runner Message:')
    message.append('Configuration Name: ' + self.config_name)
    message.append('Configuration Instance Index: %d / %d' %
                   (self.config_instance_index, self.num_config_instances))

    return '\n'.join(message)


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
      runner.put()

    runner.ran_successfully = success
    runner.exit_codes = exit_codes
    runner.done = True

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
    test_case = runner.test_request.GetTestCase()
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
                              ('n', runner.test_request.name),
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

    if (test_case.store_result == 'none' or
        (test_case.store_result == 'fail' and runner.ran_successfully)):
      test_request = runner.test_request
      runner.delete()
      test_request.RunnerDeleted()

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
        'Test Request Name: ' + runner.test_request.name,
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
    except Exception as e:  # pylint: disable-msg=W0703
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

    request = TestRequest(message=request_message)
    test_case = request.GetTestCase()  # Will raise on invalid request.
    request.name = test_case.test_case_name

    request.put()

    test_keys = {'test_case_name': test_case.test_case_name,
                 'test_keys': []}

    for config in test_case.configurations:
      # TODO(user): deal with addition_instances later!!!
      assert config.min_instances > 0
      for instance_index in range(config.min_instances):
        config.instance_index = instance_index
        config.num_instances = config.min_instances
        runner = self._QueueTestRequestConfig(request, config)

        test_keys['test_keys'].append({'config_name': config.config_name,
                                       'instance_index': instance_index,
                                       'num_instances': config.num_instances,
                                       'test_key': str(runner.key())})
    return test_keys

  def _QueueTestRequestConfig(self, request, config):
    """Queue a given request's configuration for execution.

    Args:
      request: A TestRequest object to execute.
      config: A TestConfiguration object representing the machine on which to
          run the test.
    Returns:
      A tuple containing the id key of the test runner that was created
      and saved, as well as the test runner.
    """
    logging.debug('TRM._QueueTestRequestConfig request=%s config=%s',
                  request.name, config.config_name)

    # Create a runner entity to record this request/config pair that needs
    # to be run. The runner will eventually be scheduled at a later time.
    runner = TestRunner(
        test_request=request, config_name=config.config_name,
        config_hash=request.GetConfigurationDimensionHash(config.config_name),
        config_instance_index=config.instance_index,
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
    test_request = runner.test_request.GetTestCase()
    config = runner.GetConfiguration()
    test_run = test_request_message.TestRun(
        test_run_name=test_request.test_case_name,
        env_vars=test_request.env_vars,
        instance_index=runner.config_instance_index,
        num_instances=runner.num_config_instances,
        configuration=config,
        result_url=('%s/result?r=%s&id=%s' % (server_url, str(runner.key()),
                                              runner.machine_id)),
        ping_url=('%s/runner_ping?r=%s&id=%s' % (server_url, str(runner.key()),
                                                 runner.machine_id)),
        output_destination=test_request.output_destination,
        cleanup=test_request.cleanup,
        data=(test_request.data + config.data),
        tests=test_request.tests + config.tests,
        working_dir=test_request.working_dir,
        encoding=test_request.encoding)
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
      runner = TestRunner.get(key)
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
            'output': runner.GetResultString()}

  def AutomaticallyRetryRunner(self, runner):
    """Attempt to automaticlly retry runner.

    This runner will only be retried if it has't already been automatically
    retried too many times.

    Args:
      runner: The runner to retry.

    Returns:
      True if the runner was successfully setup to get run again.
    """
    if runner.automatic_retry_count < MAX_AUTOMATIC_RETRIES:
      # Don't change the created time since it is not the user's fault
      # we are retrying it (so it should have high prority to run again).
      runner.old_machine_ids.append(runner.machine_id)
      runner.machine_id = None
      runner.done = False
      runner.started = None
      runner.ping = None
      runner.automatic_retry_count += 1
      runner.put()
      return True

    return False

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
    query = TestRunner.gql(
        'WHERE done = :1 AND ping != :2 AND ping < :3',
        False, None, timeout_cutoff)
    for runner in query:
      if self.AutomaticallyRetryRunner(runner):
        logging.warning('TRM.AbortStaleRunners retrying runner %s with key %s. '
                        'Attempt %d', runner.GetName(), runner.key(),
                        runner.automatic_retry_count)
      else:
        logging.error('TRM.AbortStaleRunners aborting runner %s with key %s',
                      runner.GetName(), runner.key())
        self.AbortRunner(runner, reason='Runner has become stale.')

    # Abort all runners that haven't been able to find a machine to run them
    # in SWARM_RUNNER_MAX_WAIT_SECS seconds.
    timecut_off = now - datetime.timedelta(seconds=SWARM_RUNNER_MAX_WAIT_SECS)
    query = TestRunner.gql('WHERE created < :1 and started = :2 and done = :3 '
                           'and automatic_retry_count = 0', timecut_off, None,
                           False)
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

  def DeleteRunner(self, key):
    """Delete the runner that the given key refers to.

    Args:
      key: The key corresponding to the runner to be deleted.

    Returns:
      True if a matching TestRunner was found and deleted.
    """
    test_runner = None
    try:
      test_runner = TestRunner.get(key)
    except (db.BadKeyError, db.KindError):
      # We don't need to take any special action with bad keys.
      pass

    if not test_runner:
      logging.debug('No matching Test Runner found for key, %s', key)
      return False

    test_request = test_runner.test_request

    test_runner.delete()
    test_request.RunnerDeleted()

    return True

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
    dimension_hashes = dimensions_utils.GenerateAllDimensionHashes(
        attribs['dimensions'])

    unfinished_test = TestRunner.gql(
        'WHERE machine_id = :1 AND done = :2', attribs['id'], False).get()
    if unfinished_test:
      logging.error('A machine is asking for a new test, but there is '
                    'already an unfinished test with key, %s, running on a '
                    'machine with the same id, %s', unfinished_test.key(),
                    attribs['id'])

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
      if AssignRunnerToMachine(attribs['id'], runner, AtomicAssignID):
        assigned_runner = True
        # Grab the new version of the runner.
        runner = db.get(runner.key())
        break

    response = {'id': attribs['id']}
    if assigned_runner:
      # Get the commands the machine needs to execute.
      try:
        commands, result_url = self._GetTestRunnerCommands(runner, server_url)
      except PrepareRemoteCommandsError:
        # Failed to load the scripts so mark the runner as 'not running'.
        runner.started = None
        runner.machine_id = None
        runner.ping = None
        runner.put()
        response['try_count'] = 0
        response['come_back'] = COMEBACK_AFTER_SERVER_ERROR_SECS

      else:
        response['commands'] = commands
        response['result_url'] = result_url
        response['try_count'] = 0
        machine_stats.RecordMachineAssignment(attribs['id'],
                                              attributes.get('tag', None))
        runner_stats.RecordRunnerAssignment(db.get(runner.key()))
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
      elif attrib == 'tag' or attrib == 'username' or attrib == 'password':
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
    if not 'try_count' in attributes:
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
      query_runner = TestRunner.gql('WHERE started = :1 and config_hash IN :2 '
                                    'ORDER BY created LIMIT 1', None,
                                    attrib_hashes[i * 30:(i + 1) * 30]).get()

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
    query = TestRunner.gql('WHERE started = :1 ORDER BY created', None)
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

    Raises:
      PrepareRemoteCommandsError: Any error occured when preparing the commands.
    """
    output_commands = []

    # Get test manifest and scripts.
    test_run = self._BuildTestRun(runner, server_url)

    # Load the scripts.
    files_to_upload = self._GetFilesToUpload(test_run)

    # TODO(user): Use separate module for RPC related stuff rather
    # than slave_machine.
    output_commands.append(slave_machine.BuildRPC('StoreFiles',
                                                  files_to_upload))

    # Define how to run the scripts.
    command_to_execute = [
        r'%s' % os.path.join(test_run.working_dir, _TEST_RUNNER_SCRIPT),
        '-f', r'%s' % os.path.join(test_run.working_dir,
                                   _TEST_RUN_SWARM_FILE_NAME)]

    test_case = runner.test_request.GetTestCase()
    if test_case.verbose:
      command_to_execute.append('-v')

    if test_case.restart_on_failure:
      command_to_execute.append('--restart_on_failure')

    output_commands.append(slave_machine.BuildRPC('RunCommands',
                                                  command_to_execute))

    return (output_commands, test_run.result_url)

  def _GetFilesToUpload(self, test_run):
    """Loads required scripts into a single list of strings to be shipped.

    Args:
      test_run: A TestCase object representing the test to run.

    Returns:
      A list of tuples containing file names and contents. Each tuple has
      the format: (path to file on remote machine, file name, file contents).

    Raises:
      PrepareRemoteCommandsErrors: If there is an error is reading the local
          files.
    """
    # A list of tuples containing script paths on local and remote machine. Each
    # tuple has the format:
    # (path on local machine, path on remote machine, file name).
    # All remote paths are relative to the working directory specified by the
    # test manifest.
    file_paths = []

    # The local script runner.
    # We place the local running script in the current working directory (cwd)
    # of the slave, and place the rest of the scripts in relation to cwd. E.g.,
    # if the local script runner imports common.url_helper, we create the folder
    # common and put url_helper.py in it.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _TEST_RUNNER_DIR, _TEST_RUNNER_SCRIPT),
         test_run.working_dir, _TEST_RUNNER_SCRIPT))

    # The swarm constants script.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _COMMON_DIR,
                      _SWARM_CONSTANTS_SCRIPT),
         os.path.join(test_run.working_dir, _COMMON_DIR),
         _SWARM_CONSTANTS_SCRIPT))

    # The trm script.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _COMMON_DIR,
                      _TEST_REQUEST_MESSAGE_SCRIPT),
         os.path.join(test_run.working_dir, _COMMON_DIR),
         _TEST_REQUEST_MESSAGE_SCRIPT))

    # The url helper script.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _COMMON_DIR,
                      _URL_HELPER_SCRIPT),
         os.path.join(test_run.working_dir, _COMMON_DIR),
         _URL_HELPER_SCRIPT))

    # The test_runner __init__.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _TEST_RUNNER_DIR, _PYTHON_INIT_SCRIPT),
         os.path.join(test_run.working_dir, _TEST_RUNNER_DIR),
         _PYTHON_INIT_SCRIPT))

    # The common __init__.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _COMMON_DIR, _PYTHON_INIT_SCRIPT),
         os.path.join(test_run.working_dir, _COMMON_DIR),
         _PYTHON_INIT_SCRIPT))

    files_to_upload = []
    for local_path, remote_path, file_name in file_paths:
      try:
        file_contents = self._LoadFile(local_path)
      except IOError as e:
        logging.exception(str(e))
        raise PrepareRemoteCommandsError

      files_to_upload.append((remote_path, file_name, file_contents))

    # Append the test manifest to files that need to be stored.
    files_to_upload.append(
        (test_run.working_dir, _TEST_RUN_SWARM_FILE_NAME,
         test_request_message.Stringize(test_run, json_readable=True)))

    return files_to_upload

  def _LoadFile(self, file_name):
    """Loads the given file and return its contents as a string.

    Having this as a separate function makes is simpler to mock for tests.

    Args:
      file_name: A string of the file name to load.

    Returns:
      A string containing the file contents.

    Raises:
      IOError: The file cannot be loaded.
    """
    # The caller should catch the IOError exception.
    file_p = open(file_name, 'r')
    file_data = file_p.read()
    file_p.close()

    return file_data


def GetAllMatchingTestRequests(test_case_name):
  """Returns a list of all Test Request that match the given test_case_name.

  Args:
      test_case_name: The test case name to search for.

  Returns:
    A list of all Test Requests that have |test_case_name| as their name.
  """
  matches = []

  # Perform the query in a transaction to ensure that it gets the most recent
  # data, otherwise it is possible for one machine to add tests, and then be
  # unable to find them through this function after.
  query = db.run_in_transaction(TestRequest.gql, 'WHERE name = :1',
                                test_case_name)

  for test_request in query:
    matches.append(test_request)

  return matches


def GetTestRunners(sort_by, ascending, limit, offset):
  """Get the list of the test runners.

  Args:
    sort_by: The string of the attribute to sort the test runners by.
    ascending: True if the runner should be sorted in ascending order.
    limit: The machine number of test runners to return.
    offset: The offset from the complete set of sorted elements and the returned
        list.

  Returns:
    An iterator of test runners in the given range.
  """
  # If we recieve an invalid sort_by parameter, just default to machine_id.
  if not sort_by in TestRunner.properties():
    sort_by = 'machine_id'

  if not ascending:
    sort_by += ' DESC'

  return TestRunner.gql('ORDER BY %s' % sort_by).run(limit=limit, offset=offset)


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.now()


def DeleteOrphanedBlobs():
  """Remove all the orphaned blobs."""
  logging.debug('DeleteOrphanedBlobs starting')

  blobstore_query = blobstore.BlobInfo.all().order('creation')
  blobs_deleted = 0

  try:
    for blob in blobstore_query:
      if not TestRunner.gql('WHERE result_string_reference = :1',
                            blob.key()).count(limit=1):
        blobstore.delete_async(blob.key())
        blobs_deleted += 1
  except db.Timeout:
    logging.warning('Hit a timeout error while deleting orphaned blobs, some '
                    'orphans may still exist')

  logging.debug('DeleteOrphanedBlobs done')

  return blobs_deleted


def DeleteOldRunners():
  """Clean up all runners that are older than a certain age and done."""
  logging.debug('DeleteOldRunners starting')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS))

  db.delete(TestRunner.gql('WHERE ended < :1', old_cutoff))

  logging.debug('DeleteOldRunners done')


def DeleteOldErrors():
  """Cleans up errors older than a certain age."""
  logging.debug('DeleteOldErrors starting')
  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=SWARM_ERROR_TIME_TO_LIVE_DAYS))

  query = SwarmError.gql('WHERE created < :1', old_cutoff)
  for error in query:
    error.delete()

  logging.debug('DeleteOldErrors done')


def PingRunner(key, machine_id):
  """Pings the runner, if the key is valid.

  Args:
    key: The key of the runner to ping.
    machine_id: The machine id of the machine pinging.

  Returns:
    True if the runner is successfully pinged.
  """
  try:
    runner = TestRunner.get(key)
  except (db.BadKeyError, db.KindError, db.BadArgumentError) as e:
    logging.error('Failed to find runner, %s', str(e))
    return False

  if not runner:
    logging.error('Failed to find runner')
    return False

  if machine_id != runner.machine_id:
    if machine_id in runner.old_machine_ids:
      # This can happen if the network on the slave went down and the server
      # thought it was dead and retried the test.
      if (runner.machine_id is None and
          AssignRunnerToMachine(machine_id, runner, AtomicAssignID)):
        logging.info('Ping from older machine has resulted in runner '
                     'reconnecting to the machine')

        # The runner stored in the db was updated by AssignRunnerToMachine,
        # so the local runner needed to get updated to contain the same values.
        runner = TestRunner.get(key)
        runner.automatic_retry_count -= 1
        runner.old_machine_ids.remove(machine_id)
        runner.put()
      else:
        logging.info('Recieved a ping from an older machine, pretending to '
                     'accept ping.')
      return True
    else:
      logging.error('Machine ids don\'t match, expected %s but got %s',
                    runner.machine_id, machine_id)
      return False

  if runner.started is None or runner.done:
    logging.error('Trying to ping a runner that is not currently running')
    return False

  runner.ping = datetime.datetime.now()
  runner.put()
  return True


def AtomicAssignID(key, machine_id):
  """Function to be run by db.run_in_transaction().

  Args:
    key: key of the test runner object to assign the machine_id to.
    machine_id: machine_id of the machine we're trying to assign.

  Raises:
    TxRunnerAlreadyAssignedError: If runner has already been assigned a machine.
  """
  runner = db.get(key)
  if runner and not runner.started:
    runner.machine_id = str(machine_id)
    runner.started = datetime.datetime.now()
    runner.ping = datetime.datetime.now()
    runner.put()
  else:
    # Tell caller to abort this operation.
    raise TxRunnerAlreadyAssignedError


def AssignRunnerToMachine(machine_id, runner, atomic_assign):
  """Will try to atomically assign runner.machine_id to machine_id.

  This function is thread safe and can be called simultaneously on the same
  runner. It will ensure all but one concurrent requests will fail.

  It uses a transaction to run atomic_assign to assign the given machine_id
  inside attribs to the given runner. If the transaction succeeds, it will
  return True which means runner.machine_id = machine_id. However, based on
  datastore documentation, it is possible for datastore to throw exceptions
  on the transaction, and we have no way to conclude if the machine_id of
  the runner was set or not.

  After investigating a few alternative approaches, we decided to return
  False in such situations. This means the runner.machine_id (1) 'might' or
  (2) 'might not' have been changed, but we return False anyways. In case
  (2) no harm, no foul. In case (1), the runner is assumed running but is
  actually not. We expect the timeout event to fire at some future time and
  restart the runner.

  An alternate solution would be to delete that runner and recreate it, set
  the correct created timestamp, etc. which seems a bit more messy. We might
  switch to that if this starts to break at large scales.

  Args:
    machine_id: the machine_id of the machine.
    runner: test runner object to assign the machine to.
    atomic_assign: function pointer to be done atomically.

  Returns:
    True is succeeded, False otherwise.
  """
  for _ in range(MAX_TRANSACTION_RETRY_COUNT):
    try:
      db.run_in_transaction(atomic_assign, runner.key(), machine_id)
      return True
    except TxRunnerAlreadyAssignedError:
      # The runner is already assigned to a machine. Abort.
      break
    except db.TransactionFailedError:
      # This exception only happens if there is a problem with datastore,
      # e.g.high contention, capacity cap, etc. It ensures the operation isn't
      # done so we can safely retry.
      # Since we are on the main server thread, we avoid sleeping between
      # calls.
      continue
    except (db.Timeout, db.InternalError):
      # These exceptions do NOT ensure the operation is done. Based on the
      # Discussion above, we assume it hasn't been assigned.
      logging.exception('Un-determined fate for runner=%s', runner.GetName())
      break

  return False


class TxRunnerAlreadyAssignedError(Exception):
  """Simple exception class signaling a transaction fail."""
  pass


class PrepareRemoteCommandsError(Exception):
  """Simple exception class signaling failure to prepare remote commands."""
  pass
