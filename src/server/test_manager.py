#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Test Request Manager.

A Test Request is a request to install and run some arbitrary binaries and data
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
import os.path
import urllib
import urllib2
import urlparse
import uuid

from google.appengine.api import files
from google.appengine.api import mail
from google.appengine.api import urlfetch
from google.appengine.api import users
from google.appengine.ext import blobstore
from google.appengine.ext import db
from common import dimensions
from common import test_request_message
from test_runner import slave_machine

# Fudge factor added to a runner's timeout before we consider it to have run
# for too long.  Runners that run for too long will be aborted automatically.
# Specified in number of seconds.
_TIMEOUT_FUDGE_FACTOR = 600

# Default Test Run Swarm filename.  This file provides parameters
# for the instance running tests.
_TEST_RUN_SWARM_FILE_NAME = 'test_run.swarm'

# Name of python script to execute on the remote machine to run a test.
_TEST_RUNNER_SCRIPT = 'local_test_runner.py'

# Name of python script to download test files.
_DOWNLOADER_SCRIPT = 'downloader.py'

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

# Number of times to try a transaction before giving up. Since most likely the
# recoverable exceptions will only happen when datastore is overloaded, it
# doesn't make much sense to extensively retry many times.
MAX_TRANSACTION_RETRY_COUNT = 3

# Root directory of Swarm scripts.
SWARM_ROOT_DIR = os.path.join(os.path.dirname(__file__), '..')

# Number of days to keep old runners around for.
SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS = 14

# Number of days to keep error logs around.
SWARM_ERROR_TIME_TO_LIVE_DAYS = 7


class UserProfile(db.Model):
  """A user profile.

  All TestRequest and TestRunner objects are associated with a specific user.
  A user has a whitelist of machine IPs that are allowed to interact with its
  data. A UserProfile can be retrieved using the user's email address as
  the key.

  A UserProfile has a 'whitelist' list of MachineWhitelist of authorized remote
  machines.
  A UserProfile has a 'test_requests' list of TestRequest of tests belonging
  to this user.
  """
  # The actual account of the user.
  user = db.UserProperty()

  def DeleteProfile(self):
    # Deletes the profile, its whitelists and test cases.
    for test_case in self.test_requests:
      test_case.delete()
    for whitelist in self.whitelist:
      whitelist.delete()
    self.delete()


class MachineWhitelist(db.Model):
  """A machine IP as part of a UserProfile whitelist."""
  # A reference to the user's profile.
  user_profile = db.ReferenceProperty(
      UserProfile, required=True, collection_name='whitelist')

  # The IP of the machine to whitelist.
  ip = db.ByteStringProperty()

  # An optional password (NOT necessarily equal to the actual user
  # account password) used to ensure requests coming from a remote machine
  # are indeed valid. Defaults to None.
  password = db.StringProperty()


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


class TestRequest(db.Model):
  """A test request.

  Test Request objects represent one test request from one client.  The client
  can be a build machine requesting a test after a build or it could be a
  developer requesting a test from their own build.
  """
  # A reference to the user's profile.
  user_profile = db.ReferenceProperty(
      UserProfile, required=True, collection_name='test_requests')

  # The message received from the caller, formatted as a Test Case as
  # specified in
  # http://code.google.com/p/swarming/wiki/SwarmFileFormat.
  message = db.TextProperty()

  # The time at which this request was received.
  requested_time = db.DateTimeProperty(auto_now_add=True)

  # The name for this test request.
  name = db.StringProperty(required=True)

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
  config_name = db.StringProperty()

  # The 0 based instance index of the request's configuration being tested.
  config_instance_index = db.IntegerProperty()

  # The number of instances running on the same configuration as ours.
  num_config_instances = db.IntegerProperty()

  # The machine that is running or ran the test. This attribute is only valid
  # once a machine has been assigned to this runner.
  machine_id = db.StringProperty()

  # Used to indicate if the runner has finished, either successfully or not.
  done = db.BooleanProperty(default=False)

  # The time at which this runner was created.  The runner may not have
  # started executing yet.
  created = db.DateTimeProperty(auto_now_add=True)

  # The time at which this runner was executed on a remote machine.  If the
  # runner isn't executing or ended, then the value is None and we use the
  # fact that it is None to identify if a test was started or not.
  started = db.DateTimeProperty()

  # The time at which this runner ended.  This attribute is valid only when
  # the runner has ended (i.e. done == True). Until then, the value is
  # unspecified.
  ended = db.DateTimeProperty(auto_now=True)

  # True if the test run finished and succeeded.  This attribute is valid only
  # when the runner has ended. Until then, the value is unspecified.
  ran_successfully = db.BooleanProperty()

  # The stringized array of exit_codes for each actions of the test.
  exit_codes = db.StringProperty()

  # The blobstore reference to the full output of the test.  This key valid only
  # when the runner has ended (i.e. done == True). Until then, it is None.
  result_string_reference = blobstore.BlobReferenceProperty()

  # The actual account of the user the runner belongs to.
  user = db.UserProperty()

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
    return '%s:%s' % (self.test_request.name, self.config_name)

  def GetConfiguration(self):
    """Gets the configuration associated with this runner.

    Returns:
      A configuration dictionary for this runner.
    """
    config = self.test_request.GetConfiguration(self.config_name)
    assert config is not None
    return config

  def GetTimeout(self):
    """Get the timeout for this runner in seconds.

    The timeout applicable to this runner is the sum of the timeouts of each
    test that it will run.  The set of tests run is the union of the tests
    specified in the TestCase object of the request plus the tests specified
    in the Configuration object associated with this runner.

    Returns:
      The timeout value for this runner in seconds.
    """
    test_case = self.test_request.GetTestCase()
    config = self.GetConfiguration()
    return (sum([test.time_out for test in test_case.tests]) +
            sum([test.time_out for test in config.tests]))

  def GetResultString(self):
    """Get the result string for the runner.

    Returns:
      The string representing the output for this runner or an empty string
        if the result hasn't been written yet.
    """
    if not self.result_string_reference:
      return ''

    blob_reader = self.result_string_reference.open()
    result = ''
    while True:
      fetch = blob_reader.read(blobstore.MAX_BLOB_FETCH_SIZE)
      if not fetch:
        break
      result += fetch

    return result.decode('utf-8')

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


class MachineAssignment(db.Model):
  """A machine's last runner assignment."""
  # The machine id of the polling machine.
  machine_id = db.StringProperty()

  # The tag of the machine polling.
  tag = db.StringProperty()

  # The actual account of the user that whitelisted this machine.
  user = db.UserProperty()

  # The time of the assignment.
  assignment_time = db.DateTimeProperty(auto_now=True)


class SwarmError(db.Model):
  """A datastore entry representing an error in Swarm."""
  # The name of the error.
  name = db.StringProperty()

  # A description of the error.
  message = db.StringProperty()

  # Optional details about the specific error instance.
  info = db.StringProperty()

  # The time at which this error was logged.  Used to clean up old errors.
  created = db.DateTimeProperty(auto_now_add=True)


class TestRequestManager(object):
  """The Test Request Manager."""

  def UpdateCacheServerURL(self, server_url):
    """Update the value of this server's url.

    Args:
      server_url: The URL to the Swarm server so that we can set the
          result_url in the Swarm file we upload to the machines.
    """
    self.server_url = server_url

  def HandleTestResults(self, web_request, user_profile):
    """Handle a response from the remote script.

    Args:
      web_request: a google.appengine.ext.webapp.Request object containing the
          results of a test run.  The URL will contain a k= CGI parameter
          holding the persistent key for the runner.
      user_profile: The profile of the user that allowed the remote machine
          to execute the test.
    """
    if not web_request:
      return

    runner = None
    key = web_request.get('k')
    if key:
      runner = TestRunner.get(db.Key(key))

    if not runner:
      logging.error('No runner associated to web request, ignoring test result')
      return

    # Make sure the remote machine is actually who they say they are by
    # comparing the user that created the test with the one allowing the
    # results.
    if runner.test_request.user_profile.user != user_profile.user:
      logging.error('Remote machine not authorized to post results')
      return

    # Find the high level success/failure from the URL. We assume failure if
    # we can't find the success parameter in the request.
    success = web_request.get('s', 'False') == 'True'
    result_string = urllib.unquote_plus(web_request.get('r'))
    exit_codes = urllib.unquote_plus(web_request.get('x'))
    self._UpdateTestResult(runner, success, exit_codes, result_string)

  def _UpdateTestResult(self, runner, success=False, exit_codes='',
                        result_string='Tests aborted'):
    """Update the runner with results of a test run.

    Args:
      runner: a TestRunner object pointing to the test request to which to
          send the results.  This argument must not be None.
      success: a boolean indicating whether the test run succeeded or not.
      exit_codes: a string containing the array of exit codes of the test run.
      result_string: a string containing the output of the test run.
    """
    if not runner:
      logging.error('runner argument must be given')
      return

    # If the runnner is marked as done, don't try to process another
    # response for it.
    if runner.done:
      logging.error('Got a second response for runner=%s, not good',
                    runner.GetName())
      return

    runner.ran_successfully = success
    runner.exit_codes = exit_codes
    runner.done = True

    filename = files.blobstore.create('text/plain')
    with files.open(filename, 'a') as f:
      f.write(result_string.encode('utf-8'))
    files.finalize(filename)

    runner.result_string_reference = files.blobstore.get_blob_key(filename)
    runner.put()

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
        except urlfetch.Error:
          # The docs for urllib2.urlopen() say it only raises urllib2.URLError.
          # However, in the appengine environment urlfetch.Error may also be
          # raised. This is normal, see the "Fetching URLs in Python" section of
          # http://code.google.com/appengine/docs/python/urlfetch/overview.html
          # for more details.
          logging.exception('Could not send results back to sender at %s',
                            test_case.result_url)
      elif result_url_parts[0] == 'mailto':
        # Only send an email if we didn't send a failure email earlier to
        # prevent repeats.
        if runner.ran_successfully or not test_case.failure_email:
          self._EmailTestResults(runner, result_url_parts[1])
      else:
        logging.exception('Unknown url given as result url, %s',
                          test_case.result_url)

    if (test_case.store_result == 'none' or
        (test_case.store_result == 'fail' and runner.ran_successfully)):
      test_request = runner.test_request
      runner.delete()
      test_request.RunnerDeleted()

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

  def GetAllMatchingTestRequests(self, test_case_name, user_profile):
    """Returns a list of all Test Request that match the given test_case_name.

    Args:
        test_case_name: The test case name to search for.
        user_profile: The user_profile the tests belong to. Should be a valid
            profile.

    Returns:
      A list of all Test Requests that have |test_case_name| as their name.
    """
    # The tests sould belong to some user.
    assert user_profile

    matches = []
    query = TestRequest.gql('WHERE user_profile = :1 and name = :2',
                            user_profile, test_case_name)
    for test_request in query:
      matches.append(test_request)

    return matches

  def ExecuteTestRequest(self, request_message, user_profile):
    """Attempts to execute a test request.

    If machines are available for running any of the test's configurations,
    they will be started immediately.  The other test configurations will be
    queued up for testing at a later time potentially requesting new machines.

    Args:
      request_message: A string representing a test request.
      user_profile: The user_profile the test belongs to. Should be a valid
          profile.

    Raises:
      test_request_message.Error: If the request's message isn't valid.

    Returns:
      A dictionary containing the test_case_name field and an array of
      dictionaries containing the config_name and test_id_key fields.
    """
    logging.debug('TRM.ExecuteTestRequest msg=%s', request_message)

    # The test sould belong to some user.
    assert user_profile

    # Will raise an exception on error.
    test_name = GetTestCase(request_message).test_case_name
    request = TestRequest(message=request_message, name=test_name,
                          user_profile=user_profile)
    test_case = request.GetTestCase()  # Will raise on invalid request.
    request.put()

    test_keys = {'test_case_name': test_case.test_case_name,
                 'test_keys': []}

    for config in test_case.configurations:
      # TODO(user): deal with addition_instances later!!!
      assert config.min_instances > 0
      for instance_index in range(config.min_instances):
        config.instance_index = instance_index
        config.num_instances = config.min_instances
        runner = self._QueueTestRequestConfig(request, config, user_profile)

        test_keys['test_keys'].append({'config_name': config.config_name,
                                       'instance_index': instance_index,
                                       'num_instances': config.num_instances,
                                       'test_key': str(runner.key())})
    return test_keys

  def _QueueTestRequestConfig(self, request, config, user_profile):
    """Queue a given request's configuration for execution.

    Args:
      request: A TestRequest object to execute.
      config: A TestConfiguration object representing the machine on which to
          run the test.
      user_profile: The user profile the runner belongs to.

    Returns:
      A tuple containing the id key of the test runner that was created
      and saved, as well as the test runner.
    """
    logging.debug('TRM._QueueTestRequestConfig request=%s config=%s',
                  request.name, config.config_name)

    # Create a runner entity to record this request/config pair that needs
    # to be run. The runner will eventually be scheduled at a later time.
    runner = TestRunner(test_request=request, config_name=config.config_name,
                        config_instance_index=config.instance_index,
                        num_config_instances=config.num_instances,
                        user=user_profile.user)
    runner.put()

    return runner

  def _BuildTestRun(self, runner):
    """Build a Test Run message for the remote test script.

    Args:
      runner: A TestRunner object for this test run.

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
        result_url=('%s/result?k=%s' % (self.server_url, str(runner.key()))),
        output_destination=test_request.output_destination,
        data=(test_request.data + test_request.binaries +
              config.data + config.binaries),
        tests=test_request.tests + config.tests,
        working_dir=test_request.working_dir)
    test_run.ExpandVariables({
        'instance_index': runner.config_instance_index,
        'num_instances': runner.num_config_instances,
    })
    errors = []
    assert test_run.IsValid(errors), errors
    return test_run

  def GetRunnerResults(self, key, user_profile):
    """Returns the results of the runner specified by key.

    Args:
      key: TestRunner key representing the runner.
      user_profile: The user requesting the results which should be the
          same as the runner's owner.

    Returns:
      A dictionary of the runner's results, or None if the runner not found.

    Raises:
      AuthenticationError: If the user was not authorized to access the runner.
    """
    assert user_profile

    try:
      runner = TestRunner.get(key)
    except (db.BadKeyError, db.KindError, db.BadArgumentError):
      return None

    if runner:
      if runner.user == user_profile.user:
        return self.GetResults(runner)
      else:
        logging.exception(
            'Invalid access: user %s trying to get results of user %s',
            user_profile.user.email(), runner.user.email())
        raise AuthenticationError

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
            'output': runner.GetResultString()}

  def AbortStaleRunners(self):
    """Abort any runners that have been running longer than expected."""
    logging.debug('TRM.AbortStaleRunners starting')
    now = _GetCurrentTime()

    query = TestRunner.gql('WHERE started != :1 AND done = :2', None, False)
    for runner in query:
      # Determine how long the runner should have taken.
      runner_timeout = runner.GetTimeout() + _TIMEOUT_FUDGE_FACTOR

      # Determine if too much time has expired since the runner began.
      delta = datetime.timedelta(seconds=runner_timeout)
      if now > runner.started + delta:
        # Runner has been going for too long, abort it.
        logging.info('TRM.AbortStaleRunners aborting runner=%s',
                     runner.GetName())
        self.AbortRunner(runner, reason='Runner has become stale.')

    logging.debug('TRM.AbortStaleRunners done')

  def AbortRunner(self, runner, reason='Not specified.'):
    """Abort the given test runner.

    Args:
      runner: An instance of TestRunner to be aborted.
      reason: A string message indicating why the TestRunner is being aborted.
    """
    r_str = 'Tests aborted. AbortRunner() called. Reason: %s' % reason
    self._UpdateTestResult(runner, result_string=r_str)

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

  def ExecuteRegisterRequest(self, attributes, user_profile):
    """Attempts to match the requesting machine with an existing TestRunner.

    If the machine is matched with a request, the machine is told what to do.
    Else, the machine is told to register at a later time.

    Args:
      attributes: A dictionary representing the attributes of the machine
      registering itself.
      user_profile: The user_profile that whitelisted the machine. Should be
      a valid profile.

    Raises:
      test_request_message.Error: If the request format/attributes aren't valid.

    Returns:
      A dictionary containing the commands the machine needs to execute.
    """
    # The machine should be whitelisted by some user.
    assert user_profile

    # Validate and fix machine attributes. Will throw exception on errors.
    attribs = self.ValidateAndFixAttributes(attributes)

    assigned_runner = False
    runner = None
    response = {}

    unfinished_test = TestRunner.gql(
        'WHERE machine_id = :1 AND done = :2', attribs['id'], False).get()
    if unfinished_test:
      logging.error('A machine is asking for a new test, but there is '
                    'already an unfinished test running on a machine '
                    'with the same id, %s', attribs['id'])

    # Try assigning machine to a runner 10 times before we give up.
    # TODO(user): Tune this parameter somehow.
    for _ in range(10):
      # Try to find a matching test runner for the machine.
      runner = self._FindMatchingRunner(attribs, user_profile)
      if runner:
        # Will atomically try to assign the machine to the runner. This could
        # fail due to a race condition on the runner. If so, we loop back to
        # finding a runner.
        if self._AssignRunnerToMachine(attribs['id'], runner, AtomicAssignID):
          # Get the commands the machine needs to execute.
          try:
            commands, result_url = self._GetTestRunnerCommands(runner)
          except PrepareRemoteCommandsError:
            # Failed to load the scripts so mark the runner as 'not running'.
            runner.started = None
            runner.put()
          else:
            response['commands'] = commands
            response['result_url'] = result_url
            assigned_runner = True

            self._RecordMachineRunnerAssignment(attribs['id'],
                                                attributes.get('tag', None),
                                                user_profile)
            break
      # We found no runner, no use in re-trying so just break out of the loop.
      else:
        break

    response['id'] = attribs['id']
    if assigned_runner:
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
          except (ValueError, AttributeError):
            raise test_request_message.Error(
                'Invalid attrib type for id: ' + str(type(value)))
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

  def _RecordMachineRunnerAssignment(self, machine_id, machine_tag,
                                     user_profile):
    """Record when a machine has a runner assigned to it.

    Args:
      machine_id: The machine id of the machine.
      machine_tag: The tag identifier of the machine.
      user_profile: The user profile that whitelisted this machine.
    """
    machine_assignment = MachineAssignment.gql('WHERE machine_id = :1',
                                               machine_id).get()

    # Check to see if we need to create the model.
    if machine_assignment is None:
      machine_assignment = MachineAssignment(machine_id=machine_id,
                                             user=user_profile.user)

    machine_assignment.tag = machine_tag
    machine_assignment.assignment_time = datetime.datetime.now()
    machine_assignment.put()

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
    return min(MAX_COMEBACK_SECS, float(2**try_count)/100 + 1)

  def _FindMatchingRunner(self, attribs, user_profile):
    """Find oldest TestRunner who hasn't already been assigned a machine.

    Args:
      attribs: The attributes defining the machine.
      user_profile: The user_profile to search for runners.

    Returns:
      A TestRunner object, or None if a matching runner is not found.
    """
    # TODO(user): limit the number of test runners checked to avoid querying
    # all the tasks all the time.

    # Assign test runners from earliest to latest.
    # We use a format argument for None, because putting None in the string
    # doesn't work.
    query = TestRunner.gql('WHERE started = :1 AND user = :2 ORDER BY created',
                           None, user_profile.user)
    for runner in query:
      runner_dimensions = runner.GetConfiguration().dimensions
      (match, output) = dimensions.MatchDimensions(runner_dimensions,
                                                   attribs['dimensions'])
      logging.info(output)
      if match:
        logging.info('matched runner %s: ' % runner.GetName()
                     + str(runner_dimensions) + ' to machine: '
                     + str(attribs['dimensions']))
        return runner

    return None

  def _AssignRunnerToMachine(self, machine_id, runner, atomic_assign):
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
    for _ in range(0, MAX_TRANSACTION_RETRY_COUNT):
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

  def _GetTestRunnerCommands(self, runner):
    """Get the commands that need to be sent to a slave to execute the runner.

    Args:
      runner: test runner object to run.

    Returns:
      A tuple (commands, result_url) where commands is a list of RPC calls that
      need to be run by the remote slave machine and result_url is where it
      should post the results.

    Raises:
      PrepareRemoteCommandsError: Any error occured when preparing the commands.
    """
    output_commands = []

    # Get test manifest and scripts.
    test_run = self._BuildTestRun(runner)

    # Load the scripts.
    try:
      files_to_upload = self._GetFilesToUpload(test_run)
    except IOError as e:
      logging.exception(str(e))
      raise PrepareRemoteCommandsError

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
      IOError: A file cannot be loaded.
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
    # if the local script runner imports common.downloader, we create the folder
    # common and put downloader.py in it.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _TEST_RUNNER_DIR, _TEST_RUNNER_SCRIPT),
         test_run.working_dir, _TEST_RUNNER_SCRIPT))

    # The downloader_file.
    file_paths.append(
        (os.path.join(SWARM_ROOT_DIR, _TEST_RUNNER_DIR, _DOWNLOADER_SCRIPT),
         os.path.join(test_run.working_dir, _TEST_RUNNER_DIR),
         _DOWNLOADER_SCRIPT))

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

    # TODO(user): On app engine we don't have access to files on disk.
    # Need to find an alternative to loading files from disk.
    for local_path, remote_path, file_name in file_paths:
      try:
        file_contents = self._LoadFile(local_path)
      except IOError:
        raise

      files_to_upload.append((remote_path, file_name, file_contents))

    # Append the test manifest to files that need to be stored.
    files_to_upload.append(
        (test_run.working_dir, _TEST_RUN_SWARM_FILE_NAME, str(test_run)))

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

  def ModifyUserProfileWhitelist(self, ip, add=True, password=None):
    """Adds/removes the given ip from the whitelist of the current user.

    If a user profile doesn't already exist, one will be created first.
    This function is thread safe.

    Args:
      ip: The ip to be added/removed.
      add: If True, will add the ip to the whitelist. Else, it will remove
      the ip. Ignores duplicate or non-existing ips regardless of the password.
      password: Optional password to associate with the machine.

    Returns:
      True if request was valid. Doesn't necessarily mean the ip was found for
      remove or didn't exist for add, but that datastore is in a sane state.
    """
    user = users.get_current_user()
    if not user:
      logging.error('User not signed in? Security breach.')
      return False

    # Validate arg types to be string and bool, respectively.
    # We accept None ips which happen in local testing. So if ip != None,
    # it should be a string.
    if ip is not None:
      if not isinstance(ip, (str, unicode)):
        logging.error('Invalid ip type: %s', str(type(ip)))
        return False
      else:
        ip = str(ip)

    if not isinstance(add, bool):
      logging.error('Invalid add type: %s', str(type(add)))
      return False

    # Atomically create the user profile or use an existing one.
    # Handle normal transaction exceptions for get_or_insert.
    try:
      user_profile = UserProfile.get_or_insert(user.email(), user=user)
    except (db.TransactionFailedError, db.Timeout, db.InternalError) as e:
      # This is a low-priority request. Abort on any failures.
      logging.exception('User profile creation exception: %s', str(e))
      return False

    assert user_profile

    # Find existing entries, if any.
    query = user_profile.whitelist.filter('ip =', ip)

    # Sanity check.
    assert query.count() == 1 or query.count() == 0

    if add:
      # Ignore duplicate requests.
      if query.count() == 0:
        # Create a new entry.
        white_list = MachineWhitelist(
            user_profile=user_profile, ip=ip, password=password)
        white_list.put()
        logging.debug('Stored ip: %s', ip)
    else:
      # Ignore non-existing requests.
      if query.count() == 1:
        # Delete existing entry.
        white_list = query.get()
        white_list.delete()
        logging.debug('Removed ip: %s', ip)

    return True


def GetAllUserMachines(user_profile):
  """Get the list of the user's whitelisted machines.

  Args:
    user_profile: The profile of the user that whitelisted the machines.

  Returns:
    An iterator of all machines whitelisted by the user.
  """
  return (machine for machine in MachineAssignment.gql('WHERE user = :1',
                                                       user_profile.user))


def FindUserWithWhitelistedIP(ip, password):
  """Finds and returns the user that has whitelisted the given IP.

  Args:
    ip: IP of the client making the request.
    password: The password provided by the client making the request.

  Returns:
    UserProfile of the user that has whitelisted the IP, None otherwise.
  """
  whitelist = MachineWhitelist.gql(
      'WHERE ip = :1 AND password = :2', ip, password)

  if whitelist.count() == 0:
    return None

  # Sanity check.
  assert whitelist.count() == 1

  return whitelist.get().user_profile


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.now()


def DeleteOldRunners():
  """Clean up all runners that are older than a certain age and done."""
  logging.debug('DeleteOldRunners starting')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS))

  query = TestRunner.gql('WHERE ended < :1', old_cutoff)
  for runner in query:
    runner.delete()

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


def GetUserProfile(user):
  """Return the user profile that the given user corresponds to.

  Args:
    user: The user to find the profile for.

  Returns:
    The user profile of the user.
  """
  return UserProfile.gql('WHERE user = :1', user).get()


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
    runner.put()
  else:
    # Tell caller to abort this operation.
    raise TxRunnerAlreadyAssignedError


class TxRunnerAlreadyAssignedError(Exception):
  """Simple exception class signaling a transaction fail."""
  pass


class PrepareRemoteCommandsError(Exception):
  """Simple exception class signaling failure to prepare remote commands."""
  pass


class AuthenticationError(Exception):
  """Exception signaling failure to authenticate a user performing a task."""
  pass
