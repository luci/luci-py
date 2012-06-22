#!/usr/bin/python2.4
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
from google.appengine.ext import blobstore
from google.appengine.ext import db
from common import dimensions
from common import test_request_message
from server import base_machine_provider
from test_runner import remote_test_runner

# Fudge factor added to a runner's timeout before we consider it to have run
# for too long.  Runners that run for too long will be aborted automatically.
# Specified in number of seconds.
_TIMEOUT_FUDGE_FACTOR = 600

# Default Test Run Swarm filename.  This file provides parameters
# for the instance running tests.
_TEST_RUN_SWARM_FILE_NAME = 'test_run.swarm'

# Name of python script to execute on the remote machine to run a test.
_TEST_RUNNER_SCRIPT = 'local_test_runner.py'

# Name of directories in source tree and/or on remote machine.
_TEST_RUNNER_DIR = 'test_runner'
_COMMON_DIR = 'common'

# Reserved UUID to indicate 'no machine assigned but waiting for one'.
NO_MACHINE_ID = '00000000-00000000-00000000-00000000'

# Reserved UUID to indicate 'no machine assigned and done'.
DONE_MACHINE_ID = 'FFFFFFFF-FFFFFFFF-FFFFFFFF-FFFFFFFF'

# Maximum value for the come_back field in a response to an idle slave machine.
# TODO(user): make this adjustable by the user.
MAX_COMEBACK_SECS = 60.0

# Maximum cap for try_count. A try_count value greater than this is clamped to
# this constant which will result in ~400M secs (>3 years).
MAX_TRY_COUNT = 30


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

  # The time at which this request was received.
  requested_time = db.DateTimeProperty(auto_now_add=True)

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
      request_object = test_request_message.TestCase()
      errors = []
      if not request_object.ParseTestRequestMessageText(self.message, errors):
        raise test_request_message.Error('\n'.join(errors))

      self._request_object = request_object

    return request_object

  def GetName(self):
    """Gets a name for this test.

    Returns:
      The  name for this test.

    Raises:
      test_request_message.Error: If the request's message isn't valid.
    """
    return self.GetTestCase().test_case_name

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

  # The machine running the test.  The meaning of this field also tells us
  # about the state of the runner, to make it easier to run queries below.
  #
  # If the machine_id is NO_MACHINE_ID, then this runner is looking to be
  # executed on a remote test runner (either for the first time or retry).
  #
  # If the machine_id is DONE_MACHINE_ID, the runner has finished, either
  # successfully or not.  The 'ended' attribute records the time at which
  # the runner ended.
  #
  # If the machine_id is neither, this means that the runner has been
  # assigned to a machine and is currently running. The 'started' attribute
  # records the time at which test started.
  machine_id = db.ByteStringProperty()

  # The time at which this runner was created.  The runner may not have
  # started executing yet.
  created = db.DateTimeProperty(auto_now_add=True)

  # The time at which this runner was executed on a remote machine.  This
  # attribute is valid only when the runner is executing or ended (i.e.
  # machine_id is != NO_MACHINE_ID).  Otherwise the value is None and we
  # use the fact that it is None to identify if a test was started or not.
  started = db.DateTimeProperty()

  # The time at which this runner ended.  This attribute is valid only when
  # the runner has ended (i.e. machine_id is == DONE_MACHINE_ID). Until then,
  # the value is unspecified.
  ended = db.DateTimeProperty(auto_now=True)

  # True if the test run finished and succeeded.  This attribute is valid only
  # when the runner has ended (i.e. machine_id is == DONE_MACHINE_ID).
  # Until then, the value is unspecified.
  ran_successfully = db.BooleanProperty()

  # The stringized array of exit_codes for each actions of the test.
  exit_codes = db.StringProperty()

  # The blobstore reference to the full output of the test.  This key valid only
  # when the runner has ended (i.e. machine_id is == DONE_MACHINE_ID).
  # Until then, it is None.
  result_string_reference = blobstore.BlobReferenceProperty()

  # The hostname of the swarm bot that ran this test. This attribute is valid
  # only when the runner has been assigned a machine. Until then, the value
  # is unspecified.
  hostname = db.TextProperty()

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
    return '%s:%s' % (self.test_request.GetName(), self.config_name)

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

  def RequiresVirginMachine(self):
    """Get the virgin machine flag for this runner.

    Returns:
      True iff this runner's test case requested a virgin machine.
    """
    return self.test_request.GetTestCase().virgin

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


class IdleMachine(db.Model):
  """An idle machine.

  Idle machines are those that have been previously acquired and are finished
  running tests. As new tests arrive, machines are taken from the idle pool
  before acquiring new ones unless we need a fresh machine (e.g., explicit
  requests for virgin machines or sharded tests need new machines).
  """
  # The Id of the machine.
  id = db.ByteStringProperty()


class TestRequestManager(object):
  """The Test Request Manager."""

  def __init__(self, machine_manager):
    """Initializes the TRM.

    Initializes the TRM to use the given machine manager to acquire and release
    test machines, as well as validating the persisted state of test requests,
    test runners, and idle machines.

    Args:
      machine_manager: An instance of machine_manager.MachineManager that is
          used to acquire machines for running tests.
    """
    logging.debug('TRM starting')

    self._machine_manager = machine_manager
    self._machine_manager.RegisterStatusChangeListener(self)

    # Make sure that all acquired machines are either idle or assigned to a
    # test runner.
    self._CheckAllAcquiredMachines()

    # Load idle machines and validate.
    machines_to_del = []
    for machine in IdleMachine.all():
      info = self._machine_manager.GetMachineInfo(machine.id)
      # "info" can be None if the machine ID is stale and doesn't reference a
      # valid machine anymore. Idle Machines MUST be ACQUIRED!
      if (not info or
          info.status != base_machine_provider.MachineStatus.ACQUIRED):
        machines_to_del.append(machine)
        continue

    # Load test runners and validate.
    for runner in TestRunner.gql('WHERE machine_id != :1 AND machine_id != :2',
                                 NO_MACHINE_ID, DONE_MACHINE_ID):
      info = self._machine_manager.GetMachineInfo(runner.machine_id)
      if not info or info.status not in (
          base_machine_provider.MachineStatus.WAITING,
          base_machine_provider.MachineStatus.ACQUIRED):
        # The machine running the test has failed (either stopped or done).
        # Tell the user about it.
        r_str = 'Tests aborted. The machine running the test has failed. '
        r_str += ('Machine status: %s' % str(info.status) if info else
                  'Status undefined.')
        self._UpdateTestResult(runner, result_string=r_str)

    # Delete all objects that were stale.
    for machine in machines_to_del:
      machine.delete()

    logging.debug('TRM created')

  def UpdateCacheServerURL(self, server_url):
    """Update the value of this server's url.

    Args:
      server_url: The URL to the Swarm server so that we can set the
          result_url in the Swarm file we upload to the machines.
    """
    self.server_url = server_url

  def _CheckAllAcquiredMachines(self):
    """Ensure acquired machines are either idle or assigned to a runner."""
    logging.debug('TRM._CheckAllAcquiredMachines')

    machines_to_release = []

    for info in self._machine_manager.ListAcquiredMachines():
      idle_machine = IdleMachine.gql('WHERE id = :1', info.id).get()
      runner = TestRunner.gql('WHERE machine_id = :1', info.id).get()

      logging.debug('Checking machine id=%s', info.id)

      # If a machine is assigned to a runner, then it should not be idle.
      # Remove it from the idle pool if it is there.
      if idle_machine and runner:
        logging.error('Machine id=%s idle and running', info.id)
        idle_machine.delete()
      elif not runner and not idle_machine:
        # If a machine is neither idle nor assigned to a test runner, then put
        # it in the idle pool if its ACQUIRED. Otherwise, release it.
        logging.debug('Machine id=%s is idle and unassigned', info.id)

        if info.status == base_machine_provider.MachineStatus.ACQUIRED:
          self._HandleIdleMachine(info=info)
        else:
          if info.status == base_machine_provider.MachineStatus.WAITING:
            # WAITING machines should ALWAYS be assigned a runner.
            # Since we don't know what to do with it, might as well release it.
            logging.error('Machine %s, should not be in WAITING state without'
                          'a runner!', info.id)
          else:
            logging.debug('Machine id=%s will be released', info.id)
          machines_to_release.append(info.id)
      else:
        # If there is a machine that is ready and assigned a runner that is
        # not started, we can't start it here because of thread safety, we
        # must wait for the next call to AssignPendingRequests().
        pass

    for machine_id in machines_to_release:
      self._machine_manager.ReleaseMachine(machine_id)
      logging.debug('Machine id=%s released', machine_id)

  def MachineStatusChanged(self, info):
    """Handles a status change of an acquired machine.

    When machines are initially acquired in _EnsureMachineAvailable()
    below, they are in the waiting state.

    If the machine whose state has changed is idle, then we only expect to
    see state changes to stopped, or done.

    If the machine whose state has changed is currently running a test, then
    we only expect to see state changes to stopped or done.  In both cases
    though, this signals an error on the machine, and this is handled as
    needed in the _RunnerMachineStateChanged() function.

    Otherwise, a WAITING machine has changed to ACQUIRED and we can now start
    the associated test runner on it.

    Args:
      info: A machine information object from the Machine Manager.
    """
    logging.debug('TRM.MachineStatusChanged')

    idle_machine = IdleMachine.gql('WHERE id = :1', info.id).get()
    if idle_machine:
      # We can't have idle machines assigned to a test runner.
      assert TestRunner.gql('WHERE machine_id = :1', info.id).get() is None
      self._IdleMachineStateChanged(info, idle_machine)
    else:
      # If the machine is currently assigned to a running a test, run/abort it.
      runner = TestRunner.gql('WHERE machine_id = :1', info.id).get()
      if runner:
        self._RunnerMachineStateChanged(info, runner)
      else:
        logging.error('Machine %s is not running nor Idle. Wazzup?', info.id)
        # Next call to _CheckAllAcquiredMachines should fix that!

  def _IdleMachineStateChanged(self, info, idle_machine):
    """Handles a status change for a machine on the idle list.

    Args:
      info: A machine information object from the Machine Manager.
      idle_machine: An instance of machine_manager.Machine representing the
          machine whose state has changed.
    """
    logging.debug('TRM._IdleMachineStateChanged id=%s status=%d', info.id,
                  info.status)

    # An idle machine should already be ready so can't transition to it.
    assert info.status != base_machine_provider.MachineStatus.ACQUIRED

    # If the machine is stopped or done, it can't be reused for another test
    # run, so forget about it.
    if info.status == base_machine_provider.MachineStatus.STOPPED:
      # No need to delete the idle_machine in this case.  The listener will
      # eventually be notified that that the machine has gone into the AVAILABLE
      # state, and this will trigger the case below.
      self._machine_manager.ReleaseMachine(idle_machine.id)
    elif info.status == base_machine_provider.MachineStatus.AVAILABLE:
      idle_machine.delete()
    else:
      logging.error('Invalid status change for idle machine_id=%s status=%d',
                    info.id, info.status)

  def _RunnerMachineStateChanged(self, info, runner):
    """Handles a status change for a machine running a test.

    Args:
      info: A machine information object from the Machine Manager.
      runner: An instance of TestRunner representing a running test whose
          machine state has changed.
    """
    logging.debug('TRM._RunnerMachineStateChanged id=%s status=%d runner=%s',
                  info.id, int(info.status), runner.GetName())

    # If the machine is switching to the ACQUIRED state, we will start the test
    # associated to it in the next call to AssignPendingRequests.
    if info.status != base_machine_provider.MachineStatus.ACQUIRED:
      # The machine running the test has failed.  Tell the user about it.
      r_str = ('Tests aborted. The machine (%s) running the test (%s) '
               'experienced a state change. Machine status: %d' %
               (info.id, str(runner.key()), int(info.status)))
      self._UpdateTestResult(runner, result_string=r_str)

      if info.status == base_machine_provider.MachineStatus.STOPPED:
        self._machine_manager.ReleaseMachine(info.id)
      elif info.status != base_machine_provider.MachineStatus.AVAILABLE:
        logging.error('Invalid status change for machine_id=%s status=%d',
                      info.id, int(info.status))

  def HandleTestResults(self, web_request):
    """Handle a response from the remote script.

    Args:
      web_request: a google.appengine.ext.webapp.Request object containing the
          results of a test run.  The URL will contain a k= CGI parameter
          holding the persistent key for the runner.
    """
    if not web_request:
      return

    runner = None

    key = web_request.get('k')
    if key:
      runner = TestRunner.get(db.Key(key))

    if runner:
      # Find the high level success/failure from the URL. We assume failure if
      # we can't find the success parameter in the request.
      success = web_request.get('s', 'False') == 'True'
      result_string = urllib.unquote_plus(web_request.get('r'))
      exit_codes = urllib.unquote_plus(web_request.get('x'))
      self._UpdateTestResult(runner, success, exit_codes, result_string, True)
    else:
      logging.error('No runner associated to web request, ignoring test result')

  def _UpdateTestResult(self, runner, success=False, exit_codes='',
                        result_string='Tests aborted', reuse_machine=False):
    """Update the runner with results of a test run.

    Args:
      runner: a TestRunner object pointing to the test request to which to
          send the results.  This argument must not be None.
      success: a boolean indicating whether the test run succeeded or not.
      exit_codes: a string containing the array of exit codes of the test run.
      result_string: a string containing the output of the test run.
      reuse_machine: a boolean indicating whether the machine that ran this
          test should be reused for another test.
    """
    if not runner:
      logging.error('runner argument must be given')
      return

    # If the machine id of this runner is DONE_MACHINE_ID, this means the
    # machine finished running this test. Don't try to process another
    # response for this runner object.
    if runner.machine_id == DONE_MACHINE_ID:
      logging.error('Got a second response for runner=%s, not good',
                    runner.GetName())
      return

    # Put the machine back onto the idle list if needed. In some cases,
    # such as when running a debugging test request server,
    # the machine id of this runner can be 0.  Since id 0 does not
    # refer to a valid assigned machine, then this id should not be put onto
    # the idle list.
    if reuse_machine and runner.machine_id != NO_MACHINE_ID:
      self._HandleIdleMachine(machine_id=runner.machine_id)

    runner.ran_successfully = success
    runner.exit_codes = exit_codes
    runner.machine_id = DONE_MACHINE_ID

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
      if result_url_parts[0] == 'http':
        # Send the result to the requested destination.
        try:
          # Encode the result string so that it's backwards-compatible with
          # ASCII. Without this, the call to "urllib.urlencode" below can
          # throw a UnicodeEncodeError if the result string contains non-ASCII
          # characters.
          encoded_result_string = runner.GetResultString().encode('utf-8')
          urllib2.urlopen(test_case.result_url,
                          urllib.urlencode((
                              ('n', runner.test_request.GetName()),
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
        'Test Request Name: ' + runner.test_request.GetName(),
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

  def GetAllMatchingTestRequests(self, test_case_name):
    """Returns a list of all Test Request that match the given test_case_name.

    Args:
      test_case_name: The test case name to search for.

    Returns:
      A list of all Test Requests that have |test_case_name| as their name.
    """
    matches = []
    for test_request in TestRequest.all():
      if test_request.GetName() == test_case_name:
        matches.append(test_request)

    return matches

  def ExecuteTestRequest(self, request_message):
    """Attempts to execute a test request.

    If machines are available for running any of the test's configurations,
    they will be started immediately.  The other test configurations will be
    queued up for testing at a later time potentially requesting new machines.

    Args:
      request_message: A string representing a test request.

    Raises:
      test_request_message.Error: If the request's message isn't valid.

    Returns:
      A dictionary containing the test_case_name field and an array of
      dictionaries containing the config_name and test_id_key fields.
    """
    logging.debug('TRM.ExecuteTestRequest msg=%s', request_message)

    # Will raise an exception on error.
    request = TestRequest(message=request_message)
    test_case = request.GetTestCase()  # Will raise on invalid request.
    request.put()

    test_keys = {'test_case_name': test_case.test_case_name,
                 'test_keys': []}

    for config in test_case.configurations:
      # TODO(user): deal with max_instances later!!!
      assert config.min_instances > 0
      for instance_index in range(config.min_instances):
        config.instance_index = instance_index
        config.num_instances = config.min_instances
        runner = self._QueueTestRequestConfig(request, config)

        self._TryAndRun(runner)

        # TODO(user): if the runner was unable to acquire a machine, check
        # that acquisition will be possible at somepoint, if not we have
        # an error

        test_keys['test_keys'].append({'config_name': config.config_name,
                                       'instance_index': instance_index,
                                       'num_instances': config.num_instances,
                                       'test_key': str(runner.key())})
    return test_keys

  def _TryAndRun(self, runner):
    """Attempt to find a machine to run the given runner.

    Args:
      runner: The test runner that we wish to run.
    """
    machine = self._FindMatchingIdleMachine(runner)
    if machine:
      logging.debug('Found machine id=%s to runner=%s',
                    machine.id, runner.GetName())
      self._ExecuteTestRunnerOnIdleMachine(runner, machine)
    else:
      # If we didn't already have a machine for this test and we couldn't
      # get an idle one, see if we can acquire a new one.
      self._RequestMachine(runner)
      # The machine may have been returned ready so we check to see if it
      # can already run the test.
      self._ExecuteTestRunnerIfPossible(runner)

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
                  request.GetName(), config.config_name)

    # Create a runner entity to record this request/config pair that needs
    # to be run. Use a machine id of NO_MACHINE_ID to indicate it has not
    # yet been executed. The runner will eventually be scheduled at a
    # later time.
    runner = TestRunner(test_request=request, config_name=config.config_name,
                        config_instance_index=config.instance_index,
                        num_config_instances=config.num_instances,
                        machine_id=NO_MACHINE_ID)
    runner.put()

    return runner

  def _AssignMachineToRunner(self, runner, machine_id):
    """Assign the given machine to the given runner.

    Args:
      runner: The runner acquring the machine.
      machine_id: The id of the machine being assigned.
    """
    machine_info = self._machine_manager.GetMachineInfo(machine_id)
    if machine_info:
      runner.machine_id = machine_id
      runner.hostname = machine_info.host
      runner.put()
    else:
      # We should never assign a machine that we can't get the MachineInfo for.
      logging.error('Assigned a machine with id %d but failed to get '
                    'MachineInfo. Aborting assignment.', machine_id)

  def _FindMatchingMachineInList(self, obj_list, config):
    """Find first object in obj_list whose machine matches the given config.

    Args:
      obj_list: a list of objects with a numeric attribute 'id' that
          corresponds to the id of a machine as returned by
          machine_manager.RequestMachine().
      config: a TestConfiguration object as specified for TRS requests.
          See go/gforce/test-request-format for details.

    Returns:
      An object from the list, or None if a matching machine is not found.
    """
    for obj in obj_list:
      info = self._machine_manager.GetMachineInfo(obj.id)
      (match, match_output) = dimensions.MatchDimensions(config.dimensions,
                                                         info.GetDimensions())
      logging.info(match_output)
      if match:
        return obj

    return None

  def _FindMatchingIdleMachine(self, runner):
    """Find an idle machine that is a match for the given runner.

    Args:
      runner: a TestRunner object for which we want to find a machine.

    Returns:
      An instance of IdleMachine, or None if a matching machine is not found.
    """
    # We can't return an idle machine if we need a virgin one
    if runner.RequiresVirginMachine():
      return None
    machines = IdleMachine.all()
    return self._FindMatchingMachineInList(machines, runner.GetConfiguration())

  def _FindMatchingAcquiredMachine(self, config):
    """Find an acquired machine that is a match for the given config.

    Args:
      config: a TestConfiguration object as specified for TRS requests.
          See go/gforce/test-request-format for details.

    Returns:
      An instance of machine_manager.Machine, or None if a matching machine
      is not found.
    """
    # Get an idle machine.
    machines = self._machine_manager.ListAcquiredMachines()
    return self._FindMatchingMachineInList(machines, config)

  def _RequestMachine(self, runner):
    """Request a machine to run the given runner.

    Args:
      runner: The runner we want to ensure a machine is available for.
    """
    config = runner.GetConfiguration()

    # If we can't acquire the machine right now, the caller will try again
    # later when assigning runners.  See AssignPendingRequests.
    machine_id = self._machine_manager.RequestMachine(
        None, config.dimensions)
    if machine_id is not DONE_MACHINE_ID:
      logging.info('New machine acquired with id=%s', machine_id)
      self._AssignMachineToRunner(runner, machine_id)

  def _ExecuteTestRunnerIfPossible(self, runner):
    """Execute a given runner on its specified machine if possible.

    Args:
      runner: A TestRunner object to execute.
    """
    if runner.machine_id == NO_MACHINE_ID:
      # No machine has been assigned yet.
      return

    assert runner.machine_id != DONE_MACHINE_ID

    info = self._machine_manager.GetMachineInfo(runner.machine_id)
    if info and info.status == base_machine_provider.MachineStatus.ACQUIRED:
      self._ExecuteTestRunner(runner)
    elif not info:
      logging.warning('Machine %s, returned no info', str(runner.machine_id))

  def _ExecuteTestRunnerOnIdleMachine(self, runner, idle_machine):
    """Execute a given runner on the specified idle machine.

    Args:
      runner: A TestRunner object to execute.
      idle_machine: An instance of IdleMachine representing the machine to run
          the test on.
    """
    logging.debug('TRM._ExecuteTestRunnerOnIdleMachine '
                  'runner=%s config=%s instance=%d num_instances=%d machine=%s',
                  runner.GetName(), runner.config_name,
                  int(runner.config_instance_index),
                  int(runner.num_config_instances),
                  str(idle_machine.id))
    config = runner.GetConfiguration()
    assert runner.config_instance_index < runner.num_config_instances
    assert (runner.num_config_instances >= config.min_instances and
            runner.num_config_instances <= config.max_instances)

    # TODO(user): make the next two lines atomic?  If we crash in the
    # middle, the data store will be inconsistent, although this will be
    # checked for and corrected then next time the test manager starts up.
    # There is a runner.put in _AssignMachineToRunner.
    self._AssignMachineToRunner(runner, idle_machine.id)
    idle_machine.delete()
    self._ExecuteTestRunner(runner)

  def _ExecuteTestRunner(self, runner):
    """Execute a given runner on it's assigned machine.

    Args:
      runner: A TestRunner object to execute.
    """

    # TODO(user): At the moment, we only run a single instance of the server
    # and thus everything is serialized. So for now, there is no contention with
    # concurrent requests. But eventually we may want to have multiple schedule
    # tasks or tasks queues doing the assignments, in which case this would
    # become an issue.
    assert runner.started is None
    runner.started = datetime.datetime.now()
    runner.put()
    info = self._machine_manager.GetMachineInfo(runner.machine_id)

    test_case = runner.test_request.GetTestCase()
    if test_case.admin:
      port = 7400
    else:
      port = 7399

    remote_runner = remote_test_runner.RemoteTestRunner(
        server_url='http://%s:%d' % (info.host, port))

    if not remote_runner.EnsureValidServer():
      logging.warning('Unable to connect to RemoteTestRunner on %s. '
                      'Marking machine as STOPPED. Test will acquire '
                      'another machine and try again.', info.host)
      # Mark the machine as STOPPED, since we can't communicate with it.
      info.status = base_machine_provider.MachineStatus.STOPPED
      info.put()

      # Reset the test runner so that it can run on another machine.
      runner.machine_id = NO_MACHINE_ID
      runner.started = None
      runner.put()
      self._TryAndRun(runner)
      return

    remote_python26_path = remote_runner.RemotePython26Path()

    # We need to run the local_test_runner script from the remote root because
    # we can't easily set the python path of the remote machine, so we make sure
    # the root is in the python path by running the script from there.
    root = os.path.join(os.path.dirname(__file__), '..')
    local_script = os.path.join(root, _TEST_RUNNER_DIR, _TEST_RUNNER_SCRIPT)
    file_pairs_to_upload = [(local_script, _TEST_RUNNER_SCRIPT)]

    downloader_file = os.path.join(_TEST_RUNNER_DIR, 'downloader.py')
    trm_file = os.path.join(_COMMON_DIR, 'test_request_message.py')
    test_runner_init = os.path.join(_TEST_RUNNER_DIR, '__init__.py')
    common_init = os.path.join(_COMMON_DIR, '__init__.py')

    files_to_upload = [downloader_file, trm_file, test_runner_init, common_init]
    file_pairs_to_upload.extend((os.path.join(root, file), file)
                                for file in files_to_upload)

    test_run = self._BuildTestRun(runner)
    command_to_execute = [remote_python26_path,
                          r'%s/%s' % (test_run.working_dir,
                                      _TEST_RUNNER_SCRIPT),
                          '-f', r'%s/%s' % (test_run.working_dir,
                                            _TEST_RUN_SWARM_FILE_NAME)]
    if test_case.verbose:
      command_to_execute.append('-v')
    commands_to_execute = [command_to_execute]

    # TODO(user): Find a way to make the working dir platform independent.
    remote_runner.SetRemoteRoot(test_run.working_dir)
    remote_runner.SetTextToUpload([(_TEST_RUN_SWARM_FILE_NAME, str(test_run))])
    remote_runner.SetFilePairsToUpload(file_pairs_to_upload)
    remote_runner.SetCommandsToExecute(commands_to_execute)

    succeeded = remote_runner.UploadFiles()
    if succeeded:
      run_results = remote_runner.RunCommands()
      logging.debug('RunCommands returned: %s', run_results)
      if run_results:
        assert len(run_results) == len(commands_to_execute)
        for result in run_results:
          if result == -1:
            logging.error('Failed to run all commands.')
            succeeded = False
            break
      else:
        succeeded = False
    else:
      logging.error('Failed to upload files.')

    if not succeeded:
      self._UpdateTestResult(runner,
                             result_string=('Tests aborted. Upload to the '
                                            'remote server failed.'))

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

  def AssignPendingRequests(self):
    """Assign test requests to available machines.

    Raises:
      test_request_message.Error: If the request's message isn't valid.
    """
    logging.debug('TRM.AssignPendingRequests')

    # Assign test runners from earliest to latest.
    # We use a format argument for None, because putting None in the string
    # doesn't work.
    query = TestRunner.gql('WHERE started = :1 ORDER BY created', None)
    for runner in query:
      if runner.machine_id != NO_MACHINE_ID:
        # TODO(user): I would like to filter this in the query above, but it
        # asks me to use the != property, i.e., machine_id to be used for ORDER.
        if runner.machine_id != DONE_MACHINE_ID:
          # We already have a machine for this test run, so check if it's
          # ready to execute.
          self._ExecuteTestRunnerIfPossible(runner)
      else:
        self._TryAndRun(runner)

  def _HandleIdleMachine(self, machine_id=None, info=None):
    """Given a newly idle machine, attempts to use it before marking it as idle.

    Args:
      machine_id: The id of the idle machine. This must be valid if info
          is None.
      info: The info of the idle machine. This must be valid if id is None.
    """
    if not id and not info:
      logging.error('Attempted to handle an idle machine with no id or info')
      return

    if not info:
      info = self._machine_manager.GetMachineInfo(machine_id)

    # Assign test runners from earliest to latest.
    # We use a format argument for None, because putting None in the string
    # doesn't work.
    query = TestRunner.gql('WHERE started = :1 ORDER BY created', None)
    for runner in query:
      (match, matching_output) = dimensions.MatchDimensions(
          runner.GetConfiguration().dimensions, info.GetDimensions())
      logging.info(matching_output)
      if match:
        self._AssignMachineToRunner(runner, info.id)
        self._ExecuteTestRunner(runner)
        return

    logging.debug('Machine id=%d put back on idle list', info.id)
    idle_machine = IdleMachine(id=info.id)
    idle_machine.put()

  def _GetCurrentTime(self):
    """Gets the current time.

    This function is defined so that it can be mocked out in tests.

    Returns:
      The current time as a datetime.datetime object.
    """
    return datetime.datetime.now()

  def GetResults(self, runner):
    """Gets the results from the given test run.

    Args:
      runner: The instance of TestRunner to get the results from.

    Returns:
      A dictionary of the results.
    """

    return {'exit_codes': runner.exit_codes,
            'hostname': runner.hostname,
            'output': runner.GetResultString()}

  def AbortStaleRunners(self):
    """Abort any runners that have been running longer than expected."""
    logging.debug('TRM.AbortStaleRunners starting')
    now = self._GetCurrentTime()

    query = TestRunner.gql('WHERE machine_id != :1 AND machine_id != :2',
                           NO_MACHINE_ID, DONE_MACHINE_ID)
    for runner in query:
      # Determine how long the runner should have taken.
      runner_timeout = runner.GetTimeout() + _TIMEOUT_FUDGE_FACTOR

      # Determine if too much time has expired since the runner began.
      delta = datetime.timedelta(seconds=runner_timeout)
      if runner.started and now > runner.started + delta:
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
    machine_id = runner.machine_id
    r_str = 'Tests aborted. AbortRunner() called. Reason: %s' % reason
    self._UpdateTestResult(runner, result_string=r_str)

    # Consider the runner's machine dead.  Release it.
    if machine_id not in [NO_MACHINE_ID, DONE_MACHINE_ID]:
      self._machine_manager.ReleaseMachine(machine_id)

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
    except db.BadKeyError:
      # We don't need to take any special action with bad keys.
      pass

    if not test_runner:
      logging.debug('No matching Test Runner found for key, %s', key)
      return False

    test_request = test_runner.test_request

    test_runner.delete()
    test_request.RunnerDeleted()

    return True

  def ExecuteRegisterRequest(self, attributes):
    """Attempts to match the requesting machine with an existing TestRunner.

    If the machine is matched with a request, the machine is told what to do.
    Else, the machine is told to register at a later time.

    Args:
      attributes: A dictionary representing the attributes of the machine
      registering itself.

    Raises:
      test_request_message.Error: If the request format/attributes aren't valid.

    Returns:
      A dictionary containing the commands the machine needs to execute.
    """
    # Validate and fix machine attributes. Will throw exception on errors.
    attribs = self.ValidateAndFixAttributes(attributes)

    assigned_runner = False

    # Try assigning machine to a runner 10 times before we give up.
    # TODO(user): Tune this parameter somehow.
    for _ in range(10):
      # Try to find a matching test runner for the machine.
      runner = self._FindMatchingRunner(attribs)
      if runner:
        # Will atomically try to assign the machine to the runner. This could
        # fail due to a race condition on the runner. If so, we loop back to
        # finding a runner.
        if self._AssignRunnerToMachine(attribs, runner):
          # TODO(user): since this change list only has additions not
          # modifications, we comment out the following line which actually runs
          # the test. This will be fixed in upcoming changelists.
          #response = self._ExecuteTestRunner(runner)
          assigned_runner = True
          break
      # We found no runner, no use in re-trying so just break out of the loop.
      else:
        break

    response = {'id': attribs['id']}
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
      elif  attrib == 'id':
        # Make sure the attribute value has proper type.
        try:
          value = uuid.UUID(value)
        except (ValueError, AttributeError):
          raise test_request_message.Error('Invalid attrib type for id')
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

    if 'id' not in attributes:
      # Try to create a unique ID for the machine if it doesn't have one. The
      # machine will send us back this ID in subsequent tries.
      # Even if the ID isn't unique, it will not create a significant problem.
      # We use machine IDs to provide users some status report on machines.
      # We loop so that in the extreme rare case that we actually generate a
      # reserved ID, we retry.
      # Note: the sun is more likely to burn out before we need an extra
      # iteration!
      i = 0
      attributes['id'] = NO_MACHINE_ID
      while (i < 10 and attributes['id'] in [NO_MACHINE_ID, DONE_MACHINE_ID]):
        attributes['id'] = str(uuid.uuid4())
        i += 1

      assert (i < 10 and attributes['id'] not in
              [NO_MACHINE_ID, DONE_MACHINE_ID])

    # Make sure attributes now has a try_count field.
    if not 'try_count' in attributes:
      attributes['try_count'] = 0

    return attributes

  def _ComputeComebackValue(self, try_count):
    """Computes when the slave machine should return based on given try_count..

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

  def _FindMatchingRunner(self, attribs):
    """Find oldest TestRunner who hasn't already been assigned a machine.

    Args:
      attribs: the attributes defining the machine.

    Returns:
      A TestRunner object, or None if a matching runner is not found.
    """

    # TODO(user): limit the number of test runners checked to avoid querying
    # all the tasks all the time.

    # Assign test runners from earliest to latest.
    # We use a format argument for None, because putting None in the string
    # doesn't work.
    query = TestRunner.gql('WHERE started = :1 AND machine_id = :2 '
                           'ORDER BY created', None, NO_MACHINE_ID)
    for runner in query:
      runner_dimensions = runner.GetConfiguration().dimensions
      (match, output) = dimensions.MatchDimensions(runner_dimensions,
                                                   attribs['dimensions'])
      logging.info(output)
      if match:
        logging.info('matched runner %s: '%runner.GetName()
                     + str(runner_dimensions) + ' to machine: '
                     + str(attribs['dimensions']))
        return runner

    return None

  def _AssignRunnerToMachine(self, attribs, runner):
    """Will try to atomically assign runner to machine defined by attributes.

    This function is thread safe and can be called simultaneously on the same
    runner. It will ensure all but one concurrent requests will fail.

    Args:
      attribs: the attributes defining the machine.
      runner: test runner object to assign the machine to.

    Returns:
      True is succeeded, False otherwise.
    """

    #TODO(user): use a transaction to make atomic!!!
    runner.machine_id = str(attribs['id'])
    runner.put()

    return True
