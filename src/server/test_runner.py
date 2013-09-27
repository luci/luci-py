# Copyright 2013 Google Inc. All Rights Reserved.

"""Test Runner.

A test runner represents a given configuration of a given test request running
on a given machine.
"""



import datetime
import logging
import urlparse

from google.appengine.api import datastore_errors
from google.appengine.api import mail
from google.appengine.datastore import datastore_query
from google.appengine.ext import blobstore
from google.appengine.ext import ndb

from common import blobstore_helper
from common import test_request_message
from common import url_helper
from stats import machine_stats
from stats import runner_stats

# The maximum number of times to retry a runner that has failed for a swarm
# related reasons (like the machine timing out).
MAX_AUTOMATIC_RETRIES = 3

# Number of days to keep old runners around for.
SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS = 14

# The amount of time, in minutes, that a runner must wait before it is
# considered to be hanging (this is usually a sign that the machines that can
# run this test are broken and not communicating with swarm).
TIME_BEFORE_RUNNER_HANGING_IN_MINS = 60

# A dictionary of all the acceptable parameters to sort TestRunners by, with
# the value being the readable value for that key that users should see.
ACCEPTABLE_SORTS = {
    'created': 'Created',
    'ended': 'Ended',
    'name': 'Name',
    'started': 'Started',
    }


def _GetCurrentTime():
  """Gets the current time.

  This function is defined so that it can be easily mocked out in tests.

  Returns:
    The current time as a datetime.datetime object.
  """
  return datetime.datetime.now()


class TestRunner(ndb.Model):

  # The test being run.
  @property
  def request(self):
    return self.key.parent()

  # The name of the request's configuration being tested.
  config_name = ndb.StringProperty()

  # The hash of the configuration. Required in order to match machines.
  config_hash = ndb.StringProperty(required=True)

  # The full name of this test runner.
  name = ndb.StringProperty(required=True)

  # The 0 based instance index of the request's configuration being tested.
  config_instance_index = ndb.IntegerProperty(indexed=False)

  # The number of instances running on the same configuration as ours.
  num_config_instances = ndb.IntegerProperty(indexed=False)

  # The machine that is running or ran the test. This attribute is only valid
  # once a machine has been assigned to this runner.
  # TODO(user): Investigate making this a reference to the MachineAssignment.
  # It would require ensuring the MachineAssignment is created when this value
  # is set.
  machine_id = ndb.StringProperty()

  # A list of the old machines ids that this runner previous ran on. This is
  # useful for knowing when a ping is from an older machine.
  old_machine_ids = ndb.StringProperty(repeated=True, indexed=False)

  # Used to indicate if the runner has finished, either successfully or not.
  done = ndb.BooleanProperty(default=False)

  # The time at which this runner was created.  The runner may not have
  # started executing yet.
  created = ndb.DateTimeProperty(auto_now_add=True)

  # The priority of the runner. A lower number is higher priority.
  priority = ndb.IntegerProperty(default=10)

  # The priority and date added together to allow queries to order the results
  # by this field to allow sorting by priority first, and then date.
  @ndb.ComputedProperty
  def priority_and_created(self):  # pylint: disable=g-bad-name
    # The first time a runner is stored, it computes this property before the
    # created value has been created, so we need to get a time to use.
    # datetime.datetime.now() will give the runner a slightly earlier time, but
    # all runners have the same change, so the order of runners should be
    # preserved.
    created_str = str(self.created if self.created else datetime.datetime.now())

    priority_str = (
        ('%%0%dd' % len(str(test_request_message.MAX_PRIORITY_VALUE))) %
        self.priority)

    return priority_str + created_str

  # The time at which this runner was executed on a remote machine.  If the
  # runner isn't executing or ended, then the value is None and we use the
  # fact that it is None to identify if a test was started or not.
  started = ndb.DateTimeProperty()

  # The number of times that this runner has been retried for swarm failures
  # (such as the machine that is running it timing out).
  automatic_retry_count = ndb.IntegerProperty(default=0)

  # The last time a ping was recieved from the remote machine currently
  # running the test. This is used to determine if a machine has become
  # unresponse, which causes the runner to become aborted.
  ping = ndb.DateTimeProperty()

  # The time at which this runner ended.  This attribute is valid only when
  # the runner has ended (i.e. done == True). Until then, the value is
  # unspecified.
  ended = ndb.DateTimeProperty()

  # True if the test run finished and succeeded.  This attribute is valid only
  # when the runner has ended. Until then, the value is unspecified.
  ran_successfully = ndb.BooleanProperty()

  # The stringized array of exit_codes for each actions of the test.
  exit_codes = ndb.StringProperty(indexed=False)

  # Contains any swarm specific errors that occured that caused the result
  # string to not get correct setup with the runner output (i.e. the runner
  # timed out so there was no data).
  errors = ndb.StringProperty(indexed=False)

  # The blobstore reference to the full output of the test.  This key valid only
  # when the runner has ended (i.e. done == True). Until then, it is None.
  result_string_reference = ndb.BlobKeyProperty()

  # The dimension string for this runner.
  dimensions = ndb.StringProperty(required=True)

  @classmethod
  def _pre_delete_hook(cls, key):  # pylint: disable=g-bad-name
    """Delete the associated blob before deleting the runner.

    Args:
      key: The key of the TestRunner to be deleted.
    """
    runner = key.get()
    # We delete the blob referenced by this model because no one
    # else will ever care about it or try to reference it, so we
    # are just cleaning up the blobstore.
    if runner.result_string_reference:
      blobstore.delete_async(runner.result_string_reference)

  def _pre_put_hook(self):  # pylint: disable=g-bad-name
    """Ensure that all runner values are properly set."""
    if not self.dimensions:
      self.dimensions = test_request_message.Stringize(
          self.GetConfiguration().dimensions)

    if not self.name:
      self.name = '%s:%s' % (self.request.get().name, self.config_name)

  def GetConfiguration(self):
    """Gets the configuration associated with this runner.

    Returns:
      A configuration dictionary for this runner.
    """
    config = self.request.get().GetConfiguration(self.config_name)
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

    return blobstore_helper.GetBlobstore(self.result_string_reference)

  def GetWaitTime(self):
    """Get the number of seconds between creation and execution start.

    Returns:
      The number of seconds the runner waited, if the runner hasn't started yet
      return 0.
    """
    if not self.started:
      return 0

    return (self.started - self.created).total_seconds()

  def GetMessage(self):
    """Get the message string representing this test runner.

    Returns:
      A string represent this test runner request.
    """
    message = ['Test Request Message:']
    message.append(self.request.get().message)

    message.append('')
    message.append('Test Runner Message:')
    message.append('Configuration Name: ' + self.config_name)
    message.append('Configuration Instance Index: %d / %d' %
                   (self.config_instance_index, self.num_config_instances))

    return '\n'.join(message)

  def ClearRunnerRun(self):
    """Clear the status of any previous run from this runner."""
    self.started = None
    self.machine_id = None
    self.old_machine_ids = []
    self.done = False
    self.started = None
    self.automatic_retry_count = 0
    self.ping = None
    self.ended = None
    self.ran_successfully = False
    self.exit_codes = None
    self.errors = None
    if self.result_string_reference:
      blobstore.delete_async(self.result_string_reference)
      self.result_string_reference = None

    self.put()

  def UpdateTestResult(self, machine_id, success=False, exit_codes='',
                       result_blob_key=None, errors=None, overwrite=False):
    """Update the runner with results of a test run.

    Args:
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
    if self.machine_id != machine_id:
      if machine_id not in self.old_machine_ids:
        logging.warning('The machine id of the runner, %s, doesn\'t match the '
                        'machine id given, %s', self.machine_id, machine_id)
        # The new results won't get stored so delete them.
        if result_blob_key:
          blobstore.delete_async(result_blob_key)
        return False
      # Update the old and active machines ids.
      logging.info('Received result from old machine, making it current '
                   'machine and storing results')
      if self.machine_id:
        self.old_machine_ids.append(self.machine_id)
      self.machine_id = machine_id
      self.old_machine_ids.remove(machine_id)

    # If the runnner is marked as done, don't try to process another
    # response for it, unless overwrite is enable.
    if self.done and not overwrite:
      stored_results = self.GetResultString()
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
                      'not good', self.name, self.key.urlsafe())
        logging.debug('Dropped result string was:\n%s',
                      new_results or errors)
        return False

    # Clear any old result strings that are stored if we are overwriting.
    if overwrite:
      if self.result_string_reference:
        blobstore.delete(self.result_string_reference)
        self.result_string_reference = None
      self.errors = None

    self.ran_successfully = success
    self.exit_codes = exit_codes
    self.done = True
    self.ended = datetime.datetime.now()

    if result_blob_key:
      assert self.result_string_reference is None, (
          'There is a reference stored, overwriting it would leak the old '
          'element.')
      self.result_string_reference = result_blob_key
    else:
      self.errors = errors

    self.put()
    logging.info('Successfully updated the results of runner %s.',
                 self.key.urlsafe())

    # If the test didn't run successfully, we send an email if one was
    # requested via the test request.
    test_case = self.request.get().GetTestCase()
    if not self.ran_successfully and test_case.failure_email:
      # TODO(user): provide better info for failure. E.g., if we don't have a
      # web_request.body, we should have info like: Failed to upload files.
      self._EmailTestResults(test_case.failure_email)

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
        data = {'n': self.request.get().name,
                'c': self.config_name,
                'i': self.config_instance_index,
                'm': self.num_config_instances,
                'x': self.exit_codes,
                's': self.ran_successfully,
                'r': self.GetResultString()}
        if not url_helper.UrlOpen(test_case.result_url, data=data):
          logging.exception('Could not send results back to sender at %s',
                            test_case.result_url)
          update_successful = False
      elif result_url_parts[0] == 'mailto':
        # Only send an email if we didn't send a failure email earlier to
        # prevent repeats.
        if self.ran_successfully or not test_case.failure_email:
          self._EmailTestResults(result_url_parts[1])
      else:
        logging.exception('Unknown url given as result url, %s',
                          test_case.result_url)
        update_successful = False

    runner_stats.RecordRunnerStats(self)

    if (test_case.store_result == 'none' or
        (test_case.store_result == 'fail' and self.ran_successfully)):
      DeleteRunner(self)

    return update_successful

  def _EmailTestResults(self, send_to):
    """Emails the test result.

    Args:
      send_to: the email address to send the result to. This must be a valid
          email address.
    """
    if not mail.is_email_valid(send_to):
      logging.error('Invalid email passed to result_url, %s', send_to)
      return

    if self.ran_successfully:
      subject = '%s succeeded.' % self.name
    else:
      subject = '%s failed.' % self.name

    message_body_parts = [
        'Test Request Name: ' + self.request.get().name,
        'Configuration Name: ' + self.config_name,
        'Configuration Instance Index: ' + str(self.config_instance_index),
        'Number of Configurations: ' + str(self.num_config_instances),
        'Exit Code: ' + str(self.exit_codes),
        'Success: ' + str(self.ran_successfully),
        'Result Output: ' + self.GetResultString()]
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


@ndb.transactional
def AtomicAssignID(key, machine_id):
  """Function to be run by db.run_in_transaction().

  Args:
    key: key of the test runner object to assign the machine_id to.
    machine_id: machine_id of the machine we're trying to assign.

  Raises:
    TxRunnerAlreadyAssignedError: If runner has already been assigned a machine.
  """
  runner = key.get()
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
  try:
    atomic_assign(runner.key, machine_id)
    return True
  except (TxRunnerAlreadyAssignedError,
          datastore_errors.TransactionFailedError):
    # Failed to assign the runner because it is probably already assigned.
    pass
  except (datastore_errors.Timeout, datastore_errors.InternalError):
    # These exceptions do NOT ensure the operation is done. Based on the
    # Discussion above, we assume it hasn't been assigned.
    logging.exception('Un-determined fate for runner=%s', runner.name)

  return False


def ShouldAutomaticallyRetryRunner(runner):
  """Decide if the given runner should be automatically retried.

  Args:
    runner: The runner to examine.

  Returns:
    True if the runner should be automatically retried.
  """
  return runner.automatic_retry_count < MAX_AUTOMATIC_RETRIES


def AutomaticallyRetryRunner(runner):
  """Attempt to automaticlly retry runner.

  Args:
    runner: The runner to retry.
  """
  # Record the stats from the attempted run before we clear them.
  runner_stats.RecordRunnerStats(runner)

  # Remember and keep the old machines and the automatic_retry_count since
  # this isn't a full runner restart.
  if runner.machine_id:
    runner.old_machine_ids.append(runner.machine_id)
  old_machine_ids = runner.old_machine_ids
  automatic_retry_count = runner.automatic_retry_count + 1

  runner.ClearRunnerRun()

  runner.automatic_retry_count = automatic_retry_count
  runner.old_machine_ids = old_machine_ids

  runner.put()


def PingRunner(key, machine_id):
  """Pings the runner, if the key is valid.

  Args:
    key: The key of the runner to ping.
    machine_id: The machine id of the machine pinging.

  Returns:
    True if the runner is successfully pinged.
  """
  runner = GetRunnerFromUrlSafeKey(key)
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
        runner = GetRunnerFromUrlSafeKey(key)
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


def GetTestRunners(sort_by='machine_id', ascending=True, limit=None,
                   offset=None, sort_by_first=None):
  """Get the list of the test runners.

  Args:
    sort_by: The string of the attribute to sort the test runners by.
    ascending: True if the runner should be sorted in ascending order.
    limit: The machine number of test runners to return.
    offset: The offset from the complete set of sorted elements and the returned
        list.
    sort_by_first: An optional string to specify a field that we should sort by
        first, this is required if the a query will contain an inequality
        filter.

  Returns:
    An query of test runners in the given range.
  """
  query = TestRunner.query(default_options=ndb.QueryOptions(limit=limit,
                                                            offset=offset))

  # If we receive an invalid sort_by parameter, just default to machine_id.
  if sort_by not in ACCEPTABLE_SORTS:
    sort_by = 'machine_id'

  if sort_by_first and sort_by_first != sort_by:
    query = query.order(datastore_query.PropertyOrder(
        'started',
        datastore_query.PropertyOrder.ASCENDING))

  direction = (datastore_query.PropertyOrder.ASCENDING if ascending else
               datastore_query.PropertyOrder.DESCENDING)
  query = query.order(datastore_query.PropertyOrder(sort_by, direction))

  return query


def ApplyFilters(query, status='', show_successfully_completed=True,
                 test_name='', machine_id=''):
  """Applies the required filters to the given query.

  Args:
    query: The query to filter.
    status: A string representing which runner states should be included.
        Options are (all, pending, running, done). Unknown values are treated as
        all.
    show_successfully_completed: True if runners that successfully completed
        should be shown.
    test_name: The test name to filter for.
    machine_id: The machine id to filter for.

  Returns:
    A query equivalent to the one given, with all the required filters applied.
  """
  # If the status isn't one of these options, then apply no filter.
  # pylint: disable=g-explicit-bool-comparison, g-equals-none
  if status == 'pending':
    query = query.filter(TestRunner.started == None,
                         TestRunner.done == False)
  elif status == 'running':
    query = query.filter(TestRunner.started != None,
                         TestRunner.done == False)
  elif status == 'done':
    query = query.filter(TestRunner.done == True)

  if not show_successfully_completed:
    query = query.filter(ndb.OR(TestRunner.done == False,
                                TestRunner.ran_successfully == False))
  # pylint: enable=g-explicit-bool-comparison, g-equals-none

  if test_name:
    query = query.filter(test_name == TestRunner.name)

  if machine_id:
    query = query.filter(machine_id == TestRunner.machine_id)

  return query


def GetHangingRunners():
  """Gets all the currently hanging runners.

  Returns:
    A list of all the hanging runners.
  """
  cutoff_time = datetime.datetime.now() - datetime.timedelta(
      minutes=TIME_BEFORE_RUNNER_HANGING_IN_MINS)

  hanging_runners = TestRunner.gql('WHERE started = :1 AND '
                                   'automatic_retry_count = :2 AND '
                                   'created < :3 AND done = :4', None, 0,
                                   cutoff_time, False)
  return [hanging_runner for hanging_runner in hanging_runners]


def GetRunnerFromUrlSafeKey(url_safe_key):
  """Returns the runner specified by the given key string.

  Args:
    url_safe_key: The key string of the runner to return.

  Returns:
    The runner with the given key, otherwise None if the key doesn't refer
    to a valid runner.
  """
  try:
    key = ndb.Key(urlsafe=url_safe_key)
    if key.kind() == 'TestRunner':
      return key.get()
    return None
  except Exception:  # pylint: disable=broad-except
    # All exceptions must be caught because some exceptions can only be caught
    # this way. See this bug report for more details:
    # https://code.google.com/p/appengine-ndb-experiment/issues/detail?id=143
    return None


def GetRunnerResults(key):
  """Returns the results of the runner specified by key.

  Args:
    key: TestRunner key representing the runner.

  Returns:
    A dictionary of the runner's results, or None if the runner not found.
  """
  runner = GetRunnerFromUrlSafeKey(key)
  if not runner:
    return None

  return {'exit_codes': runner.exit_codes,
          'machine_id': runner.machine_id,
          'machine_tag': machine_stats.GetMachineTag(runner.machine_id),
          'config_instance_index': runner.config_instance_index,
          'num_config_instances': runner.num_config_instances,
          'output': runner.GetResultString()}


def DeleteRunnerFromKey(key):
  """Delete the runner that the given key refers to.

  Args:
    key: The key corresponding to the runner to be deleted.

  Returns:
    True if a matching TestRunner was found and deleted.
  """
  runner = GetRunnerFromUrlSafeKey(key)

  if not runner:
    logging.debug('No matching Test Runner found for key, %s', key)
    return False

  DeleteRunner(runner)

  return True


def DeleteRunner(runner):
  """Delete the given runner.

  Args:
    runner: The runner to delete.
  """
  request = runner.request
  runner.key.delete()
  request.get().DeleteIfNoMoreRunners()


def DeleteOldRunners():
  """Clean up all runners that are older than a certain age and done.

  Returns:
    The rpc for the async delete call (mainly meant for tests).
  """
  logging.debug('DeleteOldRunners starting')

  old_cutoff = (
      _GetCurrentTime() -
      datetime.timedelta(days=SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS))

  # '!= None' must be used instead of 'is not None' because these arguments
  # become part of a GQL query, where 'is not None' is invalid syntax.
  old_runner_query = TestRunner.query(
      TestRunner.ended != None,  # pylint: disable-msg=g-equals-none
      TestRunner.ended < old_cutoff,
      default_options=ndb.QueryOptions(keys_only=True))

  rpc = ndb.delete_multi_async(old_runner_query)

  logging.debug('DeleteOldRunners done')

  return rpc


def DeleteOrphanedBlobs():
  """Remove all the orphaned blobs."""
  logging.debug('DeleteOrphanedBlobs starting')

  blobstore_query = blobstore.BlobInfo.all().order('creation')
  blobs_deleted = 0

  for blob in blobstore_query:
    if not TestRunner.gql('WHERE result_string_reference = :1',
                          blob.key()).count(limit=1):
      blobstore.delete_async(blob.key())
      blobs_deleted += 1

  logging.debug('DeleteOrphanedBlobs done')

  return blobs_deleted


class TxRunnerAlreadyAssignedError(Exception):
  """Simple exception class signaling a transaction fail."""
  pass
