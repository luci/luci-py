#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for TestRunner class."""



import datetime
import hashlib
import logging
import unittest


from google.appengine.ext import db
from google.appengine.ext import testbed
from common import blobstore_helper
from common import test_request_message
from server import test_request
from server import test_runner
from stats import runner_stats
from third_party.mox import mox

MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789',
               '34567890-34567890-34567890-34567890',
               '87654321-87654321-87654321-87654321']


class TestRunnerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

    self.config_name = 'c1'

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  # TODO(user): This _GetRequestMessage and _CreateRunner is used by most
  # tests, create a helper function that all the tests can use instead of
  # rolling their own.
  def _GetRequestMessage(self):
    test_case = test_request_message.TestCase()
    test_case.test_case_name = 'test_case'
    test_case.configurations = [
        test_request_message.TestConfiguration(
            config_name=self.config_name, os='win-xp',)]
    return test_request_message.Stringize(test_case, json_readable=True)

  def _CreateRunner(self, config_instance_index=0, num_instances=1):
    """Creates a new runner.

    Args:
      config_instance_index: The config_instance_index of the runner.
      num_instances: The num_config_instances for the runner.

    Returns:
      The newly created runner.
    """
    request = test_request.TestRequest(message=self._GetRequestMessage())
    request.put()

    runner = test_runner.TestRunner(
        request=request,
        config_hash=hashlib.sha1().hexdigest(),
        config_name=self.config_name,
        config_instance_index=config_instance_index,
        num_config_instances=num_instances)
    runner.put()

    return runner

  def testGetResultStringFromEmptyRunner(self):
    runner = self._CreateRunner()

    # Since the request hasn't been run yet there should be just be an
    # empty string for the result string.
    self.assertEqual('', runner.GetResultString())

  # Test with an exception.
  def testAssignRunnerToMachineTxError(self):
    def _RaiseError(key, machine_id):  # pylint: disable=unused-argument
      raise test_runner.TxRunnerAlreadyAssignedError

    runner = self._CreateRunner()

    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test with another exception.
  def testAssignRunnerToMachineTimeout(self):
    def _RaiseError(key, machine_id):  # pylint: disable=unused-argument
      raise db.Timeout

    runner = self._CreateRunner()

    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test with yet another exception.
  def testAssignRunnerToMachineTransactionFailedError(self):
    def _RaiseError(key, machine_id):  # pylint: disable=unused-argument
      raise db.TransactionFailedError

    runner = self._CreateRunner()

    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Same as above, but the transaction stops throwing exception
  # before the server gives up. So everything should be fine.
  def testAssignRunnerToMachineTransactionTempFailedError(self):
    def _StaticVar(varname, value):
      def _Decorate(func):
        setattr(func, varname, value)
        return func
      return _Decorate

    @_StaticVar('error_count', test_runner.MAX_TRANSACTION_RETRY_COUNT)
    def _RaiseTempError(key, machine_id):  # pylint: disable=unused-argument
      _RaiseTempError.error_count -= 1
      if _RaiseTempError.error_count:
        raise db.TransactionFailedError
      else:
        return True

    runner = self._CreateRunner()

    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseTempError))

  # Test with one more exception.
  def testAssignRunnerToMachineInternalError(self):
    def _RaiseError(key, machine_id):  # pylint: disable=unused-argument
      raise db.InternalError

    runner = self._CreateRunner()

    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test proper behavior of AtomicAssignID.
  def testAtomicAssignID(self):
    runners = []

    # Create some pending runners.
    for _ in range(0, 2):
      runners.append(self._CreateRunner())

    # Make sure it assigns machine_id correctly.
    test_runner.AtomicAssignID(runners[0].key(), MACHINE_IDS[0])
    runner = test_runner.TestRunner.gql(
        'WHERE machine_id = :1', MACHINE_IDS[0]).get()
    self.assertEqual(runner.machine_id, MACHINE_IDS[0])

    # Make sure it didn't touch the other machine.
    self.assertEqual(
        1, test_runner.TestRunner.gql('WHERE started = :1', None).count())

    # Try to reassign runner and raise exception.
    self.assertRaises(test_runner.TxRunnerAlreadyAssignedError,
                      test_runner.AtomicAssignID,
                      runners[0].key(), MACHINE_IDS[1])

  # Test with an exception.
  def testAssignRunnerToMachineFull(self):
    runner = self._CreateRunner()

    # First assignment should work correctly.
    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

    # Next assignment should fail.
    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

    runner.started = None
    runner.put()
    # This assignment should now work correctly.
    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

  # Test the case where the runner is deleted before the tx is done.
  def testAssignDeletedRunnerToMachine(self):
    runner = self._CreateRunner()
    runner.delete()

    # Assignment should fail without an exception.
    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

  def testShouldAutomaticallyRetryRunner(self):
    runner = self._CreateRunner()
    runner.automatic_retry_count = 0
    runner.put()
    self.assertTrue(test_runner.ShouldAutomaticallyRetryRunner(runner))

    runner.automatic_retry_count = test_runner.MAX_AUTOMATIC_RETRIES
    self.assertFalse(test_runner.ShouldAutomaticallyRetryRunner(runner))

  # There were observed cases where machine_id was somehow set to None before
  # calling AutomaticallyRetryRunner. It is unclear how this is possible, so
  # handle this case gracefully.
  def testAutomaticallyRetryMachineIdNone(self):
    runner = self._CreateRunner()
    runner.automatic_retry_count = 0
    runner.machine_id = None
    runner.put()
    self.assertTrue(test_runner.AutomaticallyRetryRunner(runner))

  def testRecordRunnerStatsAfterAutoRetry(self):
    runner = self._CreateRunner()
    runner.machine_id = MACHINE_IDS[0]
    runner.put()

    test_runner.AutomaticallyRetryRunner(runner)

    self.assertEqual(1, runner_stats.RunnerStats.all().count())
    r_stats = runner_stats.RunnerStats.all().get()
    self.assertFalse(r_stats.success)
    self.assertEqual(0, r_stats.automatic_retry_count)

  def testPingRunner(self):
    # Try with a few invalid keys.
    self.assertFalse(test_runner.PingRunner(1, None))
    self.assertFalse(test_runner.PingRunner('2', None))

    # Tests with a valid key
    runner = self._CreateRunner()

    # Runner hasn't started.
    self.assertFalse(test_runner.PingRunner(runner.key(), None))

    # Runner starts and can get pinged.
    runner.started = datetime.datetime.now()
    runner.machine_id = MACHINE_IDS[0]
    runner.put()
    self.assertTrue(test_runner.PingRunner(runner.key(),
                                           MACHINE_IDS[0]))

    # The machine ids don't match so fail.
    self.assertFalse(test_runner.PingRunner(runner.key(),
                                            MACHINE_IDS[1]))

    # Runner is done.
    runner.done = True
    runner.put()
    self.assertFalse(test_runner.PingRunner(runner.key(),
                                            MACHINE_IDS[0]))

    # Delete the runner and try to ping.
    runner.delete()
    self.assertFalse(test_runner.PingRunner(runner.key(),
                                            MACHINE_IDS[0]))

  def testAbortRunnerThenReattach(self):
    runner = self._CreateRunner()
    runner.machine_id = MACHINE_IDS[0]
    runner.started = datetime.datetime.now()
    runner.ping = datetime.datetime.now()
    runner.put()

    # Retry the runner since its taken too long to ping.
    self.assertTrue(test_runner.AutomaticallyRetryRunner(runner))

    runner = test_runner.TestRunner.all().get()
    self.assertEqual(1, runner.automatic_retry_count)
    self.assertEqual(None, runner.started)
    self.assertEqual(None, runner.machine_id)

    # Have the the machine ping the server and get reconnected to the runner.
    self.assertTrue(test_runner.PingRunner(runner.key(), MACHINE_IDS[0]))

    runner = test_runner.TestRunner.all().get()
    self.assertEqual(0, runner.automatic_retry_count)
    self.assertNotEqual(None, runner.started)
    self.assertEqual(MACHINE_IDS[0], runner.machine_id)
    self.assertEqual([], runner.old_machine_ids)

  def testGetTestRunners(self):
    self.assertEqual(0,
                     len(list(test_runner.GetTestRunners(
                         'machine_id', ascending=True, limit=0, offset=0))))

    # Create some test requests.
    test_runner_count = 3
    for i in range(test_runner_count):
      self._CreateRunner(config_instance_index=i,
                         num_instances=test_runner_count)

    # Make sure the results are sorted.
    test_runners = test_runner.GetTestRunners('config_instance_index',
                                              ascending=True,
                                              limit=3, offset=0)
    for i in range(test_runner_count):
      self.assertEqual(i, test_runners.next().config_instance_index)
    self.assertEqual(0, len(list(test_runners)))

    # Make sure the results are sorted in descending order.
    test_runners = test_runner.GetTestRunners('config_instance_index',
                                              ascending=False,
                                              limit=3, offset=0)
    for i in range(test_runner_count):
      self.assertEqual(test_runner_count - 1 - i,
                       test_runners.next().config_instance_index)
    self.assertEqual(0, len(list(test_runners)))

  def testGetRunnerFromKey(self):
    self.assertEqual(None, test_runner.GetRunnerFromKey('fake_key'))

    runner = self._CreateRunner()
    print test_runner.TestRunner.all().count()
    self.assertEqual(runner.key(),
                     test_runner.GetRunnerFromKey(runner.key()).key())

    # Check keys that are valid, but point to deleted models or models of the
    # wrong type.
    self.assertEqual(None, test_runner.GetRunnerFromKey(runner.request.key()))

    test_runner.DeleteRunner(runner)
    self.assertEqual(None, test_runner.GetRunnerFromKey(runner.key()))

  def testDeleteRunner(self):
    self._CreateRunner()

    # Make sure the request and the runner are stored.
    self.assertEqual(1, test_runner.TestRunner.all().count())
    self.assertEqual(1, test_request.TestRequest.all().count())

    # Ensure the runner is deleted and that the request is deleted (since it
    # has no remaining runners).
    runner = test_runner.TestRunner.all().get()
    test_runner.DeleteRunner(runner)
    self.assertEqual(0, test_runner.TestRunner.all().count())
    self.assertEqual(0, test_request.TestRequest.all().count())

  def testDeleteRunnerFromKey(self):
    self._CreateRunner()

    # Make sure the request and the runner are stored.
    self.assertEqual(1, test_runner.TestRunner.all().count())
    self.assertEqual(1, test_request.TestRequest.all().count())

    # Try deleting with an invalid key and make sure nothing happens.
    test_runner.DeleteRunnerFromKey(db.Key())
    self.assertEqual(1, test_runner.TestRunner.all().count())
    self.assertEqual(1, test_request.TestRequest.all().count())

    # Delete the runner by its key.
    key = test_runner.TestRunner.all().get().key()
    test_runner.DeleteRunnerFromKey(key)

    # Ensure the runner is deleted and that the request is deleted (since it
    # has no remaining runners).
    self.assertEqual(0, test_runner.TestRunner.all().count())
    self.assertEqual(0, test_request.TestRequest.all().count())

    # Now try deleting the Test Runner again, this should be a noop.
    test_runner.DeleteRunnerFromKey(key)
    self.assertEqual(0, test_runner.TestRunner.all().count())
    self.assertEqual(0, test_request.TestRequest.all().count())

  def testSwarmDeleteOldRunners(self):
    self._mox.StubOutWithMock(test_runner, '_GetCurrentTime')

    # Set the current time to the future, but not too much.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_runner.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS - 1))
    test_runner._GetCurrentTime().AndReturn(mock_now)

    # Set the current time to way in the future.
    mock_now = (datetime.datetime.now() + datetime.timedelta(
        days=test_runner.SWARM_FINISHED_RUNNER_TIME_TO_LIVE_DAYS + 1))
    test_runner._GetCurrentTime().AndReturn(mock_now)
    self._mox.ReplayAll()

    runner = self._CreateRunner()
    runner.ended = datetime.datetime.now()
    runner.put()

    # Make sure that new runners aren't deleted.
    test_runner.DeleteOldRunners().get_result()
    self.assertEqual(1, test_runner.TestRunner.all().count())

    # Make sure that old runners are deleted.
    test_runner.DeleteOldRunners().get_result()
    self.assertEqual(0, test_runner.TestRunner.all().count())

    self._mox.VerifyAll()

  def testDeleteOrphanedBlobs(self):
    self.assertEqual(0, test_runner.DeleteOrphanedBlobs())

    # Add a runner with a blob and don't delete the blob.
    runner = self._CreateRunner()
    runner.result_string_reference = blobstore_helper.CreateBlobstore(
        'owned blob')
    runner.put()
    self.assertEqual(0, test_runner.DeleteOrphanedBlobs())

    # Add an orphaned blob and delete it.
    blobstore_helper.CreateBlobstore('orphaned blob')
    self.assertEqual(1, test_runner.DeleteOrphanedBlobs())

if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
