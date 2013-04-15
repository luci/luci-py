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
from server import test_request
from server import test_runner
from third_party.mox import mox

MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789',
               '34567890-34567890-34567890-34567890',
               '87654321-87654321-87654321-87654321']


def CreateRunner(config_instance_index=0, num_instances=1):
  """Creates a new runner.

  Args:
    config_instance_index: The config_instance_index of the runner.
    num_instances: The num_config_instances for the runner.

  Returns:
    The newly created runner.
  """
  request = test_request.TestRequest()
  request.put()

  runner = test_runner.TestRunner(
      request=request,
      config_hash=hashlib.sha1().hexdigest(),
      config_instance_index=config_instance_index,
      num_config_instances=num_instances)
  runner.put()

  return runner


class TestRunnerTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testGetResultStringFromEmptyRunner(self):
    runner = CreateRunner()

    # Since the request hasn't been run yet there should be just be an
    # empty string for the result string.
    self.assertEqual('', runner.GetResultString())

  # Test with an exception.
  def testAssignRunnerToMachineTxError(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise test_runner.TxRunnerAlreadyAssignedError

    runner = CreateRunner()

    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test with another exception.
  def testAssignRunnerToMachineTimeout(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise db.Timeout

    runner = CreateRunner()

    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test with yet another exception.
  def testAssignRunnerToMachineTransactionFailedError(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise db.TransactionFailedError

    runner = CreateRunner()

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
    def _RaiseTempError(key, machine_id):  # pylint: disable-msg=W0613
      _RaiseTempError.error_count -= 1
      if _RaiseTempError.error_count:
        raise db.TransactionFailedError
      else:
        return True

    runner = CreateRunner()

    self.assertTrue(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseTempError))

  # Test with one more exception.
  def testAssignRunnerToMachineInternalError(self):
    def _RaiseError(key, machine_id):  # pylint: disable-msg=W0613
      raise db.InternalError

    runner = CreateRunner()

    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, _RaiseError))

  # Test proper behavior of AtomicAssignID.
  def testAtomicAssignID(self):
    runners = []

    # Create some pending runners.
    for _ in range(0, 2):
      runners.append(CreateRunner())

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
    runner = CreateRunner()

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
    runner = CreateRunner()
    runner.delete()

    # Assignment should fail without an exception.
    self.assertFalse(test_runner.AssignRunnerToMachine(
        MACHINE_IDS[0], runner, test_runner.AtomicAssignID))

  def testPingRunner(self):
    # Try with a few invalid keys.
    self.assertFalse(test_runner.PingRunner(1, None))
    self.assertFalse(test_runner.PingRunner('2', None))

    # Tests with a valid key
    runner = CreateRunner()

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
    runner = CreateRunner()
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
      CreateRunner(config_instance_index=i, num_instances=test_runner_count)

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

  def testDeleteRunner(self):
    CreateRunner()

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
    CreateRunner()

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

    CreateRunner()

    # Make sure that new runners aren't deleted.
    test_runner.DeleteOldRunners()
    self.assertEqual(1, test_runner.TestRunner.all().count())

    # Make sure that old runners are deleted.
    test_runner.DeleteOldRunners()
    self.assertEqual(0, test_runner.TestRunner.all().count())

    self._mox.VerifyAll()

  def testDeleteOrphanedBlobs(self):
    self.assertEqual(0, test_runner.DeleteOrphanedBlobs())

    # Add a runner with a blob and don't delete the blob.
    runner = CreateRunner()
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
