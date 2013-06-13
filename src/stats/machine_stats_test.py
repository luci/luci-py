#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for MachineStats class."""



import datetime
import logging
import unittest


from google.appengine.ext import testbed
from server import admin_user
from stats import machine_stats
from third_party.mox import mox


MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789']


class MachineStatsTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()

    # Setup a mock object.
    self._mox = mox.Mox()

  def tearDown(self):
    self.testbed.deactivate()

    self._mox.UnsetStubs()

  def testDetectDeadMachines(self):
    # Have two calls to today, then 1 in the future when stats are old.
    self._mox.StubOutWithMock(machine_stats, '_GetCurrentDay')
    machine_stats._GetCurrentDay().AndReturn(datetime.date.today())
    machine_stats._GetCurrentDay().AndReturn(datetime.date.today())
    machine_stats._GetCurrentDay().AndReturn(
        datetime.date.today() +
        datetime.timedelta(days=machine_stats.MACHINE_TIMEOUT_IN_DAYS * 2))
    self._mox.ReplayAll()

    self.assertEqual([], machine_stats.FindDeadMachines())

    m_stats = machine_stats.MachineStats.get_or_insert('id1', tag='machine')
    m_stats.put()
    self.assertEqual([], machine_stats.FindDeadMachines())

    dead_machines = machine_stats.FindDeadMachines()
    self.assertEqual(1, len(dead_machines))
    self.assertEqual('id1', dead_machines[0].MachineID())
    self.assertEqual('machine', dead_machines[0].tag)

    self._mox.VerifyAll()

  def testNotifyAdminsOfDeadMachines(self):
    dead_machine = machine_stats.MachineStats.get_or_insert(
        'id', tag='tag', last_seen=datetime.date.today())
    dead_machine.put()

    # Set an admin and ensure emails can get sent to them.
    user = admin_user.AdminUser(email='fake@email.com')
    user.put()
    self.assertTrue(machine_stats.NotifyAdminsOfDeadMachines([dead_machine]))

  def testRecordMachineQueries(self):
    dimensions = 'dimensions'
    machine_tag = 'tag'
    self.assertEqual(0, machine_stats.MachineStats.all().count())

    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions,
                                              machine_tag)
    self.assertEqual(1, machine_stats.MachineStats.all().count())

    # Ensure that last_seen is updated if it is old.
    m_stats = machine_stats.MachineStats.all().get()
    m_stats.last_seen -= datetime.timedelta(days=5)
    m_stats.put()

    old_date = m_stats.last_seen
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions,
                                              machine_tag)

    m_stats = machine_stats.MachineStats.all().get()
    self.assertNotEqual(old_date, m_stats.last_seen)

  def testDeleteMachineStats(self):
    # Try to delete with bad keys.
    self.assertFalse(machine_stats.DeleteMachineStats('bad key'))
    self.assertFalse(machine_stats.DeleteMachineStats(1))

    # Add and then delete a machine assignment.
    m_stats = machine_stats.MachineStats.get_or_insert(
        'id', last_seen=datetime.date.today())
    m_stats.put()
    self.assertEqual(1, machine_stats.MachineStats.all().count())
    self.assertTrue(
        machine_stats.DeleteMachineStats(m_stats.key()))

    # Try and delete the machine assignment again.
    self.assertFalse(
        machine_stats.DeleteMachineStats(m_stats.key()))

  def testGetAllMachines(self):
    self.assertEqual(0, len(list(machine_stats.GetAllMachines())))

    dimensions = 'dimensions'
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions, 'b')
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[1], dimensions, 'a')

    # Ensure that the default works.
    self.assertEqual(2, len(list(machine_stats.GetAllMachines())))

    # Ensure that the returned values are sorted by tags.
    machines = machine_stats.GetAllMachines('tag')
    self.assertEqual(MACHINE_IDS[1], machines.next().MachineID())
    self.assertEqual(MACHINE_IDS[0], machines.next().MachineID())
    self.assertEqual(0, len(list(machines)))

  def testGetMachineTag(self):
    # We get calls with None when trying to get the results for runners that
    # haven't started yet.
    self.assertEqual('Unknown', machine_stats.GetMachineTag(None))

    # Test with an invalid machine id still returns a value.
    self.assertEqual('Unknown', machine_stats.GetMachineTag(MACHINE_IDS[0]))

    dimensions = 'dimensions'
    tag = 'machine_tag'
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions, tag)
    self.assertEqual(tag, machine_stats.GetMachineTag(MACHINE_IDS[0]))


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
