#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for MachineStats class."""



import datetime
import logging
import unittest

from google.appengine.ext import testbed
from stats import machine_stats


MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789']


class MachineStatsTest(unittest.TestCase):
  def setUp(self):
    # Setup the app engine test bed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_all_stubs()

  def tearDown(self):
    self.testbed.deactivate()

  def testRecordMachineQueries(self):
    machine_tag = 'tag'
    self.assertEqual(0, machine_stats.MachineStats.all().count())

    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], machine_tag)
    self.assertEqual(1, machine_stats.MachineStats.all().count())

    # Ensure that last_seen is updated if it is old.
    m_stats = machine_stats.MachineStats.all().get()
    m_stats.last_seen -= datetime.timedelta(days=5)
    m_stats.put()

    old_date = m_stats.last_seen
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], machine_tag)

    m_stats = machine_stats.MachineStats.all().get()
    self.assertNotEqual(old_date, m_stats.last_seen)

  def testDeleteMachineStats(self):
    # Try to delete with bad keys.
    self.assertFalse(machine_stats.DeleteMachineStats('bad key'))
    self.assertFalse(machine_stats.DeleteMachineStats(1))

    # Add and then delete a machine assignment.
    m_stats = machine_stats.MachineStats(machine_id='id',
                                         last_seen=datetime.date.today())
    m_stats.put()
    self.assertEqual(1, machine_stats.MachineStats.all().count())
    self.assertTrue(
        machine_stats.DeleteMachineStats(m_stats.key()))

    # Try and delete the machine assignment again.
    self.assertFalse(
        machine_stats.DeleteMachineStats(m_stats.key()))

  def testGetAllMachines(self):
    self.assertEqual(0, len(list(machine_stats.GetAllMachines())))

    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], 'b')
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[1], 'a')

    # Ensure that the returned values are sorted by tags.
    machines = machine_stats.GetAllMachines('tag')
    self.assertEqual(MACHINE_IDS[1], machines.next().machine_id)
    self.assertEqual(MACHINE_IDS[0], machines.next().machine_id)
    self.assertEqual(0, len(list(machines)))

  def testGetMachineTag(self):
    # Test with an invalid machine id still returns a value.
    self.assertEqual('Unknown', machine_stats.GetMachineTag(MACHINE_IDS[0]))

    tag = 'machine_tag'
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], tag)
    self.assertEqual(tag, machine_stats.GetMachineTag(MACHINE_IDS[0]))


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
