#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from stats import machine_stats
from support import test_case


MACHINE_IDS = ['12345678-12345678-12345678-12345678',
               '23456789-23456789-23456789-23456789']


class MachineStatsTest(test_case.TestCase):
  def testRecordMachineQueries(self):
    dimensions = 'dimensions'
    machine_tag = 'tag'
    self.assertEqual(0, machine_stats.MachineStats.query().count())

    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions,
                                              machine_tag)
    self.assertEqual(1, machine_stats.MachineStats.query().count())

    # Ensure that last since isn't update, since not enough time will have
    # elapsed.
    m_stats = machine_stats.MachineStats.query().get()
    old_time = m_stats.last_seen
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions,
                                              machine_tag)
    self.assertEqual(old_time, m_stats.last_seen)

    # Ensure that last_seen is updated if it is old.
    m_stats = machine_stats.MachineStats.query().get()
    m_stats.last_seen -= 2 * machine_stats.MACHINE_UPDATE_TIME
    m_stats.put()

    old_time = m_stats.last_seen
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions,
                                              machine_tag)

    m_stats = machine_stats.MachineStats.query().get()
    self.assertNotEqual(old_time, m_stats.last_seen)

  def testDeleteMachineStats(self):
    # Try to delete with bad keys.
    self.assertFalse(machine_stats.DeleteMachineStats('bad key'))
    self.assertFalse(machine_stats.DeleteMachineStats(1))

    # Add and then delete a machine assignment.
    m_stats = machine_stats.MachineStats.get_or_insert('id')
    m_stats.put()
    self.assertEqual(1, machine_stats.MachineStats.query().count())
    self.assertTrue(
        machine_stats.DeleteMachineStats(m_stats.machine_id))

    # Try and delete the machine assignment again.
    self.assertFalse(
        machine_stats.DeleteMachineStats(m_stats.machine_id))

  def testGetAllMachines(self):
    self.assertEqual(0, len(list(machine_stats.GetAllMachines())))

    dimensions = 'dimensions'
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[0], dimensions, 'b')
    machine_stats.RecordMachineQueriedForWork(MACHINE_IDS[1], dimensions, 'a')

    # Ensure that the default works.
    self.assertEqual(2, len(list(machine_stats.GetAllMachines())))

    # Ensure that the returned values are sorted by tags.
    machines = machine_stats.GetAllMachines('tag')
    self.assertEqual(MACHINE_IDS[1], machines.next().machine_id)
    self.assertEqual(MACHINE_IDS[0], machines.next().machine_id)
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

  def testToDict(self):
    now = datetime.datetime(2014, 3, 12, 15, 5, 0, 165565)
    self.mock(machine_stats, 'utcnow', lambda: now)
    machine_stats.RecordMachineQueriedForWork(
        'id', {'foo': ['bar', 'baz']}, 'tag')
    expected = [
      {
        'dimensions': {u'foo': [u'bar', u'baz']},
        'last_seen': now,
        'machine_id': 'id',
        'tag': u'tag',
      },
    ]
    self.assertEqual(
        expected, [i.to_dict() for i in machine_stats.GetAllMachines()])


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
