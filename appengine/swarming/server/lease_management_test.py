#!/usr/bin/python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Unit tests for lease_management.py."""

import datetime
import json
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb
from protorpc.remote import protojson
import webtest

from components import machine_provider
from components import utils
from test_support import test_case

import bot_management
import lease_management
from proto import bots_pb2


def rpc_to_json(rpc_message):
  """Converts the given RPC message to a POSTable JSON dict.

  Args:
    rpc_message: A protorpc.message.Message instance.

  Returns:
    A string representing a JSON dict.
  """
  return json.loads(protojson.encode_message(rpc_message))


class DrainExcessTest(test_case.TestCase):
  """Tests for lease_management.drain_excess."""

  def test_no_machine_types(self):
    lease_management.drain_excess()

    self.failIf(lease_management.MachineLease.query().count())

  def test_nothing_to_drain(self):
    key = lease_management.MachineType(
        target_size=1,
    ).put()
    key = lease_management.MachineLease(
        id='%s-0' % key.id(),
        machine_type=key,
    ).put()

    lease_management.drain_excess()

    self.assertEqual(lease_management.MachineLease.query().count(), 1)
    self.failIf(key.get().drained)

  def test_drain_one(self):
    key = lease_management.MachineType(
        target_size=0,
    ).put()
    key = lease_management.MachineLease(
        id='%s-0' % key.id(),
        machine_type=key,
    ).put()

    lease_management.drain_excess()

    self.assertEqual(lease_management.MachineLease.query().count(), 1)
    self.assertTrue(key.get().drained)

  def test_drain_all(self):
    key = lease_management.MachineType(
        enabled=False,
        target_size=3,
    ).put()
    lease_management.MachineLease(
        id='%s-0' % key.id(),
        machine_type=key,
    ).put()
    lease_management.MachineLease(
        id='%s-1' % key.id(),
        machine_type=key,
    ).put()
    lease_management.MachineLease(
        id='%s-2' % key.id(),
        machine_type=key,
    ).put()

    lease_management.drain_excess()

    self.assertEqual(lease_management.MachineLease.query().count(), 3)
    for machine_lease in lease_management.MachineLease.query():
      self.assertTrue(machine_lease.drained)

  def test_drain_batched(self):
    key = lease_management.MachineType(
        enabled=False,
        target_size=2,
    ).put()
    lease_management.MachineLease(
        id='%s-0' % key.id(),
        machine_type=key,
    ).put()
    lease_management.MachineLease(
        id='%s-1' % key.id(),
        machine_type=key,
    ).put()
    key = lease_management.MachineType(
        enabled=False,
        target_size=2,
    ).put()
    lease_management.MachineLease(
        id='%s-0' % key.id(),
        machine_type=key,
    ).put()
    lease_management.MachineLease(
        id='%s-1' % key.id(),
        machine_type=key,
    ).put()
    key = lease_management.MachineType(
        target_size=0,
    ).put()
    lease_management.MachineLease(
        id='%s-0' % key.id(),
        machine_type=key,
    ).put()

    # Choice of 2, 2, 1 above and 3 here ensures at least one batch contains
    # MachineLease entities created for two different MachineTypes.
    lease_management.drain_excess(max_concurrent=3)

    self.assertEqual(lease_management.MachineLease.query().count(), 5)
    for machine_lease in lease_management.MachineLease.query():
      self.assertTrue(machine_lease.drained)


class EnsureBotInfoExistsTest(test_case.TestCase):
  """Tests for lease_management.ensure_bot_info_exists."""

  def test_creates(self):
    key = lease_management.MachineLease(
        hostname='hostname',
        lease_id='lease-id',
        lease_expiration_ts=utils.utcnow(),
        machine_type=ndb.Key(lease_management.MachineType, 'machine-type'),
    ).put()

    lease_management.ensure_bot_info_exists(key.get())

    machine_lease = key.get()
    bot_info = bot_management.get_info_key(machine_lease.bot_id).get()
    self.assertEqual(machine_lease.bot_id, machine_lease.hostname)
    self.assertEqual(bot_info.lease_id, machine_lease.lease_id)
    self.assertEqual(
        bot_info.lease_expiration_ts, machine_lease.lease_expiration_ts)
    self.assertEqual(bot_info.machine_type, machine_lease.machine_type.id())


class EnsureEntitiesExistTest(test_case.TestCase):
  """Tests for lease_management.ensure_entities_exist."""

  def test_no_machine_types(self):
    lease_management.ensure_entities_exist()

    self.failIf(lease_management.MachineLease.query().count())

  def test_no_enabled_machine_types(self):
    lease_management.MachineType(
        enabled=False,
        target_size=3,
    ).put()

    lease_management.ensure_entities_exist()

    self.failIf(lease_management.MachineLease.query().count())

  def test_one_enabled_machine_type(self):
    def fetch_machine_types():
      return {
          'machine-type': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type',
              target_size=1,
          ),
      }
    self.mock(
        lease_management.bot_groups_config,
        'fetch_machine_types',
        fetch_machine_types,
    )

    key = lease_management.MachineType(
        id='machine-type',
        target_size=1,
    ).put()

    lease_management.ensure_entities_exist()

    self.assertEqual(key.get().early_release_secs, 0)
    self.assertEqual(key.get().lease_duration_secs, 1)
    self.assertEqual(key.get().target_size, 1)
    self.assertEqual(lease_management.MachineLease.query().count(), 1)

  def test_two_enabled_machine_types(self):
    def fetch_machine_types():
      return {
          'machine-type-a': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type-a',
              target_size=1,
          ),
          'machine-type-b': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type-b',
              target_size=1,
          ),
      }
    self.mock(
        lease_management.bot_groups_config,
        'fetch_machine_types',
        fetch_machine_types,
    )

    lease_management.MachineType(
        id='machine-type-a',
        target_size=1,
    ).put()
    lease_management.MachineType(
        id='machine-type-b',
        target_size=1,
    ).put()

    lease_management.ensure_entities_exist()

    self.assertEqual(lease_management.MachineLease.query().count(), 2)
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-a-0'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-b-0'))

  def test_one_machine_type_multiple_batches(self):
    def fetch_machine_types():
      return {
          'machine-type': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type',
              target_size=5,
          ),
      }
    self.mock(
        lease_management.bot_groups_config,
        'fetch_machine_types',
        fetch_machine_types,
    )

    lease_management.MachineType(
        id='machine-type',
        target_size=5,
    ).put()

    # Choice of 3 here and 5 above ensures MachineLeases are created in two
    # batches of differing sizes.
    lease_management.ensure_entities_exist(max_concurrent=3)

    self.assertEqual(lease_management.MachineLease.query().count(), 5)
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-0'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-1'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-2'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-3'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-4'))

  def test_three_machine_types_multiple_batches(self):
    def fetch_machine_types():
      return {
          'machine-type-a': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type-a',
              target_size=2,
          ),
          'machine-type-b': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type-b',
              target_size=2,
          ),
          'machine-type-c': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type-c',
              target_size=1,
          ),
      }
    self.mock(
        lease_management.bot_groups_config,
        'fetch_machine_types',
        fetch_machine_types,
    )

    lease_management.MachineType(
        id='machine-type-a',
        target_size=2,
    ).put()
    lease_management.MachineType(
        id='machine-type-b',
        target_size=2,
    ).put()
    lease_management.MachineType(
        id='machine-type-c',
        target_size=1,
    ).put()

    # Choice of 2, 2, 1 above and 3 here ensures at least one batch contains
    # MachineLease entities created for two different MachineTypes.
    lease_management.ensure_entities_exist(max_concurrent=3)

    self.assertEqual(lease_management.MachineLease.query().count(), 5)
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-a-0'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-a-1'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-b-0'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-b-1'))
    self.failUnless(lease_management.MachineLease.get_by_id('machine-type-c-0'))

  def test_machine_lease_exists_mismatched_not_updated(self):
    key = lease_management.MachineType(
        early_release_secs=0,
        lease_duration_secs=1,
        mp_dimensions=machine_provider.Dimensions(
            disk_gb=100,
        ),
        target_size=1,
    ).put()
    key = lease_management.MachineLease(
        id='%s-0' % key.id(),
        early_release_secs=1,
        lease_duration_secs=2,
        machine_type=key,
        mp_dimensions=machine_provider.Dimensions(
            disk_gb=200,
        ),
    ).put()

    lease_management.ensure_entities_exist()

    self.assertEqual(lease_management.MachineLease.query().count(), 1)
    self.assertEqual(key.get().early_release_secs, 1)
    self.assertEqual(key.get().lease_duration_secs, 2)
    self.assertEqual(key.get().mp_dimensions.disk_gb, 200)

  def test_machine_lease_exists_mismatched_updated(self):
    def fetch_machine_types():
      return {
          'machine-type': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type',
              target_size=1,
          ),
      }
    self.mock(
        lease_management.bot_groups_config,
        'fetch_machine_types',
        fetch_machine_types,
    )

    key = lease_management.MachineType(
        id='machine-type',
        early_release_secs=0,
        lease_duration_secs=1,
        mp_dimensions=machine_provider.Dimensions(
            disk_gb=100,
        ),
        target_size=1,
    ).put()
    key = lease_management.MachineLease(
        id='%s-0' % key.id(),
        early_release_secs=1,
        lease_duration_secs=2,
        lease_expiration_ts=utils.utcnow(),
        machine_type=key,
        mp_dimensions=machine_provider.Dimensions(
            disk_gb=200,
        ),
    ).put()

    lease_management.ensure_entities_exist()

    self.assertEqual(lease_management.MachineLease.query().count(), 1)
    self.assertEqual(key.get().early_release_secs, 0)
    self.assertEqual(key.get().lease_duration_secs, 1)
    self.assertEqual(key.get().mp_dimensions.disk_gb, 100)

  def test_daily_schedule_enable(self):
    def fetch_machine_types():
      return {
          'machine-type': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type',
              target_size=1,
              schedule=bots_pb2.Schedule(
                  daily=[bots_pb2.DailySchedule(
                      start='0:00',
                      end='1:00',
                      days_of_the_week=xrange(7),
                  )],
              ),
          ),
      }
    self.mock(
        lease_management.bot_groups_config,
        'fetch_machine_types',
        fetch_machine_types,
    )
    self.mock(
        lease_management.utils,
        'utcnow',
        lambda: datetime.datetime(1969, 1, 1, 0, 30),
    )

    key = lease_management.MachineType(
        id='machine-type',
        early_release_secs=0,
        enabled=False,
        lease_duration_secs=1,
        mp_dimensions=machine_provider.Dimensions(
            disk_gb=100,
        ),
        target_size=1,
    ).put()

    lease_management.ensure_entities_exist()

    self.assertEqual(lease_management.MachineLease.query().count(), 1)
    self.failUnless(key.get().enabled)

  def test_daily_schedule_disable(self):
    def fetch_machine_types():
      return {
          'machine-type': bots_pb2.MachineType(
              early_release_secs=0,
              lease_duration_secs=1,
              mp_dimensions=['disk_gb:100'],
              name='machine-type',
              target_size=1,
              schedule=bots_pb2.Schedule(
                  daily=[bots_pb2.DailySchedule(
                      start='0:00',
                      end='1:00',
                  )],
              ),
          ),
      }
    self.mock(
        lease_management.bot_groups_config,
        'fetch_machine_types',
        fetch_machine_types,
    )
    self.mock(
        lease_management.utils,
        'utcnow',
        lambda: datetime.datetime(1969, 1, 1, 2),
    )

    key = lease_management.MachineType(
        id='machine-type',
        early_release_secs=0,
        enabled=True,
        lease_duration_secs=1,
        mp_dimensions=machine_provider.Dimensions(
            disk_gb=100,
        ),
        target_size=1,
    ).put()

    lease_management.ensure_entities_exist()

    self.failIf(lease_management.MachineLease.query().count())
    self.failIf(key.get().enabled)


class ShouldBeEnabledTest(test_case.TestCase):
  """Tests for lease_management.should_be_enabled."""

  def test_no_schedule(self):
    config = bots_pb2.MachineType()
    now = None

    self.failUnless(lease_management.should_be_enabled(config, now=now))

  def test_no_daily_schedule(self):
    config = bots_pb2.MachineType(schedule=bots_pb2.Schedule())
    now = None

    self.failUnless(lease_management.should_be_enabled(config, now=now))

  def test_should_be_disabled_wrong_day(self):
    # self.mock can't install mocks for built-in datetime.datetime.
    class MockDateTime(datetime.datetime):
      def __init__(self, *args, **kwargs):
        super(MockDateTime, self).__init__(*args, **kwargs)
      def weekday(self):
        return 5

    config = bots_pb2.MachineType(schedule=bots_pb2.Schedule(
        daily=[bots_pb2.DailySchedule(
            start='1:00',
            end='2:00',
            days_of_the_week=xrange(5),
        )],
    ))
    now = MockDateTime(1969, 1, 1)

    self.failIf(lease_management.should_be_enabled(config, now=now))

  def test_should_be_enabled(self):
    config = bots_pb2.MachineType(schedule=bots_pb2.Schedule(
        daily=[bots_pb2.DailySchedule(
            start='1:00',
            end='2:00',
            days_of_the_week=xrange(7),
        )],
    ))
    now = datetime.datetime(1969, 1, 1, 1, 30)

    self.failUnless(lease_management.should_be_enabled(config, now=now))

  def test_should_be_disabled_too_early(self):
    config = bots_pb2.MachineType(schedule=bots_pb2.Schedule(
        daily=[bots_pb2.DailySchedule(
            start='1:00',
            end='2:00',
            days_of_the_week=xrange(7),
        )],
    ))
    now = datetime.datetime(1969, 1, 1)

    self.failIf(lease_management.should_be_enabled(config, now=now))

  def test_should_be_disabled_too_late(self):
    config = bots_pb2.MachineType(schedule=bots_pb2.Schedule(
        daily=[bots_pb2.DailySchedule(
            start='1:00',
            end='2:00',
            days_of_the_week=xrange(7),
        )],
    ))
    now = datetime.datetime(1969, 1, 1, 3)

    self.failIf(lease_management.should_be_enabled(config, now=now))


if __name__ == '__main__':
  unittest.main()
