#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import hashlib
import logging
import sys
import unittest

import mock
from parameterized import parameterized

import test_env
test_env.setup_test_env()

from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2

from google.appengine.api import memcache
from google.appengine.ext import ndb

from components import utils
from test_support import test_case

from proto.api import swarming_pb2  # pylint: disable=no-name-in-module
from server import bq_state
from server import bot_management
from server import config
from server import task_queues


_VERSION = unicode(hashlib.sha256().hexdigest())


def _bot_event(bot_id=None,
               external_ip='8.8.4.4',
               authenticated_as=None,
               dimensions=None,
               state=None,
               version=_VERSION,
               quarantined=False,
               maintenance_msg=None,
               task_id=None,
               task_name=None,
               **kwargs):
  """Calls bot_management.bot_event with default arguments."""
  if not bot_id:
    bot_id = u'id1'
  if not dimensions:
    dimensions = {
        u'id': [bot_id],
        u'os': [u'Ubuntu', u'Ubuntu-16.04'],
        u'pool': [u'default'],
    }
  if not authenticated_as:
    authenticated_as = u'bot:%s.domain' % bot_id
  register_dimensions = kwargs.get('event_type').startswith('request_')
  return bot_management.bot_event(
      bot_id=bot_id,
      external_ip=external_ip,
      authenticated_as=authenticated_as,
      dimensions=dimensions,
      state=state or {'ram': 65},
      version=version,
      quarantined=quarantined,
      maintenance_msg=maintenance_msg,
      task_id=task_id,
      task_name=task_name,
      register_dimensions=register_dimensions,
      **kwargs)


def _ensure_bot_info(bot_id=u'id1', **kwargs):
  _bot_event(bot_id=bot_id, event_type='request_sleep', **kwargs)
  return bot_management.get_info_key(bot_id).get()


def _gen_bot_info(**kwargs):
  out = {
      'authenticated_as': u'bot:id1.domain',
      'composite': [
          bot_management.BotInfo.NOT_IN_MAINTENANCE,
          bot_management.BotInfo.ALIVE,
          bot_management.BotInfo.HEALTHY,
          bot_management.BotInfo.IDLE,
      ],
      'dimensions': {
          u'id': [u'id1'],
          u'os': [u'Ubuntu', u'Ubuntu-16.04'],
          u'pool': [u'default'],
      },
      'external_ip': u'8.8.4.4',
      'first_seen_ts': utils.utcnow(),
      'id': 'id1',
      'idle_since_ts': None,
      'is_dead': False,
      'last_seen_ts': utils.utcnow(),
      'lease_id': None,
      'lease_expiration_ts': None,
      'leased_indefinitely': None,
      'machine_lease': None,
      'machine_type': None,
      'quarantined': False,
      'maintenance_msg': None,
      'state': {
          u'ram': 65
      },
      'task_id': None,
      'task_name': None,
      'version': _VERSION,
  }
  out.update(kwargs)
  return out


def _gen_bot_event(**kwargs):
  out = {
      'authenticated_as': u'bot:id1.domain',
      'dimensions': {
          u'id': [u'id1'],
          u'os': [u'Ubuntu', u'Ubuntu-16.04'],
          u'pool': [u'default'],
      },
      'external_ip': u'8.8.4.4',
      'idle_since_ts': None,
      'last_seen_ts': None,
      'lease_id': None,
      'lease_expiration_ts': None,
      'leased_indefinitely': None,
      'machine_lease': None,
      'machine_type': None,
      'message': None,
      'quarantined': False,
      'maintenance_msg': None,
      'state': {
          u'ram': 65
      },
      'task_id': None,
      'ts': utils.utcnow(),
      'version': _VERSION,
  }
  out.update(kwargs)
  return out


class BotManagementTest(test_case.TestCase):
  APP_DIR = test_env.APP_DIR

  def setUp(self):
    super(BotManagementTest, self).setUp()
    self.now = datetime.datetime(2010, 1, 2, 3, 4, 5, 6)
    self.mock_now(self.now)

  def test_all_apis_are_tested(self):
    actual = frozenset(i[5:] for i in dir(self) if i.startswith('test_'))
    # Contains the list of all public APIs.
    expected = frozenset(
        i for i in dir(bot_management)
        if i[0] != '_' and hasattr(getattr(bot_management, i), 'func_name'))
    missing = expected - actual
    self.assertFalse(missing)

  def test_BotEvent_proto_idle(self):
    event_key = _bot_event(event_type=u'request_sleep',
                           bot_id=u'id1',
                           dimensions={
                               u'id': [u'id1'],
                           })
    actual = swarming_pb2.BotEvent()
    event_key.get().to_proto(actual)
    self.assertEqual(swarming_pb2.INSTRUCT_IDLE, actual.event)
    self.assertEqual(swarming_pb2.IDLE, actual.bot.status)

  def test_BotEvent_proto_busy(self):
    event_key = _bot_event(event_type=u'request_task',
                           task_id=u'task1',
                           bot_id=u'id1',
                           dimensions={
                               u'id': [u'id1'],
                           })
    actual = swarming_pb2.BotEvent()
    event_key.get().to_proto(actual)
    self.assertEqual(swarming_pb2.INSTRUCT_START_TASK, actual.event)
    self.assertEqual(swarming_pb2.BUSY, actual.bot.status)
    self.assertEqual(u'task1', actual.bot.current_task_id)

  def test_BotEvent_proto_empty(self):
    # Assert that it doesn't throw on empty entity.
    actual = swarming_pb2.BotEvent()
    bot_management.BotEvent().to_proto(actual)
    self.assertEqual(swarming_pb2.BotEvent(), actual)

  def test_BotEvent_proto_events(self):
    # Ensures all bot event states can be converted to a proto.
    dimensions = {
      u'id': [u'id1'],
      u'os': [u'Ubuntu', u'Ubuntu-16.04'],
      u'pool': [u'default'],
    }
    for name in bot_management.BotEvent.ALLOWED_EVENTS:
      event_key = _bot_event(
          event_type=name, bot_id=u'id1', dimensions=dimensions)
      if name == u'task_update':
        # TODO(maruel): Store request_sleep IFF the state changed.
        self.assertIsNone(event_key, name)
        continue
      # Just asserts it doesn't crash.
      actual = swarming_pb2.BotEvent()
      event_key.get().to_proto(actual)

  def test_BotEvent_proto_maintenance(self):
    # Also test a misconfigured bot not in a pool.
    event_key = _bot_event(
        event_type=u'bot_connected',
        bot_id=u'id1',
        dimensions={u'id': [u'id1']},
        maintenance_msg=u'Too hot')
    actual = swarming_pb2.BotEvent()
    event_key.get().to_proto(actual)
    expected = swarming_pb2.BotEvent(
        event=swarming_pb2.BOT_NEW_SESSION,
        bot=swarming_pb2.Bot(
            bot_id=u'id1',
            dimensions=[
                swarming_pb2.StringListPair(key=u'id', values=[u'id1']),
            ],
            status=swarming_pb2.OVERHEAD_MAINTENANCE_EXTERNAL,
            status_msg=u'Too hot',
            info=swarming_pb2.BotInfo(
                supplemental=struct_pb2.Struct(fields={
                    u'ram': struct_pb2.Value(number_value=65),
                }),
                version=_VERSION,
                external_ip=u'8.8.4.4',
                authenticated_as=u'bot:id1.domain',
            ),
        ),
    )
    expected.event_time.FromDatetime(self.now)
    self.assertEqual(unicode(expected), unicode(actual))

  def test_BotEvent_proto_quarantine(self):
    # Also test that a bot can belong to two pools.
    event_key = _bot_event(
        event_type=u'bot_connected',
        bot_id=u'id1',
        dimensions={
            u'id': [u'id1'],
            u'pool': [u'next', u'previous']
        },
        state={
            u'ram': 65,
            u'quarantined': u'sad bot'
        },
        quarantined=True)
    actual = swarming_pb2.BotEvent()
    event_key.get().to_proto(actual)
    expected = swarming_pb2.BotEvent(
        event=swarming_pb2.BOT_NEW_SESSION,
        bot=swarming_pb2.Bot(
            bot_id=u'id1',
            pools=[u'next', u'previous'],
            dimensions=[
                swarming_pb2.StringListPair(key=u'id', values=[u'id1']),
                swarming_pb2.StringListPair(
                    key=u'pool', values=[u'next', u'previous']),
            ],
            status=swarming_pb2.QUARANTINED_BY_BOT,
            status_msg=u'sad bot',
            info=swarming_pb2.BotInfo(
                supplemental=struct_pb2.Struct(
                    fields={
                        u'quarantined':
                            struct_pb2.Value(string_value=u'sad bot'),
                        u'ram':
                            struct_pb2.Value(number_value=65),
                    }),
                version=_VERSION,
                external_ip=u'8.8.4.4',
                authenticated_as=u'bot:id1.domain',
            ),
        ),
    )
    expected.event_time.FromDatetime(self.now)
    self.assertEqual(unicode(expected), unicode(actual))

  def test_bot_event(self):
    # connected.
    d = {
        u'id': [u'id1'],
        u'os': [u'Ubuntu', u'Ubuntu-16.04'],
        u'pool': [u'default'],
    }
    event = 'request_sleep'
    _bot_event(event_type=event, bot_id='id1', dimensions=d)

    expected = _gen_bot_info(idle_since_ts=self.now)
    self.assertEqual(
        expected, bot_management.get_info_key('id1').get().to_dict())

  @parameterized.expand([
      (u'task_completed', True, False),
      (u'task_error', True, False),
      (u'task_killed', True, False),
      (u'request_sleep', True, True),
      (u'task_update', False, True),
  ])
  def test_bot_event_reset_task(self, event, reset_task, skip_store_event):
    bot_id = u'id1'
    task_id = u'12311'
    task_name = u'yo'
    d = {
        u'id': [u'id1'],
        u'os': [u'Ubuntu', u'Ubuntu-16.04'],
        u'pool': [u'default'],
    }
    bot_info = _ensure_bot_info(
        bot_id=bot_id, dimensions=d, task_id=task_id, task_name=task_name)
    _bot_event(
        event_type=event,
        bot_id=bot_id,
        dimensions=d,
        task_id=task_id,
        task_name=task_name)

    # check bot_info
    composite = [
        bot_management.BotInfo.NOT_IN_MAINTENANCE,
        bot_management.BotInfo.ALIVE,
        bot_management.BotInfo.HEALTHY,
    ]
    if event == 'request_sleep':
      composite += [bot_management.BotInfo.IDLE]
      idle_since_ts = self.now
    else:
      composite += [bot_management.BotInfo.BUSY]
      idle_since_ts = None

    if reset_task:
      # bot_info.task_id and bot_info.task_name should be reset
      expected = _gen_bot_info(
          composite=composite,
          id=bot_id,
          task_name=None,
          idle_since_ts=idle_since_ts)
    else:
      # bot_info.task_id and bot_info.task_name should be kept
      expected = _gen_bot_info(
          composite=composite,
          id=bot_id,
          task_id=task_id,
          task_name=task_name,
          idle_since_ts=None)

    self.assertEqual(expected, bot_info.key.get().to_dict())

    # bot_event should have task_id
    if not skip_store_event:
      expected_event = _gen_bot_event(event_type=event, task_id=task_id)
      last_event = bot_management.get_events_query(bot_id).get()
      self.assertEqual(expected_event, last_event.to_dict())

  def test_get_events_query(self):
    _bot_event(event_type='bot_connected')
    expected = [_gen_bot_event(event_type=u'bot_connected')]
    self.assertEqual(
        expected, [i.to_dict() for i in bot_management.get_events_query('id1')])

  def test_bot_event_poll_sleep(self):
    _bot_event(event_type='request_sleep')

    # Assert that BotInfo was updated too.
    expected = _gen_bot_info(
        idle_since_ts=self.now,
        composite=[
            bot_management.BotInfo.NOT_IN_MAINTENANCE,
            bot_management.BotInfo.ALIVE,
            bot_management.BotInfo.HEALTHY,
            bot_management.BotInfo.IDLE,
        ])

    bot_info = bot_management.get_info_key('id1').get()
    self.assertEqual(expected, bot_info.to_dict())

    # BotEvent is registered for poll when BotInfo creates
    expected_event = _gen_bot_event(event_type=u'request_sleep')
    bot_events = bot_management.get_events_query('id1')
    self.assertEqual([expected_event], [e.to_dict() for e in bot_events])

    # flush bot events
    ndb.delete_multi(e.key for e in bot_events)

    # BotEvent is not registered for poll when no dimensions change
    _bot_event(event_type='request_sleep')
    self.assertEqual([], bot_management.get_events_query('id1').fetch())

    # BotEvent is registered for poll when dimensions change
    dims = {u'foo': [u'bar']}
    _bot_event(event_type='request_sleep', dimensions=dims)
    expected_event['dimensions'] = dims
    bot_events = bot_management.get_events_query('id1').fetch()
    self.assertEqual([expected_event], [e.to_dict() for e in bot_events])

  def test_bot_event_busy(self):
    _bot_event(event_type='bot_connected')
    _bot_event(event_type='request_task', task_id='12311', task_name='yo')
    expected = _gen_bot_info(
        composite=[
          bot_management.BotInfo.NOT_IN_MAINTENANCE,
          bot_management.BotInfo.ALIVE,
          bot_management.BotInfo.HEALTHY,
          bot_management.BotInfo.BUSY,
        ],
        task_id=u'12311',
        task_name=u'yo')
    bot_info = bot_management.get_info_key('id1').get()
    self.assertEqual(expected, bot_info.to_dict())

    expected = [
        _gen_bot_event(event_type=u'request_task', task_id=u'12311'),
        _gen_bot_event(event_type=u'bot_connected'),
    ]
    self.assertEqual(
        expected, [e.to_dict() for e in bot_management.get_events_query('id1')])

  def test_bot_event_update_dimensions(self):
    bot_id = 'id1'
    bot_info_key = bot_management.get_info_key(bot_id)

    # bot dimensions generated without injected bot_config.py.
    dimensions_invalid = {'id': ['id1'], 'os': ['Ubuntu'], 'pool': ['default']}

    # 'bot_connected' event creates BotInfo only with id and pool dimensions.
    _bot_event(
        bot_id=bot_id,
        event_type='bot_connected',
        dimensions=dimensions_invalid)
    self.assertEqual(bot_info_key.get().dimensions_flat,
                     [u'id:id1', u'pool:default'])

    # 'bot_error' event does not register dimensions other than id and pool.
    _bot_event(
        bot_id=bot_id, event_type='bot_error', dimensions=dimensions_invalid)
    self.assertEqual(bot_info_key.get().dimensions_flat,
                     [u'id:id1', u'pool:default'])

    # 'request_sleep' registers given dimensions to BotInfo.
    _bot_event(
        bot_id=bot_id,
        event_type='request_sleep',
        dimensions={
            'id': ['id1'],
            'os': ['Android'],
            'pool': ['default']
        })
    self.assertEqual(bot_info_key.get().dimensions_flat,
                     [u'id:id1', u'os:Android', u'pool:default'])

    # 'bot_connected' doesn't update dimensions since bot_config isn't injected.
    _bot_event(
        bot_id=bot_id,
        event_type='bot_connected',
        dimensions=dimensions_invalid)
    self.assertEqual(bot_info_key.get().dimensions_flat,
                     [u'id:id1', u'os:Android', u'pool:default'])

  def test_get_info_key(self):
    self.assertEqual(
        ndb.Key(bot_management.BotRoot, 'foo', bot_management.BotInfo, 'info'),
        bot_management.get_info_key('foo'))

  def test_get_root_key(self):
    self.assertEqual(
        ndb.Key(bot_management.BotRoot, 'foo'),
        bot_management.get_root_key('foo'))

  def test_get_settings_key(self):
    expected = ndb.Key(
        bot_management.BotRoot, 'foo', bot_management.BotSettings, 'settings')
    self.assertEqual(expected, bot_management.get_settings_key('foo'))

  def test_get_aggregation_key(self):
    expected = ndb.Key(bot_management.DimensionAggregation, 'foo')
    self.assertEqual(expected, bot_management.get_aggregation_key('foo'))

  def test_has_capacity(self):
    # The bot can service this dimensions.
    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}

    # The bot can service one of 'or' dimensions.
    or_dimensions = {
        u'pool': [u'default'],
        u'os': [u'Ubuntu-14.04|Ubuntu-16.04'],
    }
    # By default, nothing has capacity.
    self.assertEqual(False, bot_management.has_capacity(d))
    # By default, nothing has capacity.
    self.assertEqual(False, bot_management.has_capacity(or_dimensions))

    # A bot comes online. There's some capacity now.
    _bot_event(
        event_type='request_sleep',
        dimensions={'id': ['id1'], 'pool': ['default'], 'os': ['Ubuntu',
          'Ubuntu-16.04']})
    self.assertEqual(1, bot_management.BotInfo.query().count())
    self.assertEqual(True, bot_management.has_capacity(d))

    self.assertEqual(True, bot_management.has_capacity(or_dimensions))

    # Disable the memcache code path to confirm the DB based behavior.
    self.mock(task_queues, 'probably_has_capacity', lambda *_: None)
    self.assertEqual(True, bot_management.has_capacity(d))

    self.assertEqual(True, bot_management.has_capacity(or_dimensions))

  def test_has_capacity_BotEvent(self):
    # Disable the memcache code path to confirm the DB based behavior.
    self.mock(task_queues, 'probably_has_capacity', lambda *_: None)

    d = {u'pool': [u'default'], u'os': [u'Ubuntu-16.04']}
    botid = 'id1'
    _bot_event(
        event_type='request_sleep',
        dimensions={'id': [botid], 'pool': ['default'], 'os': ['Ubuntu',
          'Ubuntu-16.04']})
    self.assertEqual(True, bot_management.has_capacity(d))

    or_dimensions = {
        u'pool': [u'default'],
        u'os': [u'Ubuntu-14.04|Ubuntu-16.04'],
    }

    # Delete the BotInfo, so the bot will disappear.
    bot_management.get_info_key(botid).delete()
    # The capacity is still found due to a recent BotEvent with this dimension.
    self.assertEqual(True, bot_management.has_capacity(d))
    self.assertEqual(True, bot_management.has_capacity(or_dimensions))

    self.mock_now(self.now, config.settings().bot_death_timeout_secs-1)
    self.assertEqual(True, bot_management.has_capacity(d))
    self.assertEqual(True, bot_management.has_capacity(or_dimensions))

    self.mock_now(self.now, config.settings().bot_death_timeout_secs)
    self.assertEqual(False, bot_management.has_capacity(d))
    self.assertEqual(False, bot_management.has_capacity(or_dimensions))

  def test_get_pools_from_dimensions_flat(self):
    pools = bot_management.get_pools_from_dimensions_flat(
        ['id:id1', 'os:Linux', 'pool:pool1', 'pool:pool2'])
    self.assertEqual(pools, ['pool1', 'pool2'])

  def test_cron_update_bot_info(self):
    # Create two bots, one becomes dead, updating the cron job fixes composite.
    timeout = bot_management.config.settings().bot_death_timeout_secs

    def check_dead(bots):
      q = bot_management.filter_availability(
          bot_management.BotInfo.query(), quarantined=None, in_maintenance=None,
          is_dead=True, is_busy=None)
      self.assertEqual(bots, [t.to_dict() for t in q])

    def check_alive(bots):
      q = bot_management.filter_availability(
          bot_management.BotInfo.query(),
          quarantined=None,
          in_maintenance=None,
          is_dead=False,
          is_busy=None)
      self.assertEqual(bots, [t.to_dict() for t in q])

    _bot_event(event_type='request_sleep')
    # One second before the timeout value.
    then = self.mock_now(self.now, timeout-1)
    _bot_event(
        event_type='request_sleep',
        bot_id='id2',
        external_ip='8.8.4.4', authenticated_as='bot:id2.domain',
        dimensions={'id': ['id2'], 'foo': ['bar']})

    bot1_alive = _gen_bot_info(
        first_seen_ts=self.now, idle_since_ts=self.now, last_seen_ts=self.now)
    bot1_dead = _gen_bot_info(
        first_seen_ts=self.now,
        last_seen_ts=self.now,
        composite=[
            bot_management.BotInfo.NOT_IN_MAINTENANCE,
            bot_management.BotInfo.DEAD,
            bot_management.BotInfo.HEALTHY,
            bot_management.BotInfo.BUSY,
        ],
        is_dead=True)
    bot2_alive = _gen_bot_info(
        authenticated_as=u'bot:id2.domain',
        dimensions={
            u'foo': [u'bar'],
            u'id': [u'id2']
        },
        first_seen_ts=then,
        id='id2',
        idle_since_ts=then,
        last_seen_ts=then)
    check_dead([])
    check_alive([bot1_alive, bot2_alive])
    self.assertEqual(0, bot_management.cron_update_bot_info())
    check_dead([])
    check_alive([bot1_alive, bot2_alive])

    # Just stale enough to trigger the dead logic.
    then = self.mock_now(self.now, timeout)
    # The cron job didn't run yet, so it still has ALIVE bit.
    check_dead([])
    check_alive([bot1_alive, bot2_alive])
    self.assertEqual(1, bot_management.cron_update_bot_info())
    # The cron job ran, so it's now correct.
    check_dead([bot1_dead])
    check_alive([bot2_alive])

    # the last event should be bot_missing
    events = list(bot_management.get_events_query('id1'))
    event = events[0]
    bq_event = swarming_pb2.BotEvent()
    event.to_proto(bq_event)

    self.assertEqual(event.event_type, 'bot_missing')
    self.assertEqual(event.last_seen_ts, bot1_dead['last_seen_ts'])
    self.assertEqual(bq_event.event, swarming_pb2.BOT_MISSING)
    self.assertEqual(bq_event.bot.status, swarming_pb2.MISSING)
    last_seen_ts = timestamp_pb2.Timestamp()
    last_seen_ts.FromDatetime(bot1_dead['last_seen_ts'])
    self.assertEqual(bq_event.bot.info.last_seen_ts, last_seen_ts)

  def test_cron_delete_old_bot_events(self):
    # Create an old BotEvent right at the cron job cut off, and another one one
    # second later (that will be kept).
    _bot_event(event_type='bot_connected')
    now = self.now
    self.mock_now(now, 1)
    event_key = _bot_event(event_type='bot_connected')
    self.mock_now(now + bot_management._OLD_BOT_EVENTS_CUT_OFF, 0)
    self.assertEqual(1, bot_management.cron_delete_old_bot_events())
    actual = bot_management.BotEvent.query().fetch(keys_only=True)
    self.assertEqual([event_key], actual)

  def test_cron_delete_old_bot(self):
    # Create a Bot with no BotEvent and another bot with one.
    event_key = _bot_event(bot_id=u'id1', event_type='request_sleep')
    # Delete the BotEvent entity.
    _bot_event(bot_id=u'id2', event_type='request_sleep').delete()
    # BotRoot + BotInfo.
    self.assertEqual(2, bot_management.cron_delete_old_bot())
    actual = bot_management.BotEvent.query().fetch(keys_only=True)
    self.assertEqual([event_key], actual)
    self.assertEqual(
        [u'id1'],
        [k.string_id() for k in
          bot_management.BotRoot.query().fetch(keys_only=True)])

  def test_cron_aggregate_dimensions(self):
    _ensure_bot_info(
        bot_id='id1', dimensions={
            'pool': ['p1', 'p2'],
            'foo1': ['bar1']
        })
    _ensure_bot_info(
        bot_id='id2', dimensions={
            'pool': ['p3'],
            'foo2': ['bar2']
        })
    bot_management.cron_aggregate_dimensions()

    # dimensions of all pools.
    dims_all = bot_management.get_aggregation_key('all').get().dimensions
    self.assertEqual(dims_all, [
        bot_management.DimensionValues(dimension='foo1', values=['bar1']),
        bot_management.DimensionValues(dimension='foo2', values=['bar2']),
        bot_management.DimensionValues(
            dimension='pool', values=['p1', 'p2', 'p3']),
    ])

    # dimensions of p1.
    dims_p1 = bot_management.get_aggregation_key('p1').get().dimensions
    self.assertEqual(dims_p1, [
        bot_management.DimensionValues(dimension='foo1', values=['bar1']),
        bot_management.DimensionValues(dimension='pool', values=['p1', 'p2']),
    ])

    # dimensions of p2.
    dims_p1 = bot_management.get_aggregation_key('p2').get().dimensions
    self.assertEqual(dims_p1, [
        bot_management.DimensionValues(dimension='foo1', values=['bar1']),
        bot_management.DimensionValues(dimension='pool', values=['p1', 'p2']),
    ])

    # dimensions of p2.
    dims_p1 = bot_management.get_aggregation_key('p3').get().dimensions
    self.assertEqual(dims_p1, [
        bot_management.DimensionValues(dimension='foo2', values=['bar2']),
        bot_management.DimensionValues(dimension='pool', values=['p3']),
    ])

  def test_filter_dimensions(self):
    pass # Tested in handlers_endpoints_test

  def test_filter_availability(self):
    pass # Tested in handlers_endpoints_test

  def test_task_bq_empty(self):
    # Empty, nothing is done.
    start = utils.utcnow()
    end = start+datetime.timedelta(seconds=60)
    self.assertEqual(0, bot_management.task_bq_events(start, end))

  def test_task_bq_events(self):
    payloads = []
    def send_to_bq(table_name, rows):
      self.assertEqual('bot_events', table_name)
      payloads.append(rows)

    self.mock(bq_state, 'send_to_bq', send_to_bq)

    # Generate a few events.
    start = self.mock_now(self.now, 10)
    _bot_event(bot_id=u'id1', event_type='bot_connected')
    self.mock_now(self.now, 11)
    _bot_event(event_type='request_sleep')  # stored
    self.mock_now(self.now, 12)
    _bot_event(event_type='request_sleep')  # not stored
    self.mock_now(self.now, 13)
    _bot_event(event_type='request_sleep', quarantined=True)  # stored
    self.mock_now(self.now, 14)
    _bot_event(event_type='request_sleep', quarantined=True)  # not stored
    self.mock_now(self.now, 15)
    _bot_event(event_type='request_sleep')  # stored
    self.mock_now(self.now, 16)
    _bot_event(event_type='request_sleep')  # not stored
    self.mock_now(self.now, 17)
    _bot_event(event_type='request_sleep', maintenance_msg='foo')  # stored
    self.mock_now(self.now, 18)
    _bot_event(event_type='request_sleep', maintenance_msg='bar')  # not stored
    self.mock_now(self.now, 19)
    _bot_event(event_type='request_sleep')  # stored
    self.mock_now(self.now, 20)
    _bot_event(event_type='request_sleep')  # not stored
    self.mock_now(self.now, 21)
    _bot_event(event_type='request_task', task_id='12311', task_name='yo')
    self.mock_now(self.now, 22)
    _bot_event(event_type='task_update', task_id='12311') # not stored
    self.mock_now(self.now, 23)
    _bot_event(event_type='task_completed', task_id='12311')
    self.mock_now(self.now, 24)
    _bot_event(event_type='request_sleep')  # stored
    end = self.mock_now(self.now, 25)

    with mock.patch(
        'components.pubsub.publish_multi',
        side_effect=lambda _topic, messages: list(messages)) as mocked:
      # normal request_sleep is not streamed.
      bot_management.task_bq_events(start, end)
      mocked.assert_called()
    self.assertEqual(1, len(payloads))
    actual_rows = payloads[0]
    expected = [
        (r[0], bot_management.BotEvent._MAPPING[r[1]]) for r in [
            # (bq_key, event)
            ('id1:2010-01-02T03:04:15.000006Z', 'bot_connected'),
            ('id1:2010-01-02T03:04:16.000006Z',
             'request_sleep'),  # dimensions update
            ('id1:2010-01-02T03:04:18.000006Z',
             'request_sleep'),  # quarantine start
            ('id1:2010-01-02T03:04:20.000006Z', 'request_sleep'),  # recovered
            ('id1:2010-01-02T03:04:22.000006Z',
             'request_sleep'),  # maintenance start
            ('id1:2010-01-02T03:04:24.000006Z', 'request_sleep'),  # recovered
            ('id1:2010-01-02T03:04:26.000006Z', 'request_task'),
            ('id1:2010-01-02T03:04:28.000006Z', 'task_completed'),
            ('id1:2010-01-02T03:04:29.000006Z', 'request_sleep'),  # first idle
        ]
    ]
    self.assertEqual(expected, [(r[0], r[1].event) for r in actual_rows])


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
