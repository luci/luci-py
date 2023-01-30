#!/usr/bin/env vpython
# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import logging
import mock
import unittest
import sys

import swarming_test_env
swarming_test_env.setup_test_env()

from google.appengine.api import datastore_errors
from parameterized import parameterized

from test_support import test_case
from server import bot_management
import api_common
import handlers_exceptions


def _bot_event(bot_id=None,
               external_ip='8.8.4.4',
               authenticated_as=None,
               dimensions=None,
               state=None,
               version=u"12345",
               quarantined=False,
               maintenance_msg=None,
               task_id=None,
               task_name=None,
               register_dimensions=False,
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
  return bot_management.bot_event(bot_id=bot_id,
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


class ApiCommonTest(test_case.TestCase):
  APP_DIR = swarming_test_env.APP_DIR
  no_run = 1

  @parameterized.expand([
      ("BadValueError", datastore_errors.BadValueError),
      ("TypeError", TypeError),
      ("ValueError", ValueError),
  ])
  def test_correct_error_handling_terminate_bot(self, _name, error_type):
    with mock.patch('server.realms.check_bot_terminate_acl'):
      _bot_event(bot_id='bot1', event_type='bot_connected')
      with mock.patch('server.task_request.create_termination_task') as m:
        m.side_effect = error_type
        with self.assertRaises(handlers_exceptions.BadRequestException):
          api_common.terminate_bot('bot1')

  def test_race_condition_handling_get_bot(self):
    """Tests a specific race condition described http://go/crb/1407381"""
    with mock.patch('server.realms.check_bot_get_acl'):
      self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5, 6))
      bot_id = 'bot1'
      _bot_event(bot_id=bot_id, event_type='bot_connected')
      expected_bot = bot_management.get_info_key(bot_id).get()
      bot_management.get_info_key(bot_id).delete()
      get_info_key = bot_management.get_info_key
      with mock.patch('server.bot_management.get_info_key',
                      new_callable=mock.Mock) as m:

        def side_effect_once(bot_id):
          m.side_effect = get_info_key
          bot_key = get_info_key(bot_id=bot_id)
          self.assertIsNone(bot_key.get())
          _bot_event(bot_id=bot_id, event_type='bot_connected')
          return bot_key

        m.side_effect = side_effect_once
        bot, deleted = api_common.get_bot(bot_id)
        self.assertFalse(deleted)
        self.assertEqual(expected_bot, bot)


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.ERROR)
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
