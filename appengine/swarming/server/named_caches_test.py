#!/usr/bin/env python
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import hashlib
import logging
import sys
import unittest

# pylint: disable=wrong-import-position
import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import utils
from server import bot_management
from server import named_caches
from server import pools_config
from test_support import test_case


def _bot_event(bot_id, pool, caches):
  """Calls bot_management.bot_event with default arguments."""
  dimensions = {
    u'id': [bot_id],
    u'os': [u'Ubuntu', u'Ubuntu-16.04'],
    u'pool': [pool],
  }
  # Format is named_cache: {name: [['shortname', size], timestamp]}.
  state = {
    'named_cache': {
      name: [['a', size], 10] for name, size in caches.iteritems()
    }
  }
  bot_management.bot_event(
      event_type='bot_connected',
      bot_id=bot_id,
      external_ip='8.8.4.4',
      authenticated_as=u'bot:%s.domain' % bot_id,
      dimensions=dimensions,
      state=state or {'ram': 65},
      version=unicode(hashlib.sha256().hexdigest()),
      quarantined=False,
      maintenance_msg=None,
      task_id=None,
      task_name=None)


class NamedCachesTest(test_case.TestCase):
  APP_DIR = test_env.APP_DIR

  def test_all(self):
    self.mock(utils, 'enqueue_task', self._enqueue_task)
    self.mock(pools_config, 'known', lambda: ['first', 'second'])
    _bot_event('first1', 'first', {'git': 1})
    _bot_event('first2', 'first', {'build': 100000, 'git': 1000})
    # Create 45 bots with cache 'foo' size between 1 and 45.
    for i in xrange(45):
      _bot_event('second%d' % i, 'second', {'foo': i+1})
    named_caches.cron_update_named_caches()

    hints = named_caches.get_hints('first', ['git', 'build', 'new'])
    self.assertEqual([1000, 100000, -1], hints)
    hints = named_caches.get_hints('second', ['foo'])
    # Roughly P(95).
    self.assertEqual([43], hints)

  @ndb.non_transactional
  def _enqueue_task(self, url, queue_name, params):
    if queue_name == 'named-cache-task':
      self.assertEqual(True, named_caches.task_update_pool(params['pool']))
      return True
    self.fail(url)
    return False


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  logging.basicConfig(
      level=logging.DEBUG if '-v' in sys.argv else logging.CRITICAL)
  unittest.main()
