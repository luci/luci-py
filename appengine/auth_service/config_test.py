#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from test_support import test_case
from components import config as config_component

import config


class ConfigTest(test_case.TestCase):
  def test_refetch_config(self):
    # Mock of "current known revision" state.
    revs = {
      'a.cfg': config.Revision('old_a_rev', 'urla'),
      'b.cfg': config.Revision('old_b_rev', 'urlb'),
      'c.cfg': config.Revision('old_c_rev', 'urlc'),
    }

    bumps = []
    def bump_rev(pkg, rev, conf):
      revs[pkg] = rev
      bumps.append((pkg, rev, conf, ndb.in_transaction()))
      return True

    self.mock(config, 'is_remote_configured', lambda: True)
    self.mock(config, '_CONFIG_SCHEMAS', {
      # Will be updated outside of auth db transaction.
      'a.cfg': {
        'proto_class': None,
        'revision_getter': lambda: revs['a.cfg'],
        'validator': lambda body: self.assertEqual(body, 'new a body'),
        'updater': lambda rev, conf: bump_rev('a.cfg', rev, conf),
        'use_authdb_transaction': False,
      },
      # Will not be changed.
      'b.cfg': {
        'proto_class': None,
        'revision_getter': lambda: revs['b.cfg'],
        'validator': lambda _body: True,
        'updater': lambda rev, conf: bump_rev('b.cfg', rev, conf),
        'use_authdb_transaction': False,
      },
      # Will be updated instance auth db transaction.
      'c.cfg': {
        'proto_class': None,
        'revision_getter': lambda: revs['c.cfg'],
        'validator': lambda body: self.assertEqual(body, 'new c body'),
        'updater': lambda rev, conf: bump_rev('c.cfg', rev, conf),
        'use_authdb_transaction': True,
      },
    })
    self.mock(config, '_fetch_configs', lambda _: {
      'a.cfg': (config.Revision('new_a_rev', 'urla'), 'new a body'),
      'b.cfg': (config.Revision('old_b_rev', 'urlb'), 'old b body'),
      'c.cfg': (config.Revision('new_c_rev', 'urlc'), 'new c body'),
    })

    # Initial update.
    config.refetch_config()
    self.assertEqual([
      ('a.cfg', config.Revision('new_a_rev', 'urla'), 'new a body', False),
      ('c.cfg', config.Revision('new_c_rev', 'urlc'), 'new c body', True),
    ], bumps)
    del bumps[:]

    # Refetch, nothing new.
    config.refetch_config()
    self.assertFalse(bumps)

  def test_update_imports_config(self):
    new_rev = config.Revision('rev', 'url')
    body = 'tarball{url:"a" systems:"b"}'
    self.assertTrue(config._update_imports_config(new_rev, body))
    self.assertEqual(new_rev, config._get_imports_config_revision())

  def test_fetch_configs_ok(self):
    fetches = {
      'imports.cfg': ('imports_cfg_rev', 'tarball{url:"a" systems:"b"}'),
      'ip_whitelist.cfg': ('ip_whitelist_cfg_rev', 'TODO'),
      'oauth.cfg': ('oauth_cfg_rev', 'TODO'),
    }
    @ndb.tasklet
    def get_self_config_mock(path, *_args, **_kwargs):
      self.assertIn(path, fetches)
      raise ndb.Return(fetches.pop(path))
    self.mock(config_component, 'get_self_config_async', get_self_config_mock)
    self.mock(config, '_get_configs_url', lambda: 'http://url')
    result = config._fetch_configs(fetches.keys())
    self.assertFalse(fetches)
    self.assertEqual({
      'imports.cfg': (
          config.Revision('imports_cfg_rev', 'http://url'),
          'tarball{url:"a" systems:"b"}'),
      'ip_whitelist.cfg': (
          config.Revision('ip_whitelist_cfg_rev', 'http://url'),
          'TODO'),
      'oauth.cfg': (
          config.Revision('oauth_cfg_rev', 'http://url'),
          'TODO'),
    }, result)

  def test_fetch_configs_not_valid(self):
    @ndb.tasklet
    def get_self_config_mock(*_args, **_kwargs):
      raise ndb.Return(('imports_cfg_rev', 'bad config'))
    self.mock(config_component, 'get_self_config_async', get_self_config_mock)
    self.mock(config, '_get_configs_url', lambda: 'http://url')
    with self.assertRaises(config_component.CannotLoadConfigError):
      config._fetch_configs(['imports.cfg'])

  def test_gitiles_url(self):
    self.assertEqual(
        'https://host/repo/+/aaa/path/b/c.cfg',
        config._gitiles_url('https://host/repo/+/HEAD/path', 'aaa', 'b/c.cfg'))
    self.assertEqual(
        'https://not-gitiles',
        config._gitiles_url('https://not-gitiles', 'aaa', 'b/c.cfg'))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
