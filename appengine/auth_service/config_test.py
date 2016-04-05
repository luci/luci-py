#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import datetime
import logging
import sys
import unittest

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import config as config_component
from components import utils
from components.auth import model
from components.config import validation_context
from test_support import test_case

from proto import config_pb2
import config


class ConfigTest(test_case.TestCase):
  def setUp(self):
    super(ConfigTest, self).setUp()
    utils.clear_cache(config.get_delegation_config)

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

  def test_validate_ip_whitelist_config_ok(self):
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(
              name='abc',
              subnets=['127.0.0.1/32', '0.0.0.0/0']),
          config_pb2.IPWhitelistConfig.IPWhitelist(
              name='bots',
              subnets=[]),
        ],
        assignments=[
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:abc@example.com',
              ip_whitelist_name='abc'),
        ])
    config._validate_ip_whitelist_config(conf)

  def test_validate_ip_whitelist_config_empty(self):
    config._validate_ip_whitelist_config(config_pb2.IPWhitelistConfig())

  def test_validate_ip_whitelist_config_bad_name(self):
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(name='<bad name>'),
        ])
    with self.assertRaises(ValueError):
      config._validate_ip_whitelist_config(conf)

  def test_validate_ip_whitelist_config_duplicated_wl(self):
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(name='abc'),
          config_pb2.IPWhitelistConfig.IPWhitelist(name='abc'),
        ])
    with self.assertRaises(ValueError):
      config._validate_ip_whitelist_config(conf)

  def test_validate_ip_whitelist_config_bad_subnet(self):
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(
              name='abc',
              subnets=['not a subnet']),
        ])
    with self.assertRaises(ValueError):
      config._validate_ip_whitelist_config(conf)

  def test_validate_ip_whitelist_config_bad_identity(self):
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(name='abc')
        ],
        assignments=[
          config_pb2.IPWhitelistConfig.Assignment(
              identity='bad identity',
              ip_whitelist_name='abc'),
        ])
    with self.assertRaises(ValueError):
      config._validate_ip_whitelist_config(conf)

  def test_validate_ip_whitelist_config_unknown_whitelist(self):
    conf = config_pb2.IPWhitelistConfig(
        assignments=[
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:abc@example.com',
              ip_whitelist_name='missing'),
        ])
    with self.assertRaises(ValueError):
      config._validate_ip_whitelist_config(conf)

  def test_validate_ip_whitelist_config_identity_twice(self):
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(name='abc'),
          config_pb2.IPWhitelistConfig.IPWhitelist(name='def'),
        ],
        assignments=[
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:abc@example.com',
              ip_whitelist_name='abc'),
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:abc@example.com',
              ip_whitelist_name='def'),
        ])
    with self.assertRaises(ValueError):
      config._validate_ip_whitelist_config(conf)

  def test_update_ip_whitelist_config(self):
    @ndb.transactional
    def run(conf):
      return config._update_ip_whitelist_config(
          config.Revision('ip_whitelist_cfg_rev', 'http://url'), conf)
    # Pushing empty config to empty DB -> no changes.
    self.assertFalse(run(config_pb2.IPWhitelistConfig()))

    # Added a bunch of IP whitelists and assignments.
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(
              name='abc',
              subnets=['0.0.0.1/32']),
          config_pb2.IPWhitelistConfig.IPWhitelist(
              name='bots',
              subnets=['0.0.0.2/32']),
          config_pb2.IPWhitelistConfig.IPWhitelist(name='empty'),
        ],
        assignments=[
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:abc@example.com',
              ip_whitelist_name='abc'),
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:def@example.com',
              ip_whitelist_name='bots'),
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:xyz@example.com',
              ip_whitelist_name='bots'),
        ])
    self.mock_now(datetime.datetime(2014, 1, 2, 3, 4, 5))
    self.assertTrue(run(conf))

    # Verify everything is there.
    self.assertEqual({
      'assignments': [
        {
          'comment':
              u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
          'created_by': model.Identity(kind='service', name='sample-app'),
          'created_ts': datetime.datetime(2014, 1, 2, 3, 4, 5),
          'identity': model.Identity(kind='user', name='abc@example.com'),
          'ip_whitelist': u'abc',
        },
        {
          'comment':
              u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
          'created_by': model.Identity(kind='service', name='sample-app'),
          'created_ts': datetime.datetime(2014, 1, 2, 3, 4, 5),
          'identity': model.Identity(kind='user', name='def@example.com'),
          'ip_whitelist': u'bots',
        },
        {
          'comment':
              u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
          'created_by': model.Identity(kind='service', name='sample-app'),
          'created_ts': datetime.datetime(2014, 1, 2, 3, 4, 5),
          'identity': model.Identity(kind='user', name='xyz@example.com'),
          'ip_whitelist': u'bots',
        },
      ],
      'auth_db_rev': 1,
      'auth_db_prev_rev': None,
      'modified_by': model.get_service_self_identity(),
      'modified_ts': datetime.datetime(2014, 1, 2, 3, 4, 5),
    }, model.ip_whitelist_assignments_key().get().to_dict())
    self.assertEqual(
        {
          'abc': {
            'created_by': 'service:sample-app',
            'created_ts': 1388631845000000,
            'description':
                u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
            'modified_by': 'service:sample-app',
            'modified_ts': 1388631845000000,
            'subnets': [u'0.0.0.1/32'],
          },
          'bots': {
            'created_by': 'service:sample-app',
            'created_ts': 1388631845000000,
            'description':
                u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
            'modified_by': 'service:sample-app',
            'modified_ts': 1388631845000000,
            'subnets': [u'0.0.0.2/32'],
          },
          'empty': {
            'created_by': 'service:sample-app',
            'created_ts': 1388631845000000,
            'description':
                u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
            'modified_by': 'service:sample-app',
            'modified_ts': 1388631845000000,
            'subnets': [],
          },
        },
        {
          x.key.id(): x.to_serializable_dict()
          for x in model.AuthIPWhitelist.query(ancestor=model.root_key())
        })

    # Exact same config a bit later -> no changes applied.
    self.mock_now(datetime.datetime(2014, 2, 2, 3, 4, 5))
    self.assertFalse(run(conf))

    # Modify whitelist, add new one, remove some. Same for assignments.
    conf = config_pb2.IPWhitelistConfig(
        ip_whitelists=[
          config_pb2.IPWhitelistConfig.IPWhitelist(
              name='abc',
              subnets=['0.0.0.3/32']),
          config_pb2.IPWhitelistConfig.IPWhitelist(
              name='bots',
              subnets=['0.0.0.2/32']),
          config_pb2.IPWhitelistConfig.IPWhitelist(name='another'),
        ],
        assignments=[
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:abc@example.com',
              ip_whitelist_name='abc'),
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:def@example.com',
              ip_whitelist_name='another'),
          config_pb2.IPWhitelistConfig.Assignment(
              identity='user:zzz@example.com',
              ip_whitelist_name='bots'),
        ])
    self.mock_now(datetime.datetime(2014, 3, 2, 3, 4, 5))
    self.assertTrue(run(conf))

    # Verify everything is there.
    self.assertEqual({
      'assignments': [
        {
          'comment':
              u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
          'created_by': model.Identity(kind='service', name='sample-app'),
          'created_ts': datetime.datetime(2014, 1, 2, 3, 4, 5),
          'identity': model.Identity(kind='user', name='abc@example.com'),
          'ip_whitelist': u'abc',
        },
        {
          'comment':
              u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
          'created_by': model.Identity(kind='service', name='sample-app'),
          'created_ts': datetime.datetime(2014, 3, 2, 3, 4, 5),
          'identity': model.Identity(kind='user', name='def@example.com'),
          'ip_whitelist': u'another',
        },
        {
          'comment':
              u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
          'created_by': model.Identity(kind='service', name='sample-app'),
          'created_ts': datetime.datetime(2014, 3, 2, 3, 4, 5),
          'identity': model.Identity(kind='user', name='zzz@example.com'),
          'ip_whitelist': u'bots',
        },
      ],
      'auth_db_rev': 1,
      'auth_db_prev_rev': 1, # replicate_auth_db is mocked, so no version bump
      'modified_by': model.get_service_self_identity(),
      'modified_ts': datetime.datetime(2014, 3, 2, 3, 4, 5),
    }, model.ip_whitelist_assignments_key().get().to_dict())
    self.assertEqual(
        {
          'abc': {
            'created_by': 'service:sample-app',
            'created_ts': 1388631845000000,
            'description':
                u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
            'modified_by': 'service:sample-app',
            'modified_ts': 1393729445000000,
            'subnets': [u'0.0.0.3/32'],
          },
          'bots': {
            'created_by': 'service:sample-app',
            'created_ts': 1388631845000000,
            'description':
                u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
            'modified_by': 'service:sample-app',
            'modified_ts': 1388631845000000,
            'subnets': [u'0.0.0.2/32'],
          },
          'another': {
            'created_by': 'service:sample-app',
            'created_ts': 1393729445000000,
            'description':
                u'Imported from ip_whitelist.cfg at rev ip_whitelist_cfg_rev',
            'modified_by': 'service:sample-app',
            'modified_ts': 1393729445000000,
            'subnets': [],
          },
        },
        {
          x.key.id(): x.to_serializable_dict()
          for x in model.AuthIPWhitelist.query(ancestor=model.root_key())
        })

  def test_update_oauth_config(self):
    self.mock_now(datetime.datetime(2014, 1, 2, 3, 4, 5))
    @ndb.transactional
    def run(conf):
      return config._update_oauth_config(
          config.Revision('oauth_cfg_rev', 'http://url'), conf)
    model.AuthGlobalConfig(key=model.root_key()).put()
    # Pushing empty config to empty state -> no changes.
    self.assertFalse(run(config_pb2.OAuthConfig()))
    # Updating config.
    self.assertTrue(run(config_pb2.OAuthConfig(
        primary_client_id='a',
        primary_client_secret='b',
        client_ids=['c', 'd'])))
    self.assertEqual({
      'auth_db_rev': 1,
      'auth_db_prev_rev': None,
      'modified_by': model.get_service_self_identity(),
      'modified_ts': datetime.datetime(2014, 1, 2, 3, 4, 5),
      'oauth_additional_client_ids': ['c', 'd'],
      'oauth_client_id': 'a',
      'oauth_client_secret': 'b',
    }, model.root_key().get().to_dict())
    # Same config again -> no changes.
    self.assertFalse(run(config_pb2.OAuthConfig(
        primary_client_id='a',
        primary_client_secret='b',
        client_ids=['c', 'd'])))

  def test_fetch_configs_ok(self):
    fetches = {
      'imports.cfg': ('imports_cfg_rev', 'tarball{url:"a" systems:"b"}'),
      'ip_whitelist.cfg': (
          'ip_whitelist_cfg_rev', config_pb2.IPWhitelistConfig()),
      'oauth.cfg': (
          'oauth_cfg_rev', config_pb2.OAuthConfig(primary_client_id='a')),
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
          config_pb2.IPWhitelistConfig()),
      'oauth.cfg': (
          config.Revision('oauth_cfg_rev', 'http://url'),
          config_pb2.OAuthConfig(primary_client_id='a')),
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

  @staticmethod
  def call_validate_delegation_config(r):
    ctx = validation_context.Context()
    conf = config_pb2.DelegationConfig(rules=[r])
    config.validate_delegation_config(conf, ctx)
    return [m.text for m in ctx.messages]

  def test_validate_delegation_config_ok(self):
    # Good one.
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            user_id=['service:abc'],
            target_service=['service:def'],
            max_validity_duration=3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual([], msg)

  def test_validate_delegation_config_user_id(self):
    # Empty user_id list.
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            target_service=['service:def'],
            max_validity_duration=3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual(['rules #0: missing user_id field'], msg)

    # Bad user_id.
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            user_id=['wat'],
            target_service=['service:def'],
            max_validity_duration=3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual([
      'rules #0: bad identity string "wat" in user_id: '
      'Missing \':\' separator in Identity string'
    ], msg)

    # Star + redundant entries.
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            user_id=['*', 'user:a@a.com'],
            target_service=['service:def'],
            max_validity_duration=3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual(
        ['rules #0: redundant entries in user_id, it has "*"" already'], msg)

  def test_validate_delegation_config_target_service(self):
    # Empty target_service list.
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            user_id=['service:abc'],
            max_validity_duration=3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual(['rules #0: missing target_service field'], msg)

    # Bad target_service.
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            user_id=['service:abc'],
            target_service=['wat'],
            max_validity_duration=3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual([
      'rules #0: bad identity string "wat" in target_service: '
      'Missing \':\' separator in Identity string'
    ], msg)

    # Star + redundant entries.
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            user_id=['service:abc'],
            target_service=['*', 'service:def'],
            max_validity_duration=3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual(
        ['rules #0: redundant entries in target_service, it has "*"" already'],
        msg)

  def test_validate_delegation_config_max_validity_duration(self):
    msg = self.call_validate_delegation_config(
        config_pb2.DelegationConfig.Rule(
            user_id=['service:abc'],
            target_service=['service:def'],
            max_validity_duration=-3600,
            allowed_to_impersonate=['user:a@a.com', 'bot:*', 'group:abc']))
    self.assertEqual(
        ['rules #0: max_validity_duration must be a positive integer'], msg)

  def test_validate_delegation_config_allowed_to_impersonate(self):
    def call(uid):
      return self.call_validate_delegation_config(
          config_pb2.DelegationConfig.Rule(
              user_id=['*'],
              target_service=['*'],
              max_validity_duration=3600,
              allowed_to_impersonate=[uid]))
    # Good cases.
    self.assertEqual([], call('group:abc'))
    self.assertEqual([], call('group:external/abc'))
    self.assertEqual([], call('user:*'))
    self.assertEqual([], call('service:abc'))
    # Bad cases.
    self.assertEqual(
        ['rules #0: not a valid group name: bad\\name'],
        call('group:bad\\name'))
    self.assertEqual([
      'rules #0: not a valid identity glob "hmm:*": '
      'Invalid Identity kind: hmm'
    ], call('hmm:*'))
    self.assertEqual([
      'rules #0: not a valid identity glob "*": Missing \':\' '
      'separator in IdentityGlob string'
    ], call('*'))
    self.assertEqual([
      'rules #0: not a valid identity "a@a.com": Missing \':\' '
      'separator in Identity string'
    ], call('a@a.com'))

  def test_update_service_config(self):
    # Missing.
    self.assertIsNone(config._get_service_config('abc.cfg'))
    self.assertIsNone(config._get_service_config_rev('abc.cfg'))
    # Updated.
    rev = config.Revision('rev', 'url')
    self.assertTrue(config._update_service_config('abc.cfg', rev, 'body'))
    self.assertEqual('body', config._get_service_config('abc.cfg'))
    self.assertEqual(rev, config._get_service_config_rev('abc.cfg'))
    # Same body, returns False, though updates rev.
    rev2 = config.Revision('rev2', 'url')
    self.assertFalse(config._update_service_config('abc.cfg', rev2, 'body'))
    self.assertEqual(rev2, config._get_service_config_rev('abc.cfg'))

  def test_get_delegation_config(self):
    # Missing -> returns empty proto.
    proto = config.get_delegation_config()
    self.assertFalse(proto.rules)
    # Add some.
    body = """rules {
      user_id: "service:abc"
      target_service: "*"
      max_validity_duration: 3600
    }"""
    config._update_service_config(
        'delegation.cfg', config.Revision('rev', 'url'), body)
    utils.clear_cache(config.get_delegation_config)
    proto = config.get_delegation_config()
    self.assertEqual(1, len(proto.rules))


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
