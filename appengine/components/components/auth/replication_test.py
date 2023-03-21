#!/usr/bin/env vpython
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import datetime
import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from components import utils
from components.auth import model
from components.auth import realms
from components.auth import replication
from components.auth.proto import realms_pb2
from components.auth.proto import replication_pb2
from test_support import test_case


def entity_to_dict(e):
  """Same as e.to_dict() but also adds entity key to the dict."""
  d = e.to_dict()
  d['__id__'] = e.key.id()
  d['__parent__'] = e.key.parent()
  return d


def snapshot_to_dict(snapshot):
  """AuthDBSnapshot -> dict (for comparisons)."""
  result = {
    'global_config': entity_to_dict(snapshot.global_config),
    'groups': [entity_to_dict(g) for g in snapshot.groups],
    'ip_whitelists': [entity_to_dict(l) for l in snapshot.ip_whitelists],
    'ip_whitelist_assignments':
        entity_to_dict(snapshot.ip_whitelist_assignments),
    'realms_globals': entity_to_dict(snapshot.realms_globals),
    'project_realms': [entity_to_dict(e) for e in snapshot.project_realms],
  }
  # Ensure no new keys are forgotten.
  assert len(snapshot) == len(result)
  return result


def make_snapshot_obj(
    global_config=None, groups=None,
    ip_whitelists=None, ip_whitelist_assignments=None,
    realms_globals=None, project_realms=None):
  """Returns AuthDBSnapshot with empty list of groups and whitelists."""
  return replication.AuthDBSnapshot(
      global_config=global_config or model.AuthGlobalConfig(
          key=model.root_key(),
          oauth_client_id='oauth client id',
          oauth_client_secret='oauth client secret',
          token_server_url='token server',
          security_config='security config blob'),
      groups=groups or [],
      ip_whitelists=ip_whitelists or [],
      ip_whitelist_assignments=(
          ip_whitelist_assignments or
          model.AuthIPWhitelistAssignments(
              key=model.ip_whitelist_assignments_key())),
      realms_globals=realms_globals or model.AuthRealmsGlobals(
          key=model.realms_globals_key()),
      project_realms=project_realms or [],
  )


def make_auth_db_proto(**kwargs):
  return replication.auth_db_snapshot_to_proto(make_snapshot_obj(**kwargs))


class NewAuthDBSnapshotTest(test_case.TestCase):
  """Tests for new_auth_db_snapshot function."""

  def test_empty(self):
    state, snapshot = replication.new_auth_db_snapshot()
    self.assertIsNone(state)
    expected_snapshot = {
      'global_config': {
        '__id__': 'root',
        '__parent__': None,
        'auth_db_rev': None,
        'auth_db_prev_rev': None,
        'modified_by': None,
        'modified_ts': None,
        'oauth_additional_client_ids': [],
        'oauth_client_id': u'',
        'oauth_client_secret': u'',
        'security_config': None,
        'token_server_url': u'',
      },
      'groups': [],
      'ip_whitelists': [],
      'ip_whitelist_assignments': {
        '__id__': 'default',
        '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
        'assignments': [],
        'auth_db_rev': None,
        'auth_db_prev_rev': None,
        'modified_by': None,
        'modified_ts': None,
      },
      'realms_globals': {
        '__id__': 'globals',
        '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
        'auth_db_prev_rev': None,
        'auth_db_rev': None,
        'modified_by': None,
        'modified_ts': None,
        'permissions': [],
      },
      'project_realms': [],
    }
    self.assertEqual(expected_snapshot, snapshot_to_dict(snapshot))

  def test_non_empty(self):
    self.mock_now(datetime.datetime(2014, 1, 1, 1, 1, 1))

    state = model.AuthReplicationState(
        key=model.replication_state_key(),
        primary_id='blah',
        primary_url='https://blah',
        auth_db_rev=123)
    state.put()

    global_config = model.AuthGlobalConfig(
        key=model.root_key(),
        modified_ts=utils.utcnow(),
        modified_by=model.Identity.from_bytes('user:modifier@example.com'),
        oauth_client_id='oauth_client_id',
        oauth_client_secret='oauth_client_secret',
        oauth_additional_client_ids=['a', 'b'],
        token_server_url='https://token-server',
        security_config='security config blob')
    global_config.put()

    group = model.AuthGroup(
        key=model.group_key('Some group'),
        members=[model.Identity.from_bytes('user:a@example.com')],
        globs=[model.IdentityGlob.from_bytes('user:*@example.com')],
        nested=[],
        description='Some description',
        owners='owning-group',
        created_ts=utils.utcnow(),
        created_by=model.Identity.from_bytes('user:creator@example.com'),
        modified_ts=utils.utcnow(),
        modified_by=model.Identity.from_bytes('user:modifier@example.com'))
    group.put()

    another = model.AuthGroup(
        key=model.group_key('Another group'),
        nested=['Some group'])
    another.put()

    ip_whitelist = model.AuthIPWhitelist(
        key=model.ip_whitelist_key('bots'),
        subnets=['127.0.0.1/32'],
        description='Some description',
        created_ts=utils.utcnow(),
        created_by=model.Identity.from_bytes('user:creator@example.com'),
        modified_ts=utils.utcnow(),
        modified_by=model.Identity.from_bytes('user:modifier@example.com'))
    ip_whitelist.put()

    ip_whitelist_assignments = model.AuthIPWhitelistAssignments(
        key=model.ip_whitelist_assignments_key(),
        modified_ts=utils.utcnow(),
        modified_by=model.Identity.from_bytes('user:modifier@example.com'),
        assignments=[
          model.AuthIPWhitelistAssignments.Assignment(
            identity=model.Identity.from_bytes('user:bot_account@example.com'),
            ip_whitelist='bots',
            comment='some comment',
            created_ts=utils.utcnow(),
            created_by=model.Identity.from_bytes('user:creator@example.com')),
        ])
    ip_whitelist_assignments.put()

    realms_globals = model.AuthRealmsGlobals(
        key=model.realms_globals_key(),
        permissions=[
            realms_pb2.Permission(name='luci.dev.p1'),
            realms_pb2.Permission(name='luci.dev.p2'),
        ])
    realms_globals.put()

    model.AuthProjectRealms(
        key=model.project_realms_key('proj_id1'),
        realms=realms_pb2.Realms(api_version=1234),
        config_rev='rev1',
        perms_rev='rev1').put()
    model.AuthProjectRealms(
        key=model.project_realms_key('proj_id2'),
        realms=realms_pb2.Realms(api_version=1234),
        config_rev='rev2',
        perms_rev='rev2').put()

    captured_state, snapshot = replication.new_auth_db_snapshot()

    expected_state =  {
      'auth_db_rev': 123,
      'modified_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
      'primary_id': u'blah',
      'primary_url': u'https://blah',
      'shard_ids': [],
    }
    self.assertEqual(expected_state, captured_state.to_dict())

    expected_snapshot = {
      'global_config': {
        '__id__': 'root',
        '__parent__': None,
        'auth_db_rev': None,
        'auth_db_prev_rev': None,
        'modified_by': model.Identity(kind='user', name='modifier@example.com'),
        'modified_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
        'oauth_additional_client_ids': [u'a', u'b'],
        'oauth_client_id': u'oauth_client_id',
        'oauth_client_secret': u'oauth_client_secret',
        'security_config': 'security config blob',
        'token_server_url': u'https://token-server',
      },
      'groups': [
        {
          '__id__': 'Another group',
          '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
          'auth_db_rev': None,
          'auth_db_prev_rev': None,
          'created_by': None,
          'created_ts': None,
          'description': u'',
          'globs': [],
          'members': [],
          'modified_by': None,
          'modified_ts': None,
          'nested': [u'Some group'],
          'owners': u'administrators',
        },
        {
          '__id__': 'Some group',
          '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
          'auth_db_rev': None,
          'auth_db_prev_rev': None,
          'created_by': model.Identity(kind='user', name='creator@example.com'),
          'created_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
          'description': u'Some description',
          'globs': [model.IdentityGlob(kind='user', pattern='*@example.com')],
          'members': [model.Identity(kind='user', name='a@example.com')],
          'modified_by': model.Identity(
              kind='user', name='modifier@example.com'),
          'modified_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
          'nested': [],
          'owners': u'owning-group',
        },
      ],
      'ip_whitelists': [
        {
          '__id__': 'bots',
          '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
          'auth_db_rev': None,
          'auth_db_prev_rev': None,
          'created_by': model.Identity(kind='user', name='creator@example.com'),
          'created_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
          'description': u'Some description',
          'modified_by': model.Identity(
              kind='user', name='modifier@example.com'),
          'modified_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
          'subnets': [u'127.0.0.1/32'],
        },
      ],
      'ip_whitelist_assignments': {
        '__id__': 'default',
        '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
        'assignments': [
          {
            'comment': u'some comment',
            'created_by': model.Identity(
                kind='user', name='creator@example.com'),
            'created_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
            'identity': model.Identity(
                kind='user', name='bot_account@example.com'),
            'ip_whitelist': u'bots',
          },
        ],
        'auth_db_rev': None,
        'auth_db_prev_rev': None,
        'modified_by': model.Identity(kind='user', name='modifier@example.com'),
        'modified_ts': datetime.datetime(2014, 1, 1, 1, 1, 1),
      },
      'realms_globals': {
        '__id__': 'globals',
        '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
        'auth_db_prev_rev': None,
        'auth_db_rev': None,
        'modified_by': None,
        'modified_ts': None,
        'permissions': [
          realms_pb2.Permission(name='luci.dev.p1'),
          realms_pb2.Permission(name='luci.dev.p2'),
        ],
      },
      'project_realms': [
        {
          '__id__': 'proj_id1',
          '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
          'auth_db_prev_rev': None,
          'auth_db_rev': None,
          'config_rev': u'rev1',
          'perms_rev': u'rev1',
          'modified_by': None,
          'modified_ts': None,
          'realms': realms_pb2.Realms(api_version=1234),
        },
        {
          '__id__': 'proj_id2',
          '__parent__': ndb.Key('AuthGlobalConfig', 'root'),
          'auth_db_prev_rev': None,
          'auth_db_rev': None,
          'config_rev': u'rev2',
          'perms_rev': u'rev2',
          'modified_by': None,
          'modified_ts': None,
          'realms': realms_pb2.Realms(api_version=1234),
        }
      ],
    }
    self.assertEqual(expected_snapshot, snapshot_to_dict(snapshot))


class SnapshotToProtoConversionTest(test_case.TestCase):
  """Tests for entities -> proto conversion."""

  def test_global_config_serialization(self):
    """Serializing snapshot with non-trivial AuthGlobalConfig."""
    auth_db = make_auth_db_proto(
        global_config=model.AuthGlobalConfig(
            key=model.root_key(),
            oauth_client_id=u'some-client-id',
            oauth_client_secret=u'some-client-secret',
            oauth_additional_client_ids=[u'id1', u'id2'],
            token_server_url=u'https://example.com',
            security_config='security config blob'))
    self.assertEqual(auth_db, replication_pb2.AuthDB(
        oauth_client_id=u'some-client-id',
        oauth_client_secret=u'some-client-secret',
        oauth_additional_client_ids=[u'id1', u'id2'],
        token_server_url=u'https://example.com',
        security_config='security config blob',
        realms={'api_version': realms.API_VERSION},
    ))

  def test_group_serialization(self):
    """Serializing snapshot with non-trivial AuthGroup."""
    group = model.AuthGroup(
        key=model.group_key('some-group'),
        members=[
            model.Identity.from_bytes('user:punch@example.com'),
            model.Identity.from_bytes('user:judy@example.com'),
        ],
        globs=[model.IdentityGlob.from_bytes('user:*@example.com')],
        nested=['Group A', 'Group B'],
        description='Blah blah blah',
        created_ts=datetime.datetime(2020, 1, 1, 1, 1, 1),
        created_by=model.Identity.from_bytes('user:creator@example.com'),
        modified_ts=datetime.datetime(2020, 2, 2, 2, 2, 2),
        modified_by=model.Identity.from_bytes('user:modifier@example.com'),
    )
    auth_db = make_auth_db_proto(groups=[group])
    self.assertEqual(list(auth_db.groups), [replication_pb2.AuthGroup(
        name='some-group',
        members=['user:punch@example.com', 'user:judy@example.com'],
        globs=['user:*@example.com'],
        nested=['Group A', 'Group B'],
        description='Blah blah blah',
        created_ts=1577840461000000,
        created_by='user:creator@example.com',
        modified_ts=1580608922000000,
        modified_by='user:modifier@example.com',
        owners='administrators',
    )])

  def test_ip_whitelists_serialization(self):
    """Serializing snapshot with non-trivial IP whitelist."""
    ip_whitelist = model.AuthIPWhitelist(
        key=model.ip_whitelist_key('bots'),
        subnets=['127.0.0.1/32'],
        description='Blah blah blah',
        created_ts=datetime.datetime(2020, 1, 1, 1, 1, 1),
        created_by=model.Identity.from_bytes('user:creator@example.com'),
        modified_ts=datetime.datetime(2020, 2, 2, 2, 2, 2),
        modified_by=model.Identity.from_bytes('user:modifier@example.com'),
    )
    auth_db = make_auth_db_proto(ip_whitelists=[ip_whitelist])
    self.assertEqual(
        list(auth_db.ip_whitelists), [replication_pb2.AuthIPWhitelist(
            name='bots',
            subnets=['127.0.0.1/32'],
            description='Blah blah blah',
            created_ts=1577840461000000,
            created_by='user:creator@example.com',
            modified_ts=1580608922000000,
            modified_by='user:modifier@example.com',
        )])

  def test_ip_whitelist_assignments_serialization(self):
    """Serializing snapshot with non-trivial AuthIPWhitelistAssignments."""
    entity = model.AuthIPWhitelistAssignments(
        key=model.ip_whitelist_assignments_key(),
        assignments=[
            model.AuthIPWhitelistAssignments.Assignment(
                identity=model.Identity.from_bytes('user:a@example.com'),
                ip_whitelist='some whitelist',
                comment='some comment',
                created_ts=datetime.datetime(2020, 1, 1, 1, 1, 1),
                created_by=model.Identity.from_bytes(
                    'user:creator@example.com'),
            ),
        ],
    )
    auth_db = make_auth_db_proto(ip_whitelist_assignments=entity)
    self.assertEqual(
        list(auth_db.ip_whitelist_assignments),
        [replication_pb2.AuthIPWhitelistAssignment(
            identity='user:a@example.com',
            ip_whitelist='some whitelist',
            comment='some comment',
            created_ts=1577840461000000,
            created_by='user:creator@example.com',
        )])

  def test_realms_serialization(self):
    """Serializing snapshot with non-trivial realms configs."""
    realms_globals = model.AuthRealmsGlobals(
        key=model.realms_globals_key(),
        permissions=[
            realms_pb2.Permission(name='luci.dev.p1'),
            realms_pb2.Permission(name='luci.dev.p2'),
        ],
    )
    p1 = model.AuthProjectRealms(
        key=model.project_realms_key('proj1'),
        realms=realms_pb2.Realms(
            permissions=[{'name': 'luci.dev.p2'}],
            realms=[{
                'name': 'proj1:@root',
                'bindings': [
                    {
                        'permissions': [0],
                        'principals': ['group:gr1'],
                    },
                ],
            }],
        ),
    )
    p2 = model.AuthProjectRealms(
        key=model.project_realms_key('proj2'),
        realms=realms_pb2.Realms(
            permissions=[{'name': 'luci.dev.p1'}],
            realms=[{
                'name': 'proj2:@root',
                'bindings': [
                    {
                        'permissions': [0],
                        'principals': ['group:gr2'],
                    },
                ],
            }],
        ),
    )
    auth_db = make_auth_db_proto(
        realms_globals=realms_globals,
        project_realms=[p1, p2])
    self.assertEqual(auth_db.realms, realms_pb2.Realms(
        api_version=realms.API_VERSION,
        permissions=[{'name': 'luci.dev.p1'}, {'name': 'luci.dev.p2'}],
        realms=[
            {
                'name': 'proj1:@root',
                'bindings': [
                    {
                        'permissions': [1],
                        'principals': ['group:gr1'],
                    },
                ],
            },
            {
                'name': 'proj2:@root',
                'bindings': [
                    {
                        'permissions': [0],
                        'principals': ['group:gr2'],
                    },
                ],
            },
        ],
    ))


class ShardedAuthDBTest(test_case.TestCase):
  PRIMARY_URL = 'https://primary'
  AUTH_DB_REV = 1234

  def test_no_shards(self):
    with self.assertRaises(ValueError):
      replication.load_sharded_auth_db(self.PRIMARY_URL, self.AUTH_DB_REV, [])

  def test_works(self):
    # Make some non-empty snapshot, its contents is not important.
    auth_db = replication.auth_db_snapshot_to_proto(
        make_snapshot_obj(
            global_config=model.AuthGlobalConfig(
                key=model.root_key(),
                oauth_client_id=u'some-client-id',
                oauth_client_secret=u'some-client-secret',
                oauth_additional_client_ids=[u'id1', u'id2'],
                token_server_url=u'https://example.com',
                security_config='security config blob')))

    # Store in 50-byte shards.
    shard_ids = replication.store_sharded_auth_db(auth_db, self.PRIMARY_URL,
                                                  self.AUTH_DB_REV, 50)
    self.assertEqual(2, len(shard_ids))

    # Verify keys look OK and the shard size is respected.
    for shard_id in shard_ids:
      self.assertEqual(len(shard_id), 16)
      shard = model.snapshot_shard_key(self.PRIMARY_URL, self.AUTH_DB_REV,
                                       shard_id).get()
      self.assertTrue(len(shard.blob) <= 50)

    # Verify it can be reassembled back.
    reassembled = replication.load_sharded_auth_db(self.PRIMARY_URL,
                                                   self.AUTH_DB_REV, shard_ids)
    self.assertEqual(reassembled, auth_db)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
