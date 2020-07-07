#!/usr/bin/env vpython
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# pylint: disable=no-value-for-parameter

import logging
import os
import sys
import unittest

import test_env
test_env.setup_test_env()

import mock
import parameterized

from test_support import test_case

from components.auth import model
from components.config import fs

from realms import config
from realms import permissions


def fake_db(rev, perms=None):
  b = permissions.Builder(rev)
  for p in (perms or []):
    b.permission(p)
  return b.finish()


def fake_realms_rev(project_id, config_digest, perms_rev):
  return config.RealmsCfgRev(
      project_id, 'config-rev', config_digest, 'config-body', perms_rev)


class CheckConfigChangesTest(test_case.TestCase):
  @mock.patch('realms.config.update_realms', autospec=True)
  @mock.patch('realms.config.delete_realms', autospec=True)
  def call(self, db, latest, stored, delete_realms_mock, update_realms_mock):
    updated = set()
    deleted = set()
    batches = []

    def do_update(_db, revs, _comment):
      batches.append(len(revs))
      for r in revs:
        self.assertNotIn(r.project_id, updated)
        self.assertNotIn(r.project_id, deleted)
        updated.add(r.project_id)
    update_realms_mock.side_effect = do_update

    def do_delete(project_id):
      self.assertNotIn(project_id, updated)
      self.assertNotIn(project_id, deleted)
      deleted.add(project_id)
    delete_realms_mock.side_effect = do_delete

    jobs = config.check_config_changes(db, latest, stored)
    self.assertTrue(config.execute_jobs(jobs, 0.0))

    return updated, deleted, batches

  def test_noop_when_up_to_date(self):
    updated, deleted, _ = self.call(
        fake_db('db-rev'),
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
            fake_realms_rev('proj2', 'digest1', 'db-rev'),
        ],
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
            fake_realms_rev('proj2', 'digest1', 'db-rev'),
        ])
    self.assertEqual(updated, set())
    self.assertEqual(deleted, set())

  def test_new_projects(self):
    updated, deleted, _ = self.call(
        fake_db('db-rev'),
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
            fake_realms_rev('proj2', 'digest1', 'db-rev'),
        ],
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
        ])
    self.assertEqual(updated, {'proj2'})
    self.assertEqual(deleted, set())

  def test_updated_projects(self):
    updated, deleted, _ = self.call(
        fake_db('db-rev'),
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
            fake_realms_rev('proj2', 'digest1', 'db-rev'),
        ],
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
            fake_realms_rev('proj2', 'digest2', 'db-rev'),
        ])
    self.assertEqual(updated, {'proj2'})
    self.assertEqual(deleted, set())

  def test_deleted_projects(self):
    updated, deleted, _ = self.call(
        fake_db('db-rev'),
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
        ],
        [
            fake_realms_rev('proj1', 'digest1', 'db-rev'),
            fake_realms_rev('proj2', 'digest2', 'db-rev'),
        ])
    self.assertEqual(updated, set())
    self.assertEqual(deleted, {'proj2'})

  def test_perms_revision_change(self):
    revs = [
        fake_realms_rev('proj%d' % i, 'digest1', 'db-rev1')
        for i in range(20)
    ]
    updated, deleted, batches = self.call(fake_db('db-rev2'), revs, revs)
    self.assertEqual(updated, {p.project_id for p in revs})  # all of them
    self.assertEqual(deleted, set())
    self.assertEqual(len(batches), config.DB_REEVAL_REVISIONS)


class CheckPermissionChangesTest(test_case.TestCase):
  def call(self, db):
    jobs = config.check_permission_changes(db)
    self.assertTrue(config.execute_jobs(jobs, 0.0))

  def test_works(self):
    def perms_from_authdb():
      e = model.realms_globals_key().get()
      return [p.name for p in e.permissions] if e else []

    # The initial state.
    self.assertEqual(model.get_auth_db_revision(), 0)
    self.assertEqual(perms_from_authdb(), [])

    # Create the initial copy of AuthRealmsGlobals.
    self.call(fake_db('rev1', ['luci.dev.p1', 'luci.dev.p2']))
    self.assertEqual(model.get_auth_db_revision(), 1)
    self.assertEqual(perms_from_authdb(), ['luci.dev.p1', 'luci.dev.p2'])

    # Noop change.
    self.call(fake_db('rev1', ['luci.dev.p1', 'luci.dev.p2']))
    self.assertEqual(model.get_auth_db_revision(), 1)
    self.assertEqual(perms_from_authdb(), ['luci.dev.p1', 'luci.dev.p2'])

    # Real change.
    self.call(fake_db('rev2', ['luci.dev.p3']))
    self.assertEqual(model.get_auth_db_revision(), 2)
    self.assertEqual(perms_from_authdb(), ['luci.dev.p3'])


class ProjectConfigFetchTest(test_case.TestCase):
  @mock.patch('components.config.common.self_config_set', autospec=True)
  @mock.patch('components.config.fs.get_provider', autospec=True)
  def test_works(self, get_provider_mock, self_config_set_mock):
    TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
    get_provider_mock.return_value = fs.Provider(
        os.path.join(TESTS_DIR, 'test_data'))

    # See test_data/... layout.
    self_config_set_mock.return_value = 'services/auth-service-app-id'

    revs = config.get_latest_revs_async().get_result()
    self.assertEqual(sorted(revs, key=lambda r: r.project_id), [
        config.RealmsCfgRev(
            project_id='@internal',
            config_rev='unknown',
            config_digest='90549bf56e8be6c0ff6001d2376db' +
                'def519b97cc89e65b2813237b252300dea8',
            config_body='realms {\n  name: "internal-realm"\n}\n',
            perms_rev=None,
        ),
        config.RealmsCfgRev(
            project_id='proj1',
            config_rev='unknown',
            config_digest='05105846cbabf80e1ab2979b7787' +
                'f1df1aca9751661fe4b4d28494e0b442459b',
            config_body='realms {\n  name: "realm1"\n}\n',
            perms_rev=None,
        ),
        config.RealmsCfgRev(
            project_id='proj2',
            config_rev='unknown',
            config_digest='fe0857c4fe4282083c0295ee835e7' +
                '96403027d13c652f4959a0c6a41957dbc18',
            config_body='realms {\n  name: "realm2"\n}\n',
            perms_rev=None,
        ),
    ])


class RealmsUpdateTest(test_case.TestCase):
  @parameterized.parameterized.expand([
      ('some-proj',),
      ('@internal',),
  ])
  def test_realms_config_lifecycle(self, project_id):
    self.assertEqual(model.get_auth_db_revision(), 0)

    # A new config appears.
    rev = config.RealmsCfgRev(
        project_id=project_id,
        config_rev='cfg_rev1',
        config_digest='digest1',
        config_body='realms{ name: "realm1" }',
        perms_rev=None)
    config.update_realms(fake_db('db-rev1'), [rev], 'New config')

    # Generated new AuthDB revisions.
    self.assertEqual(model.get_auth_db_revision(), 1)

    # Stored now in the expanded form.
    ent = model.project_realms_key(project_id).get()
    self.assertEqual(
        [r.name for r in ent.realms.realms],
        ['%s:@root' % project_id, '%s:realm1' % project_id])
    self.assertEqual(ent.config_rev, 'cfg_rev1')
    self.assertEqual(ent.perms_rev, 'db-rev1')

    # Permissions DB changes in a way that doesn't affect the expanded form.
    config.update_realms(fake_db('db-rev2'), [rev], 'Reeval')

    # Seeing the same AuthDB version.
    self.assertEqual(model.get_auth_db_revision(), 1)

    # The config body changes in a way that doesn't affect the expanded form.
    rev = config.RealmsCfgRev(
        project_id=project_id,
        config_rev='cfg_rev2',
        config_digest='digest2',
        config_body='realms{ name: "realm1" }  # blah blah',
        perms_rev=None)
    config.update_realms(fake_db('db-rev2'), [rev], 'Updated config')

    # Still the same AuthDB version.
    self.assertEqual(model.get_auth_db_revision(), 1)

    # The config change significantly now.
    rev = config.RealmsCfgRev(
        project_id=project_id,
        config_rev='cfg_rev3',
        config_digest='digest3',
        config_body='realms{ name: "realm2" }',
        perms_rev=None)
    config.update_realms(fake_db('db-rev2'), [rev], 'Updated config')

    # New revision.
    self.assertEqual(model.get_auth_db_revision(), 2)

    # And new body.
    ent = model.project_realms_key(project_id).get()
    self.assertEqual(
        [r.name for r in ent.realms.realms],
        ['%s:@root' % project_id, '%s:realm2' % project_id])
    self.assertEqual(ent.config_rev, 'cfg_rev3')
    self.assertEqual(ent.perms_rev, 'db-rev2')

    # The config is gone.
    config.delete_realms(project_id)

    # This generated a new revision.
    self.assertEqual(model.get_auth_db_revision(), 3)

    # And it is indeed gone.
    ent = model.project_realms_key(project_id).get()
    self.assertIsNone(ent)

    # The second deletion is noop.
    config.delete_realms(project_id)
    self.assertEqual(model.get_auth_db_revision(), 3)


  def test_update_many_projects(self):
    self.assertEqual(model.get_auth_db_revision(), 0)

    cfg_rev = lambda proj, realm, rev_sfx: config.RealmsCfgRev(
        project_id=proj,
        config_rev='cfg-rev-'+rev_sfx,
        config_digest='digest-'+rev_sfx,
        config_body='realms{ name: "%s" }' % realm,
        perms_rev=None)

    # Create a bunch of project configs at once.
    config.update_realms(
        fake_db('db-rev1'),
        [
            cfg_rev('proj1', 'realm1', 'p1s1'),
            cfg_rev('proj2', 'realm1', 'p2s1'),
        ],
        'New config')

    # Produced a single revision.
    self.assertEqual(model.get_auth_db_revision(), 1)

    # Present now.
    revs = config.get_stored_revs_async().get_result()
    self.assertEqual(revs, [
        config.RealmsCfgRev(
            project_id='proj1',
            config_rev=u'cfg-rev-p1s1',
            config_digest=u'digest-p1s1',
            config_body=None,
            perms_rev=u'db-rev1',
        ),
        config.RealmsCfgRev(
            project_id='proj2',
            config_rev=u'cfg-rev-p2s1',
            config_digest=u'digest-p2s1',
            config_body=None,
            perms_rev=u'db-rev1',
        ),
    ])
    self.assertEqual(
        model.project_realms_key('proj1').get().config_rev, 'cfg-rev-p1s1')
    self.assertEqual(
        model.project_realms_key('proj2').get().config_rev, 'cfg-rev-p2s1')

    # One is modified significantly, another not.
    config.update_realms(
        fake_db('db-rev1'),
        [
            cfg_rev('proj1', 'realm1', 'p1s2'),  # noop change
            cfg_rev('proj2', 'realm2', 'p2s2'),  # significant change
        ],
        'New config')

    revs = config.get_stored_revs_async().get_result()
    self.assertEqual(
        model.project_realms_key('proj1').get().config_rev, 'cfg-rev-p1s1')
    self.assertEqual(
        model.project_realms_key('proj2').get().config_rev, 'cfg-rev-p2s2')

    # One config is broken.
    config.update_realms(
        fake_db('db-rev1'),
        [
            cfg_rev('proj1', 'realm3', 'p1s3'),
            cfg_rev('proj2', '@@@@@@', 'p2s3'),
        ],
        'New config')
    revs = config.get_stored_revs_async().get_result()
    self.assertEqual(
        model.project_realms_key('proj1').get().config_rev, 'cfg-rev-p1s3')
    self.assertEqual(
        model.project_realms_key('proj2').get().config_rev, 'cfg-rev-p2s2')


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
