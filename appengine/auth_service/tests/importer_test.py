#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import collections
import datetime
import logging
import os
import StringIO
import sys
import tarfile
import tempfile
import unittest

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from test_support import test_case

from components import auth
from components import auth_testing
from components.auth import model

from common import importer


def build_tar_gz(content):
  """Returns bytes of tar.gz archive build from {filename -> body} dict."""
  out = StringIO.StringIO()
  with tarfile.open(mode='w|gz', fileobj=out) as tar:
    for name, value in content.iteritems():
      # tarfile module doesn't support in-memory files (it tries to os.stat
      # them), so dump to disk first to keep the code simple.
      path = None
      try:
        fd, path = tempfile.mkstemp(prefix='importer_test')
        with os.fdopen(fd, 'w') as f:
          f.write(value)
        tar.add(path, arcname=name)
      finally:
        if path:
          os.remove(path)
  return out.getvalue()


def ident(name):
  if '@' not in name:
    return auth.Identity(auth.IDENTITY_USER, '%s@example.com' % name)
  else:
    return auth.Identity(auth.IDENTITY_USER, name)


def group(name, members, nested=None):
  return model.AuthGroup(
      key=model.group_key(name),
      created_by=ident('admin'),
      created_ts=datetime.datetime(1999, 1, 2, 3, 4, 5, 6),
      modified_by=ident('admin'),
      modified_ts=datetime.datetime(1999, 1, 2, 3, 4, 5, 6),
      members=[ident(x) for x in members],
      nested=nested or [])


def fetch_groups():
  return {x.key.id(): x.to_dict() for x in model.AuthGroup.query()}


class ImporterTest(test_case.TestCase):
  def setUp(self):
    super(ImporterTest, self).setUp()
    auth_testing.mock_is_admin(self, True)
    auth_testing.mock_get_current_identity(self)

  def mock_urlfetch(self, urls):
    def mock_get_access_token(_scope):
      return 'token', 0
    self.mock(importer.app_identity, 'get_access_token', mock_get_access_token)

    @ndb.tasklet
    def mock_fetch(**kwargs):
      self.assertIn(kwargs['url'], urls)
      self.assertEqual({'Authorization': 'OAuth token'}, kwargs['headers'])
      class ReturnValue(object):
        status_code = 200
        content = urls[kwargs['url']]
      raise ndb.Return(ReturnValue())
    self.mock(ndb.get_context(), 'urlfetch', mock_fetch)

  def test_extract_tar_archive(self):
    expected = {
      '0': '0',
      'a/1': '1',
      'a/2': '2',
      'b/1': '3',
      'b/c/d': '4',
    }
    out = {
      name: fileobj.read()
      for name, fileobj in importer.extract_tar_archive(build_tar_gz(expected))
    }
    self.assertEqual(expected, out)

  def test_load_group_file_ok(self):
    body = '\n'.join(['', 'b', 'a', ''])
    expected = [
      auth.Identity.from_bytes('user:a@example.com'),
      auth.Identity.from_bytes('user:b@example.com'),
    ]
    self.assertEqual(expected, importer.load_group_file(body, 'example.com'))

  def test_load_group_file_bad_id(self):
    body = 'bad id'
    with self.assertRaises(importer.BundleBadFormatError):
      importer.load_group_file(body, 'example.com')

  def test_prepare_import(self):
    service_id = auth.Identity.from_bytes('service:some-service')
    self.mock(auth, 'get_service_self_identity', lambda: service_id)

    existing_groups = [
      group('normal-group', [], ['ldap/cleared']),
      group('not-ldap/some', []),
      group('ldap/updated', ['a']),
      group('ldap/unchanged', ['a']),
      group('ldap/deleted', ['a']),
      group('ldap/cleared', ['a']),
    ]
    imported_groups = {
      'ldap/new': [ident('a')],
      'ldap/updated': [ident('a'), ident('b')],
      'ldap/unchanged': [ident('a')],
    }
    to_put, to_delete = importer.prepare_import(
        'ldap',
        existing_groups,
        imported_groups,
        datetime.datetime(2010, 1, 2, 3, 4, 5, 6))

    expected_to_put = {
      'ldap/cleared': {
        'created_by': ident('admin'),
        'created_ts': datetime.datetime(1999, 1, 2, 3, 4, 5, 6),
        'description': '',
        'globs': [],
        'members': [],
        'modified_by': service_id,
        'modified_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'nested': [],
      },
      'ldap/new': {
        'created_by': service_id,
        'created_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'description': '',
        'globs': [],
        'members': [ident('a')],
        'modified_by': service_id,
        'modified_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'nested': [],
      },
      'ldap/updated': {
        'created_by': ident('admin'),
        'created_ts': datetime.datetime(1999, 1, 2, 3, 4, 5, 6),
        'description': '',
        'globs': [],
        'members': [ident('a'), ident('b')],
        'modified_by': service_id,
        'modified_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'nested': [],
      },
    }
    self.assertEqual(expected_to_put, {x.key.id(): x.to_dict() for x in to_put})
    self.assertEqual([model.group_key('ldap/deleted')], to_delete)

  def test_load_tarball(self):
    bundle = build_tar_gz({
      'at_root': 'a\nb',
      'ldap/ bad name': 'a\nb',
      'ldap/group-a': 'a\nb',
      'ldap/group-b': 'a\nb',
      'ldap/group-c': 'a\nb',
      'ldap/deeper/group-a': 'a\nb',
      'not-ldap/group-a': 'a\nb',
    })
    result = importer.load_tarball(
        content=bundle,
        systems=['ldap'],
        groups=['ldap/group-a', 'ldap/group-b'],
        domain='example.com')

    expected = {
      'ldap': {
        'ldap/group-a': [
          auth.Identity.from_bytes('user:a@example.com'),
          auth.Identity.from_bytes('user:b@example.com')
        ],
        'ldap/group-b': [
          auth.Identity.from_bytes('user:a@example.com'),
          auth.Identity.from_bytes('user:b@example.com')
        ],
      }
    }
    self.assertEqual(expected, result)

  def test_load_tarball_bad_group(self):
    bundle = build_tar_gz({
      'at_root': 'a\nb',
      'ldap/group-a': 'a\n!!!!!',
    })
    with self.assertRaises(importer.BundleBadFormatError):
      importer.load_tarball(
        content=bundle,
        systems=['ldap'],
        groups=['ldap/group-a', 'ldap/group-b'],
        domain='example.com')

  def test_import_external_groups(self):
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5, 6))

    service_id = auth.Identity.from_bytes('service:some-service')
    self.mock(auth, 'get_service_self_identity', lambda: service_id)

    importer.write_config([
      {
        'domain': 'example.com',
        'format': 'tarball',
        'groups': ['ldap/new'],
        'oauth_scopes': ['scope'],
        'systems': ['ldap'],
        'url': 'https://fake_tarball',
      },
      {
        'format': 'plainlist',
        'group': 'external_1',
        'oauth_scopes': ['scope'],
        'url': 'https://fake_external_1',
      },
      {
        'description': 'Some external group',
        'domain': 'example.com',
        'format': 'plainlist',
        'group': 'external_2',
        'oauth_scopes': ['scope'],
        'url': 'https://fake_external_2',
      },
    ])

    self.mock_urlfetch({
      'https://fake_tarball': build_tar_gz({
        'ldap/new': 'a\nb',
      }),
      'https://fake_external_1': 'abc@test.com\ndef@test.com\n',
      'https://fake_external_2': '123\n456',
    })

    # Should be deleted during import, since not in a imported bundle.
    group('ldap/deleted', []).put()
    # Should be updated.
    group('external/external_1', ['x', 'y']).put()
    # Should be removed, since not in list of external groups.
    group('external/deleted', []).put()

    # Run the import.
    initial_auth_db_rev = model.get_auth_db_revision()
    importer.import_external_groups()
    self.assertEqual(initial_auth_db_rev + 1, model.get_auth_db_revision())

    # Verify final state.
    expected_groups = {
      'ldap/new': {
        'created_by': service_id,
        'created_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'description': u'',
        'globs': [],
        'members': [ident('a'), ident('b')],
        'modified_by': service_id,
        'modified_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'nested': [],
      },
      'external/external_1': {
        'created_by': ident('admin'),
        'created_ts': datetime.datetime(1999, 1, 2, 3, 4, 5, 6),
        'description': u'',
        'globs': [],
        'members': [ident('abc@test.com'), ident('def@test.com')],
        'modified_by': service_id,
        'modified_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'nested': [],
      },
      'external/external_2': {
        'created_by': service_id,
        'created_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'description': u'',
        'globs': [],
        'members': [ident('123'), ident('456')],
        'modified_by': service_id,
        'modified_ts': datetime.datetime(2010, 1, 2, 3, 4, 5, 6),
        'nested': [],
      },
    }
    self.assertEqual(expected_groups, fetch_groups())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
