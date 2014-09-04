#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

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

from support import test_case

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
  return auth.Identity(auth.IDENTITY_USER, '%s@example.com' % name)


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

  def mock_urlfetch(self, url, bundle):
    def mock_get_access_token(_scope):
      return 'token', 0
    self.mock(importer.app_identity, 'get_access_token', mock_get_access_token)

    @ndb.tasklet
    def mock_fetch(**kwargs):
      self.assertEqual(url, kwargs['url'])
      self.assertEqual({'Authorization': 'OAuth token'}, kwargs['headers'])
      class ReturnValue(object):
        status_code = 200
        content = build_tar_gz(bundle)
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

  def test_parse_group_file_ok(self):
    body = '\n'.join(['b', 'a'])
    expected = [
      auth.Identity.from_bytes('user:a@example.com'),
      auth.Identity.from_bytes('user:b@example.com'),
    ]
    self.assertEqual(expected, importer.parse_group_file(body, 'example.com'))

  def test_parse_group_file_bad_id(self):
    body = 'bad id'
    with self.assertRaises(ValueError):
      importer.parse_group_file(body, 'example.com')

  def test_parse_group_file_empty_lines(self):
    body = 'a\n\n'
    with self.assertRaises(ValueError):
      importer.parse_group_file(body, 'example.com')

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

  def test_fetch_bundle_async(self):
    bundle = {
      'at_root': 'a\nb',
      'ldap/ bad name': 'a\nb',
      'ldap/group-a': 'a\nb',
      'ldap/group-b': 'a\nb',
      'ldap/group-c': 'a\nb',
      'ldap/deeper/group-a': 'a\nb',
      'not-ldap/group-a': 'a\nb',
    }
    self.mock_urlfetch('https://fake_url', bundle)

    result = importer.fetch_bundle_async(
        url='https://fake_url',
        systems=['ldap'],
        groups=['ldap/group-a', 'ldap/group-b'],
        oauth_scopes=['scope'],
        domain='example.com').get_result()

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

  def test_fetch_bundle_async_bad_group(self):
    bundle = {
      'at_root': 'a\nb',
      'ldap/group-a': 'a\n\n\n',
    }
    self.mock_urlfetch('https://fake_url', bundle)

    future = importer.fetch_bundle_async(
        url='https://fake_url',
        systems=['ldap'],
        groups=['ldap/group-a', 'ldap/group-b'],
        oauth_scopes=['scope'],
        domain='example.com')

    with self.assertRaises(importer.BundleBadFormatError):
      future.get_result()

  def test_import_external_groups(self):
    self.mock_now(datetime.datetime(2010, 1, 2, 3, 4, 5, 6))

    service_id = auth.Identity.from_bytes('service:some-service')
    self.mock(auth, 'get_service_self_identity', lambda: service_id)

    importer.write_config([
      {
        'domain': 'example.com',
        'groups': ['ldap/new'],
        'systems': ['ldap'],
        'url': 'https://fake_url',
      }
    ])

    @ndb.tasklet
    def mock_fetch_bundle_async(*_args, **_kwargs):
      raise ndb.Return({
        'ldap': {
          'ldap/new': [ident('a'), ident('b')],
        },
      })
    self.mock(importer, 'fetch_bundle_async', mock_fetch_bundle_async)

    # Should be deleted during import, since not in a imported bundle.
    group('ldap/deleted', []).put()
    initial_auth_db_rev = model.get_auth_db_revision()

    # Verify initial state.
    self.assertEqual(['ldap/deleted'], fetch_groups().keys())

    # Run the import.
    ret = importer.import_external_groups()
    self.assertTrue(ret)

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
    }
    self.assertEqual(expected_groups, fetch_groups())
    self.assertEqual(initial_auth_db_rev + 1, model.get_auth_db_revision())


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.FATAL)
  unittest.main()
