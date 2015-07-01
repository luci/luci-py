#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import logging

from test_env import future
import test_env
test_env.setup_test_env()

from google.appengine.ext import ndb

from test_support import test_case
import mock

from components import config
from components import net
from components.config import validation_context

from proto import project_config_pb2
from proto import service_config_pb2
import storage
import validation


class ValidationTestCase(test_case.TestCase):
  def test_validate_validation_cfg(self):
    cfg = '''
      rules {
        config_set: "projects/foo"
        path: "bar.cfg"
        url: "https://foo.com/validate_config"
      }
      rules {
        config_set: "regex:projects\/foo"
        path: "regex:.+"
        url: "https://foo.com/validate_config"
      }
      rules {
        config_set: "bad config set name"
        path: "regex:))bad regex"
        # no url
      }
      rules {
        config_set: "regex:)("
        path: "/bar.cfg"
        url: "http://not-https.com"
      }
      rules {
        config_set: "projects/foo"
        path: "a/../b.cfg"
        url: "https://foo.com/validate_config"
      }
      rules {
        config_set: "projects/foo"
        path: "a/./b.cfg"
        url: "/no/hostname"
      }
    '''
    result = validation.validate_config(
        config.self_config_set(), 'validation.cfg', cfg)

    self.assertEqual(
        [m.text for m in result.messages],
        [
          'Rule #3: config_set: invalid config set: bad config set name',
          ('Rule #3: path: invalid regular expression "))bad regex": '
           'unbalanced parenthesis'),
          'Rule #3: url: not specified',
          ('Rule #4: config_set: invalid regular expression ")(": '
            'unbalanced parenthesis'),
          'Rule #4: path: must not be absolute: /bar.cfg',
          'Rule #4: url: scheme must be "https"',
          'Rule #5: path: must not contain ".." or "." components: a/../b.cfg',
          'Rule #6: path: must not contain ".." or "." components: a/./b.cfg',
          'Rule #6: url: hostname not specified',
          'Rule #6: url: scheme must be "https"',
        ]
    )

  def test_validate_project_registry(self):
    cfg = '''
      projects {
        id: "a"
        config_storage_type: GITILES
        config_location: "https://a.googlesource.com/project"
      }
      projects {
        id: "b"
      }
      projects {
        id: "a"
        config_storage_type: GITILES
        config_location: "https://no-project.googlesource.com"
      }
      projects {
        config_storage_type: GITILES
        config_location: "https://no-project.googlesource.com/bad_plus/+"
      }
    '''
    result = validation.validate_config(
        config.self_config_set(), 'projects.cfg', cfg)

    self.assertEqual(
        [m.text for m in result.messages],
        [
          'Project b: config_storage_type is not set',
          'Project a: id is not unique',
          ('Project a: config_location: Invalid Gitiles repo url: '
           'https://no-project.googlesource.com'),
          'Project #4: id is not specified',
          ('Project #4: config_location: Invalid Gitiles repo url: '
           'https://no-project.googlesource.com/bad_plus/+'),
          'Project list is not sorted by id. First offending id: a',
        ]
    )

  def test_validate_schemas(self):
    cfg = '''
      schemas {
        name: "services/config:foo"
        url: "https://foo"
      }
      schemas {
        name: "projects:foo"
        url: "https://foo"
      }
      schemas {
        name: "projects/refs:foo"
        url: "https://foo"
      }
      # Invalid schemas.
      schemas {
      }
      schemas {
        name: "services/config:foo"
        url: "https://foo"
      }
      schemas {
        name: "no_colon"
        url: "http://foo"
      }
      schemas {
        name: "bad_prefix:foo"
        url: "https://foo"
      }
      schemas {
        name: "projects:foo/../a.cfg"
        url: "https://foo"
      }
    '''
    result = validation.validate_config(
        config.self_config_set(), 'schemas.cfg', cfg)

    self.assertEqual(
        [m.text for m in result.messages],
        [
          'Schema #4: name is not specified',
          'Schema #4: url: not specified',
          'Schema services/config:foo: duplicate schema name',
          'Schema no_colon: name must contain ":"',
          'Schema no_colon: url: scheme must be "https"',
          (
            'Schema bad_prefix:foo: left side of ":" must be a service config '
            'set, "projects" or "projects/refs"'),
          (
            'Schema projects:foo/../a.cfg: '
            'must not contain ".." or "." components: foo/../a.cfg'),
        ]
    )

  def test_validate_refs(self):
    cfg = '''
      refs {
        name: "refs/heads/master"
      }
      # Invalid configs
      refs {

      }
      refs {
        name: "refs/heads/master"
        config_path: "non_default"
      }
      refs {
        name: "does_not_start_with_ref"
        config_path: "../bad/path"
      }
    '''
    result = validation.validate_config('projects/x', 'refs.cfg', cfg)

    self.assertEqual(
        [m.text for m in result.messages],
        [
          'Ref #2: name is not specified',
          'Ref #3: duplicate ref: refs/heads/master',
          'Ref #4: name does not start with "refs/": does_not_start_with_ref',
          'Ref #4: must not contain ".." or "." components: ../bad/path'
        ],
    )

  def test_endpoint_validate_async(self):
    cfg = '# a config'
    cfg_b64 = base64.b64encode(cfg)

    self.mock(storage, 'get_self_config_async', mock.Mock())
    storage.get_self_config_async.return_value = future(
        service_config_pb2.ValidationCfg(
            rules=[
              service_config_pb2.ValidationCfg.Rule(
                config_set='services/foo',
                path='bar.cfg',
                url='https://bar.verifier',
              ),
              service_config_pb2.ValidationCfg.Rule(
                config_set='regex:projects/[^/]+',
                path='regex:.+.\cfg',
                url='https://bar2.verifier',
              ),
              service_config_pb2.ValidationCfg.Rule(
                config_set='regex:.+',
                path='regex:.+',
                url='https://ultimate.verifier',
              ),
            ]
          ))

    @ndb.tasklet
    def json_request_async(url, **kwargs):
      raise ndb.Return({
        'messages': [{
          'text': 'OK from %s' % url,
          # default severity
        }],
      })

    self.mock(
        net, 'json_request_async', mock.Mock(side_effect=json_request_async))

    ############################################################################

    result = validation.validate_config('services/foo', 'bar.cfg', cfg)
    self.assertEqual(
        result.messages,
        [
          validation_context.Message(
              text='OK from https://bar.verifier', severity=logging.INFO),
          validation_context.Message(
              text='OK from https://ultimate.verifier', severity=logging.INFO)
        ])
    net.json_request_async.assert_any_call(
      'https://bar.verifier',
      method='POST',
      payload={
        'config_set': 'services/foo',
        'path': 'bar.cfg',
        'content': cfg_b64,
      },
      scope='https://www.googleapis.com/auth/userinfo.email',
    )
    net.json_request_async.assert_any_call(
      'https://ultimate.verifier',
      method='POST',
      payload={
        'config_set': 'services/foo',
        'path': 'bar.cfg',
        'content': cfg_b64,
      },
      scope='https://www.googleapis.com/auth/userinfo.email',
    )

    ############################################################################

    result = validation.validate_config('projects/foo', 'bar.cfg', cfg)
    self.assertEqual(
        result.messages,
        [
          validation_context.Message(
              text='OK from https://bar2.verifier', severity=logging.INFO),
          validation_context.Message(
              text='OK from https://ultimate.verifier', severity=logging.INFO)
        ])
    net.json_request_async.assert_any_call(
      'https://bar2.verifier',
      method='POST',
      payload={
        'config_set': 'projects/foo',
        'path': 'bar.cfg',
        'content': cfg_b64,
      },
      scope='https://www.googleapis.com/auth/userinfo.email',
    )
    net.json_request_async.assert_any_call(
      'https://ultimate.verifier',
      method='POST',
      payload={
        'config_set': 'projects/foo',
        'path': 'bar.cfg',
        'content': cfg_b64,
      },
      scope='https://www.googleapis.com/auth/userinfo.email',
    )

    ############################################################################
    # Error found

    net.json_request_async.side_effect = None
    net.json_request_async.return_value = ndb.Future()
    net.json_request_async.return_value.set_result({
      'messages': [{
        'text': 'error',
        'severity': 'ERROR'
      }]
    })

    result = validation.validate_config('projects/baz/refs/x', 'qux.cfg', cfg)
    self.assertEqual(
        result.messages,
        [
          validation_context.Message(text='error', severity=logging.ERROR)
        ])

    ############################################################################
    # Less-expected responses

    res = {
      'messages': [
        {'severity': 'invalid severity'},
        {},
        []
      ]
    }
    net.json_request_async.return_value = ndb.Future()
    net.json_request_async.return_value.set_result(res)

    result = validation.validate_config('projects/baz/refs/x', 'qux.cfg', cfg)
    self.assertEqual(
        result.messages,
        [
          validation_context.Message(
              severity=logging.CRITICAL,
              text=(
                  'Error during external validation: invalid response: '
                  'unexpected message severity: invalid severity\n'
                  'url: https://ultimate.verifier\n'
                  'config_set: projects/baz/refs/x\n'
                  'path: qux.cfg\n'
                  'response: %r' % res),
              ),
          validation_context.Message(severity=logging.INFO, text=''),
          validation_context.Message(
              severity=logging.CRITICAL,
              text=(
                  'Error during external validation: invalid response: '
                  'message is not a dict: []\n'
                  'url: https://ultimate.verifier\n'
                  'config_set: projects/baz/refs/x\n'
                  'path: qux.cfg\n'
                  'response: %r' % res),
              ),
        ],
    )


if __name__ == '__main__':
  test_env.main()
