#!/usr/bin/env python
# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import test_env
test_env.setup_test_env()

import mock
import webtest

from test_support import test_case

from proto import service_config_pb2
import main
import storage



class HandlersTest(test_case.TestCase):
  def setUp(self):
    super(HandlersTest, self).setUp()
    self.app = webtest.TestApp(main.create_html_app())

  def test_schemas(self):
    self.mock(storage, 'get_self_config', mock.Mock())
    storage.get_self_config.return_value = service_config_pb2.SchemasCfg(
        schemas=[
          service_config_pb2.SchemasCfg.Schema(
              name='projects/refs.cfg',
              url='http://somehost/refs.proto',
          )],
    )

    response = self.app.get('/schemas/projects/refs.cfg', status=302)
    self.assertEqual(
        'http://somehost/refs.proto', response.headers.get('Location'))

    self.app.get('/schemas/non-existent', status=404)

  def test_schemas_no_schemas_cfg(self):
    self.mock(storage, 'get_latest_as_message', mock.Mock())
    storage.get_latest_as_message.return_value = None
    self.app.get('/schemas/non-existent', status=404)


if __name__ == '__main__':
  test_env.main()
