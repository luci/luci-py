#!/usr/bin/env python3
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import unittest

import main


class Test(unittest.TestCase):

  def setUp(self):
    main.app.testing = True

  def test_index(self):
    client = main.app.test_client()
    r = client.get('/')
    assert r.status_code == 200
    assert 'Please try again!' in r.data.decode('utf-8')

  def test_warmup(self):
    client = main.app.test_client()
    r = client.get('/')
    assert r.status_code == 200


def main_impl():
  # TODO: Start the Datastore emulator. This requires Java!
  # https://cloud.google.com/datastore/docs/tools/datastore-emulator
  unittest.main()


if __name__ == '__main__':
  main_impl()
