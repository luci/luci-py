#!/usr/bin/env python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Starts local Swarming and CAS servers."""

from __future__ import print_function

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time


APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LUCI_DIR = os.path.dirname(os.path.dirname(APP_DIR))
CLIENT_DIR = os.path.join(LUCI_DIR, 'client')
sys.path.insert(0, APP_DIR)
sys.path.insert(0, os.path.join(CLIENT_DIR, 'tests'))
sys.path.insert(0, os.path.join(CLIENT_DIR, 'third_party'))

EXECUTABLE_SUFFIX = '.exe' if sys.platform == 'win32' else ''
FAKECAS_BIN = os.path.join(LUCI_DIR, 'luci-go', 'fakecas' + EXECUTABLE_SUFFIX)

from depot_tools import fix_encoding
sys.path.pop(0)

import swarming_test_env
swarming_test_env.setup_test_env()

import cas_util
from tool_support import local_app


class LocalServers(object):
  """Local Swarming and CAS servers."""

  def __init__(self, listen_all, root):
    self._swarming_server = None
    self._cas = None
    self._listen_all = listen_all
    self._root = root

  @property
  def swarming_server(self):
    return self._swarming_server

  @property
  def cas_server(self):
    return self._cas

  @property
  def http_client(self):
    """Returns the raw local_app.HttpClient."""
    return self._swarming_server.client

  def start(self):
    """Starts both the Swarming and CAS and CAS servers."""
    self._cas = cas_util.LocalCAS(os.path.join(self._root, 'cas-local'))
    self._cas.start()
    self._swarming_server = local_app.LocalApplication(
        APP_DIR, 9050, self._listen_all, self._root, 'swarming-local')
    self._swarming_server.start()
    self._swarming_server.ensure_serving()

    self.http_client.login_as_admin('smoke-test@example.com')
    self.http_client.url_opener.addheaders.append(
        ('X-XSRF-Token', self._swarming_server.client.xsrf_token))

  def stop(self):
    """Stops the local Swarming and CAS servers.

    Returns the exit code with priority to non-zero.
    """
    exit_code = None
    try:
      self._cas.stop()
    finally:
      if self._swarming_server:
        exit_code = exit_code or self._swarming_server.stop()
    return exit_code

  def wait(self):
    """Wait for the processes to normally exit."""
    if self._swarming_server:
      self._swarming_server.wait()

  def dump_log(self):
    if self._swarming_server:
      self._swarming_server.dump_log()


def main():
  fix_encoding.fix_encoding()
  parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
  parser.add_argument(
      '-a', '--all', action='store_true', help='allow non local connection')
  parser.add_argument(
      '-l', '--leak', action='store_true',
      help='leak logs instead of deleting on shutdown')
  args = parser.parse_args()
  root = tempfile.mkdtemp(prefix='start_servers')
  try:
    servers = LocalServers(args.all, root)
    dump_log = True
    try:
      servers.start()
      print('Logs    : %s' % root)
      print('Swarming: %s' % servers.swarming_server.url)
      print('CAS     : %s' % servers.cas_server.address)
      servers.wait()
    except KeyboardInterrupt:
      print('<Ctrl-C> received; stopping servers', file=sys.stderr)
      dump_log = False
    finally:
      exit_code = servers.stop()
      if dump_log:
        servers.dump_log()
  finally:
    if not args.leak:
      shutil.rmtree(root)
  return exit_code


if __name__ == '__main__':
  sys.exit(main())
