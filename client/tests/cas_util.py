# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging
import os
import subprocess
import tempfile
import time

CLIENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LUCI_DIR = os.path.dirname(CLIENT_DIR)
FAKECAS_BIN = os.path.join(LUCI_DIR, 'luci-go', 'fakecas')
CAS_CLI = os.path.join(LUCI_DIR, 'luci-go', 'cas')


class LocalCAS(object):
  def __init__(self, root):
    self._root = root
    self._proc = None
    self._addr = None
    self._log = None

  @property
  def address(self):
    return self._addr

  @property
  def _log_path(self):
    return os.path.join(self._root, 'cas.log')

  def start(self):
    if not os.path.exists(self._root):
      os.makedirs(self._root)
    self._log = open(self._log_path, 'wb')
    addr_file = os.path.join(self._root, 'addr')
    cmd = [
        FAKECAS_BIN,
        '-port',
        '0',
        '-addr-file',
        addr_file,
    ]
    self._proc = subprocess.Popen(cmd,
                                  stdout=self._log,
                                  stderr=subprocess.STDOUT)
    while not os.path.exists(addr_file):
      logging.info('Waiting cas to start...')
      time.sleep(0.1)
    with open(addr_file) as f:
      self._addr = f.read()
    logging.info('Launched cas local at %s, log is %s', self._addr,
                 self._log_path)

  def stop(self):
    if self._proc:
      self._proc.terminate()
      self._proc.wait()
      self._log.close()

  def archive_files(self, files):
    """Uploads contents to the local CAS server"""
    with tempfile.TemporaryDirectory() as tmpdir:
      for path, content in files.items():
        with open(os.path.join(tmpdir, path), 'wb') as f:
          f.write(content)
      return self.archive_dir(tmpdir)

  def archive_dir(self, upload_dir):
    """Uploads directory to the local CAS server"""
    with tempfile.TemporaryDirectory() as tmpdir:
      digest_dump = os.path.join(tmpdir, 'digest')
      cmd = [
          CAS_CLI,
          'archive',
          '-cas-addr',
          self.address,
          '-paths',
          upload_dir + ':.',
          '-dump-digest',
          digest_dump,
          '-log-level',
          'debug',
      ]
      proc = subprocess.Popen(cmd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
      out = proc.communicate()[0]
      if proc.returncode:
        raise Exception(
            'Failed to run cas archive. exit_code=%d, cmd="%s"\n%s' %
            (proc.returncode, ' '.join(cmd), out.decode('unicode-escape')))
      with open(digest_dump) as f:
        return f.read()
