#!/usr/bin/env vpython3
# coding=utf-8
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import os
import sys
import tempfile
import unittest

# Mutates sys.path.
import test_env

from utils import file_path
from utils import fs


def write_content(path, content):
  with fs.open(path, 'wb') as f:
    f.write(content)


class FSTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    if not file_path.enable_symlink():
      raise Exception('Failed to enable symlink support')

  def setUp(self):
    super(FSTest, self).setUp()
    self._tempdir = None

  def tearDown(self):
    try:
      if self._tempdir:
        file_path.rmtree(self._tempdir)
    finally:
      super(FSTest, self).tearDown()

  @property
  def tempdir(self):
    if not self._tempdir:
      self._tempdir = tempfile.mkdtemp(prefix='fs_test')
    return self._tempdir

  def test_symlink_relative(self):
    # A symlink to a relative path is valid.
    # /dir
    # /dir/file
    # /ld -> /dir
    # /lf -> /ld/file
    dirpath = os.path.join(self.tempdir, 'dir')
    filepath = os.path.join(dirpath, 'file')
    fs.mkdir(dirpath)
    write_content(filepath, b'hello')

    linkfile = os.path.join(self.tempdir, 'lf')
    linkdir = os.path.join(self.tempdir, 'ld')
    dstfile = os.path.join('ld', 'file')
    fs.symlink(dstfile, linkfile)
    fs.symlink('dir', linkdir)

    self.assertEqual(True, fs.islink(linkfile))
    self.assertEqual(True, fs.islink(linkdir))
    self.assertEqual(dstfile, fs.readlink(linkfile))
    self.assertEqual('dir', fs.readlink(linkdir))
    self.assertEqual(['file'], fs.listdir(linkdir))
    # /lf resolves to /dir/file.
    with fs.open(linkfile) as f:
      self.assertEqual('hello', f.read())

    # Ensures that followlinks is respected in walk().
    expected = [
      (self.tempdir, ['dir', 'ld'], ['lf']),
      (dirpath, [], ['file']),
    ]
    actual = [
      (r, sorted(d), sorted(f))
      for r, d, f in sorted(fs.walk(self.tempdir, followlinks=False))
    ]
    self.assertEqual(expected, actual)
    expected = [
      (self.tempdir, ['dir', 'ld'], ['lf']),
      (dirpath, [], ['file']),
      (linkdir, [], ['file']),
    ]
    actual = [
      (r, sorted(d), sorted(f))
      for r, d, f in sorted(fs.walk(self.tempdir, followlinks=True))
    ]
    self.assertEqual(expected, actual)

  def test_symlink_absolute(self):
    # A symlink to an absolute path is valid.
    # /dir
    # /dir/file
    # /ld -> /dir
    # /lf -> /ld/file
    dirpath = os.path.join(self.tempdir, 'dir')
    filepath = os.path.join(dirpath, 'file')
    fs.mkdir(dirpath)
    write_content(filepath, b'hello')

    linkfile = os.path.join(self.tempdir, 'lf')
    linkdir = os.path.join(self.tempdir, 'ld')
    dstfile = os.path.join(linkdir, 'file')
    fs.symlink(dstfile, linkfile)
    fs.symlink(dirpath, linkdir)

    self.assertEqual(True, fs.islink(linkfile))
    self.assertEqual(True, fs.islink(linkdir))
    self.assertEqual(dstfile, fs.readlink(linkfile))
    self.assertEqual(dirpath, fs.readlink(linkdir))
    self.assertEqual(['file'], fs.listdir(linkdir))
    # /lf resolves to /dir/file.
    with fs.open(linkfile) as f:
      self.assertEqual('hello', f.read())

    # Ensures that followlinks is respected in walk().
    expected = [
      (self.tempdir, ['dir', 'ld'], ['lf']),
      (dirpath, [], ['file']),
    ]
    actual = [
      (r, sorted(d), sorted(f))
      for r, d, f in sorted(fs.walk(self.tempdir, followlinks=False))
    ]
    self.assertEqual(expected, actual)
    expected = [
      (self.tempdir, ['dir', 'ld'], ['lf']),
      (dirpath, [], ['file']),
      (linkdir, [], ['file']),
    ]
    actual = [
      (r, sorted(d), sorted(f))
      for r, d, f in sorted(fs.walk(self.tempdir, followlinks=True))
    ]
    self.assertEqual(expected, actual)

  def test_symlink_missing_destination_rel(self):
    # A symlink to a missing destination is valid and can be read back.
    filepath = 'file'
    linkfile = os.path.join(self.tempdir, 'lf')
    fs.symlink(filepath, linkfile)

    self.assertEqual(True, fs.islink(linkfile))
    self.assertEqual(filepath, fs.readlink(linkfile))

  def test_symlink_missing_destination_abs(self):
    # A symlink to a missing destination is valid and can be read back.
    filepath = os.path.join(self.tempdir, 'file')
    linkfile = os.path.join(self.tempdir, 'lf')
    fs.symlink(filepath, linkfile)

    self.assertEqual(True, fs.islink(linkfile))
    self.assertEqual(filepath, fs.readlink(linkfile))

  def test_symlink_existing(self):
    # Creating a symlink that overrides a file fails.
    filepath = os.path.join(self.tempdir, 'file')
    linkfile = os.path.join(self.tempdir, 'lf')
    write_content(linkfile, b'hello')
    with self.assertRaises(OSError):
      fs.symlink(filepath, linkfile)

  def test_readlink_fail(self):
    # Reading a non-existing symlink fails. Obvious but it's to make sure the
    # Windows part acts the same.
    with self.assertRaises(OSError):
      fs.readlink(os.path.join(self.tempdir, 'not_there'))

  def test_remove_invalid_symlink(self):
    src = os.path.join(self.tempdir, 'src')
    dst = os.path.join(self.tempdir, 'dst')
    os.symlink(src, dst)  # This is invalid symlink.
    fs.remove(dst)


if __name__ == '__main__':
  test_env.main()
