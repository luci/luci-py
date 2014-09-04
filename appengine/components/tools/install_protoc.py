#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Downloads, builds and installs Protocol Buffers compiler (protoc)."""

import optparse
import os
import shutil
import subprocess
import sys
import tempfile


# Directory with this file.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))

# Where to install 'protoc' by default.
DEFAULT_INSTALL_PREFIX = os.path.join(THIS_DIR, 'protoc')

# Version of protobuf package to fetch and build.
PROTOC_VERSION = '2.5.0'

# Where to fetch source code from.
PACKAGE_URL = (
    'https://protobuf.googlecode.com/files/protobuf-%s.tar.bz2' %
    PROTOC_VERSION)

# Where to fetch Windows binaries from.
WIN_PACKAGE_URL = (
    'https://protobuf.googlecode.com/files/protobuf-%s.win32.zip' %
    PROTOC_VERSION)


def run(*args, **kwargs):
  print ' '.join(args)
  return subprocess.check_call(args, **kwargs)


def cd(path):
  print 'cd %s' % path
  os.chdir(path)


def build_and_install(install_prefix):
  if not os.path.exists(install_prefix):
    os.makedirs(install_prefix)
  build_dir = tempfile.mkdtemp(dir=os.getcwd())
  try:
    cd(build_dir)
    run('curl', PACKAGE_URL, '-o', 'downloaded.tar.bz2')
    run('tar', 'jxf', 'downloaded.tar.bz2', '--strip-components=1')
    run('./configure', '--prefix=%s' % install_prefix)
    run('make')
    run('make', 'install')
  finally:
    shutil.rmtree(build_dir)


def main(args):
  if sys.platform == 'win32':
    print >> sys.stderr, (
        'Please install protoc manually by downloading v%s binary from %s' %
        (PROTOC_VERSION, WIN_PACKAGE_URL))
    return 1

  parser = optparse.OptionParser(
      description=sys.modules['__main__'].__doc__,
      usage='%prog [install prefix]')
  _, args = parser.parse_args(args)

  install_prefix = None
  if not args:
    install_prefix = DEFAULT_INSTALL_PREFIX
  elif len(args) == 1:
    install_prefix = args[0]
  else:
    parser.error('Too many arguments')

  install_prefix = os.path.abspath(install_prefix)
  build_and_install(install_prefix)

  # If installing into default location, nuke include/ directory. It is not
  # needed, but confuses pylint a lot.
  if install_prefix == DEFAULT_INSTALL_PREFIX:
    shutil.rmtree(os.path.join(install_prefix, 'include'))

  protoc = os.path.join(install_prefix, 'bin', 'protoc')
  out = subprocess.check_output([protoc, '--version']).strip()
  print 'Installed protoc version: %s at %s' % (out, install_prefix)
  return 0


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
