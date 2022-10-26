#!/usr/bin/env python3
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Compiles all *.proto files it finds into *_pb2.py."""

import logging
import optparse
import os
import re
import shutil
import subprocess
import sys
import tempfile

assert sys.version_info.major >= 3

# Directory with this file.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# Version of protoc CIPD package to install.
PROTOC_PKG_VERSION = 'version:2@3.17.3'


# Paths that should not be searched for *.proto.
IGNORED_PATHS = [
  re.compile(r'.*(/|\\)third_party(/|\\)?'),
]


def is_ignored(path):
  """True if |path| matches any regexp in IGNORED_PATHS."""
  return any(b.match(path) for b in IGNORED_PATHS)


def find_proto_files(path):
  """Recursively searches for *.proto files, yields absolute paths to them."""
  path = os.path.abspath(path)
  for dirpath, dirnames, filenames in os.walk(path, followlinks=True):
    # Skip hidden and ignored directories
    skipped = [
      x for x in dirnames
      if x[0] == '.' or is_ignored(os.path.join(dirpath, x))
    ]
    for dirname in skipped:
      dirnames.remove(dirname)
    # Yield *.proto files.
    for name in filenames:
      if name.endswith('.proto'):
        yield os.path.join(dirpath, name)


def install_protoc():
  """Installs protoc from CIPD and returns an absolute path to it."""
  root = os.path.join(THIS_DIR, '.protoc')
  cipd = subprocess.Popen(
      ['cipd', 'ensure', '-ensure-file', '-', '-root', root],
      stdin=subprocess.PIPE,
      text=True)
  cipd.communicate('infra/3pp/tools/protoc/${platform} %s' % PROTOC_PKG_VERSION)
  if cipd.returncode:
    raise subprocess.SubprocessError('Failed to install protoc')
  return os.path.join(root, 'bin',
                      'protoc.exe' if sys.platform == 'win32' else 'protoc')


def compile_proto(protoc, proto_file, proto_path, output_path=None):
  """Invokes 'protoc', compiling single *.proto file into *_pb2.py file.

  Args:
    protoc: path to `protoc` to use.
    proto_file: the file to compile.
    proto_path: the root of proto file directory tree.
    output_path: the root of the output directory tree.
      Defaults to `proto_path`.

  Returns:
    The path of the generated _pb2.py file.
  """
  output_path = output_path or proto_path
  cmd = [protoc]
  cmd.append('--proto_path=%s' % proto_path)
  # Reuse embedded google protobuf.
  root = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
  cmd.append('--proto_path=%s' % os.path.join(root, 'client', 'third_party'))
  cmd.append('--python_out=%s' % output_path)
  cmd.append('--prpc-python_out=%s' % output_path)
  cmd.append(proto_file)
  logging.debug('Running %s', cmd)
  env = os.environ.copy()
  env['PATH'] = os.pathsep.join([THIS_DIR, env.get('PATH', '')])
  subprocess.check_call(cmd, env=env)
  return proto_file.replace('.proto', '_pb2.py').replace(proto_path,
                                                         output_path)


def check_proto_compiled(protoc, proto_file, proto_path):
  """Return True if *_pb2.py on disk is up to date."""
  # Missing?
  expected_path = proto_file.replace('.proto', '_pb2.py')
  if not os.path.exists(expected_path):
    return False

  # Helper to read contents of a file.
  def read(path):
    with open(path, 'r') as f:
      return f.read()

  # Compile *.proto into temp file to compare the result with existing file.
  tmp_dir = tempfile.mkdtemp()
  try:
    try:
      compiled = compile_proto(protoc,
                               proto_file,
                               proto_path,
                               output_path=tmp_dir)
    except subprocess.CalledProcessError:
      return False
    return read(compiled) == read(expected_path)
  finally:
    shutil.rmtree(tmp_dir)


def compile_all_files(protoc, root_dir, proto_path):
  """Compiles all *.proto files it recursively finds in |root_dir|."""
  root_dir = os.path.abspath(root_dir)
  success = True
  for path in find_proto_files(root_dir):
    try:
      compile_proto(protoc, path, proto_path)
    except subprocess.CalledProcessError:
      print('Failed to compile: %s' % path[len(root_dir) + 1:], file=sys.stderr)
      success = False
  return success


def check_all_files(protoc, root_dir, proto_path):
  """Returns True if all *_pb2.py files on disk are up to date."""
  root_dir = os.path.abspath(root_dir)
  success = True
  for path in find_proto_files(root_dir):
    if not check_proto_compiled(protoc, path, proto_path):
      print(
          'Need to recompile file: %s' % path[len(root_dir) + 1:],
          file=sys.stderr)
      success = False
  return success


def main(args, app_dir=None):
  parser = optparse.OptionParser(
      description=sys.modules['__main__'].__doc__,
      usage='%prog [options]' + ('' if app_dir else ' <root dir>'))
  parser.add_option(
      '-c', '--check', action='store_true',
      help='Only check that all *.proto files are up to date')
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option(
      '--proto_path',
      help=(
          'Used to calculate relative paths of proto files in the registry. '
          'Defaults to the input directory.'
      ))

  options, args = parser.parse_args(args)
  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.ERROR)
  root_dir = None
  if not app_dir:
    if len(args) != 1:
      parser.error('Expecting single argument')
    root_dir = args[0]
  else:
    if args:
      parser.error('Unexpected arguments')
    root_dir = app_dir

  protoc = install_protoc()
  proto_path = os.path.abspath(options.proto_path or root_dir)

  if options.check:
    success = check_all_files(protoc, root_dir, proto_path)
  else:
    success = compile_all_files(protoc, root_dir, proto_path)

  return int(not success)


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
