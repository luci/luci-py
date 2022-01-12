#!/usr/bin/env vpython3
# Copyright 2012 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import base64
import contextlib
import ctypes
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import textwrap
import time
import unittest

import six

# Mutates sys.path.
import test_env

import cas_util
import cipd

import run_isolated
from utils import file_path
from utils import large


_LUCI_GO = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), 'luci-go')

OUTPUT_CONTENT = 'foooo'
CONTENTS = {
    'file1.txt':
        b'File1\n',
    'repeated_files.py':
        textwrap.dedent("""
      from __future__ import print_function
      import os, sys
      expected = ['file1.txt', 'file1_copy.txt', 'repeated_files.py']
      actual = sorted(os.listdir(os.path.dirname(os.path.abspath(
          __file__))))
      if expected != actual:
        print('Expected list doesn\\'t match:', file=sys.stderr)
        print(
            '%s\\n%s' % (','.join(expected), ','.join(actual)),
            file=sys.stderr)
        sys.exit(1)
      print('Success')""").encode(),
    'max_path.py':
        textwrap.dedent("""
      from __future__ import print_function
      import os, sys
      prefix = u'\\\\\\\\?\\\\' if sys.platform == 'win32' else u''
      path = os.path.join(os.getcwd(), 'a' * 200, 'b' * 200)
      with open(prefix + path, 'rb') as f:
        actual = f.read()
        if actual != b'File1\\n':
          print('Unexpected content: %s' % actual, file=sys.stderr)
          sys.exit(1)
      print('Success')""").encode(),
    'output.py':
        textwrap.dedent("""
      import sys
      with open(sys.argv[1], 'w') as fh:
        fh.writelines(['{}'])""".format(OUTPUT_CONTENT)).encode(),
}


CMD_REPEATED_FILES = ['python', 'repeated_files.py']

CMD_OUTPUT = ['python', 'output.py', '${ISOLATED_OUTDIR}/foo.txt']

_repeated_files = {
    'file1.txt': CONTENTS['file1.txt'],
    'file1_copy.txt': CONTENTS['file1.txt'],
    'repeated_files.py': CONTENTS['repeated_files.py'],
}


def list_files_tree(directory):
  """Returns the list of all the files in a tree."""
  actual = []
  for root, _dirs, files in os.walk(directory):
    actual.extend(os.path.join(root, f)[len(directory)+1:] for f in files)
  return sorted(actual)


def read_content(filepath):
  with open(filepath, 'rb') as f:
    return f.read()


def write_content(filepath, content):
  with open(filepath, 'wb') as f:
    f.write(content)


def tree_modes(root):
  """Returns the dict of files in a directory with their filemode.

  Includes |root| as '.'.
  """
  out = {}
  offset = len(root.rstrip('/\\')) + 1
  out[u'.'] = oct(os.stat(root).st_mode)
  for dirpath, dirnames, filenames in os.walk(root):
    for filename in filenames:
      p = os.path.join(dirpath, filename)
      out[p[offset:]] = oct(os.stat(p).st_mode)
    for dirname in dirnames:
      p = os.path.join(dirpath, dirname)
      out[p[offset:]] = oct(os.stat(p).st_mode)
  return out


def load_isolated_stats(stats_json_path, key):
  actual = json.loads(read_content(stats_json_path))
  stats = actual['stats']['isolated'].get(key)
  for k in ['items_cold', 'items_hot']:
    if not stats[k]:
      continue
    stats[k] = large.unpack(base64.b64decode(stats[k]))
  return stats


class RunIsolatedTest(unittest.TestCase):
  def setUp(self):
    super(RunIsolatedTest, self).setUp()
    self.tempdir = run_isolated.make_temp_dir(
        u'run_isolated_smoke_test', test_env.CLIENT_DIR)
    logging.debug(self.tempdir)
    self._root_dir = os.path.join(self.tempdir, 'w')
    # The run_isolated local cache.
    self._named_cache_dir = os.path.join(self.tempdir, 'n')
    self._cipd_cache_dir = os.path.join(self.tempdir, u'cipd')
    self._cipd_packages_cache_dir = os.path.join(self._cipd_cache_dir, 'cache')

    self._cas_cache_dir = os.path.join(self.tempdir, 'c')
    self._cas_kvs = os.path.join(self.tempdir, 'cas_kvs')

    self._fakecas = cas_util.LocalCAS(self.tempdir)
    self._fakecas.start()
    self._cas_addr = self._fakecas.address

  def tearDown(self):
    try:
      self._fakecas.stop()
      file_path.rmtree(self.tempdir)
    finally:
      super(RunIsolatedTest, self).tearDown()

  def _run_cmd(self, cmd):
    pipe = subprocess.PIPE
    logging.debug(' '.join(cmd))
    env = os.environ.copy()
    env['RUN_ISOLATED_CAS_ADDRESS'] = self._cas_addr
    proc = subprocess.Popen(cmd,
                            stdout=pipe,
                            stderr=pipe,
                            universal_newlines=True,
                            cwd=self.tempdir,
                            env=env)
    out, err = proc.communicate()
    return out, err, proc.returncode

  def _run(self, args):
    cmd = [sys.executable, os.path.join(test_env.CLIENT_DIR, 'run_isolated.py')]
    cmd.extend(args)
    return self._run_cmd(cmd)

  def _run_cas(self, args):
    return self._run_cmd([os.path.join(_LUCI_GO, 'cas')] + args)

  def _download_from_cas(self, root_digest, dest):
    """Downloads files from CAS."""
    cmd = [
        'download',
        '-cas-addr',
        self._cas_addr,
        '-digest',
        root_digest,
        '-cache-dir',
        self._cas_cache_dir,
        '-dir',
        dest,
        '-kvs-dir',
        self._cas_kvs,
    ]
    _, err, returncode = self._run_cas(cmd)
    self.assertEqual('', err)
    self.assertEqual(0, returncode)

  def _cmd_args(self, digest):
    """Generates the standard arguments used with |digest| as the CAS digest.

    Returns a list of the required arguments.
    """
    return [
        '--cas-digest',
        digest,
        '--cas-cache',
        self._cas_cache_dir,
    ]

  def assertTreeModes(self, root, expected):
    """Compares the file modes of everything in |root| with |expected|.

    Arguments:
      root: directory to list its tree.
      expected: dict(relpath: (linux_mode, mac_mode, win_mode)) where each mode
                is the expected file mode on this OS. For practical purposes,
                linux is "anything but OSX or Windows". The modes should be
                ints.
    """
    actual = tree_modes(root)
    if sys.platform == 'win32':
      index = 2
    elif sys.platform == 'darwin':
      index = 1
    else:
      index = 0
    expected_mangled = dict((k, oct(v[index])) for k, v in expected.items())
    self.assertEqual(expected_mangled, actual)

  def test_simple(self):
    out, err, returncode = self._run(
        ['--', 'python', '-c', 'print("no --root-dir")'])
    self.assertEqual('no --root-dir\n', out)
    self.assertEqual('', err)
    self.assertEqual(0, returncode)

  def test_isolated_normal(self):
    # Upload files from test dir having files with the same content (same
    # digest), listed under two different names and ensure both are created.
    cas_digest = self._fakecas.archive_files(_repeated_files)
    expected = [
        'state.json',
        cas_util.cache_hash(CONTENTS['file1.txt']),
        cas_util.cache_hash(CONTENTS['repeated_files.py']),
    ]

    out, err, returncode = self._run(
        self._cmd_args(cas_digest) + ['--'] + CMD_REPEATED_FILES)
    self.assertEqual('', cas_util.filter_out_go_logs(err))
    self.assertEqual('Success\n', out, out)
    self.assertEqual(0, returncode)
    actual = list_files_tree(self._cas_cache_dir)
    self.assertEqual(sorted(set(expected)), actual)

  def test_isolated_max_path(self):
    # Make sure we can map and delete a tree that has paths longer than
    # MAX_PATH.
    cas_digest = self._fakecas.archive_files({
        os.path.join('a' * 200, 'b' * 200):
        CONTENTS['file1.txt'],
        'max_path.py':
        CONTENTS['max_path.py'],
    })
    expected = [
        'state.json',
        cas_util.cache_hash(CONTENTS['file1.txt']),
        cas_util.cache_hash(CONTENTS['max_path.py']),
    ]
    out, err, returncode = self._run(
        self._cmd_args(cas_digest) + ['--', 'python', 'max_path.py'])
    err = cas_util.filter_out_go_logs(err)
    self.assertEqual('', err)
    self.assertEqual('Success\n', out, out)
    self.assertEqual(0, returncode)
    actual = list_files_tree(self._cas_cache_dir)
    self.assertEqual(sorted(set(expected)), actual)

  def test_isolated_fail_empty_args(self):
    out, err, returncode = self._run([])
    self.assertEqual('', out)
    self.assertEqual(
        'Usage: run_isolated.py <options> [command to run or extra args]\n\n'
        'run_isolated.py: error: command to run is required.\n', err)
    self.assertEqual(2, returncode)
    actual = list_files_tree(self._cas_cache_dir)
    self.assertEqual([], actual)

  def _test_corruption_common(self, new_content):
    cas_digest = self._fakecas.archive_files({
        'file1.txt': CONTENTS['file1.txt'],
    })
    file1_hash = cas_util.cache_hash(CONTENTS['file1.txt'])

    # Run the test once to generate the cache.
    # The weird file mode is because of test_env.py that sets umask(0070).
    out, err, returncode = self._run(
        self._cmd_args(cas_digest) + ['--', 'python', '-V'])
    self.assertEqual(0, returncode, (out, err, returncode))
    expected = {
        u'.': (0o40707, 0o40707, 0o40777),
        u'state.json': (0o100606, 0o100606, 0o100666),
        # The reason for 0100666 on Windows is that the file node had to be
        # modified to delete the hardlinked node. The read only bit is reset on
        # load.
        six.text_type(file1_hash): (0o100604, 0o100604, 0o100604),
    }
    self.assertTreeModes(self._cas_cache_dir, expected)

    # Modify one of the files in the cache to be invalid.
    cached_file_path = os.path.join(self._cas_cache_dir, file1_hash)
    previous_mode = os.stat(cached_file_path).st_mode
    os.chmod(cached_file_path, 0o600)
    write_content(cached_file_path, new_content)
    os.chmod(cached_file_path, previous_mode)
    logging.info('Modified %s', cached_file_path)
    # Ensure that the cache has an invalid file.
    self.assertNotEqual(CONTENTS['file1.txt'], read_content(cached_file_path))

    # Clean up the cache
    out, err, returncode = self._run([
        '--clean',
        '--cas-cache',
        self._cas_cache_dir,
    ])
    self.assertEqual(0, returncode, (out, err, returncode))

    # Rerun the test and make sure the cache contains the right file afterwards.
    out, err, returncode = self._run(
        self._cmd_args(cas_digest) + ['--', 'python', '-V'])
    self.assertEqual(0, returncode, (out, err, returncode))
    expected = {
        u'.': (0o40700, 0o40700, 0o40700),
        u'state.json': (0o100600, 0o100600, 0o100600),
        six.text_type(file1_hash): (0o100604, 0o100604, 0o100604),
    }
    self.assertTreeModes(self._cas_cache_dir, expected)
    return cached_file_path

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_isolated_corrupted_cache_entry_different_size(self):
    # Test that an entry with an invalid file size properly gets removed and
    # fetched again. This test case also check for file modes.
    cached_file_path = self._test_corruption_common(CONTENTS['file1.txt'] +
                                                    b' now invalid size')
    self.assertEqual(CONTENTS['file1.txt'], read_content(cached_file_path))

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_isolated_corrupted_cache_entry_same_size(self):
    # Test that an entry with an invalid file content but same size is NOT
    # detected property.
    cached_file_path = self._test_corruption_common(CONTENTS['file1.txt'][:-1] +
                                                    b' ')
    self.assertEqual(CONTENTS['file1.txt'], read_content(cached_file_path))

  @unittest.skipIf(sys.platform == 'win32', 'crbug.com/1148174')
  def test_minimal_lower_priority(self):
    cmd = ['--lower-priority', '--', sys.executable, '-c']
    if sys.platform == 'win32':
      cmd.append(
          'import ctypes,sys; v=ctypes.windll.kernel32.GetPriorityClass(-1);'
          'sys.stdout.write(hex(v))')
    else:
      cmd.append('import os,sys; sys.stdout.write(str(os.nice(0)))')
    out, err, returncode = self._run(cmd)
    self.assertEqual('', err)
    if sys.platform == 'win32':
      # See
      # https://docs.microsoft.com/en-us/windows/desktop/api/processthreadsapi/nf-processthreadsapi-getpriorityclass
      BELOW_NORMAL_PRIORITY_CLASS = 0x4000
      self.assertEqual(hex(BELOW_NORMAL_PRIORITY_CLASS), out)
    else:
      self.assertEqual(str(os.nice(0)+1), out)
    self.assertEqual(0, returncode)

  def test_limit_processes(self):
    # Execution fails because it tries to run a second process.
    cmd = [
        '--limit-processes',
        '1',
    ]
    if sys.platform == 'win32':
      cmd.extend(('--containment-type', 'JOB_OBJECT'))
    cmd.extend(('--', sys.executable, '-c'))
    if sys.platform == 'win32':
      cmd.append('import subprocess,sys; '
                 'subprocess.call([sys.executable, "-c", "print(0)"])')
    else:
      cmd.append('import os,sys; sys.stdout.write(str(os.nice(0)))')
    out, err, returncode = self._run(cmd)
    if sys.platform == 'win32':
      self.assertIn('WinError', err)
      # Value for ERROR_NOT_ENOUGH_QUOTA. See
      # https://docs.microsoft.com/windows/desktop/debug/system-error-codes--1700-3999-
      self.assertIn('1816', err)
      self.assertEqual('', out)
      self.assertEqual(1, returncode)
    else:
      # TODO(maruel): Add containment on other platforms.
      self.assertEqual('', err)
      self.assertEqual('0', out, out)
      self.assertEqual(0, returncode)

  def test_named_cache(self):
    # Runs a task that drops a file in the named cache, and assert that it's
    # correctly saved.
    # Remove two seconds, because lru.py time resolution is one second, which
    # means that it could get rounded *down* and match the value of now.
    now = time.time() - 2
    cmd = [
        '--cas-cache', self._cas_cache_dir, '--named-cache-root',
        self._named_cache_dir, '--named-cache', 'cache1', 'a', '100', '--',
        sys.executable, '-c',
        'open("a/hello","wb").write(b"world");print("Success")'
    ]
    out, err, returncode = self._run(cmd)
    self.assertEqual('', err)
    self.assertEqual('Success\n', out, out)
    self.assertEqual(0, returncode)
    self.assertEqual(['state.json'], list_files_tree(self._cas_cache_dir))

    # Load the state file manually. This assumes internal knowledge in
    # local_caching.py.
    with open(os.path.join(self._named_cache_dir, u'state.json'), 'rb') as f:
      data = json.load(f)
    name, ((rel_path, size), timestamp) = data['items'][0]
    self.assertEqual(u'cache1', name)
    self.assertGreaterEqual(timestamp, now)
    self.assertEqual(len('world'), size)
    self.assertEqual(
        [u'hello'],
        list_files_tree(os.path.join(self._named_cache_dir, rel_path)))

  def test_cas_input(self):
    # Prepare inputs on the remote CAS instance.
    inputs_root_digest = self._fakecas.archive_files({
        'repeated_files.py':
        CONTENTS['repeated_files.py'],
        'file1.txt':
        CONTENTS['file1.txt'],
        'file1_copy.txt':
        CONTENTS['file1.txt'],
    })

    # Path to the result json file.
    result_json = os.path.join(self.tempdir, 'run_isolated_result.json')

    def assertRunIsolatedWithCAS(optional_args, expected_retcode=0):
      args = optional_args + [
          '--root-dir',
          self._root_dir,
          '--cas-digest',
          inputs_root_digest,
          '--cas-cache',
          self._cas_cache_dir,
          '--cipd-cache',
          self._cipd_cache_dir,
          '--json',
          result_json,
          '--',
      ] + CMD_REPEATED_FILES
      out, _, ret = self._run(args)

      if expected_retcode == 0:
        self.assertEqual('Success\n', out)
      self.assertEqual(expected_retcode, ret)

    # Runs run_isolated with cas options.
    assertRunIsolatedWithCAS([])
    download_stats = load_isolated_stats(result_json, 'download')
    download_stats.pop('duration')
    self.assertEqual(
        {
            'items_cold': [
                len(CONTENTS['file1.txt']),
                len(CONTENTS['repeated_files.py']),
            ],
            'items_hot': None,
        }, download_stats)
    self.assertEqual([
        'ebea1137c5ece3f8a58f0e1a0da1411fe0a2648501419d190b3b154f3f191259',
        'f0a8a1a7050bfae60a591d0cb7d74de2ef52963b9913253fc9ec7151aa5d421e',
        'state.json',
    ], list_files_tree(self._cas_cache_dir))

    # Cleanup all caches.
    # TODO(crbug.com/1129290):
    # '--max-cache-size=0' is ignored by local_caching.py unexpectedly.
    # change it to 0 after fixing the bug.
    _, _, returncode = self._run([
        '--clean',
        '--cas-cache',
        self._cas_cache_dir,
        '--max-cache-size=1',
    ])
    self.assertEqual(0, returncode)
    self.assertEqual(['state.json'], list_files_tree(self._cas_cache_dir))

    # Specify --max-cache-size option.
    optional_args = [
        '--max-cache-size',
        '1', # 0 means infinity
    ]
    assertRunIsolatedWithCAS(optional_args)
    download_stats = load_isolated_stats(result_json, 'download')
    self.assertEqual(2, len(download_stats['items_cold']))
    self.assertEqual(['state.json'], list_files_tree(self._cas_cache_dir))

    # Specify --min-free-space option. This shouldn't fail even if there are no
    # required space.
    optional_args = [
        '--min-free-space',
        str(2**63 - 1),
    ]
    assertRunIsolatedWithCAS(optional_args)

  def test_cas_output(self):
    # Prepare inputs on CAS instance for `output.py` task.
    inputs_root_digest = self._fakecas.archive_files(
        {'output.py': CONTENTS['output.py']})

    # Path to the result json file.
    result_json = os.path.join(self.tempdir, 'run_isolated_result.json')

    args = [
        '--root-dir',
        self._root_dir,
        '--cas-digest',
        inputs_root_digest,
        '--cas-cache',
        self._cas_cache_dir,
        '--cipd-cache',
        self._cipd_cache_dir,
        '--json',
        result_json,
        '--',
    ] + CMD_OUTPUT
    out, err, ret = self._run(args)

    self.assertEqual(0, ret,
                     "stdout\n%s\nstderr\n%s\nret: %d" % (out, err, ret))
    upload_stats = load_isolated_stats(result_json, 'upload')
    upload_size = upload_stats['items_cold'][0]
    self.assertEqual(len(OUTPUT_CONTENT), upload_size)
    result = json.loads(read_content(result_json))

    output_dir = os.path.join(self.tempdir, 'out')
    d = result['cas_output_root']['digest']
    output_root_digest = "%s/%s" % (d['hash'], d['size_bytes'])
    self._download_from_cas(output_root_digest, output_dir)
    self.assertEqual(['foo.txt'], list_files_tree(output_dir))
    self.assertEqual(OUTPUT_CONTENT.encode(),
                     read_content(os.path.join(output_dir, 'foo.txt')))


if __name__ == '__main__':
  test_env.main()
