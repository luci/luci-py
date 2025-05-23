#!/usr/bin/env python
# Copyright 2012 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Runs a command with optional isolated input/output.

run_isolated takes cares of setting up a temporary environment, running a
command, and tearing it down.

It handles downloading and uploading isolated files, mapping CIPD packages and
reusing stateful named caches.

The isolated files, CIPD packages and named caches are kept as a global LRU
cache.

Any ${EXECUTABLE_SUFFIX} on the command line or the environment variables passed
with the --env option will be replaced with ".exe" string on Windows and "" on
other platforms.

Any ${ISOLATED_OUTDIR} on the command line or the environment variables passed
with the --env option will be replaced by the location of a temporary directory
upon execution of the command specified in the .isolated file. All content
written to this directory will be uploaded upon termination and the .isolated
file describing this directory will be printed to stdout.

Any ${SWARMING_BOT_FILE} on the command line or the environment variables passed
with the --env option will be replaced by the value of the --bot-file parameter.
This file is used by a swarming bot to communicate state of the host to tasks.
It is written to by the swarming bot's on_before_task() hook in the swarming
server's custom bot_config.py.

Any ${SWARMING_TASK_ID} on the command line will be replaced by the
SWARMING_TASK_ID value passed with the --env option.

See
https://chromium.googlesource.com/infra/luci/luci-py.git/+/main/appengine/swarming/doc/Magic-Values.md
for all the variables.

See
https://chromium.googlesource.com/infra/luci/luci-py.git/+/main/appengine/swarming/swarming_bot/config/bot_config.py
for more information about bot_config.py.
"""

from __future__ import print_function

__version__ = '1.0.1'

import argparse
import base64
import collections
import contextlib
import errno
import json
import logging
import optparse
import os
import platform
import re
import shutil
import signal
import sys
import tempfile
import time

from utils import tools
tools.force_local_third_party()

# third_party/
from depot_tools import fix_encoding

# pylint: disable=ungrouped-imports
import auth
import cipd
import errors
import local_caching
from libs import luci_context
from utils import file_path
from utils import fs
from utils import logging_utils
from utils import net
from utils import on_error
from utils import subprocess42


# Magic variables that can be found in the isolate task command line.
ISOLATED_OUTDIR_PARAMETER = '${ISOLATED_OUTDIR}'
EXECUTABLE_SUFFIX_PARAMETER = '${EXECUTABLE_SUFFIX}'
SWARMING_BOT_FILE_PARAMETER = '${SWARMING_BOT_FILE}'
SWARMING_TASK_ID_PARAMETER = '${SWARMING_TASK_ID}'


# The name of the log file to use.
RUN_ISOLATED_LOG_FILE = 'run_isolated.log'


# Use short names for temporary directories. This is driven by Windows, which
# imposes a relatively short maximum path length of 260 characters, often
# referred to as MAX_PATH. It is relatively easy to create files with longer
# path length. A use case is with recursive dependency trees like npm packages.
#
# It is recommended to start the script with a `root_dir` as short as
# possible.
# - ir stands for isolated_run
# - io stands for isolated_out
# - it stands for isolated_tmp
ISOLATED_RUN_DIR = 'ir'
ISOLATED_OUT_DIR = 'io'
ISOLATED_TMP_DIR = 'it'
_CAS_CLIENT_DIR = 'cc'
# TODO(tikuta): take these parameter from luci-config?
_CAS_PACKAGE = 'infra/tools/luci/cas/${platform}'
_LUCI_GO_REVISION = 'git_revision:d290e92048ea30ad4f74232430604cbf7053557c'

# Keep synced with task_request.py
CACHE_NAME_RE = re.compile(r'^[a-z0-9_]{1,4096}$')

_FREE_SPACE_BUFFER_FOR_CIPD_PACKAGES = 2 * 1024 * 1024 * 1024

OUTLIVING_ZOMBIE_MSG = """\
*** Swarming tried multiple times to delete the %s directory and failed ***
*** Hard failing the task ***

Swarming detected that your testing script ran an executable, which may have
started a child executable, and the main script returned early, leaving the
children executables playing around unguided.

You don't want to leave children processes outliving the task on the Swarming
bot, do you? The Swarming bot doesn't.

How to fix?
- For any process that starts children processes, make sure all children
  processes terminated properly before each parent process exits. This is
  especially important in very deep process trees.
  - This must be done properly both in normal successful task and in case of
    task failure. Cleanup is very important.
- The Swarming bot sends a SIGTERM in case of timeout.
  - You have %s seconds to comply after the signal was sent to the process
    before the process is forcibly killed.
- To achieve not leaking children processes in case of signals on timeout, you
  MUST handle signals in each executable / python script and propagate them to
  children processes.
  - When your test script (python or binary) receives a signal like SIGTERM or
    CTRL_BREAK_EVENT on Windows), send it to all children processes and wait for
    them to terminate before quitting.

See
https://chromium.googlesource.com/infra/luci/luci-py.git/+/main/appengine/swarming/doc/Bot.md#Graceful-termination_aka-the-SIGTERM-and-SIGKILL-dance
for more information.

*** May the SIGKILL force be with you ***
"""


# Currently hardcoded. Eventually could be exposed as a flag once there's value.
# 3 weeks
MAX_AGE_SECS = 21*24*60*60

_CAS_KVS_CACHE_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5 GiB

TaskData = collections.namedtuple(
    'TaskData',
    [
        # List of strings; the command line to use, independent of what was
        # specified in the isolated file.
        'command',
        # Relative directory to start command into.
        'relative_cwd',
        # Digest of the input root on RBE-CAS.
        'cas_digest',
        # Full CAS instance name.
        'cas_instance',
        # List of paths relative to root_dir to put into the output isolated
        # bundle upon task completion (see link_outputs_to_outdir).
        'outputs',
        # Function (run_dir) => context manager that installs named caches into
        # |run_dir|.
        'install_named_caches',
        # If True, the temporary directory will be deliberately leaked for later
        # examination.
        'leak_temp_dir',
        # Path to the directory to use to create the temporary directory. If not
        # specified, a random temporary directory is created.
        'root_dir',
        # Kills the process if it lasts more than this amount of seconds.
        'hard_timeout',
        # Number of seconds to wait between SIGTERM and SIGKILL.
        'grace_period',
        # Path to a file with bot state, used in place of ${SWARMING_BOT_FILE}
        # task command line argument.
        'bot_file',
        # Logical account to switch LUCI_CONTEXT into.
        'switch_to_account',
        # Context manager dir => CipdInfo, see install_client_and_packages.
        'install_packages_fn',
        # Cache directory for `cas` client.
        'cas_cache_dir',
        # Parameters passed to `cas` client.
        'cas_cache_policies',
        # Parameters for kvs file used by `cas` client.
        'cas_kvs',
        # Environment variables to set.
        'env',
        # Environment variables to mutate with relative directories.
        # Example: {"ENV_KEY": ['relative', 'paths', 'to', 'prepend']}
        'env_prefix',
        # Lowers the task process priority.
        'lower_priority',
        # subprocess42.Containment instance. Can be None.
        'containment',
        # Function to trim caches before installing cipd packages and
        # downloading isolated files.
        'trim_caches_fn',
    ])

def make_temp_dir(prefix, root_dir):
  """Returns a new unique temporary directory."""
  return tempfile.mkdtemp(prefix=prefix, dir=root_dir)


@contextlib.contextmanager
def set_luci_context_account(account, tmp_dir):
  """Sets LUCI_CONTEXT account to be used by the task.

  If 'account' is None or '', does nothing at all. This happens when
  run_isolated.py is called without '--switch-to-account' flag. In this case,
  if run_isolated.py is running in some LUCI_CONTEXT environment, the task will
  just inherit whatever account is already set. This may happen if users invoke
  run_isolated.py explicitly from their code.

  If the requested account is not defined in the context, switches to
  non-authenticated access. This happens for Swarming tasks that don't use
  'task' service accounts.

  If not using LUCI_CONTEXT-based auth, does nothing.
  If already running as requested account, does nothing.
  """
  if not account:
    # Not actually switching.
    yield
    return

  local_auth = luci_context.read('local_auth')
  if not local_auth:
    # Not using LUCI_CONTEXT auth at all.
    yield
    return

  # See LUCI_CONTEXT.md for the format of 'local_auth'.
  if local_auth.get('default_account_id') == account:
    # Already set, no need to switch.
    yield
    return

  available = {a['id'] for a in local_auth.get('accounts') or []}
  if account in available:
    logging.info('Switching default LUCI_CONTEXT account to %r', account)
    local_auth['default_account_id'] = account
  else:
    logging.warning(
        'Requested LUCI_CONTEXT account %r is not available (have only %r), '
        'disabling authentication', account, sorted(available))
    local_auth.pop('default_account_id', None)

  with luci_context.write(_tmpdir=tmp_dir, local_auth=local_auth):
    yield


def process_command(command, out_dir, bot_file):
  """Replaces parameters in a command line.

  Raises:
    ValueError if a parameter is requested in |command| but its value is not
      provided.
  """
  return [replace_parameters(arg, out_dir, bot_file) for arg in command]


def replace_parameters(arg, out_dir, bot_file):
  """Replaces parameter tokens with appropriate values in a string.

  Raises:
    ValueError if a parameter is requested in |arg| but its value is not
      provided.
  """
  arg = arg.replace(EXECUTABLE_SUFFIX_PARAMETER, cipd.EXECUTABLE_SUFFIX)
  replace_slash = False
  if ISOLATED_OUTDIR_PARAMETER in arg:
    if not out_dir:
      raise ValueError(
          'output directory is requested in command or env var, but not '
          'provided; please specify one')
    arg = arg.replace(ISOLATED_OUTDIR_PARAMETER, out_dir)
    replace_slash = True
  if SWARMING_BOT_FILE_PARAMETER in arg:
    if bot_file:
      arg = arg.replace(SWARMING_BOT_FILE_PARAMETER, bot_file)
      replace_slash = True
    else:
      logging.warning('SWARMING_BOT_FILE_PARAMETER found in command or env '
                      'var, but no bot_file specified. Leaving parameter '
                      'unchanged.')
  if SWARMING_TASK_ID_PARAMETER in arg:
    task_id = os.environ.get('SWARMING_TASK_ID')
    if task_id:
      arg = arg.replace(SWARMING_TASK_ID_PARAMETER, task_id)
  if replace_slash:
    # Replace slashes only if parameters are present
    # because of arguments like '${ISOLATED_OUTDIR}/foo/bar'
    arg = arg.replace('/', os.sep)
  return arg


def set_temp_dir(env, tmp_dir):
  """Set temp dir to given env var dictionary"""
  # pylint: disable=line-too-long
  # * python respects $TMPDIR, $TEMP, and $TMP in this order, regardless of
  #   platform. So $TMPDIR must be set on all platforms.
  #   https://github.com/python/cpython/blob/2.7/Lib/tempfile.py#L155
  env['TMPDIR'] = tmp_dir
  if sys.platform == 'win32':
    # * chromium's base utils uses GetTempPath().
    #   https://cs.chromium.org/chromium/src/base/files/file_util_win.cc?q=GetTempPath
    # * Go uses GetTempPath().
    # * GetTempDir() uses %TMP%, then %TEMP%, then other stuff. So %TMP% must be
    #   set.
    #   https://docs.microsoft.com/en-us/windows/desktop/api/fileapi/nf-fileapi-gettemppathw
    env['TMP'] = tmp_dir
    # https://blogs.msdn.microsoft.com/oldnewthing/20150417-00/?p=44213
    env['TEMP'] = tmp_dir
  elif sys.platform == 'darwin':
    # * Chromium uses an hack on macOS before calling into
    #   NSTemporaryDirectory().
    #   https://cs.chromium.org/chromium/src/base/files/file_util_mac.mm?q=GetTempDir
    #   https://developer.apple.com/documentation/foundation/1409211-nstemporarydirectory
    env['MAC_CHROMIUM_TMPDIR'] = tmp_dir
  else:
    # TMPDIR is specified as the POSIX standard envvar for the temp directory.
    # http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap08.html
    # * mktemp on linux respects $TMPDIR.
    # * Chromium respects $TMPDIR on linux.
    #   https://cs.chromium.org/chromium/src/base/files/file_util_posix.cc?q=GetTempDir
    # * Go uses $TMPDIR.
    #   https://go.googlesource.com/go/+/go1.10.3/src/os/file_unix.go#307
    pass


def get_command_env(tmp_dir, cipd_info, run_dir, env, env_prefixes, out_dir,
                    bot_file):
  """Returns full OS environment to run a command in.

  Sets up TEMP, puts directory with cipd binary in front of PATH, exposes
  CIPD_CACHE_DIR env var, and installs all env_prefixes.

  Args:
    tmp_dir: temp directory.
    cipd_info: CipdInfo object is cipd client is used, None if not.
    run_dir: The root directory the isolated tree is mapped in.
    env: environment variables to use
    env_prefixes: {"ENV_KEY": ['cwd', 'relative', 'paths', 'to', 'prepend']}
    out_dir: Isolated output directory. Required to be != None if any of the
        env vars contain ISOLATED_OUTDIR_PARAMETER.
    bot_file: Required to be != None if any of the env vars contain
        SWARMING_BOT_FILE_PARAMETER.
  """
  out = os.environ.copy()
  for k, v in env.items():
    if not v:
      out.pop(k, None)
    else:
      out[k] = replace_parameters(v, out_dir, bot_file)

  if cipd_info:
    bin_dir = os.path.dirname(cipd_info.client.binary_path)
    out['PATH'] = '%s%s%s' % (bin_dir, os.pathsep, out['PATH'])
    out['CIPD_CACHE_DIR'] = cipd_info.cache_dir
    cipd_info_path = os.path.join(tmp_dir, 'cipd_info.json')
    with open(cipd_info_path, 'w') as f:
      json.dump(cipd_info.pins, f)
    out['ISOLATED_RESOLVED_PACKAGE_VERSIONS_FILE'] = cipd_info_path

  for key, paths in env_prefixes.items():
    assert isinstance(paths, list), paths
    paths = [os.path.normpath(os.path.join(run_dir, p)) for p in paths]
    cur = out.get(key)
    if cur:
      paths.append(cur)
    out[key] = os.path.pathsep.join(paths)

  set_temp_dir(out, tmp_dir)
  return out


def run_command(
    command, cwd, env, hard_timeout, grace_period, lower_priority, containment):
  """Runs the command.

  Returns:
    tuple(process exit code, bool if had a hard timeout)
  """
  logging_utils.user_logs('run_command(%s, %s, %s, %s, %s, %s)', command, cwd,
                          hard_timeout, grace_period, lower_priority,
                          containment)

  timeout_kill_sent = False
  exit_code = None
  had_hard_timeout = False
  with tools.Profiler('RunTest'):
    proc = None
    had_signal = []
    try:
      # TODO(maruel): This code is imperfect. It doesn't handle well signals
      # during the download phase and there's short windows were things can go
      # wrong.
      def handler(signum, _frame):
        if proc and not had_signal:
          logging.info('Received signal %d', signum)
          had_signal.append(True)
          raise subprocess42.TimeoutExpired(command, None)

      proc = subprocess42.Popen(
          command, cwd=cwd, env=env, detached=True, close_fds=True,
          lower_priority=lower_priority, containment=containment)
      logging.info('Subprocess for command started')
      with subprocess42.set_signal_handler(subprocess42.STOP_SIGNALS, handler):
        try:
          exit_code = proc.wait(hard_timeout or None)
          logging.info('finished with exit code %d before hard_timeout %s',
                       exit_code, hard_timeout)
        except subprocess42.TimeoutExpired:
          if not had_signal:
            logging.warning('Hard timeout')
            had_hard_timeout = True
          logging.warning('Sending SIGTERM')
          proc.terminate()

      # Ignore signals in grace period. Forcibly give the grace period to the
      # child process.
      if exit_code is None:
        ignore = lambda *_: None
        with subprocess42.set_signal_handler(subprocess42.STOP_SIGNALS, ignore):
          try:
            exit_code = proc.wait(grace_period or None)
            logging.info(
                'finished with exit code %d before grace_period exhausted %s',
                exit_code, grace_period)
          except subprocess42.TimeoutExpired:
            # Now kill for real. The user can distinguish between the
            # following states:
            # - signal but process exited within grace period,
            #   hard_timed_out will be set but the process exit code will be
            #   script provided.
            # - processed exited late, exit code will be -9 on posix.
            logging.warning('Grace exhausted; sending SIGKILL')
            proc.kill()
            timeout_kill_sent = True
      logging.info('Waiting for process exit')
      exit_code = proc.wait()

      # the process group / job object may be dangling so if we didn't kill
      # it already, give it a poke now.
      if not timeout_kill_sent:
        proc.kill()
    except OSError as e:
      # This is not considered to be an internal error. The executable simply
      # does not exit.
      sys.stderr.write(
          '<The executable does not exist, a dependent library is missing or '
          'the command line is too long>\n'
          '<Check for missing .so/.dll in the .isolate or GN file or length of '
          'command line args>\n'
          '<Command: %s>\n'
          '<Exception: %s>\n' % (command, e))
      if os.environ.get('SWARMING_TASK_ID'):
        # Give an additional hint when running as a swarming task.
        sys.stderr.write(
            '<See the task\'s page for commands to help diagnose this issue '
            'by reproducing the task locally>\n')
      exit_code = 1

  logging.info(
      'Command finished with exit code %d (%s)',
      exit_code, hex(0xffffffff & exit_code))

  if sys.platform != 'win32':
    if exit_code == -int(signal.SIGKILL):
      if timeout_kill_sent:
        logging_utils.user_logs(
            'The task root process was killed by SIGKILL due to exceeding '
            'the graceful termination timeout')
      else:
        logging_utils.user_logs(
            'The task root process was killed by SIGKILL. It may mean the '
            'process ran out of memory was killed by the oom-killer')

  return exit_code, had_hard_timeout


def _run_go_cmd_and_wait(cmd, tmp_dir):
  """
  Runs an external Go command, `isolated` or `cas`, and wait for its completion.

  While this is a generic function to launch a subprocess, it has logic that
  is specific to Go `isolated` and `cas` for waiting and logging.

  Returns:
    The subprocess object
  """
  cmd_str = ' '.join(cmd)
  try:
    env = os.environ.copy()
    set_temp_dir(env, tmp_dir)
    proc = subprocess42.Popen(cmd, env=env)

    exceeded_max_timeout = True
    check_period_sec = 30
    max_checks = 100
    # max timeout = max_checks * check_period_sec = 50 minutes
    for i in range(max_checks):
      # This is to prevent I/O timeout error during setup.
      try:
        retcode = proc.wait(check_period_sec)
        if retcode != 0:
          raise subprocess42.CalledProcessError(retcode, cmd=cmd_str)
        exceeded_max_timeout = False
        break
      except subprocess42.TimeoutExpired:
        print('still running (after %d seconds)' % ((i + 1) * check_period_sec))

    if exceeded_max_timeout:
      proc.terminate()
      try:
        proc.wait(check_period_sec)
      except subprocess42.TimeoutExpired:
        logging.exception(
            "failed to terminate? timeout happened after %d seconds",
            check_period_sec)
        proc.kill()
        proc.wait()
      # Raise unconditionally, because |proc| was forcefully terminated.
      raise ValueError("timedout after %d seconds (cmd=%s)" %
                       (check_period_sec * max_checks, cmd_str))

    return proc
  except Exception:
    logging.exception('Failed to run Go cmd %s', cmd_str)
    raise


def _fetch_and_map(cas_client, digest, instance, output_dir, cache_dir,
                            policies, kvs_dir, tmp_dir):
  """
  Fetches a CAS tree using cas client, create the tree and returns download
  stats.
  """

  start = time.time()
  result_json_handle, result_json_path = tempfile.mkstemp(
      prefix='fetch-and-map-result-', suffix='.json')
  os.close(result_json_handle)
  profile_dir = tempfile.mkdtemp(dir=tmp_dir)

  try:
    cmd = [
        cas_client,
        'download',
        '-digest',
        digest,
        # flags for cache.
        '-cache-dir',
        cache_dir,
        '-cache-max-size',
        str(policies.max_cache_size),
        '-cache-min-free-space',
        str(policies.min_free_space),
        # flags for output.
        '-dir',
        output_dir,
        '-dump-json',
        result_json_path,
        '-log-level',
        'info',
    ]

    # When RUN_ISOLATED_CAS_ADDRESS is set in test mode,
    # Use it and ignore CAS instance option.
    cas_addr = os.environ.get('RUN_ISOLATED_CAS_ADDRESS')
    if cas_addr:
      cmd.extend([
        '-cas-addr',
        cas_addr,
      ])
    else:
      cmd.extend([
        '-cas-instance',
        instance
      ])

    if kvs_dir:
      cmd.extend(['-kvs-dir', kvs_dir])

    def open_json_and_check(result_json_path, cleanup_dirs):
      cas_error = False
      result_json = {}
      error_digest = digest
      try:
        with open(result_json_path) as json_file:
          result_json = json.load(json_file)
          cas_error = result_json.get('result') in ('digest_invalid',
                                                    'authentication_error',
                                                    'arguments_invalid')
          if cas_error and result_json.get('error_details'):
            error_digest = result_json['error_details'].get('digest', digest)

      except (IOError, ValueError):
        logging.error('Failed to read json file: %s', result_json_path)
        raise
      finally:
        if cleanup_dirs:
          file_path.rmtree(kvs_dir)
          file_path.rmtree(output_dir)
      if cas_error:
        raise errors.NonRecoverableCasException(result_json['result'],
                                                error_digest, instance)
      return result_json

    try:
      _run_go_cmd_and_wait(cmd, tmp_dir)
    except subprocess42.CalledProcessError as ex:
      if not kvs_dir:
        open_json_and_check(result_json_path, False)
        raise
      open_json_and_check(result_json_path, True)
      logging.exception('Failed to run cas, removing kvs cache dir and retry.')
      on_error.report("Failed to run cas %s" % ex)
      _run_go_cmd_and_wait(cmd, tmp_dir)

    result_json = open_json_and_check(result_json_path, False)

    return {
        'duration': time.time() - start,
        'items_cold': result_json['items_cold'],
        'items_hot': result_json['items_hot'],
    }
  finally:
    fs.remove(result_json_path)
    file_path.rmtree(profile_dir)


def link_outputs_to_outdir(run_dir, out_dir, outputs):
  """Links any named outputs to out_dir so they can be uploaded.

  Raises an error if the file already exists in that directory.
  """
  if not outputs:
    return
  file_path.create_directories(out_dir, outputs)
  for o in outputs:
    copy_recursively(os.path.join(run_dir, o), os.path.join(out_dir, o))


def copy_recursively(src, dst):
  """Efficiently copies a file or directory from src_dir to dst_dir.

  `item` may be a file, directory, or a symlink to a file or directory.
  All symlinks are replaced with their targets, so the resulting
  directory structure in dst_dir will never have any symlinks.

  To increase speed, copy_recursively hardlinks individual files into the
  (newly created) directory structure if possible, unlike Python's
  shutil.copytree().
  """
  orig_src = src
  try:
    # Replace symlinks with their final target.
    while fs.islink(src):
      res = fs.readlink(src)
      src = os.path.realpath(os.path.join(os.path.dirname(src), res))
    # TODO(sadafm): Explicitly handle cyclic symlinks.

    if not fs.exists(src):
      logging.warning('Path %s does not exist or %s is a broken symlink', src,
                      orig_src)
      return

    if fs.isfile(src):
      file_path.link_file(dst, src, file_path.HARDLINK_WITH_FALLBACK)
      return

    if not fs.exists(dst):
      os.makedirs(dst)

    for child in fs.listdir(src):
      copy_recursively(os.path.join(src, child), os.path.join(dst, child))

  except OSError as e:
    if e.errno == errno.ENOENT:
      logging.warning('Path %s does not exist or %s is a broken symlink',
                      src, orig_src)
    else:
      logging.info("Couldn't collect output file %s: %s", src, e)


def upload_outdir(cas_client, cas_instance, outdir, tmp_dir):
  """Uploads the results in |outdir|, if there is any.

  Returns:
    tuple(root_digest, stats)
    - root_digest: a digest of the output directory.
    - stats: uploading stats.
  """
  if not fs.listdir(outdir):
    return None, None
  digest_file_handle, digest_path = tempfile.mkstemp(prefix='cas-digest',
                                                     suffix='.txt')
  os.close(digest_file_handle)
  stats_json_handle, stats_json_path = tempfile.mkstemp(prefix='upload-stats',
                                                        suffix='.json')
  os.close(stats_json_handle)

  try:
    cmd = [
        cas_client,
        'archive',
        '-log-level',
        'debug',
        '-paths',
        # Format: <working directory>:<relative path to dir>
        outdir + ':',
        # output
        '-dump-digest',
        digest_path,
        '-dump-json',
        stats_json_path,
    ]

    # When RUN_ISOLATED_CAS_ADDRESS is set in test mode,
    # Use it and ignore CAS instance option.
    cas_addr = os.environ.get('RUN_ISOLATED_CAS_ADDRESS')
    if cas_addr:
      cmd.extend([
        '-cas-addr',
        cas_addr,
      ])
    elif cas_instance:
      cmd.extend([
        '-cas-instance',
        cas_instance
      ])
    else:
      return None, None

    start = time.time()

    _run_go_cmd_and_wait(cmd, tmp_dir)

    with open(digest_path) as digest_file:
      digest = digest_file.read()
    h, s = digest.split('/')
    cas_output_root = {
        'cas_instance': cas_instance,
        'digest': {
            'hash': h,
            'size_bytes': int(s)
        }
    }
    with open(stats_json_path) as stats_file:
      stats = json.load(stats_file)

    stats['duration'] = time.time() - start

    return cas_output_root, stats
  finally:
    fs.remove(digest_path)
    fs.remove(stats_json_path)


def map_and_run(data, constant_run_path):
  """Runs a command with optional isolated input/output.

  Arguments:
  - data: TaskData instance.
  - constant_run_path: TODO

  Returns metadata about the result.
  """

  # TODO(tikuta): take stats from state.json in this case too.
  download_stats = {
      # 'duration': 0.,
      # 'initial_number_items': len(data.cas_cache),
      # 'initial_size': data.cas_cache.total_size,
      # 'items_cold': '<large.pack()>',
      # 'items_hot': '<large.pack()>',
  }

  result = {
      'duration': None,
      'exit_code': None,
      'had_hard_timeout': False,
      'internal_failure': 'run_isolated did not complete properly',
      'stats': {
          'trim_caches': {
              'duration': 0,
          },
          #'cipd': {
          #  'duration': 0.,
          #  'get_client_duration': 0.,
          #},
          'isolated': {
              'download': download_stats,
              #'upload': {
              #  'duration': 0.,
              #  'items_cold': '<large.pack()>',
              #  'items_hot': '<large.pack()>',
              #},
          },
          'named_caches': {
              'install': {
                  'duration': 0,
              },
              'uninstall': {
                  'duration': 0,
              },
          },
          'cleanup': {
              'duration': 0,
          }
      },
      #'cipd_pins': {
      #  'packages': [
      #    {'package_name': ..., 'version': ..., 'path': ...},
      #    ...
      #  ],
      # 'client_package': {'package_name': ..., 'version': ...},
      #},
      'outputs_ref': None,
      'cas_output_root': None,
      'version': 5,
  }

  assert os.path.isabs(data.root_dir), ("data.root_dir is not abs path: %s" %
                                        data.root_dir)
  file_path.ensure_tree(data.root_dir, 0o700)

  # See comment for these constants.
  # TODO(maruel): This is not obvious. Change this to become an error once we
  # make the constant_run_path an exposed flag.
  if constant_run_path and data.root_dir:
    run_dir = os.path.join(data.root_dir, ISOLATED_RUN_DIR)
    if os.path.isdir(run_dir):
      file_path.rmtree(run_dir)
    os.mkdir(run_dir, 0o700)
  else:
    run_dir = make_temp_dir(ISOLATED_RUN_DIR, data.root_dir)

  # storage should be normally set but don't crash if it is not. This can happen
  # as Swarming task can run without an isolate server.
  out_dir = make_temp_dir(ISOLATED_OUT_DIR, data.root_dir)
  tmp_dir = make_temp_dir(ISOLATED_TMP_DIR, data.root_dir)
  cwd = run_dir
  if data.relative_cwd:
    cwd = os.path.normpath(os.path.join(cwd, data.relative_cwd))
  command = data.command

  cas_client_dir = make_temp_dir(_CAS_CLIENT_DIR, data.root_dir)
  cas_client = os.path.join(cas_client_dir, 'cas' + cipd.EXECUTABLE_SUFFIX)

  data.trim_caches_fn(result['stats']['trim_caches'])

  try:
    with data.install_packages_fn(run_dir, cas_client_dir) as cipd_info:
      if cipd_info:
        result['stats']['cipd'] = cipd_info.stats
        result['cipd_pins'] = cipd_info.pins

      isolated_stats = result['stats'].setdefault('isolated', {})

      if data.cas_digest:
        stats = _fetch_and_map(
            cas_client=cas_client,
            digest=data.cas_digest,
            instance=data.cas_instance,
            output_dir=run_dir,
            cache_dir=data.cas_cache_dir,
            policies=data.cas_cache_policies,
            kvs_dir=data.cas_kvs,
            tmp_dir=tmp_dir)
        isolated_stats['download'].update(stats)
        logging_utils.user_logs('Fetched CAS inputs')

      if not command:
        # Handle this as a task failure, not an internal failure.
        sys.stderr.write(
            '<No command was specified!>\n'
            '<Please specify a command when triggering your Swarming task>\n')
        result['exit_code'] = 1
        return result

      if not cwd.startswith(run_dir):
        # Handle this as a task failure, not an internal failure. This is a
        # 'last chance' way to gate against directory escape.
        sys.stderr.write('<Relative CWD is outside of run directory!>\n')
        result['exit_code'] = 1
        return result

      if not os.path.isdir(cwd):
        # Accepts relative_cwd that does not exist.
        os.makedirs(cwd, 0o700)

      # If we have an explicit list of files to return, make sure their
      # directories exist now.
      if data.outputs:
        file_path.create_directories(run_dir, data.outputs)

      with data.install_named_caches(run_dir, result['stats']['named_caches']):
        logging_utils.user_logs('Installed named caches')
        sys.stdout.flush()
        start = time.time()
        try:
          # Need to switch the default account before 'get_command_env' call,
          # so it can grab correct value of LUCI_CONTEXT env var.
          with set_luci_context_account(data.switch_to_account, tmp_dir):
            env = get_command_env(
                tmp_dir, cipd_info, run_dir, data.env, data.env_prefix, out_dir,
                data.bot_file)
            command = tools.find_executable(command, env)
            command = process_command(command, out_dir, data.bot_file)
            file_path.ensure_command_has_abs_path(command, cwd)

            result['exit_code'], result['had_hard_timeout'] = run_command(
                command, cwd, env, data.hard_timeout, data.grace_period,
                data.lower_priority, data.containment)
        finally:
          result['duration'] = max(time.time() - start, 0)

      # Try to link files to the output directory, if specified.
      link_outputs_to_outdir(run_dir, out_dir, data.outputs)
      isolated_stats = result['stats'].setdefault('isolated', {})
      result['cas_output_root'], upload_stats = upload_outdir(
          cas_client, data.cas_instance, out_dir, tmp_dir)
      if upload_stats:
        isolated_stats['upload'] = upload_stats

    # We successfully ran the command, set internal_failure back to
    # None (even if the command failed, it's not an internal error).
    result['internal_failure'] = None
  except errors.NonRecoverableCasException as e:
    # We could not find the CAS package. The swarming task should not
    # be retried automatically
    result['missing_cas'] = [e.to_dict()]
    logging.exception('internal failure: %s', e)
    result['internal_failure'] = str(e)
    on_error.report(None)

  except errors.NonRecoverableCipdException as e:
    # We could not find the CIPD package. The swarming task should not
    # be retried automatically
    result['missing_cipd'] = [e.to_dict()]
    logging.exception('internal failure: %s', e)
    result['internal_failure'] = str(e)
    on_error.report(None)
  except Exception as e:
    # An internal error occurred. Report accordingly so the swarming task will
    # be retried automatically.
    logging.exception('internal failure: %s', e)
    result['internal_failure'] = str(e)
    on_error.report(None)

  # Clean up
  finally:
    try:
      cleanup_start = time.time()
      success = True
      if data.leak_temp_dir:
        success = True
        logging.warning(
            'Deliberately leaking %s for later examination', run_dir)
      else:
        # On Windows rmtree(run_dir) call above has a synchronization effect: it
        # finishes only when all task child processes terminate (since a running
        # process locks *.exe file). Examine out_dir only after that call
        # completes (since child processes may write to out_dir too and we need
        # to wait for them to finish).
        dirs_to_remove = [run_dir, tmp_dir, cas_client_dir]
        if out_dir:
          dirs_to_remove.append(out_dir)
        for directory in dirs_to_remove:
          if not fs.isdir(directory):
            continue
          start = time.time()
          try:
            file_path.rmtree(directory)
          except OSError as e:
            logging.error('rmtree(%r) failed: %s', directory, e)
            success = False
          finally:
            logging.info('Cleanup: rmtree(%r) took %d seconds', directory,
                         time.time() - start)
          if not success:
            sys.stderr.write(
                OUTLIVING_ZOMBIE_MSG % (directory, data.grace_period))
            if sys.platform == 'win32':
              subprocess42.check_call(['tasklist.exe', '/V'], stdout=sys.stderr)
            else:
              subprocess42.check_call(['ps', 'axu'], stdout=sys.stderr)
            if result['exit_code'] == 0:
              result['exit_code'] = 1

      if not success and result['exit_code'] == 0:
        result['exit_code'] = 1
    except Exception as e:
      # Swallow any exception in the main finally clause.
      if out_dir:
        logging.exception('Leaking out_dir %s: %s', out_dir, e)
      result['internal_failure'] = str(e)
      on_error.report(None)
    finally:
      cleanup_duration = time.time() - cleanup_start
      result['stats']['cleanup']['duration'] = cleanup_duration
      logging.info('Cleanup: removing directories took %d seconds',
                   cleanup_duration)
  return result


def run_tha_test(data, result_json):
  """Runs an executable and records execution metadata.

  If isolated_hash is specified, downloads the dependencies in the cache,
  hardlinks them into a temporary directory and runs the command specified in
  the .isolated.

  A temporary directory is created to hold the output files. The content inside
  this directory will be uploaded back to |storage| packaged as a .isolated
  file.

  Arguments:
  - data: TaskData instance.
  - result_json: File path to dump result metadata into. If set, the process
    exit code is always 0 unless an internal error occurred.

  Returns:
    Process exit code that should be used.
  """
  if result_json:
    # Write a json output file right away in case we get killed.
    result = {
        'exit_code': None,
        'had_hard_timeout': False,
        'internal_failure': 'Was terminated before completion',
        'outputs_ref': None,
        'cas_output_root': None,
        'version': 5,
    }
    tools.write_json(result_json, result, dense=True)

  # run_isolated exit code. Depends on if result_json is used or not.
  result = map_and_run(data, True)
  logging.info('Result:\n%s', tools.format_json(result, dense=True))

  if result_json:
    # We've found tests to delete 'work' when quitting, causing an exception
    # here. Try to recreate the directory if necessary.
    file_path.ensure_tree(os.path.dirname(result_json))
    tools.write_json(result_json, result, dense=True)
    # Only return 1 if there was an internal error.
    return int(bool(result['internal_failure']))

  # Marshall into old-style inline output.
  if result['outputs_ref']:
    # pylint: disable=unsubscriptable-object
    data = {
        'hash': result['outputs_ref']['isolated'],
        'namespace': result['outputs_ref']['namespace'],
        'storage': result['outputs_ref']['isolatedserver'],
    }
    sys.stdout.flush()
    print('[run_isolated_out_hack]%s[/run_isolated_out_hack]' %
          tools.format_json(data, dense=True))
    sys.stdout.flush()
  return result['exit_code'] or int(bool(result['internal_failure']))


# Yielded by 'install_client_and_packages'.
CipdInfo = collections.namedtuple('CipdInfo', [
  'client',     # cipd.CipdClient object
  'cache_dir',  # absolute path to bot-global cipd tag and instance cache
  'stats',      # dict with stats to return to the server
  'pins',       # dict with installed cipd pins to return to the server
])


@contextlib.contextmanager
def copy_local_packages(_run_dir, cas_dir):
  """Copies CIPD packages from luci/luci-go dir."""
  go_client_dir = os.environ.get('LUCI_GO_CLIENT_DIR')
  assert go_client_dir, ('Please set LUCI_GO_CLIENT_DIR env var to install CIPD'
                         ' packages locally.')
  shutil.copy2(os.path.join(go_client_dir, 'cas' + cipd.EXECUTABLE_SUFFIX),
               os.path.join(cas_dir, 'cas' + cipd.EXECUTABLE_SUFFIX))
  yield None


def _install_packages(run_dir, cipd_cache_dir, client, packages, timeout=None):
  """Calls 'cipd ensure' for packages.

  Args:
    run_dir (str): root of installation.
    cipd_cache_dir (str): the directory to use for the cipd package cache.
    client (CipdClient): the cipd client to use
    packages: packages to install, list [(path, package_name, version), ...].
    timeout (int): if not None, timeout in seconds for cipd ensure to run.

  Returns: list of pinned packages.  Looks like [
    {
      'path': 'subdirectory',
      'package_name': 'resolved/package/name',
      'version': 'deadbeef...',
    },
    ...
  ]
  """
  package_pins = [None]*len(packages)
  def insert_pin(path, name, version, idx):
    package_pins[idx] = {
      'package_name': name,
      # swarming deals with 'root' as '.'
      'path': path or '.',
      'version': version,
    }

  by_path = collections.defaultdict(list)
  for i, (path, name, version) in enumerate(packages):
    # cipd deals with 'root' as ''
    if path == '.':
      path = ''
    by_path[path].append((name, version, i))

  pins = client.ensure(
      run_dir,
      {
          subdir: [(name, vers) for name, vers, _ in pkgs]
          for subdir, pkgs in by_path.items()
      },
      cache_dir=cipd_cache_dir,
      timeout=timeout,
  )

  for subdir, pin_list in sorted(pins.items()):
    this_subdir = by_path[subdir]
    for i, (name, version) in enumerate(pin_list):
      insert_pin(subdir, name, version, this_subdir[i][2])

  assert None not in package_pins, (packages, pins, package_pins)

  return package_pins


@contextlib.contextmanager
def install_client_and_packages(run_dir, packages, service_url,
                                client_package_name, client_version, cache_dir,
                                cas_dir):
  """Bootstraps CIPD client and installs CIPD packages.

  Yields CipdClient, stats, client info and pins (as single CipdInfo object).

  Pins and the CIPD client info are in the form of:
    [
      {
        "path": path, "package_name": package_name, "version": version,
      },
      ...
    ]
  (the CIPD client info is a single dictionary instead of a list)

  such that they correspond 1:1 to all input package arguments from the command
  line. These dictionaries make their all the way back to swarming, where they
  become the arguments of CipdPackage.

  If 'packages' list is empty, will bootstrap CIPD client, but won't install
  any packages.

  The bootstrapped client (regardless whether 'packages' list is empty or not),
  will be made available to the task via $PATH.

  Args:
    run_dir (str): root of installation.
    packages: packages to install, list [(path, package_name, version), ...].
    service_url (str): CIPD server url, e.g.
      "https://chrome-infra-packages.appspot.com."
    client_package_name (str): CIPD package name of CIPD client.
    client_version (str): Version of CIPD client.
    cache_dir (str): where to keep cache of cipd clients, packages and tags.
    cas_dir (str): where to download cas client.
  """
  assert cache_dir

  start = time.time()

  cache_dir = os.path.abspath(cache_dir)
  cipd_cache_dir = os.path.join(cache_dir, 'cache')  # tag and instance caches
  run_dir = os.path.abspath(run_dir)
  packages = packages or []

  get_client_start = time.time()
  client_manager = cipd.get_client(cache_dir, service_url, client_package_name,
                                   client_version)

  with client_manager as client:
    logging_utils.user_logs('Installed CIPD client')
    get_client_duration = time.time() - get_client_start

    package_pins = []
    if packages:
      package_pins = _install_packages(run_dir, cipd_cache_dir, client,
                                       packages)
      logging_utils.user_logs('Installed task packages')

    # Install cas client to |cas_dir|.
    _install_packages(cas_dir,
                      cipd_cache_dir,
                      client, [('', _CAS_PACKAGE, _LUCI_GO_REVISION)],
                      timeout=10 * 60)
    logging_utils.user_logs('Installed CAS client')

    file_path.make_tree_files_read_only(run_dir)

    total_duration = time.time() - start
    logging_utils.user_logs(
        'Installing CIPD client and packages took %d seconds', total_duration)

    yield CipdInfo(
        client=client,
        cache_dir=cipd_cache_dir,
        stats={
            'duration': total_duration,
            'get_client_duration': get_client_duration,
        },
        pins={
            'client_package': {
                'package_name': client.package_name,
                'version': client.instance_id,
            },
            'packages': package_pins,
        })


def create_option_parser():
  parser = logging_utils.OptionParserWithLogging(
      usage='%prog <options> [command to run or extra args]',
      version=__version__,
      log_file=RUN_ISOLATED_LOG_FILE)
  parser.add_option(
      '--clean',
      action='store_true',
      help='Cleans the cache, trimming it necessary and remove corrupted items '
      'and returns without executing anything; use with -v to know what '
      'was done')
  parser.add_option(
      '--json',
      help='dump output metadata to json file. When used, run_isolated returns '
      'non-zero only on internal failure')
  parser.add_option(
      '--hard-timeout', type='float', help='Enforce hard timeout in execution')
  parser.add_option(
      '--grace-period',
      type='float',
      help='Grace period between SIGTERM and SIGKILL')
  parser.add_option(
      '--relative-cwd',
      help='Ignore the isolated \'relative_cwd\' and use this one instead')
  parser.add_option(
      '--env',
      default=[],
      action='append',
      help='Environment variables to set for the child process')
  parser.add_option(
      '--env-prefix',
      default=[],
      action='append',
      help='Specify a VAR=./path/fragment to put in the environment variable '
      'before executing the command. The path fragment must be relative '
      'to the isolated run directory, and must not contain a `..` token. '
      'The path will be made absolute and prepended to the indicated '
      '$VAR using the OS\'s path separator. Multiple items for the same '
      '$VAR will be prepended in order.')
  parser.add_option(
      '--bot-file',
      help='Path to a file describing the state of the host. The content is '
      'defined by on_before_task() in bot_config.')
  parser.add_option(
      '--switch-to-account',
      help='If given, switches LUCI_CONTEXT to given logical service account '
      '(e.g. "task" or "system") before launching the isolated process.')
  parser.add_option(
      '--output',
      action='append',
      help='Specifies an output to return. If no outputs are specified, all '
      'files located in $(ISOLATED_OUTDIR) will be returned; '
      'otherwise, outputs in both $(ISOLATED_OUTDIR) and those '
      'specified by --output option (there can be multiple) will be '
      'returned. Note that if a file in OUT_DIR has the same path '
      'as an --output option, the --output version will be returned.')
  parser.add_option(
      '-a',
      '--argsfile',
      # This is actually handled in parse_args; it's included here purely so it
      # can make it into the help text.
      help='Specify a file containing a JSON array of arguments to this '
      'script. If --argsfile is provided, no other argument may be '
      'provided on the command line.')
  parser.add_option(
      '--report-on-exception',
      action='store_true',
      help='Whether report exception during execution to isolate server. '
      'This flag should only be used in swarming bot.')

  group = optparse.OptionGroup(parser,
                               'Data source - Content Addressed Storage')
  group.add_option(
      '--cas-instance', help='Full CAS instance name for input/output files.')
  group.add_option(
      '--cas-digest',
      help='Digest of the input root on RBE-CAS. The format is '
      '`{hash}/{size_bytes}`.')
  parser.add_option_group(group)

  # Cache options.
  add_cas_cache_options(parser)

  cipd.add_cipd_options(parser)

  group = optparse.OptionGroup(parser, 'Named caches')
  group.add_option(
      '--named-cache',
      dest='named_caches',
      action='append',
      nargs=3,
      default=[],
      help='A named cache to request. Accepts 3 arguments: name, path, hint. '
      'name identifies the cache, must match regex [a-z0-9_]{1,4096}. '
      'path is a path relative to the run dir where the cache directory '
      'must be put to. '
      'This option can be specified more than once.')
  group.add_option(
      '--named-cache-root',
      default='named_caches',
      help='Cache root directory. Default=%default')
  parser.add_option_group(group)

  group = optparse.OptionGroup(parser, 'Process containment')
  parser.add_option(
      '--lower-priority', action='store_true',
      help='Lowers the child process priority')
  parser.add_option('--containment-type',
                    choices=('NONE', 'AUTO', 'JOB_OBJECT'),
                    default='NONE',
                    help='Type of container to use')
  parser.add_option(
      '--limit-processes',
      type='int',
      default=0,
      help='Maximum number of active processes in the containment')
  parser.add_option(
      '--limit-total-committed-memory',
      type='int',
      default=0,
      help='Maximum sum of committed memory in the containment')
  parser.add_option_group(group)

  group = optparse.OptionGroup(parser, 'Debugging')
  group.add_option(
      '--leak-temp-dir',
      action='store_true',
      help='Deliberately leak isolate\'s temp dir for later examination. '
      'Default: %default')
  group.add_option('--root-dir', help='Use a directory instead of a random one')
  parser.add_option_group(group)

  auth.add_auth_options(parser)

  parser.set_defaults(cache='cache')
  return parser


def add_cas_cache_options(parser):
  group = optparse.OptionGroup(parser, 'CAS cache management')
  group.add_option(
      '--cas-cache',
      metavar='DIR',
      default='cas-cache',
      help='Directory to keep a local cache of the files. Accelerates download '
      'by reusing already downloaded files. Default=%default')
  group.add_option(
      '--kvs-dir',
      default='',
      help='CAS cache dir using kvs for small files. Default=%default')
  group.add_option(
      '--max-cache-size',
      type='int',
      metavar='NNN',
      default=50 * 1024 * 1024 * 1024,
      help='Trim if the cache gets larger than this value, default=%default')
  group.add_option(
      '--min-free-space',
      type='int',
      metavar='NNN',
      default=2 * 1024 * 1024 * 1024,
      help='Trim if disk free space becomes lower than this value, '
      'default=%default')
  parser.add_option_group(group)


def process_cas_cache_options(options):
  if options.cas_cache:
    policies = local_caching.CachePolicies(
        max_cache_size=options.max_cache_size,
        min_free_space=options.min_free_space,
        # max_items isn't used for CAS cache for now.
        max_items=None,
        max_age_secs=MAX_AGE_SECS)

    return local_caching.DiskContentAddressedCache(os.path.abspath(
        options.cas_cache),
                                                   policies,
                                                   trim=False)
  return local_caching.MemoryContentAddressedCache()


def process_named_cache_options(parser, options, time_fn=None):
  """Validates named cache options and returns a CacheManager."""
  if options.named_caches and not options.named_cache_root:
    parser.error('--named-cache is specified, but --named-cache-root is empty')
  for name, path, hint in options.named_caches:
    if not CACHE_NAME_RE.match(name):
      parser.error(
          'cache name %r does not match %r' % (name, CACHE_NAME_RE.pattern))
    if not path:
      parser.error('cache path cannot be empty')
    try:
      int(hint)
    except ValueError:
      parser.error('cache hint must be a number')
  if options.named_cache_root:
    # Make these configurable later if there is use case but for now it's fairly
    # safe values.
    # In practice, a fair chunk of bots are already recycled on a daily schedule
    # so this code doesn't have any effect to them, unless they are preloaded
    # with a really old cache.
    policies = local_caching.CachePolicies(
        # 1TiB.
        max_cache_size=1024 * 1024 * 1024 * 1024,
        min_free_space=options.min_free_space,
        max_items=50,
        max_age_secs=MAX_AGE_SECS)
    keep = [name for name, _, _ in options.named_caches]
    root_dir = os.path.abspath(options.named_cache_root)
    cache = local_caching.NamedCache(root_dir,
                                     policies,
                                     time_fn=time_fn,
                                     keep=keep)
    # Touch any named caches we're going to use to minimize thrashing
    # between tasks that request some (but not all) of the same named caches.
    cache.touch(*[name for name, _, _ in options.named_caches])
    return cache
  return None


def parse_args(args):
  # Create a fake mini-parser just to get out the "-a" command. Note that
  # it's not documented here; instead, it's documented in create_option_parser
  # even though that parser will never actually get to parse it. This is
  # because --argsfile is exclusive with all other options and arguments.
  file_argparse = argparse.ArgumentParser(add_help=False)
  file_argparse.add_argument('-a', '--argsfile')
  (file_args, nonfile_args) = file_argparse.parse_known_args(args)
  if file_args.argsfile:
    if nonfile_args:
      file_argparse.error('Can\'t specify --argsfile with'
                          'any other arguments (%s)' % nonfile_args)
    try:
      with open(file_args.argsfile, 'r') as f:
        args = json.load(f)
    except (IOError, OSError, ValueError) as e:
      # We don't need to error out here - "args" is now empty,
      # so the call below to parser.parse_args(args) will fail
      # and print the full help text.
      print('Couldn\'t read arguments: %s' % e, file=sys.stderr)

  # Even if we failed to read the args, just call the normal parser now since it
  # will print the correct help message.
  parser = create_option_parser()
  options, args = parser.parse_args(args)
  if not isinstance(options.cipd_enabled, (bool, int)):
    options.cipd_enabled = _str_to_bool(options.cipd_enabled)
  return (parser, options, args)


def _str_to_bool(val):
  """Converts boolean-ish string to True or False."""
  val = val.lower()
  if val in ('y', 'yes', 't', 'true', 'on', '1'):
    return True
  if val in ('n', 'no', 'f', 'false', 'off', '0'):
    return False
  raise ValueError('Invalid truth value %r' % val)


def _calc_named_cache_hint(named_cache, named_caches):
  """Returns the expected size of the missing named caches."""
  present = named_cache.available
  size = 0
  logging.info('available named cache %s', present)
  for name, _, hint in named_caches:
    if name not in present:
      hint = int(hint)
      if hint > 0:
        logging.info("named cache hint: %s, %d", name, hint)
        size += hint
  logging.info("total size of named cache hint: %d", size)
  return size


def _clean_cmd(parser, options, caches, root):
  """Cleanup cache dirs/files."""
  if options.json:
    parser.error('Can\'t use --json with --clean.')
  if options.named_caches:
    parser.error('Can\t use --named-cache with --clean.')
  if options.cas_instance or options.cas_digest:
    parser.error('Can\t use --cas-instance, --cas-digest with --clean.')

  logging.info("initial free space: %d", file_path.get_free_space(root))

  if options.kvs_dir and fs.isdir(options.kvs_dir):
    # Remove kvs file if its size exceeds fixed threshold.
    kvs_dir = options.kvs_dir
    size = file_path.get_recursive_size(kvs_dir)
    if size >= _CAS_KVS_CACHE_THRESHOLD:
      logging.info("remove kvs dir with size: %d", size)
      file_path.rmtree(kvs_dir)

  # Trim first, then clean.
  local_caching.trim_caches(
      caches,
      root,
      min_free_space=options.min_free_space,
      max_age_secs=MAX_AGE_SECS)
  logging.info("free space after trim: %d", file_path.get_free_space(root))
  for c in caches:
    c.cleanup()
  logging.info("free space after cleanup: %d", file_path.get_free_space(root))


def main(args):
  # Warning: when --argsfile is used, the strings are unicode instances, when
  # parsed normally, the strings are str instances.
  (parser, options, args) = parse_args(args)

  # adds another log level for logs which are directed to standard output
  # these logs will be uploaded to cloudstorage
  logging_utils.set_user_level_logging()

  # Must be logged after parse_args(), which eventually calls
  # logging_utils.prepare_logging() which expects no logs before its call.
  logging_utils.user_logs('Starting run_isolated script')

  SWARMING_SERVER = os.environ.get('SWARMING_SERVER')
  SWARMING_TASK_ID = os.environ.get('SWARMING_TASK_ID')
  if options.report_on_exception and SWARMING_SERVER:
    task_url = None
    if SWARMING_TASK_ID:
      task_url = '%s/task?id=%s' % (SWARMING_SERVER, SWARMING_TASK_ID)
    on_error.report_on_exception_exit(SWARMING_SERVER, source=task_url)

  if not file_path.enable_symlink():
    logging.warning('Symlink support is not enabled')

  named_cache = process_named_cache_options(parser, options)
  # hint is 0 if there's no named cache.
  hint = _calc_named_cache_hint(named_cache, options.named_caches)
  if hint:
    # Increase the --min-free-space value by the hint, and recreate the
    # NamedCache instance so it gets the updated CachePolicy.
    options.min_free_space += hint
    named_cache = process_named_cache_options(parser, options)

  # TODO(maruel): CIPD caches should be defined at an higher level here too, so
  # they can be cleaned the same way.

  cas_cache = process_cas_cache_options(options)

  caches = []
  if cas_cache:
    caches.append(cas_cache)
  if named_cache:
    caches.append(named_cache)
  root = caches[0].cache_dir if caches else os.getcwd()
  if options.clean:
    _clean_cmd(parser, options, caches, root)
    return 0

  # Trim must still be done for the following case:
  # - named-cache was used
  # - some entries, with a large hint, where missing
  # - --min-free-space was increased accordingly, thus trimming is needed
  # Otherwise, this will have no effect, as bot_main calls run_isolated with
  # --clean after each task.
  additional_buffer = _FREE_SPACE_BUFFER_FOR_CIPD_PACKAGES
  if options.kvs_dir:
    additional_buffer += _CAS_KVS_CACHE_THRESHOLD
  # Add some buffer for Go CLI.
  min_free_space = options.min_free_space + additional_buffer

  def trim_caches_fn(stats):
    start = time.time()
    local_caching.trim_caches(
        caches, root, min_free_space=min_free_space, max_age_secs=MAX_AGE_SECS)
    duration = time.time() - start
    stats['duration'] = duration
    logging_utils.user_logs('trim_caches: took %d seconds', duration)

  # Save state of cas cache not to overwrite state from go client.
  if cas_cache:
    cas_cache.save()
    cas_cache = None

  if not args:
    parser.error('command to run is required.')

  auth.process_auth_options(parser, options)

  if ISOLATED_OUTDIR_PARAMETER in args and not options.cas_instance:
    parser.error('%s in args requires --cas-instance' %
                 ISOLATED_OUTDIR_PARAMETER)

  if options.root_dir:
    options.root_dir = os.path.abspath(options.root_dir)
  else:
    options.root_dir = tempfile.mkdtemp(prefix='root')
  if options.json:
    options.json = os.path.abspath(options.json)

  if any('=' not in i for i in options.env):
    parser.error(
        '--env required key=value form. value can be skipped to delete '
        'the variable')
  options.env = dict(i.split('=', 1) for i in options.env)

  prefixes = {}
  cwd = os.path.realpath(os.getcwd())
  for item in options.env_prefix:
    if '=' not in item:
      parser.error(
        '--env-prefix %r is malformed, must be in the form `VAR=./path`'
        % item)
    key, opath = item.split('=', 1)
    if os.path.isabs(opath):
      parser.error('--env-prefix %r path is bad, must be relative.' % opath)
    opath = os.path.normpath(opath)
    if not os.path.realpath(os.path.join(cwd, opath)).startswith(cwd):
      parser.error(
          '--env-prefix %r path is bad, must be relative and not contain `..`.'
          % opath)
    prefixes.setdefault(key, []).append(opath)
  options.env_prefix = prefixes

  cipd.validate_cipd_options(parser, options)

  install_packages_fn = copy_local_packages
  tmp_cipd_cache_dir = None
  if options.cipd_enabled:
    cache_dir = options.cipd_cache
    if not cache_dir:
      tmp_cipd_cache_dir = tempfile.mkdtemp()
      cache_dir = tmp_cipd_cache_dir
    install_packages_fn = (lambda run_dir, cas_dir: install_client_and_packages(
        run_dir,
        cipd.parse_package_args(options.cipd_packages),
        options.cipd_server,
        options.cipd_client_package,
        options.cipd_client_version,
        cache_dir=cache_dir,
        cas_dir=cas_dir))

  @contextlib.contextmanager
  def install_named_caches(run_dir, stats):
    # WARNING: this function depends on "options" variable defined in the outer
    # function.
    assert str(run_dir), repr(run_dir)
    assert os.path.isabs(run_dir), run_dir
    named_caches = [(os.path.join(run_dir, str(relpath)), name)
                    for name, relpath, _ in options.named_caches]
    install_start = time.time()
    for path, name in named_caches:
      named_cache.install(path, name)
    install_duration = time.time() - install_start
    stats['install']['duration'] = install_duration
    logging.info('named_caches: install took %d seconds', install_duration)
    try:
      yield
    finally:
      # Uninstall each named cache, returning it to the cache pool. If an
      # uninstall fails for a given cache, it will remain in the task's
      # temporary space, get cleaned up by the Swarming bot, and be lost.
      #
      # If the Swarming bot cannot clean up the cache, it will handle it like
      # any other bot file that could not be removed.
      uninstall_start = time.time()
      for path, name in reversed(named_caches):
        try:
          # uninstall() doesn't trim but does call save() implicitly. Trimming
          # *must* be done manually via periodic 'run_isolated.py --clean'.
          named_cache.uninstall(path, name)
        except local_caching.NamedCacheError:
          if sys.platform == 'win32':
            # Show running processes.
            sys.stderr.write("running process\n")
            subprocess42.check_call(['tasklist.exe', '/V'], stdout=sys.stderr)

          error = (
              'Error while removing named cache %r at %r. The cache will be'
              ' lost.' % (path, name))
          logging.exception(error)
          on_error.report(error)
      uninstall_duration = time.time() - uninstall_start
      stats['uninstall']['duration'] = uninstall_duration
      logging.info('named_caches: uninstall took %d seconds',
                   uninstall_duration)

  command = args
  if options.relative_cwd:
    a = os.path.normpath(os.path.abspath(options.relative_cwd))
    if not a.startswith(os.getcwd()):
      parser.error(
          '--relative-cwd must not try to escape the working directory')

  containment_type = subprocess42.Containment.NONE
  if options.containment_type == 'AUTO':
    containment_type = subprocess42.Containment.AUTO
  if options.containment_type == 'JOB_OBJECT':
    containment_type = subprocess42.Containment.JOB_OBJECT
  containment = subprocess42.Containment(
      containment_type=containment_type,
      limit_processes=options.limit_processes,
      limit_total_committed_memory=options.limit_total_committed_memory)

  data = TaskData(command=command,
                  relative_cwd=options.relative_cwd,
                  cas_instance=options.cas_instance,
                  cas_digest=options.cas_digest,
                  outputs=options.output,
                  install_named_caches=install_named_caches,
                  leak_temp_dir=options.leak_temp_dir,
                  root_dir=options.root_dir,
                  hard_timeout=options.hard_timeout,
                  grace_period=options.grace_period,
                  bot_file=options.bot_file,
                  switch_to_account=options.switch_to_account,
                  install_packages_fn=install_packages_fn,
                  cas_cache_dir=options.cas_cache,
                  cas_cache_policies=local_caching.CachePolicies(
                      max_cache_size=options.max_cache_size,
                      min_free_space=options.min_free_space,
                      max_items=None,
                      max_age_secs=None,
                  ),
                  cas_kvs=options.kvs_dir,
                  env=options.env,
                  env_prefix=options.env_prefix,
                  lower_priority=bool(options.lower_priority),
                  containment=containment,
                  trim_caches_fn=trim_caches_fn)
  try:
    return run_tha_test(data, options.json)
  except (cipd.Error, local_caching.NamedCacheError,
          local_caching.NoMoreSpace) as ex:
    print(ex.message, file=sys.stderr)
    on_error.report(None)
    return 1
  finally:
    if tmp_cipd_cache_dir is not None:
      try:
        file_path.rmtree(tmp_cipd_cache_dir)
      except OSError:
        logging.exception('Remove tmp_cipd_cache_dir=%s failed',
                          tmp_cipd_cache_dir)
        # Best effort clean up. Failed to do so doesn't affect the outcome.

if __name__ == '__main__':
  subprocess42.inhibit_os_error_reporting()
  # Ensure that we are always running with the correct encoding.
  fix_encoding.fix_encoding()
  net.set_user_agent('run_isolated.py/' + __version__)
  sys.exit(main(sys.argv[1:]))
