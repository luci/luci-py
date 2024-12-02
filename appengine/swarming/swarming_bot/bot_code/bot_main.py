# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Swarming bot main process.

This is the program that communicates with the Swarming server, ensures the code
is always up to date and executes a child process to run tasks and upload
results back.

It manages self-update and rebooting the host in case of problems.

Sections are:
  - Globals
  - Monitoring
  - bot_config handler
  - Public functions used by __main__.py
  - Sub process management
  - Bot lifetime management
"""

from __future__ import print_function

import argparse
import contextlib
import fnmatch
import functools
import json
import logging
import os
import platform
import random
import shutil
import sys
import tempfile
import threading
import time
import traceback
import types
import zipfile

# Import _strptime before threaded code. datetime.datetime.strptime is
# threadsafe except for the initial import of the _strptime module.
# See https://bugs.python.org/issue7980.
import _strptime  # pylint: disable=unused-import

from api import bot
from api import os_utilities
from api import platforms
from bot_code import bot_auth
from bot_code import clock
from bot_code import common
from bot_code import file_refresher
from bot_code import remote_client
from bot_code import remote_client_errors
from bot_code import singleton
from utils import file_path
from utils import fs
from utils import net
from utils import on_error
from utils import subprocess42
from utils import tools
from utils import zip_package


### Globals


# Used to opportunistically set the error handler to notify the server when the
# process exits due to an exception.
_ERROR_HANDLER_WAS_REGISTERED = False


# If False (happens in tests), avoid catching broad Exception in the bot loop.
# Otherwise failed test assertions are also getting trapped, which makes tests
# hard to debug.
_TRAP_ALL_EXCEPTIONS = True


# Set to the zip's name containing this file. This is set to the absolute path
# to swarming_bot.zip when run as part of swarming_bot.zip. This value is
# overriden in unit tests.
#
# Note: this more or less requires the bot to be in a path without non-ASCII
# characters.
THIS_FILE = os.path.abspath(zip_package.get_main_script_path())
THIS_DIR = os.path.dirname(THIS_FILE)


# The singleton, initially unset.
SINGLETON = singleton.Singleton(THIS_DIR)

# Dirname of kvs cache used to store small files from cas.
_CAS_KVS_CACHE_DB = 'cas_kvs_cache_db'

# Allowlist files that can be present in the bot's directory. Anything else
# will be forcibly deleted on startup! Note that 'w' (work) is not in this list,
# as we want it to be deleted on startup.
# See
# https://chromium.googlesource.com/infra/luci/luci-py.git/+/main/appengine/swarming/doc/Bot.md
# for more details.
PASSLIST = (
    '*-cacert.pem',
    '.vpython3',
    'README',
    'README.md',
    'c',
    'cas_cache',
    'cipd_cache',
    'logs',
    'swarming.lck',
    'swarming_bot.1.zip',
    'swarming_bot.2.zip',
    'swarming_bot.zip',
    'tmp',
    _CAS_KVS_CACHE_DB,
)


# These settings are documented in ../config/bot_config.py.
# Keep in sync with ../config/bot_config.py. This is enforced by a unit test.
DEFAULT_SETTINGS = {
    'free_partition': {
        'root': {
            'size': 1 * 1024 * 1024 * 1024,
            'max_percent': 10.,
            'min_percent': 6.,
        },
        'bot': {
            'size': 4 * 1024 * 1024 * 1024,
            'max_percent': 15.,
            'min_percent': 7.,
            'wiggle': 250 * 1024 * 1024,
        },
    },
    'caches': {
        'isolated': {
            'size': 50 * 1024 * 1024 * 1024,
            'items': 50 * 1024,
        },
    },
}

# Flag to decide if bot is running in test mode. This is mostly used by smoke
# and integration tests.
# TODO(1099655): Remove once we have fully enabled CIPD in both prod and tests.
_IN_TEST_MODE = False


### Monitoring


def _monitor_call(func):
  """Decorates a functions and reports the runtime into logs."""
  def hook(chained, botobj, name, *args, **kwargs):
    start = time.time()
    try:
      return func(chained, botobj, name, *args, **kwargs)
    finally:
      duration = max(0, (time.time() - start) * 1000)
      logging.info('%s(): %gs', name, round(duration/1000., 3))
  return hook


### bot_config handler


# Reference to the config/bot_config.py module inside the swarming_bot.zip file.
# This variable is initialized inside _get_bot_config().
_BOT_CONFIG = None
# Reference to the second bot_config.py module injected by the server. This
# variable is initialized inside _do_handshake().
_EXTRA_BOT_CONFIG = None
# Super Sticky quarantine string. This variable is initialized inside
# _set_quarantined() and be set at various places when a hook throws an
# exception. Restarting the bot will clear the quarantine, which includes
# updated the bot due to new bot_config or new bot code.
_QUARANTINED = None


def _set_quarantined(reason):
  """Sets the Super Sticky Quarantine string."""
  logging.error('_set_quarantined(%s)', reason)
  global _QUARANTINED
  _QUARANTINED = _QUARANTINED or reason


def _get_bot_config():
  """Returns the bot_config.py module. Imports it only once.

  This file is called implicitly by _call_hook() and _call_hook_safe().
  """
  global _BOT_CONFIG
  if not _BOT_CONFIG:
    from config import bot_config as _BOT_CONFIG
  return _BOT_CONFIG


def _register_extra_bot_config(content, rev, script):
  """Registers the server injected extra injected.py bot_config.

  This file is called implicitly by _call_hook() and _call_hook_safe().
  """
  global _EXTRA_BOT_CONFIG
  if isinstance(content, str):
    # compile will throw if there's a '# coding: utf-8' line and the string is
    # in unicode. <3 python.
    content = content.encode('utf-8')
  try:
    compiled = compile(content, 'injected.py', 'exec')
    _EXTRA_BOT_CONFIG = types.ModuleType('injected')
    exec(compiled, _EXTRA_BOT_CONFIG.__dict__)
    logging.debug('extra bot_config %s at rev %s was injected.', script, rev)
  except (SyntaxError, TypeError) as e:
    _set_quarantined(
        'handshake returned invalid injected bot_config.py: %s' % e)


@_monitor_call
def _call_hook(chained, botobj, name, *args, **kwargs):
  """Calls a hook function named `name` in bot_config.py.

  If `chained` is True, calls the general bot_config.py then the injected
  version.

  If `chained` is False, the injected bot_config version is called first, and
  only if not present the general bot_config version is called.
  """
  try:
    if not chained:
      # Injected version has higher priority.
      hook = getattr(_EXTRA_BOT_CONFIG, name, None)
      if hook:
        return hook(botobj, *args, **kwargs)
      hook = getattr(_get_bot_config(), name, None)
      if hook:
        return hook(botobj, *args, **kwargs)
      # The hook is not defined.
      return None

    # In the case of chained=True, call both hooks. Call the generic one first,
    # then the specialized.
    ret = None
    hook = getattr(_get_bot_config(), name, None)
    if hook:
      ret = hook(botobj, *args, **kwargs)
    hook = getattr(_EXTRA_BOT_CONFIG, name, None)
    if hook:
      # Ignores the previous return value.
      ret = hook(botobj, *args, **kwargs)
    return ret
  finally:
    # TODO(maruel): Handle host_reboot() request the same way.
    if botobj:
      msg = botobj.bot_restart_msg()
      if msg:
        # The hook requested a bot restart. Do it right after the hook call.
        _bot_restart(botobj, msg)


def _call_hook_safe(chained, botobj, name, *args):
  """Calls a hook function in bot_config.py.

  Like _call_hook() but traps most exceptions.
  """
  try:
    return _call_hook(chained, botobj, name, *args)
  except Exception as e:
    traceback.print_exc()
    logging.exception('%s() threw', name)
    msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
    if botobj:
      botobj.post_error('Failed to call hook %s(): %s' % (name, msg))
    return None


def _get_dimensions(botobj):
  """Returns bot_config.py's get_dimensions() dict.

  Traps exceptions, quarantining the bot if they happen.
  """
  out = _call_hook_safe(False, botobj, 'get_dimensions')
  if _is_jsonish_dict(out):
    return out.copy()
  try:
    _set_quarantined('get_dimensions(): expected a JSON dict, got %r' % out)
    out = os_utilities.get_dimensions()
    out['quarantined'] = ['1']
    return out
  except Exception:
    logging.exception('os.utilities.get_dimensions() failed')
    return {
        'bot_error': ['bot_main:_get_dimensions'],
        'id': [_get_botid_safe()],
        'quarantined': ['1'],
    }


@tools.cached
def _get_botid_safe():
  """Paranoid version of get_hostname_short()."""
  try:
    return os_utilities.get_hostname_short()
  except Exception as e:
    logging.exception('os.utilities.get_hostname_short() failed')
    return 'error_%s' % str(e)


def _get_settings(botobj):
  """Returns settings for this bot.

  The way used to make it work safely is to take the default settings, then
  merge the custom settings. This way, a user can only specify a subset of the
  desired settings.

  The function won't alert on unknown settings. This is so bot_config.py can be
  updated in advance before pushing new bot_main.py. The main drawback is that
  it will make typos silently fail. CHECK FOR TYPOS in get_settings() in your
  bot_config.py.
  """
  settings = _call_hook_safe(False, botobj, 'get_settings')
  try:
    if isinstance(settings, dict):
      return _dict_deep_merge(DEFAULT_SETTINGS, settings)
  except (KeyError, TypeError, ValueError):
    logging.exception('get_settings() failed')
  return DEFAULT_SETTINGS


def _get_rbe_worker_properties():
  """Extracts remote_client.WorkerProperties from the environment."""
  md = platforms.gce.get_metadata()
  if not md:
    return None
  attrs = md.get('instance', {}).get('attributes', {})
  pool_id = attrs.get('pool_id', '')
  pool_version = attrs.get('pool_version', '')
  if pool_id or pool_version:
    return remote_client.WorkerProperties(pool_id, pool_version)
  return None


def _get_state(botobj, sleep_streak):
  """Returns dict with a state of the bot reported to the server with each poll.

  Traps exceptions, quarantining the bot if they happen.
  """
  state = _call_hook_safe(False, botobj, 'get_state')
  if not _is_jsonish_dict(state):
    _set_quarantined('get_state(): expected a JSON dict, got %r' % state)
    state = {'broken': state}

  if not state.get('quarantined'):
    if not _is_base_dir_ok(botobj):
      # Use super hammer in case of dangerous environment.
      _set_quarantined('Can\'t run from the base directory')
    if _QUARANTINED:
      state['quarantined'] = _QUARANTINED

  if not state.get('quarantined'):
    try:
      # Reuse the data from 'state/disks'.
      disks = state.get('disks', {})
      err = _get_disks_quarantine(botobj, disks)
      if err:
        state['quarantined'] = err
        _cleanup_purgeable_space(botobj)
    except Exception as e:
      logging.exception('checking free or purgeable space failed')
      state['quarantined'] = '%s\n%s' % (e, traceback.format_exc()[-2048:])

  state['sleep_streak'] = sleep_streak
  return state


def _get_disks_quarantine(botobj, disks):
  """Returns a quarantine error message when there's not enough free space.

  It looks at both root partition and the current partition the bot is running
  in.
  """
  settings = _get_settings(botobj)['free_partition']
  # On Windows, drive letters are always lower case.
  root = 'c:\\' if sys.platform == 'win32' else '/'

  errors = []

  def _check_for_quarantine(r, i, key):
    min_free = _min_free_disk(i, settings[key])
    if int(i['free_mb']*1024*1024) < min_free:
      errors.append(
          'Not enough free disk space on %s. %.1fmib < %.1fmib' %
          (r, i['free_mb'], round(min_free / 1024. / 1024., 1)))

  # root may be missing in the case of netbooted devices.
  if root in disks:
    _check_for_quarantine(root, disks[root], 'root')

  # Try again with the bot's base directory. It is frequent to run the bot
  # from a secondary partition, to reduce the risk of OS failure due to full
  # root partition.
  # This code is similar to os_utilities.get_disk_size().
  path = botobj.base_dir
  case_insensitive = sys.platform in ('darwin', 'win32')
  if case_insensitive:
    path = path.lower()
  for mount, infos in sorted(disks.items(), key=lambda x: -len(x[0])):
    if path.startswith(mount.lower() if case_insensitive else mount):
      # Apply 'bot' check if bot is on its own partition, or it's on
      # root partition and there are no errors reported yet.
      if mount != root or not errors:
        _check_for_quarantine(mount, infos, 'bot')
      break
  if errors:
    return '\n'.join(errors)


def _update_bot_attributes(botobj, sleep_streak):
  """Queries environment and hooks for dimensions and state and updates Bot.

  This is generally pretty slow.
  """
  dims = _get_dimensions(botobj)
  state = _get_state(botobj, sleep_streak)
  logging.debug('Dimensions %s', dims)
  logging.debug('State %s', state)
  with botobj.mutate_internals() as mut:
    mut.update_dimensions(dims)
    mut.update_state(state)


def _cleanup_purgeable_space(botobj):
  """Frees up purgeable space by creating large files and removing them.

  It runs only on ARM Macmini.

  TODO(crbug.com/1142848): remove this workaround after bot code is fixed to
  recognize purgeable space.
  """
  # Skip if it's not ARM MMac.
  mac_model = botobj.dimensions.get('mac_model')
  if not mac_model or mac_model[0] != 'Macmini9,1':
    return

  start = time.time()
  logging.info('_cleanup_purgeable_space: creating large files.')

  tempdir = os.path.join(botobj.base_dir, 'largefiles')
  fs.mkdir(tempdir)

  for cnt in range(3):
    # create a largefile until it stops with 'No space left on device'.
    fname = os.path.join(tempdir, 'largefile%d' % cnt)
    cmd = ['dd', 'if=/dev/zero', 'of=%s' % fname, 'bs=32m']
    output, _ = _Popen(botobj, cmd).communicate(None)
    logging.info(
        '_cleanup_purgeable_space: created a large file. cnt=%d. output=%s',
        cnt, output)

  # remove created all files and the temp directory.
  file_path.rmtree(tempdir)

  logging.info(
      '_cleanup_purgeable_space: removed the created files. %s sec elapsed.',
      time.time() - start)


def _get_authentication_headers(botobj):
  """Calls bot_config.get_authentication_headers() if it is defined.

  See remote_client.RemoteClient doc for the expected format of the return
  value.

  Doesn't catch exceptions. RemoteClient knows how to deal with them.
  """
  return _call_hook(False, botobj, 'get_authentication_headers') or (None, None)


def _on_shutdown_hook(b):
  """Called when the bot is restarting."""
  _call_hook_safe(True, b, 'on_bot_shutdown')
  # Aggressively set itself up so we ensure the auto-reboot configuration is
  # fine before restarting the host. This is important as some tasks delete the
  # autorestart script (!)
  setup_bot(True)


def _min_free_disk(infos, settings):
  """Returns the calculated minimum free disk space for this partition.

  See _get_settings() in ../config/bot_config.py for an explanation.
  """
  size = int(infos['size_mb']*1024*1024)
  x1 = settings['size'] or 0
  x2 = int(round(size * float(settings['max_percent'] or 0) * 0.01))
  # Select the lowest non-zero value.
  x = min(x1, x2) if (x1 and x2) else (x1 or x2)
  # Select the maximum value.
  return max(x, int(round(size * float(settings['min_percent'] or 0) * 0.01)))


def _dict_deep_merge(x, y):
  """Returns the union of x and y.

  y takes predescence.
  """
  if x is None:
    return y
  if y is None:
    return x
  if isinstance(x, dict):
    if isinstance(y, dict):
      return {k: _dict_deep_merge(x.get(k), y.get(k)) for k in set(x).union(y)}
    assert y is None, repr(y)
    return x
  if isinstance(y, dict):
    assert x is None, repr(x)
    return y
  # y is overriding x.
  return y


def _is_base_dir_ok(botobj):
  """Returns False if the bot must be quarantined at all cost."""
  if not botobj:
    # This can happen very early in the process lifetime.
    return THIS_DIR != os.path.expanduser('~')
  return botobj.base_dir != os.path.expanduser('~')


def _is_jsonish_dict(d):
  """Returns True if `d` is a JSON-serializable dict."""
  if not isinstance(d, dict):
    return False
  try:
    _ = json.dumps(d)
  except Exception:
    return False
  return True


### Public functions used by __main__.py


def setup_bot(skip_reboot):
  """Calls bot_config.setup_bot() to have the bot self-configure itself.

  Reboots the host if bot_config.setup_bot() returns False, unless skip_reboot
  is also true.

  Does nothing if SWARMING_EXTERNAL_BOT_SETUP env var is set to 1. It is set in
  case bot's autostart configuration is managed elsewhere, and we don't want
  the bot itself to interfere.
  """
  if os.environ.get('SWARMING_EXTERNAL_BOT_SETUP') == '1':
    logging.info('Skipping setup_bot, SWARMING_EXTERNAL_BOT_SETUP is set')
    return

  botobj = get_bot(get_config())
  try:
    from config import bot_config
  except Exception as e:
    msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
    botobj.post_error('bot_config.py is bad: %s' % msg)
    return

  # TODO(maruel): Convert the should_continue return value to the hook calling
  # botobj.host_reboot() by itself.
  try:
    should_continue = bot_config.setup_bot(botobj)
  except Exception as e:
    msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
    botobj.post_error('bot_config.setup_bot() threw: %s' % msg)
    return

  if not should_continue and not skip_reboot:
    botobj.host_reboot('Starting new swarming bot: %s' % THIS_FILE)


@tools.cached
def generate_version():
  """Returns the bot's code version."""
  try:
    return zip_package.generate_version()
  except Exception as e:
    return 'Error: %s' % e


def get_attributes(botobj):
  """Returns the attributes sent to the server in /handshake.

  Each called function catches all exceptions so the bot doesn't die on startup,
  which is annoying to recover. In that case, we set a special property to catch
  these and help the admin fix the swarming_bot code more quickly.

  Arguments:
  - botobj: bot.Bot instance.
  """
  return {
    'dimensions': _get_dimensions(botobj),
    'state': _get_state(botobj, 0),
    'version': generate_version(),
  }


def get_bot(config):
  """Returns a valid Bot instance.

  Should only be called once in the process lifetime.

  It can be called by ../__main__.py, something to keep in mind.
  """
  # This variable is used to bootstrap the initial bot.Bot object, which then is
  # used to get the dimensions and state.
  attributes = {
    'dimensions': {'id': ['none']},
    'state': {},
    'version': generate_version(),
  }
  hostname = _get_botid_safe()
  base_dir = THIS_DIR
  # Use temporary Bot object to call get_attributes. Attributes are needed to
  # construct the "real" bot.Bot.
  attributes = get_attributes(
      bot.Bot(
          remote_client.RemoteClientNative(config['server'], None, hostname,
                                           base_dir), attributes,
          config['server'], base_dir, _on_shutdown_hook))

  # Make remote client callback use the returned bot object. We assume here
  # RemoteClient doesn't call its callback in the constructor (since 'botobj' is
  # undefined during the construction).
  botobj = bot.Bot(
      remote_client.RemoteClientNative(
          config['server'], lambda: _get_authentication_headers(botobj),
          hostname, base_dir), attributes, config['server'], base_dir,
      _on_shutdown_hook)
  return botobj


@tools.cached
def get_config():
  """Returns the data from config.json."""
  global _ERROR_HANDLER_WAS_REGISTERED
  try:
    with contextlib.closing(zipfile.ZipFile(THIS_FILE, 'r')) as f:
      config = json.load(f.open('config/config.json', 'r'))
    if config['server'].endswith('/'):
      raise ValueError('Invalid server entry %r' % config['server'])
  except (zipfile.BadZipfile, IOError, OSError, TypeError, ValueError):
    logging.exception('Invalid config.json!')
    config = {'server': ''}
  if not _ERROR_HANDLER_WAS_REGISTERED and config['server']:
    on_error.report_on_exception_exit(config['server'])
    _ERROR_HANDLER_WAS_REGISTERED = True
  return config


### Sub process management


def _cleanup_bot_directory(botobj):
  """Delete anything not expected in the swarming bot directory.

  This helps with stale work directory or any unexpected junk that could cause
  this bot to self-quarantine. Do only this when running from the zip.
  """
  if not _is_base_dir_ok(botobj):
    # That's an important one-off check as cleaning the $HOME directory has
    # really bad effects on normal host.
    logging.error('Not cleaning root directory because of bad base directory')
    return
  for i in fs.listdir(botobj.base_dir):
    if any(fnmatch.fnmatch(i, w) for w in PASSLIST):
      continue
    try:
      p = os.path.join(botobj.base_dir, i)
      if fs.isdir(p):
        file_path.rmtree(p)
      else:
        file_path.remove(p)
    except (IOError, OSError) as e:
      botobj.post_error(
          'Failed to remove %s from bot\'s directory: %s' % (i, e))


def _run_isolated_flags(botobj):
  """Returns flags to pass to run_isolated.

  These are not meant to be processed by task_runner.py.
  """
  settings = _get_settings(botobj)
  partition = settings['free_partition']['bot']
  size = os_utilities.get_disk_size(THIS_FILE)
  min_free = (
      _min_free_disk({'size_mb': size}, partition) +
      partition['wiggle'])
  logging.info('size %d, partition %s, min_free %s', size, partition, min_free)
  args = [
      # Shared option.
      '--min-free-space',
      str(min_free),
      '--max-cache-size',
      str(settings['caches']['isolated']['size']),
      # CAS cache option.
      '--cas-cache',
      os.path.join(botobj.base_dir, 'cas_cache'),
      # Named cache option.
      '--named-cache-root',
      os.path.join(botobj.base_dir, 'c'),
  ]

  use_kvs = True
  # bot with small memory or inside docker causes out of memory.
  if os_utilities.get_physical_ram() < 2048:
    use_kvs = False

  if sys.platform == 'linux' and platforms.linux.get_inside_docker():
    use_kvs = False

  if sys.platform == 'win32' and platform.architecture()[0] == '32bit':
    # 32 bit windows may not work with kvs.
    # https://crbug.com/1207762
    use_kvs = False

  if use_kvs:
    args += [
        '--kvs-dir',
        os.path.join(botobj.base_dir, _CAS_KVS_CACHE_DB),
    ]

  if _IN_TEST_MODE:
    args += ['--cipd-enabled', 'false']

  return args


def _Popen(botobj, cmd, **kwargs):
  """Wraps subprocess42.Popen.

  Creates a 'detached' process as per subprocess42 description.

  On Windows, also create a separate console.
  """
  kwargs.setdefault('stdout', subprocess42.PIPE)
  if sys.platform == 'win32':
    prev = kwargs.get('creationflags', 0)
    kwargs['creationflags'] = prev | subprocess42.CREATE_NEW_CONSOLE
  else:
    kwargs['close_fds'] = True
  return subprocess42.Popen(
      cmd,
      stdin=subprocess42.PIPE,
      stderr=subprocess42.STDOUT,
      cwd=botobj.base_dir,
      detached=True,
      **kwargs)


def _clean_cache(botobj):
  """Asks run_isolated to clean its cache.

  This may take a while but it ensures that in the case of a run_isolated run
  failed and it temporarily used more space than _min_free_disk, it can cleans
  up the mess properly.

  It will remove unexpected files, remove corrupted files, trim the cache size
  based on the policies and update state.json.
  """
  cmd = [
    sys.executable, THIS_FILE, 'run_isolated',
    '--clean',
    '--log-file', os.path.join(botobj.base_dir, 'logs', 'run_isolated.log'),
  ]
  cmd.extend(_run_isolated_flags(botobj))
  logging.info('Running: %s', cmd)
  try:
    # Intentionally do not use a timeout, it can take a while to hash 50gb but
    # better be safe than sorry.
    proc = _Popen(botobj, cmd)
    output, _ = proc.communicate(None)
    logging.info('Result:\n%s', output)
    if proc.returncode:
      botobj.post_error(
          'swarming_bot.zip failure during run_isolated --clean:\n%s' % output)
  except OSError:
    botobj.post_error(
        'swarming_bot.zip internal failure during run_isolated --clean')


def _post_error_task(botobj, error, task_id):
  """Posts given error as failure cause for the task.

  This is used in case of internal code error, and this causes the task to
  become BOT_DIED.

  Arguments:
    botobj: A bot.Bot instance.
    error: String representing the problem.
    task_id: Task that had an internal error. When the Swarming server sends
        commands to a bot, even though they could be completely wrong, the
        server assumes the job as running. Thus this function acts as the
        exception handler for incoming commands from the Swarming server. If for
        any reason the local test runner script can not be run successfully,
        this function is invoked.
  """
  logging.error('Error: %s', error)
  return botobj.remote.post_task_error(task_id, error)


def _run_manifest(botobj, manifest, rbe_session):
  """Defers to task_runner.py.

  Return True if the task succeeded.
  """
  # Ensure the manifest is valid. This can throw a json decoding error. Also
  # raise if it is empty.
  if not manifest:
    raise ValueError('Empty manifest')

  # Necessary to signal an internal_failure. This occurs when task_runner fails
  # to execute the command. It is important to note that this data is extracted
  # before any I/O is done, like writting the manifest to disk.
  task_id = manifest['task_id']
  last_ditch_timeout = manifest['hard_timeout'] or None
  # The grace period is the time between SIGTERM and SIGKILL.
  grace_period = max(manifest['grace_period'] or 0, 30)
  if last_ditch_timeout:
    # One for the child process, one for run_isolated, one for task_runner.
    last_ditch_timeout += 3 * grace_period
    # CIPD, isolated download time, plus named cache cleanup is not counted for
    # hard timeout so add more time; hard_timeout is handled by run_isolated.
    last_ditch_timeout += max(manifest['io_timeout'] or 0, 1200)

  task_dimensions = manifest['dimensions']
  task_result = {}

  failure = False
  internal_failure = False
  internal_error_reported = False
  msg = None
  auth_params_dumper = None
  must_reboot_reason = None
  # Use 'w' instead of 'work' because path length is precious on Windows.
  work_dir = os.path.join(botobj.base_dir, 'w')
  session_state_file = os.path.join(work_dir, 'session.json')
  try:
    try:
      if fs.isdir(work_dir):
        file_path.rmtree(work_dir)
    except OSError:
      # If a previous task created an undeleteable file/directory inside 'w',
      # make sure that following tasks are not affected. This is done by working
      # around the undeleteable directory by creating a temporary directory
      # instead. This is not normal behavior. The bot will report a failure on
      # start.
      work_dir = tempfile.mkdtemp(dir=botobj.base_dir, prefix='w')
    else:
      try:
        fs.makedirs(work_dir)
      except OSError:
        # Sometimes it's a race condition, so do a last ditch attempt.
        work_dir = tempfile.mkdtemp(dir=botobj.base_dir, prefix='w')

    env = os.environ.copy()
    env['CIPD_ARCHITECTURE'] = os_utilities.get_cipd_architecture()
    env['SWARMING_TASK_ID'] = task_id
    env['SWARMING_SERVER'] = botobj.server

    task_in_file = os.path.join(work_dir, 'task_runner_in.json')
    with fs.open(task_in_file, 'w') as f:
      f.write(json.dumps(manifest))
    handle, bot_file = tempfile.mkstemp(
        prefix='bot_file', suffix='.json', dir=work_dir)
    os.close(handle)
    task_result_file = os.path.join(work_dir, 'task_runner_out.json')
    if fs.exists(task_result_file):
      fs.remove(task_result_file)

    # Pass information about the Swarming bot session to the task runner. It is
    # needed to call task_update and other RPCs. If running in RBE mode, also
    # need to propagate the RBE session state: it will use it to ping the RBE
    # lease (if there's an active lease) or just the entire RBE session (if
    # there are no active leases). RBE session pinging is happening using
    # MAINTENANCE status.
    if rbe_session:
      if rbe_session.active_lease:
        logging.info('RBE lease: %r', rbe_session.active_lease.to_dict())
      else:
        logging.info('RBE lease: none')
    session_state = remote_client.SessionState(
        botobj.session_id, botobj.session_token,
        rbe_session.to_dict() if rbe_session else None)
    session_state.dump(session_state_file)

    # Start a thread that periodically puts authentication headers and other
    # authentication related information to a file on disk. task_runner reads it
    # from there before making authenticated HTTP calls.
    #
    # TODO(vadimsh): Switch to pipes or local sockets if the latency tokens
    # propagation here becomes an issue.
    auth_params_file = os.path.join(work_dir, 'bot_auth_params.json')
    auth_params_dumper = file_refresher.FileRefresherThread(
        auth_params_file,
        lambda: bot_auth.prepare_auth_params_json(botobj, manifest))
    auth_params_dumper.start()

    command = [
        sys.executable,
        THIS_FILE,
        'task_runner',
        '--swarming-server',
        botobj.server,
        '--in-file',
        task_in_file,
        '--out-file',
        task_result_file,
        '--cost-usd-hour',
        str(botobj.state.get('cost_usd_hour') or 0.),
        # Include the time taken to poll the task in the cost.
        # TODO(vadimsh): Remove this, it doesn't add much value, just
        # complicates code.
        '--start',
        str(time.time()),
        '--bot-file',
        bot_file,
        '--session-state-file',
        session_state_file,
        '--auth-params-file',
        auth_params_file,
    ]

    # Flags for run_isolated.py are passed through by task_runner.py as-is
    # without interpretation.
    command.append('--')
    command.extend(_run_isolated_flags(botobj))

    _call_hook_safe(True, botobj, 'on_before_task', bot_file, command, env)
    logging.debug('Running command: %s', command)

    base_log = os.path.join(botobj.base_dir, 'logs')
    if not fs.isdir(base_log):
      # It was observed that this directory may be unexpectedly deleted.
      # Recreate as needed, otherwise it may throw at the open() call below.
      fs.mkdir(base_log)
    log_path = os.path.join(base_log, 'task_runner_stdout.log')
    os_utilities.roll_log(log_path)
    os_utilities.trim_rolled_log(log_path)
    with fs.open(log_path, 'a+b') as f:
      proc = _Popen(botobj, command, stdout=f, env=env)
      logging.info('Subprocess for task_runner started.')
      try:
        proc.wait(last_ditch_timeout)
      except subprocess42.TimeoutExpired:
        # That's the last ditch effort; as task_runner should have completed a
        # while ago and had enforced the io_timeout or run_isolated for
        # hard_timeout.
        logging.error('Sending SIGTERM to task_runner')
        proc.terminate()
        internal_failure = True
        msg = 'task_runner hung'
        try:
          proc.wait(2*grace_period)
        except subprocess42.TimeoutExpired:
          logging.error('Sending SIGKILL to task_runner')
          proc.kill()
        proc.wait()
        return False

    logging.info('task_runner exit: %d', proc.returncode)
    if fs.exists(task_result_file):
      with fs.open(task_result_file, 'rb') as fd:
        task_result = json.load(fd)
        if task_result:
          internal_error_reported = task_result.get('internal_error_reported',
                                                    False)
    if proc.returncode:
      # STATUS_DLL_INIT_FAILED generally means that something bad happened, and
      # a reboot magically clears things out. :(
      if sys.platform == 'win32' and proc.returncode == -1073741502:
        must_reboot_reason = ('Working around STATUS_DLL_INIT_FAILED by '
                              'task_runner')
      msg = 'Execution failed: internal error (%d).' % proc.returncode
      internal_failure = True
    elif not task_result:
      logging.warning('task_runner failed to write metadata')
      msg = 'Execution failed: internal error (no metadata).'
      internal_failure = True
    elif task_result['internal_error']:
      msg = ('Execution failed: %s' % task_result['internal_error'])
      internal_failure = True

    # Load the up-to-date session state, it might have changed while the task
    # runner was driving it.
    session_state = remote_client.SessionState.load(session_state_file)
    with botobj.mutate_internals() as mut:
      mut.update_session_token(session_state.session_token)
    if rbe_session and session_state.rbe_session:
      rbe_session.restore(session_state.rbe_session)

    failure = bool(task_result.get('exit_code')) if task_result else False
    return not internal_failure and not failure
  except Exception as e:
    # Failures include IOError when writing if the disk is full, OSError if
    # swarming_bot.zip doesn't exist anymore, etc.
    logging.exception('_run_manifest failed')
    msg = 'Internal exception occurred: %s\n%s' % (
        e, traceback.format_exc()[-2048:])
    internal_failure = True
  finally:
    if auth_params_dumper:
      auth_params_dumper.stop()
    if internal_failure and not internal_error_reported:
      _post_error_task(botobj, msg, task_id)
    if rbe_session and rbe_session.active_lease:
      try:
        rbe_session.finish_active_lease({}, flush=True)
      except remote_client_errors.RBEServerError as e:
        botobj.post_error('finish_active_lease failed: %s' % e)
    logging.info(
        'calling on_after_task: failure=%s, internal_failure=%s, '
        'task_dimensions=%s, task_result=%s', failure, internal_failure,
        task_dimensions, task_result)
    _call_hook_safe(True, botobj, 'on_after_task', failure, internal_failure,
                    task_dimensions, task_result)
    if fs.isdir(work_dir):
      try:
        file_path.rmtree(work_dir)
      except Exception:
        botobj.post_error('Failed to delete work directory %s: %s' %
                          (work_dir, traceback.format_exc()[-2048:]))
        # Failure to delete could be due to a proc with open file handles. Just
        # reboot the machine in that case.
        must_reboot_reason = 'Failure to remove %s' % work_dir
    if must_reboot_reason:
      botobj.host_reboot(must_reboot_reason)


### Bot lifetime management


def _run_bot(arg_error):
  """Runs _run_bot_inner() with a signal handler."""
  # The quit_bit is to signal that the bot process must shutdown. It is
  # different from a request to restart the bot process or reboot the host.
  quit_bit = threading.Event()
  def handler(sig, _):
    # A signal terminates the bot process, it doesn't cause it to restart.
    logging.info('Got signal %s', sig)
    quit_bit.set()

  # TODO(maruel): Set quit_bit when stdin is closed on Windows.

  with subprocess42.set_signal_handler(subprocess42.STOP_SIGNALS, handler):
    return _run_bot_inner(arg_error, quit_bit)


def _run_bot_inner(arg_error, quit_bit):
  """Runs the bot until an event occurs.

  One of the three following even can occur:
  - host reboots
  - bot process restarts (this includes self-update)
  - bot process shuts down (this includes a signal is received)
  """
  config = get_config()

  try:
    # First thing is to get an arbitrary url. This also ensures the network is
    # up and running, which is necessary before trying to get the FQDN below.
    # There's no need to do error handling here - the "ping" is just to "wake
    # up" the network; if there's something seriously wrong, the handshake will
    # fail and we'll handle it there.
    hostname = _get_botid_safe()
    base_dir = os.path.dirname(THIS_FILE)
    remote = remote_client.RemoteClientNative(config['server'], None, hostname,
                                              base_dir)
    remote.ping()
  except Exception:
    # url_read() already traps pretty much every exceptions. This except
    # clause is kept there "just in case".
    logging.exception('server_ping threw')

  # If we are on GCE, we want to make sure GCE metadata server responds, since
  # we use the metadata to derive bot ID, dimensions and state.
  if platforms.is_gce():
    logging.info('Running on GCE, waiting for the metadata server')
    platforms.gce.wait_for_metadata(quit_bit)
    if quit_bit.is_set():
      logging.info('Early quit 1')
      return 0

  # Next we make sure the bot can make authenticated calls by grabbing the auth
  # headers, retrying on errors a bunch of times. We don't give up if it fails
  # though (maybe the bot will "fix itself" later).
  botobj = get_bot(config)
  try:
    botobj.remote.initialize(quit_bit)
  except remote_client.InitializationError as exc:
    botobj.post_error('failed to grab auth headers: %s' % exc.last_error)
    logging.error('Can\'t grab auth headers, continuing anyway...')

  if arg_error:
    botobj.post_error('Bootstrapping error: %s' % arg_error)

  # Pick up RBE-related worker properties from the environment. Do it as soon as
  # possible to report them to Swarming during the handshake as part of the
  # state. This may be useful in debugging.
  if platforms.is_gce():
    with botobj.mutate_internals() as mut:
      props = _get_rbe_worker_properties()
      if props:
        logging.info('RBE worker properties: %s', props.to_dict())
      else:
        logging.info('No RBE worker properties discovered')
      mut.update_rbe_worker_properties(props)

  if quit_bit.is_set():
    logging.info('Early quit 2')
    return 0

  _call_hook_safe(True, botobj, 'on_bot_startup')

  # Initial attributes passed to bot.Bot in get_bot above were constructed for
  # 'fake' bot ID ('none'). Refresh them to match the real bot ID, now that we
  # have fully initialize bot.Bot object. Note that 'get_dimensions' and
  # 'get_state' may depend on actions done by 'on_bot_startup' hook, that's why
  # we do it here and not in 'get_bot'.
  _update_bot_attributes(botobj, 0)
  if quit_bit.is_set():
    logging.info('Early quit 3')
    return 0

  rbe_params = _do_handshake(botobj, quit_bit)

  if quit_bit.is_set():
    logging.info('Early quit 4')
    return 0

  # Let the bot to finish the initialization, now that it knows its server
  # defined dimensions.
  _call_hook_safe(True, botobj, 'on_handshake')

  _cleanup_bot_directory(botobj)
  _clean_cache(botobj)

  if quit_bit.is_set():
    logging.info('Early quit 5')
    return 0

  # This environment variable is accessible to the tasks executed by this bot.
  os.environ['SWARMING_BOT_ID'] = botobj.id

  # RBE expects the version string to use a specific format.
  rbe_bot_version = '%s_%s_swarming/%s' % (
      os_utilities.get_cipd_os(),
      os_utilities.get_cipd_architecture(),
      botobj.bot_version,
  )

  # Spin until getting a termination signal. Shutdown RBE on exit.
  state = _BotLoopState(botobj, rbe_params, rbe_bot_version, quit_bit)
  with botobj.mutate_internals() as mut:
    mut.set_exit_hook(lambda _: state.on_bot_exit())
  state.run()

  # Do the final cleanup, if any.
  _bot_exit_hook(botobj)

  return 0


def _trap_all_exceptions(method):
  """A decorator for catching totally unexpected exceptions in _BotLoopState.

  The bot loop must never fully stop, even if the code is broken. If it does,
  the bot will not be able to self-update to a healthy state.
  """

  @functools.wraps(method)
  def wrapper(self, *args, **kwargs):
    try:
      return method(self, *args, **kwargs)
    except Exception as e:
      if not _TRAP_ALL_EXCEPTIONS:
        raise
      self._loop_iteration_had_errors = True
      self.report_exception('%s in %s: %s' %
                            (e.__class__.__name__, method.__name__, e))
    return None

  return wrapper


def _backoff(cycle, exponent, max_val=300.0):
  """Calculates randomized exponential retry delay."""
  # Do the first retry ASAP.
  cycle -= 1
  if cycle <= 0:
    return 0.0
  # Avoid overflows when calculating the power function.
  if cycle > 100:
    return max_val
  dur = float(exponent)**cycle
  if dur > max_val:
    return max_val
  return random.uniform(dur * 0.8, dur * 1.2)


class _BotLoopState:
  """The state of the main bot poll loop."""

  def __init__(self,
               botobj,
               rbe_params,
               rbe_bot_version,
               quit_bit,
               clock_impl=None):
    # Instance of Bot.
    self._bot = botobj
    # threading.Event signaled when the bot should gracefully exit.
    self._quit_bit = quit_bit
    # Tracks time and can sleep.
    self._clock = clock_impl or clock.Clock(quit_bit)

    # Snapshot of the dimensions dict used in the last poll cycle.
    self._prev_dimensions = {}
    # True if the current loop iteration had an unexpected error.
    self._loop_iteration_had_errors = False
    # Number of consecutive loop iterations with errors.
    self._loop_consecutive_errors = 0
    # Number of times the bot failed to exit when asked. Never resets.
    self._bad_bot_state_errors = 0
    # The last reported error message.
    self._error_last_msg = None
    # When it was reported the last time.
    self._error_last_time = None

    # Number of consecutive Swarming poll failures.
    self._swarming_consecutive_errors = 0
    # True if the last Swarming poll returned no tasks. None before the poll.
    self._swarming_idle = None
    # When we should poll Swarming next time. Initially set to ASAP.
    self._swarming_poll_timer = self._clock.timer(0.0)

    # The version string to report to RBE for monitoring and logs.
    self._rbe_bot_version = rbe_bot_version
    # The RBE instance Swarming told us to use.
    self._rbe_intended_instance = None
    # The intended status of the RBE session.
    self._rbe_intended_status = remote_client.RBESessionStatus.OK
    # True to poll from both Swarming and RBE in an interleaved way.
    self._rbe_hybrid_mode = False
    # The current healthy RBE session if we managed to open it.
    self._rbe_session = None
    # Number of consecutive RBE poll failures.
    self._rbe_consecutive_errors = 0
    # True if the last RBE poll returned no tasks. None before the poll.
    self._rbe_idle = None
    # When we should poll RBE next time (if at all).
    self._rbe_poll_timer = None
    # True if the RBE server asked the bot to terminate. Sticky.
    self._rbe_termination_pending = False

    # Number of consecutive poll cycles when bot did no tasks.
    self._consecutive_idle_cycles = 0
    # When the last task finished running (successfully or not).
    self._last_task_time = self._clock.now()

    # Randomize what scheduler should be used on the first poll in a hybrid
    # mode. We should not give preference to Swarming. There are bots that
    # reboot after each task. If we prefer Swarming, such bots may never pick up
    # tasks from RBE.
    self._next_scheduler = random.choice(['swarming', 'rbe'])

    # Turn on RBE mode if `/bot/handshake` indicated it should be used. This is
    # important in case the first poll should happen through RBE. Except we
    # still need to contact Swarming ASAP (perhaps to do a "maintenance" poll),
    # so make sure the swarming timer is firing now because rbe_enable(...) is
    # scheduling it to fire in the future.
    if rbe_params:
      self.rbe_enable(rbe_params)
      self._swarming_poll_timer.reset(0.0)

  def run(self, test_hook=None):
    """Spins executing Swarming commands until getting a termination signal.

    Arguments:
      test_hook: a callback that returns True to keep spinning or False to exit.
    """
    while not self._quit_bit.is_set():
      if self._rbe_hybrid_mode:
        logging.debug('Hybrid mode: %s', self._next_scheduler)

      # Note: on_before_poll should always be called before get_state and
      # get_dimensions (i.e. before _update_bot_attributes). It's part of the
      # bot hooks API contract.
      _call_hook_safe(False, self._bot, 'on_before_poll')
      _update_bot_attributes(self._bot, self._consecutive_idle_cycles)

      # If dimensions has changed, notify Swarming to update the bot listing.
      # This matters in the RBE mode when Swarming is not otherwise contacted
      # on every loop cycle.
      dims = self._bot.dimensions
      if dims != self._prev_dimensions:
        logging.info('Dimensions has changed, need to notify Swarming')
        self._prev_dimensions = dims
        self._swarming_poll_timer.reset(0.0)

      # True if calling Swarming at all this cycle.
      swarming_poll = self._swarming_poll_timer.firing

      # If we are in the hybrid mode, poll *tasks* from the Swarming scheduler
      # only if it is Swarming's turn. Do it by "forcing" a full poll. Otherwise
      # (if it is RBE's turn), we still need to call Swarming to report the bot
      # status and to get lifecycle commands. This is done by a non-forced poll.
      swarming_forced = (self._rbe_hybrid_mode
                         and self._next_scheduler == 'swarming')

      # Periodically call Python Swarming server to know when to update,
      # restart, pick up new config, etc.
      cmd, param = None, None
      if swarming_poll:
        cmd, param = self.swarming_poll(swarming_forced)
        # Call again no matter what. Note this timer may be rescheduled below,
        # e.g. in cmd_sleep(...) or rbe_enable(...).
        self._swarming_poll_timer.reset(
            _backoff(self._swarming_consecutive_errors, 1.4))

      # Recognize commands that affect RBE state before we call `on_after_poll`
      # hook. We need to poll from RBE before we call the hook, and thus need
      # to know if RBE is enabled.
      if cmd == 'rbe':
        # Swarming asked us to use RBE and returned the fresh poll token.
        self.rbe_enable(param)
      elif cmd == 'run':
        _manifest, rbe_params = param
        if rbe_params:
          # This can happen on a bot in a hybrid RBE mode when the server asks
          # us to execute a Swarming task, but also to maintain an RBE session.
          self.rbe_enable(rbe_params)
        else:
          # No RBE params => it is a native Swarming bot, turn off RBE.
          self.rbe_disable(remote_client.RBESessionStatus.BOT_TERMINATING)
      elif cmd == 'sleep':
        # This can only happen for native Swarming bot, turn off RBE.
        self.rbe_disable(remote_client.RBESessionStatus.BOT_TERMINATING)
      elif cmd == 'terminate':
        # We are about to terminate the process for good. Tell RBE about that.
        self.rbe_status(remote_client.RBESessionStatus.BOT_TERMINATING)
      elif cmd in ('update', 'bot_restart', 'host_reboot', 'restart'):
        # We are about to restart the bot or the host. Tell RBE about that.
        self.rbe_status(remote_client.RBESessionStatus.HOST_REBOOTING)

      # RBE polls from bots in hybrid mode are synchronized to Swarming timer.
      #
      # Possible cases here:
      #   * Pure Swarming bot: not in hybrid mode, no RBE timer => do nothing.
      #   * Pure RBE bot: not in hybrid mode, has RBE timer => use the timer.
      #   * Hybrid bot: in hybrid mode, has no RBE timer => use Swarming timer.
      if self._rbe_hybrid_mode:
        rbe_poll = swarming_poll
      else:
        rbe_poll = self._rbe_poll_timer and self._rbe_poll_timer.firing

      # Ask RBE for a new lease or just notify it about bot status. This also
      # closes the previous lease, if any. For that reason we do it even if the
      # bot is about to terminate or execute a task from Swarming scheduler
      # (when in the hybrid mode). The RBE will notice a non-OK status and won't
      # actually assign a new lease.
      rbe_lease = None
      if rbe_poll:
        rbe_lease = self.rbe_poll(
            # If we are in the hybrid mode, report maintenance status if it is
            # not RBE's turn to poll tasks from (i.e. we force-polled from
            # Swarming). That way we "ping" the RBE session and keep it alive,
            # but do not pick up any tasks.
            maintenance=swarming_forced,
            # Never block in the hybrid mode. If there are no RBE leases we need
            # to sleep a bit and call Swarming instead of blocking on the
            # server side waiting for an RBE lease.
            #
            # Also do not block if the previous poll returned some tasks. That
            # way if the queue becomes empty, we'll notice it ASAP (instead of
            # waiting for a new task to appear). If the queue was empty on the
            # previous cycle, we'll block for real here. This mechanism is
            # needed to reduce latency of reporting of the idle status to
            # Swarming server, since some Swarming clients rely on it for
            # scheduling.
            blocking=not self._rbe_hybrid_mode and self.idle,
        )
        # Poll ASAP if in pure RBE mode. In hybrid mode the Swarming timer is
        # used instead.
        if self._rbe_poll_timer:
          self._rbe_poll_timer.reset(_backoff(self._rbe_consecutive_errors,
                                              1.4))

      # In the hybrid mode after a polling cycle (even if idle or failed),
      # switch what scheduler should be used next time. We are alternating
      # between Swarming and RBE schedulers.
      if self._rbe_hybrid_mode:
        if self._next_scheduler == 'swarming':
          self._next_scheduler = 'rbe'
        else:
          self._next_scheduler = 'swarming'

      # The polling operation is done. Treat an RBE lease as a `run` command
      # from Swarming. Many existing `on_after_poll` hooks expect to see `run`
      # command here before the task execution.
      _call_hook_safe(False, self._bot, 'on_after_poll',
                      'run' if rbe_lease else cmd)

      # Execute the Swarming instruction (unless it is `rbe`, which was already
      # handled) or the RBE lease. Note that both can be None if both polls
      # failed. We'll just do nothing this cycle.
      if cmd and cmd != 'rbe':
        self.swarming_handle_cmd(cmd, param)
      elif rbe_lease:
        self.rbe_handle_lease(rbe_lease)
      elif self._rbe_termination_pending:
        self.rbe_handle_termination()

      # If the bot is already exiting, skip calling on_bot_idle hook.
      if self._quit_bit.is_set():
        break

      # If we ran a command, need to poll from the scheduler ASAP without
      # sleeping. In pure RBE mode this is already taken care of via
      # self._rbe_poll_timer.reset(...) above. Same for pure Swarming mode. In
      # the hybrid mode we need to reset the Swarming timer, since it controls
      # polling.
      if self._rbe_hybrid_mode and (cmd == 'run' or rbe_lease):
        self._swarming_poll_timer.reset(0.0)

      # Decide if this was an idle cycle.
      logging.info('Swarming idle: %s, RBE idle: %s', self._swarming_idle,
                   self._rbe_idle)
      report_idle = False
      if self._rbe_hybrid_mode:
        # In hybrid mode we need to do full polls from both Swarming and RBE
        # (i.e. do two loop cycles) before putting the bot into the idle state,
        # since we need to check that both queues are empty.
        report_idle = self._rbe_idle and self._swarming_idle
      elif self._rbe_intended_instance:
        # Pure RBE mode.
        report_idle = self._rbe_idle
      else:
        # Pure Swarming mode.
        report_idle = self._swarming_idle
      if report_idle:
        # Reset idleness flags, need to collect them again based on schedulers
        # replies before reporting the next cycle as idle. Important in the
        # hybrid mode which needs *both* flags collected.
        self._swarming_idle = None
        self._rbe_idle = None
        # This actually flips the bot into the idle state making self.idle True.
        self.on_idle_poll_cycle()

      # Update the bot idleness state reported to Swarming and details about
      # the RBE session (if any).
      currently_idle = self.idle
      with self._bot.mutate_internals() as mut:
        prev_idle = mut.update_idleness(currently_idle)
        mut.update_rbe_state(
            self._rbe_intended_instance, self._rbe_hybrid_mode,
            self._rbe_session.session_id if self._rbe_session else None)

      # If the pure RBE bot switched into the idle state just now, report this
      # to Swarming ASAP. Bot idleness status surfaces in the UI and some
      # clients rely on it for scheduling.
      if currently_idle and not prev_idle and self._rbe_poll_timer:
        logging.info('RBE: the bot became idle, need to report')
        self._swarming_poll_timer.reset(0.0)

      # Call `on_bot_idle` hook only if the bot didn't execute any task this
      # cycle. This is part of `on_bot_idle` hook API contract: if the bot
      # executes tasks back to back with no idle time in-between, the hook
      # should **not** be called.
      if currently_idle:
        _call_hook_safe(True, self._bot, 'on_bot_idle', self.idle_duration)

      # If there are "global" unexpected errors, throttle the loop. This is
      # a precaution against accidentally DDoS the server if there are bugs in
      # the bot code or configuration.
      self.maybe_throttle_on_errors()
      # Wait until there's something we need to do.
      self._clock.wait_next_timer()
      # In unit tests allow the hook to examine the loop state and abort it.
      if test_hook and not test_hook():
        break

  ##############################################################################
  ## Loop state helpers.

  @property
  def idle(self):
    """True if the bot didn't execute any tasks the last poll cycle."""
    return (self._consecutive_idle_cycles or self._loop_consecutive_errors
            or self._swarming_consecutive_errors or self._rbe_consecutive_errors
            or self._bad_bot_state_errors)

  @property
  def idle_duration(self):
    """Duration since the bot finished executing the last task."""
    return self._clock.now() - self._last_task_time

  def on_task_completed(self, _success):
    """Called after finishing running a task."""
    # Used to track bot idleness.
    self._consecutive_idle_cycles = 0
    self._last_task_time = self._clock.now()
    # Unconditionally clean up cache after each task. This is done *after* the
    # task is terminated, so that:
    # - there's no task overhead
    # - if there's an exception while cleaning, it's not logged in the task
    _clean_cache(self._bot)
    # Managed to run a full bot loop cycle. The bot code is good enough.
    _update_lkgbc(self._bot)

  def on_idle_poll_cycle(self):
    """Called if a **successful** poll cycle didn't produce any tasks."""
    if self._rbe_hybrid_mode:
      logging.debug('Hybrid mode: idle cycle')
    self._consecutive_idle_cycles += 1
    # Managed to run an idle bot loop cycle. The bot code is good enough.
    _update_lkgbc(self._bot)

  def on_bot_exit(self):
    """Called when the bot is about to terminate.

    In particular, this is called after the bot:
      * Received SIGTERM.
      * Asked to terminate by the RBE server.
      * Asked to restart by the Swarming server.
      * Asked to update by the Swarming server.
      * Asked to restart by some hook.
      * Asked to reboot the machine by the Swarming server.
      * Asked to reboot the machine by some hook.

    These calls happen through a convoluted chain of hooks and callbacks. See
    Bot.set_exit_hook, Bot.host_reboot (can be called from bot hooks, calls the
    exit hook itself), _bot_exit_hook (and its callers in this file).
    """
    if not self._bot.shutdown_event_posted:
      self._bot.post_event('bot_shutdown', 'Signal was received')
    if self._quit_bit.is_set():
      self.rbe_disable(remote_client.RBESessionStatus.BOT_TERMINATING)
    else:
      self.rbe_disable(remote_client.RBESessionStatus.HOST_REBOOTING)

  def report_exception(self, msg):
    """Called to report an unexpected exception to Swarming server."""
    logging.exception('%s', msg)
    body = '%s\n%s' % (msg, traceback.format_exc()[-2048:])
    # Skip reporting the same error over and over again. Report it at most once
    # every 10 min.
    now = self._clock.now()
    if body != self._error_last_msg or now > self._error_last_time + 600:
      self._bot.post_error(body)
      self._error_last_msg = body
      self._error_last_time = now

  def maybe_throttle_on_errors(self):
    """Called at the end of every iteration to throttle looping on errors."""
    if self._loop_iteration_had_errors:
      self._loop_consecutive_errors += 1
    else:
      self._loop_consecutive_errors = 0
    self._loop_iteration_had_errors = False

    worst_error_count = max([
        # If not zero, something super sad happened. The bot is quarantined
        # already, but if it doesn't help, reduce the poll frequency to avoid
        # busy-looping and DDoS of the server. This state is reset only upon bot
        # process restart.
        self._bad_bot_state_errors,
        # If not zero, there was some unexpected errors in the loop and they
        # were reported. Slow down if they keep happening, the bot is not
        # healthy. This is reset on a successful poll cycle.
        self._loop_consecutive_errors,
    ])

    # If there are errors, throttle the looping frequency.
    if worst_error_count:
      delay = _backoff(worst_error_count, 2.0)
      logging.warning('Loop throttling: sleeping %.1f sec due to errors', delay)
      self._clock.sleep(delay)

  ##############################################################################
  ## RBE.

  @_trap_all_exceptions
  def rbe_enable(self, rbe_state):
    """Called when Swarming instructs the bot to poll tasks from the RBE.

    For a bot which is already in the RBE mode, this will be called periodically
    after every Python Swarming poll (e.g. every minute or so).

    For a bot in the hybrid mode, this is additionally called before executing
    a task obtained through Swarming scheduler.
    """
    # Update the intended RBE state. This state will be eventually realized in
    # rbe_poll(...), perhaps after some retries.
    self._rbe_intended_instance = rbe_state['instance']
    self._rbe_intended_status = remote_client.RBESessionStatus.OK
    self._rbe_hybrid_mode = rbe_state['hybrid_mode']

    # In the hybrid mode RBE polling is synchronized to the Swarming timer.
    # Cancel RBE timer, if any.
    if self._rbe_hybrid_mode and self._rbe_poll_timer:
      self._rbe_poll_timer.cancel()
      self._rbe_poll_timer = None

    # In pure RBE mode start the RBE polling loop if it was stopped before.
    if not self._rbe_hybrid_mode and not self._rbe_poll_timer:
      self._rbe_poll_timer = self._clock.timer(0.0)

    # If we are changing the RBE instance, terminate the old session and
    # schedule a poll from the new session ASAP.
    if (self._rbe_session
        and self._rbe_session.instance != self._rbe_intended_instance):
      logging.info('RBE: terminating session %s (switching instances)',
                   self._rbe_session.session_id)
      self._rbe_session.terminate()
      self._rbe_session = None
      if self._rbe_poll_timer:
        self._rbe_poll_timer.reset(0.0)

    if self._rbe_hybrid_mode:
      # In the hybrid mode poll Swarming again when the Swarming server tells
      # us, just like in the pure Swarming mode. This is similar to what
      # cmd_sleep(...) does, except we reduce sleeping time 2x because we sleep
      # *twice* every full polling cycle (once after polling Swarming, once
      # after polling RBE). Note this timer may be rescheduled later if we end
      # up running a task (no need to sleep before the next poll in this case).
      self._swarming_poll_timer.reset(rbe_state['sleep'] / 2.0)
    else:
      # In pure RBE mode contact Python Swarming again ~2m from now. We'll
      # maintain this relatively low and constant polling frequency, since in
      # the RBE mode Python Swarming is used only for maintenance commands. All
      # "low-latency" tasks are scheduled via RBE.
      self._swarming_poll_timer.reset(random.uniform(100.0, 140.0))

  @_trap_all_exceptions
  def rbe_disable(self, status):
    """Terminates and forgets the RBE session and RBE state."""
    assert status in (
        remote_client.RBESessionStatus.HOST_REBOOTING,
        remote_client.RBESessionStatus.BOT_TERMINATING,
    ), status
    self._rbe_intended_instance = None
    self._rbe_intended_status = status
    self._rbe_hybrid_mode = False
    self._rbe_idle = None
    self._rbe_consecutive_errors = 0
    if self._rbe_session:
      logging.info('RBE: terminating session %s with status %s',
                   self._rbe_session.session_id, self._rbe_intended_status)
      self._rbe_session.terminate(self._rbe_intended_status)
      self._rbe_session = None
    if self._rbe_poll_timer:
      self._rbe_poll_timer.cancel()
      self._rbe_poll_timer = None

  @_trap_all_exceptions
  def rbe_status(self, status):
    """Updates the intended RBE BotSession status (reported on next poll)."""
    # Note that MAINTENANCE status is used by rbe_poll(...) internally while
    # doing a "maintenance" pool. It can't be set via rbe_status(...).
    assert status in (
        remote_client.RBESessionStatus.OK,
        remote_client.RBESessionStatus.HOST_REBOOTING,
        remote_client.RBESessionStatus.BOT_TERMINATING,
    ), status
    if self._rbe_intended_status != status:
      self._rbe_intended_status = status
      # If we are actually polling RBE now, try to report the new status ASAP.
      if self._rbe_poll_timer:
        self._rbe_poll_timer.reset(0.0)

  @_trap_all_exceptions
  def rbe_poll(self, maintenance, blocking):
    """Polls RBE for a lease. Establishes the session if necessary.

    Arguments:
      maintenance: if True, just ping RBE, but do not pick up any tasks.
      blocking: if True, allow waiting for a bit for new leases to appear.
    """
    assert self._rbe_intended_instance

    if self._rbe_hybrid_mode:
      logging.debug('Hybrid mode: poll RBE, maintenance:%s', maintenance)

    # An RBE session can die "between polls" while the bot was running a task
    # via task_runner subprocess. Abandon such session to recreate it below.
    # Note that we also check session state right after polling as well (this is
    # where session status changes) to avoid holding on to sessions known to be
    # dead.
    self.rbe_maybe_abandon_closed_session()
    # RBE can ask the session to terminate gracefully while the task is running.
    self.rbe_recognize_pending_termination()

    if not self._rbe_session:
      # Don't know if there are pending tasks, need a session for it.
      self._rbe_idle = None
      # If we don't actually need a healthy session anymore, do nothing. This
      # can happen if the bot is already shutting down. To avoid potential weird
      # issues if the shutdown fails due to errors, reschedule the poll some
      # time later by treating this state as a transient error.
      if (self._rbe_intended_status != remote_client.RBESessionStatus.OK
          or self._rbe_termination_pending):
        logging.warning('The bot is terminating, refusing to open RBE session')
        self._rbe_consecutive_errors += 1
        return None
      # We need a healthy session. Try to create it.
      try:
        logging.info('RBE: opening session at %s', self._rbe_intended_instance)
        self._rbe_session = remote_client.RBESession(
            self._bot.remote, self._rbe_intended_instance,
            self._rbe_bot_version, self._bot.rbe_worker_properties)
        logging.info('RBE: session is %s', self._rbe_session.session_id)
        self._rbe_consecutive_errors = 0
      except remote_client_errors.RBEServerError as e:
        self.report_exception('Failed to open RBE Session: %s' % e)
        self._rbe_consecutive_errors += 1
        return None

    # This can happen if the loop crashed on the previous iteration before it
    # could finish the lease. We don't really know what its actual status is.
    # Need to close it, otherwise update(...) below will perpetually complain
    # that there's an open lease. This should not really be happening.
    if self._rbe_session.active_lease:
      self._rbe_session.finish_active_lease({
          'bot_internal_error':
          'Bot crashed before finishing the lease',
      })
      self._rbe_consecutive_errors += 1
      self._bot.post_error('Orphaned RBE lease, bot loop crashing?')

    # There's a healthy session, we can update it.
    try:
      # Note this may be one of the termination statuses as well. The session
      # will react by changing its `alive` and `terminating` properties.
      report_status = self._rbe_intended_status
      if self._rbe_termination_pending:
        report_status = remote_client.RBESessionStatus.BOT_TERMINATING
      elif report_status == remote_client.RBESessionStatus.OK and maintenance:
        report_status = remote_client.RBESessionStatus.MAINTENANCE
      logging.info('RBE: updating %s as %s (blocking=%s)',
                   self._rbe_session.session_id, report_status, blocking)
      lease = self._rbe_session.update(report_status, blocking)

      # This session could have been closed (either by us or by the server).
      # We should abandon this session and create a new one on the next loop
      # iteration (if the loop is still running at all). If the bot is stopping,
      # this is the last iteration and we won't open a new session.
      self.rbe_maybe_abandon_closed_session()
      if not self._rbe_session:
        return None

      # The server can ask us to terminate gracefully (but see a caveat in
      # rbe_maybe_abandon_closed_session).
      self.rbe_recognize_pending_termination()

      # The session is healthy! Return whatever was polled, if anything.
      if report_status == remote_client.RBESessionStatus.OK:
        self._rbe_idle = not lease
      self._rbe_consecutive_errors = 0
      return lease

    except remote_client_errors.RBEServerError as e:
      if self._rbe_consecutive_errors > 3:
        self.report_exception('Failed to update RBE Session: %s' % e)
      self._rbe_consecutive_errors += 1
      self._rbe_idle = None
      return None

  def rbe_maybe_abandon_closed_session(self):
    """Abandons the RBE session if it looks dead or terminated."""
    if not self._rbe_session:
      return  # nothing to abandon

    if self._rbe_session.alive:
      if not self._rbe_session.terminating:
        return  # the session is very much alive

      # The session exists, but is terminating. This can happen in two cases:
      #   1. This is an RBE Worker VM and the RBE wants it gone.
      #   2. This is a bare metal bot and its session has expired.
      #
      # For RBE Worker VMs we should obey and terminate the bot: the RBE will
      # recreate the VM if it is still needed. Termination will be handled by
      # rbe_poll(...) when it notices `terminating == true`. So keep the
      # session.
      if self._bot.rbe_worker_properties:
        return

      # But for the bare metal bots, we can't just shut down. There's nothing
      # that can revive them. Instead we'll treat such session as dead and get
      # a new one. Proceed to abandoning the session.
      logging.warning('RBE: session %s is unexpectedly terminated by RBE',
                      self._rbe_session.session_id)

    # Abandoning a session is a rare event. As a precaution against busy-looping
    # if something goes wrong treat a session closure as a transient error to
    # slow down spinning.
    self._rbe_consecutive_errors += 1
    self._rbe_session.abandon()
    self._rbe_session = None
    self._rbe_idle = None

  def rbe_recognize_pending_termination(self):
    """Sets "termination is pending" flag and logs this."""
    if (self._rbe_session and self._rbe_session.terminating
        and not self._rbe_termination_pending):
      logging.warning('RBE: the bot is terminated by the RBE server')
      self._rbe_termination_pending = True

  @_trap_all_exceptions
  def rbe_handle_lease(self, rbe_lease):
    """Executes the RBE lease."""
    logging.info('RBE: got lease %s: %s', rbe_lease.id, rbe_lease.payload)

    # Close "noop" leases ASAP without hitting any other Swarming machinery.
    # They can be used to measure end-to-end RBE polling latency.
    if rbe_lease.payload.get('noop'):
      self._rbe_session.finish_active_lease({})
      self.on_task_completed(True)
      return

    # At least `task_id` must be set. Integer-valued fields may be missing if
    # they are equal to 0 (per protobuf jsonpb encoding rules).
    task_id = rbe_lease.payload['task_id']
    task_to_run_shard = rbe_lease.payload.get('task_to_run_shard', 0)
    task_to_run_id = rbe_lease.payload.get('task_to_run_id', 0)

    # Tell Swarming we are about to start working on this lease.
    logging.info('RBE: claiming %s (shard %d, id %d)', task_id,
                 task_to_run_shard, task_to_run_id)
    cmd, param = self._bot.remote.claim(self._bot.attributes, rbe_lease.id,
                                        task_id, task_to_run_shard,
                                        task_to_run_id)

    if cmd == 'skip':
      logging.warning('RBE: skipping %s: %s', task_id, param)
      self._rbe_session.finish_active_lease({'skip_reason': param or 'unknown'})
    elif cmd == 'terminate':
      self.cmd_terminate(param)
      self._rbe_session.finish_active_lease({})
    elif cmd == 'run':
      self.cmd_run(param, self._rbe_session)
    else:
      raise ValueError('Unexpected claim outcome: %s' % cmd)

  @_trap_all_exceptions
  def rbe_handle_termination(self):
    """Called when RBE asks the bot to gracefully terminate."""
    with self._bot.mutate_internals() as mut:
      # A terminating bot is not running any tasks => it is idle.
      mut.update_idleness(True)
      # If it is an RBE worker VM, ask Swarming to cleanup after it.
      if self._bot.rbe_worker_properties:
        mut.update_auto_cleanup(True)
    # Set the quit bit only after the server acknowledged the termination.
    if not self._bot.shutdown_event_posted:
      self._bot.post_event('bot_shutdown', 'Terminated by RBE')
    self._quit_bit.set()

  ##############################################################################
  ## Python Swarming.

  def swarming_poll(self, force):
    """Polls Python Swarming for an instruction.

    Arguments:
      force: if True and the bot has an RBE instance assigned, do a full poll.

    Returns:
      (cmd string, params dict) on success.
      (None, None) on error.
    """
    # Note that using @_trap_all_exceptions decorator here may be dangerous,
    # since it reports the trapped exceptions to Python Swarming server. But
    # the most common reason for remote.poll(...) to fail is unavailability of
    # the Python Swarming server! Using @_trap_all_exceptions may result in
    # undesirable amplification of requests to the server in case it is down.
    # We'll just log errors locally instead.
    try:
      if self._rbe_hybrid_mode:
        logging.debug('Hybrid mode: poll Swarming, force:%s', force)
      cmd, param = self._bot.remote.poll(self._bot.attributes, force)
      logging.debug('Swarming poll response:\n%s: %s', cmd, param)
      if cmd == 'sleep':
        # Pure Swarming bot, no tasks.
        self._swarming_idle = True
      elif cmd != 'rbe':
        # Any kind of bot, have a pending task or some command.
        self._swarming_idle = False
      elif not self._rbe_hybrid_mode or force:
        # Ignore `rbe` when it is returned as part of non-forced poll in hybrid
        # mode. In that case there still can be Swarming tasks pending.
        # Otherwise `rbe` indicates the Swarming queue is empty.
        self._swarming_idle = True
      self._swarming_consecutive_errors = 0
      return cmd, param
    except remote_client_errors.PollError as e:
      logging.error('Swarming poll error: %s' % e)
      self._swarming_consecutive_errors += 1
      self._swarming_idle = None
      return None, None

  def swarming_handle_cmd(self, cmd, param):
    """Executes a command (except `rbe`) returned by swarming_poll(...).

    The `rbe` command is more complicated and is handled in the main loop body.
    """
    if cmd == 'sleep':
      self.cmd_sleep(param)
    elif cmd == 'terminate':
      self.cmd_terminate(param)
    elif cmd == 'run':
      manifest, _rbe_params = param
      self.cmd_run(manifest, self._rbe_session)
    elif cmd == 'update':
      self.cmd_update(param)
    elif cmd in ('host_reboot', 'restart'):
      self.cmd_host_reboot(param)
    elif cmd == 'bot_restart':
      self.cmd_bot_restart(param)
    else:
      self.cmd_unknown(cmd)

  @_trap_all_exceptions
  def cmd_unknown(self, cmd):
    """Called if the Swarming command is unrecognized."""
    # Just report it via _trap_all_exceptions mechanism. It needs an exception
    # context for a stack trace.
    raise ValueError('Unexpected Swarming command %s' % cmd)

  @_trap_all_exceptions
  def cmd_sleep(self, duration):
    """Called when Swarming asks the bot to sleep."""
    self._swarming_poll_timer.reset(duration)

  @_trap_all_exceptions
  def cmd_terminate(self, task_id):
    """Called when Swarming asks the bot to gracefully terminate."""
    self._quit_bit.set()
    try:
      # Duration must be set or server IEs. For that matter, we've never cared
      # if there's an error here before, so let's preserve that behavior
      # (though anything that's not a remote_client.InternalError will make
      # it through, again preserving prior behavior).
      self._bot.remote.post_task_update(task_id, {'duration': 0}, None, 0)
    except remote_client_errors.InternalError:
      pass

  @_trap_all_exceptions
  def cmd_run(self, manifest, rbe_session):
    """Called when Swarming asks the bot to execute a task."""
    # Actually execute the task. This can block for many hours.
    success = _run_manifest(self._bot, manifest, rbe_session)
    self.on_task_completed(success)

  @_trap_all_exceptions
  def cmd_update(self, version):
    """Called when Swarming asks the bot to self-update."""
    _update_bot(self._bot, version)
    self._should_have_exited_but_didnt('Failed to self-update the bot')

  @_trap_all_exceptions
  def cmd_host_reboot(self, message):
    """Called when Swarming asks the bot to reboot the host machine."""
    self._bot.host_reboot(message)
    self._should_have_exited_but_didnt('Failed to reboot the host')

  @_trap_all_exceptions
  def cmd_bot_restart(self, message):
    """Called when Swarming asks the bot to restart its own process."""
    _bot_restart(self._bot, message)
    self._should_have_exited_but_didnt('Failed to restart the bot process')

  def _should_have_exited_but_didnt(self, message):
    _set_quarantined(message)
    self._bad_bot_state_errors += 1


def _do_handshake(botobj, quit_bit):
  """Connects to /handshake and reads the bot_config if specified.

  Returns:
    A dict with RBE parameters fetched from the server config or None if the bot
    is not using RBE.
  """
  # This is the first authenticated request to the server. If the bot is
  # misconfigured, the request may fail with HTTP 401 or HTTP 403. Instead of
  # dying right away, spin in a loop, hoping the bot will "fix itself"
  # eventually. Authentication errors in /handshake are logged on the server and
  # generate error reports, so bots stuck in this state are discoverable.
  loop_clock = clock.Clock(quit_bit)
  attempt = 0
  while not quit_bit.is_set():
    logging.info('Swarming bot session ID: %s', botobj.session_id)
    resp = botobj.remote.do_handshake(botobj.attributes, botobj.session_id)
    if resp:
      logging.info('Connected to %s', resp.get('server_version'))
      if resp.get('bot_version') != botobj.bot_version:
        logging.warning(
            'Found out we\'ll need to update: server said %s; we\'re %s',
            resp.get('bot_version'), botobj.bot_version)
      # Register the Python module with bot hooks.
      content = resp.get('bot_config')
      rev = resp.get('bot_config_rev')
      script = resp.get('bot_config_name')
      if content:
        _register_extra_bot_config(content, rev, script)
      # Remember the server-provided per-bot configuration. '/handshake' is
      # the only place where the server returns it. The bot will be sending
      # the 'bot_group_cfg_version' back in each /poll (as part of 'state'),
      # so that the server can instruct the bot to restart itself when
      # config changes.
      with botobj.mutate_internals() as mut:
        mut.update_bot_config(script, rev)
        rbe_params = resp.get('rbe')
        if rbe_params:
          mut.update_rbe_state(rbe_params['instance'],
                               rbe_params['hybrid_mode'], None)
        cfg_version = resp.get('bot_group_cfg_version')
        if cfg_version:
          mut.update_bot_group_cfg(cfg_version, resp.get('bot_group_cfg'))
      return rbe_params
    attempt += 1
    sleep_time = _backoff(attempt, 2.0)
    logging.error(
        'Failed to contact for handshake, retrying in %d sec...', sleep_time)
    loop_clock.sleep(sleep_time)
  return None


def _update_bot(botobj, version):
  """Downloads the new version of the bot code and then runs it.

  Use alternating files; first load swarming_bot.1.zip, then swarming_bot.2.zip,
  never touching swarming_bot.zip which was the originally bootstrapped file.

  LKGBC is handled by _update_lkgbc().

  Returns only in case of failure to get the new bot code.
  """
  # Alternate between .1.zip and .2.zip.
  new_zip = 'swarming_bot.1.zip'
  if os.path.basename(THIS_FILE) == new_zip:
    new_zip = 'swarming_bot.2.zip'
  new_zip = os.path.join(botobj.base_dir, new_zip)

  # Download as a new file.
  try:
    botobj.remote.get_bot_code(new_zip, version)
  except remote_client.BotCodeError as e:
    botobj.post_error(str(e))
  else:
    _bot_restart(botobj, 'Updating to %s' % version, filepath=new_zip)


def _bot_restart(botobj, message, filepath=None):
  """Restarts the bot process, optionally in a new file.

  The function will return if the new bot code is not valid.
  """
  filepath = filepath or THIS_FILE
  s = fs.stat(filepath)
  logging.info('Restarting to %s; %d bytes.', filepath, s.st_size)
  sys.stdout.flush()
  sys.stderr.flush()

  proc = _Popen(botobj, [sys.executable, filepath, 'is_fine'])
  output, _ = proc.communicate()
  if proc.returncode:
    botobj.post_error(
        'New bot code is bad: proc exit = %s. stdout:\n%s' %
        (proc.returncode, output))
    if sys.platform == 'win32' and proc.returncode == -1073741502:
      # STATUS_DLL_INIT_FAILED generally means that something bad happened, and
      # a reboot magically clears things out. :(
      botobj.host_reboot(
          'Working around STATUS_DLL_INIT_FAILED when restarting the bot')
    return

  botobj.post_event('bot_shutdown', 'About to restart: %s' % message)

  # Do the final cleanup, if any.
  _bot_exit_hook(botobj)

  # Sleep a bit to make sure new bot process connects to a GAE instance with
  # the fresh bot group config cache (it gets refreshed each second). This makes
  # sure the bot doesn't accidentally pick up the old config after restarting
  # and connecting to an instance with a stale cache.
  time.sleep(2)

  # Don't forget to release the singleton before restarting itself.
  SINGLETON.release()

  # Do not call on_bot_shutdown.
  # On OSX, launchd will be unhappy if we quit so the old code bot process has
  # to outlive the new code child process. Launchd really wants the main process
  # to survive, and it'll restart it if it disappears. os.exec*() replaces the
  # process so this is fine.
  cmd = [filepath, 'start_slave', '--survive']
  if _IN_TEST_MODE:
    cmd.append('--test-mode')
  logging.debug('Restarting bot, cmd: %s', cmd)
  ret = common.exec_python(cmd)
  if ret in (1073807364, -1073741510):
    # 1073807364 is returned when the process is killed due to shutdown. No need
    # to alert anyone in that case.
    # -1073741510 is returned when rebooting too. This can happen when the
    # parent code was running the old version and gets confused and decided to
    # poll again.
    # In any case, zap out the error code.
    ret = 0
  elif ret:
    botobj.post_error('Bot failed to respawn after update: %s' % ret)
  sys.exit(ret)


def _bot_exit_hook(botobj):
  """Calls the registered bot exit hook."""
  with botobj.mutate_internals() as mut:
    exit_hook = mut.get_exit_hook()
  if exit_hook:
    try:
      exit_hook(botobj)
    except Exception as e:
      logging.exception('exit hook failed: %s', e)


def _update_lkgbc(botobj):
  """Updates the Last Known Good Bot Code if necessary.

  Returns True if LKGBC was updated.
  """
  try:
    if not fs.isfile(THIS_FILE):
      # TODO(maruel): Try to download the code again from the server.
      botobj.post_error('Missing file %s for LKGBC' % THIS_FILE)
      return False

    golden = os.path.join(botobj.base_dir, 'swarming_bot.zip')
    if fs.isfile(golden):
      org = fs.stat(golden)
      cur = fs.stat(THIS_FILE)
      if org.st_mtime >= cur.st_mtime:
        return False

    # Copy the current file back to LKGBC.
    logging.info('Updating LKGBC')
    shutil.copy(THIS_FILE, golden)
    return True
  except Exception as e:
    botobj.post_error('Failed to update LKGBC: %s' % e)
    return False


def main(argv):
  subprocess42.inhibit_os_error_reporting()

  # Disable magical auto-detection of OAuth config. bot_main.py prepares auth
  # headers on its own explicitly (via get_authentication_headers hook) when
  # using 'net' library through RemoteClientNative class and auto-configured
  # auth in net.py may interfere with this. We also disable auto-detection in
  # task_runner.py (since it also uses special mechanism for getting auth
  # headers from bot_main.py). We do _not_ disable auto-detection in
  # run_isolated.py, since at this layer we have an auth context (setup by
  # task_runner.py) and it is correctly getting recognized by the auto-detection
  # in net.py.
  net.disable_oauth_config()

  # Add SWARMING_HEADLESS into environ so subcommands know that they are running
  # in a headless (non-interactive) mode.
  os.environ['SWARMING_HEADLESS'] = '1'

  # Kick out any lingering env vars that will be set later.
  os.environ.pop('SWARMING_SERVER', None)
  os.environ.pop('SWARMING_TASK_ID', None)

  # The only reason this is kept is to enable the unit test to use --help to
  # quit the process.
  parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
  parser.add_argument('unsupported', nargs='*', help=argparse.SUPPRESS)
  parser.add_argument('--test-mode', action='store_true')
  args = parser.parse_args(argv)

  global _IN_TEST_MODE
  _IN_TEST_MODE = args.test_mode
  if _IN_TEST_MODE:
    logging.debug('bot_main running in TEST mode')

  if sys.platform == 'win32':
    if not file_path.enable_privilege('SeShutdownPrivilege'):
      logging.error('Failed to enable SeShutdownPrivilege')

  # Enforces that only one process with a bot in this directory can be run on
  # this host at once.
  if not SINGLETON.acquire():
    if sys.platform == 'darwin':
      msg = (
          'Found a previous bot, %d rebooting as a workaround for '
          'https://crbug.com/569610.') % os.getpid()
    else:
      msg = ('Found a previous bot, %d rebooting as a workaround for '
             'https://crbug.com/1061531' % os.getpid())
    print(msg, file=sys.stderr)
    _PRE_REBOOT_SLEEP_SECS = 5 * 60
    print('Sleeping for %s secs' % _PRE_REBOOT_SLEEP_SECS, file=sys.stderr)
    time.sleep(_PRE_REBOOT_SLEEP_SECS)
    os_utilities.host_reboot(msg)
    return 1

  base_dir = os.path.dirname(THIS_FILE)
  for t in ('out', 'err'):
    log_path = os.path.join(base_dir, 'logs', 'bot_std%s.log' % t)
    os_utilities.roll_log(log_path)
    os_utilities.trim_rolled_log(log_path)

  error = None
  if len(args.unsupported) != 0:
    error = 'Unexpected arguments: %s' % args
  try:
    return _run_bot(error)
  finally:
    _call_hook_safe(True, bot.Bot(None, None, None, base_dir, None),
                    'on_bot_shutdown')
    logging.info('main() returning')
