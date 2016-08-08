# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Swarming bot main process.

This is the program that communicates with the Swarming server, ensures the code
is always up to date and executes a child process to run tasks and upload
results back.

It manages self-update and rebooting the host in case of problems.

Set the environment variable SWARMING_LOAD_TEST=1 to disable the use of
server-provided bot_config.py. This permits safe load testing.
"""

import contextlib
import fnmatch
import json
import logging
import optparse
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
import urllib
import zipfile

import bot_auth
import common
import file_refresher
import remote_client
import singleton
from api import bot
from api import os_utilities
from api import platforms
from utils import file_path
from utils import net
from utils import on_error
from utils import subprocess42
from utils import zip_package


# Used to opportunistically set the error handler to notify the server when the
# process exits due to an exception.
_ERROR_HANDLER_WAS_REGISTERED = False


# Set to the zip's name containing this file. This is set to the absolute path
# to swarming_bot.zip when run as part of swarming_bot.zip. This value is
# overriden in unit tests.
THIS_FILE = os.path.abspath(zip_package.get_main_script_path())


# The singleton, initially unset.
SINGLETON = singleton.Singleton(os.path.dirname(THIS_FILE))


# Whitelist of files that can be present in the bot's directory. Anything else
# will be forcibly deleted on startup! Note that 'w' (work) is not in this list,
# as we want it to be deleted on startup.
# See
# https://github.com/luci/luci-py/tree/master/appengine/swarming/doc/LifeOfABot.md
# for more details.
PASSLIST = (
  '*-cacert.pem',
  'cipd_cache',
  'isolated_cache',
  'logs',
  'README',
  'swarming.lck',
  'swarming_bot.1.zip',
  'swarming_bot.2.zip',
  'swarming_bot.zip',
)

### bot_config handler part.


def _in_load_test_mode():
  """Returns True if the default values should be used instead of the server
  provided bot_config.py.

  This also disables server telling the bot to restart.
  """
  return os.environ.get('SWARMING_LOAD_TEST') == '1'


def get_dimensions(botobj):
  """Returns bot_config.py's get_attributes() dict."""
  # Importing this administrator provided script could have side-effects on
  # startup. That is why it is imported late.
  try:
    if _in_load_test_mode():
      # Returns a minimal set of dimensions so it doesn't run tasks by error.
      dimensions = os_utilities.get_dimensions()
      return {
        'id': dimensions['id'],
        'load_test': ['1'],
      }

    from config import bot_config
    out = bot_config.get_dimensions(botobj)
    if not isinstance(out, dict):
      raise ValueError('Unexpected type %s' % out.__class__)
    return out
  except Exception as e:
    logging.exception('get_dimensions() failed')
    try:
      out = os_utilities.get_dimensions()
      out['error'] = [str(e)]
      out['quarantined'] = ['1']
      return out
    except Exception as e:
      try:
        botid = os_utilities.get_hostname_short()
      except Exception as e2:
        botid = 'error_%s' % str(e2)
      return {
          'id': [botid],
          'error': ['%s\n%s' % (e, traceback.format_exc()[-2048:])],
          'quarantined': ['1'],
        }


def get_state(botobj, sleep_streak):
  """Returns dict with a state of the bot reported to the server with each poll.
  """
  try:
    if _in_load_test_mode():
      state = os_utilities.get_state()
      state['dimensions'] = os_utilities.get_dimensions()
    else:
      from config import bot_config
      state = bot_config.get_state(botobj)
      if not isinstance(state, dict):
        state = {'error': state}
  except Exception as e:
    logging.exception('get_state() failed')
    state = {
      'error': '%s\n%s' % (e, traceback.format_exc()[-2048:]),
      'quarantined': True,
    }

  state['sleep_streak'] = sleep_streak
  return state


def call_hook(botobj, name, *args):
  """Calls a hook function in bot_config.py."""
  try:
    if _in_load_test_mode():
      return

    logging.info('call_hook(%s)', name)
    from config import bot_config
    hook = getattr(bot_config, name, None)
    if hook:
      return hook(botobj, *args)
  except Exception as e:
    msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
    botobj.post_error('Failed to call hook %s(): %s' % (name, msg))


def setup_bot(skip_reboot):
  """Calls bot_config.setup_bot() to have the bot self-configure itself.

  Reboots the host if bot_config.setup_bot() returns False, unless skip_reboot
  is also true.

  Does nothing if SWARMING_EXTERNAL_BOT_SETUP env var is set to 1. It is set in
  case bot's autostart configuration is managed elsewhere, and we don't want
  the bot itself to interfere.
  """
  if _in_load_test_mode():
    return

  if os.environ.get('SWARMING_EXTERNAL_BOT_SETUP') == '1':
    logging.info('Skipping setup_bot, SWARMING_EXTERNAL_BOT_SETUP is set')
    return

  botobj = get_bot()
  try:
    from config import bot_config
  except Exception as e:
    msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
    botobj.post_error('bot_config.py is bad: %s' % msg)
    return

  try:
    should_continue = bot_config.setup_bot(botobj)
  except Exception as e:
    msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
    botobj.post_error('bot_config.setup_bot() threw: %s' % msg)
    return

  if not should_continue and not skip_reboot:
    botobj.restart('Starting new swarming bot: %s' % THIS_FILE)


def get_authentication_headers(botobj):
  """Calls bot_config.get_authentication_headers() if it is defined.

  Doesn't catch exceptions.
  """
  if _in_load_test_mode():
    return (None, None)
  logging.info('get_authentication_headers()')
  from config import bot_config
  func = getattr(bot_config, 'get_authentication_headers', None)
  return func(botobj) if func else (None, None)


### end of bot_config handler part.


def get_min_free_space():
  """Returns free disk space needed.

  Add a "250 MiB slack space" for logs, temporary files and whatever other leak.
  """
  return int((os_utilities.get_min_free_space(THIS_FILE) + 250.) * 1024 * 1024)


def generate_version():
  """Returns the bot's code version."""
  try:
    return zip_package.generate_version()
  except Exception as e:
    return 'Error: %s' % e


def get_attributes(botobj):
  """Returns the attributes sent to the server.

  Each called function catches all exceptions so the bot doesn't die on startup,
  which is annoying to recover. In that case, we set a special property to catch
  these and help the admin fix the swarming_bot code more quickly.

  Arguments:
  - botobj: bot.Bot instance or None
  """
  return {
    'dimensions': get_dimensions(botobj),
    'state': get_state(botobj, 0),
    'version': generate_version(),
  }


def post_error_task(botobj, error, task_id):
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
  data = {
    'id': botobj.id,
    'message': error,
    'task_id': task_id,
  }
  return botobj.remote.url_read_json(
      '/swarming/api/v1/bot/task_error/%s' % task_id, data=data)


def on_shutdown_hook(b):
  """Called when the bot is restarting."""
  call_hook(b, 'on_bot_shutdown')
  # Aggressively set itself up so we ensure the auto-reboot configuration is
  # fine before restarting the host. This is important as some tasks delete the
  # autorestart script (!)
  setup_bot(True)


def get_bot():
  """Returns a valid Bot instance.

  Should only be called once in the process lifetime.
  """
  # This variable is used to bootstrap the initial bot.Bot object, which then is
  # used to get the dimensions and state.
  attributes = {
    'dimensions': {u'id': ['none']},
    'state': {},
    'version': generate_version(),
  }
  config = get_config()
  assert not config['server'].endswith('/'), config

  # Use temporary Bot object to call get_attributes. Attributes are needed to
  # construct the "real" bot.Bot.
  attributes = get_attributes(
    bot.Bot(
      remote_client.RemoteClient(config['server'], None),
      attributes,
      config['server'],
      config['server_version'],
      os.path.dirname(THIS_FILE),
      on_shutdown_hook))

  # Make remote client callback use the returned bot object. We assume here
  # RemoteClient doesn't call its callback in the constructor (since 'botobj' is
  # undefined during the construction).
  botobj = bot.Bot(
      remote_client.RemoteClient(
          config['server'],
          lambda: get_authentication_headers(botobj)),
      attributes,
      config['server'],
      config['server_version'],
      os.path.dirname(THIS_FILE),
      on_shutdown_hook)
  return botobj


def cleanup_bot_directory(botobj):
  """Delete anything not expected in the swarming bot directory.

  This helps with stale work directory or any unexpected junk that could cause
  this bot to self-quarantine. Do only this when running from the zip.
  """
  for i in os.listdir(botobj.base_dir):
    if any(fnmatch.fnmatch(i, w) for w in PASSLIST):
      continue
    try:
      p = unicode(os.path.join(botobj.base_dir, i))
      if os.path.isdir(p):
        file_path.rmtree(p)
      else:
        file_path.remove(p)
    except (IOError, OSError) as e:
      botobj.post_error(
          'Failed to remove %s from bot\'s directory: %s' % (i, e))


def clean_isolated_cache(botobj):
  """Asks run_isolated to clean its cache.

  This may take a while but it ensures that in the case of a run_isolated run
  failed and it temporarily used more space than min_free_disk, it can cleans up
  the mess properly.

  It will remove unexpected files, remove corrupted files, trim the cache size
  based on the policies and update state.json.
  """
  bot_dir = botobj.base_dir
  cmd = [
    sys.executable, THIS_FILE, 'run_isolated',
    '--clean',
    '--log-file', os.path.join(bot_dir, 'logs', 'run_isolated.log'),
    '--cache', os.path.join(bot_dir, 'isolated_cache'),
    '--min-free-space', str(get_min_free_space()),
  ]
  logging.info('Running: %s', cmd)
  try:
    # Intentionally do not use a timeout, it can take a while to hash 50gb but
    # better be safe than sorry.
    proc = subprocess42.Popen(
        cmd,
        stdin=subprocess42.PIPE,
        stdout=subprocess42.PIPE, stderr=subprocess42.STDOUT,
        cwd=bot_dir,
        detached=True,
        close_fds=sys.platform != 'win32')
    output, _ = proc.communicate(None)
    logging.info('Result:\n%s', output)
    if proc.returncode:
      botobj.post_error(
          'swarming_bot.zip failure during run_isolated --clean:\n%s' % output)
  except OSError:
    botobj.post_error(
        'swarming_bot.zip internal failure during run_isolated --clean')


def run_bot(arg_error):
  """Runs the bot until it reboots or self-update or a signal is received.

  When a signal is received, simply exit.
  """
  quit_bit = threading.Event()
  def handler(sig, _):
    logging.info('Got signal %s', sig)
    quit_bit.set()

  # TODO(maruel): Set quit_bit when stdin is closed on Windows.

  with subprocess42.set_signal_handler(subprocess42.STOP_SIGNALS, handler):
    config = get_config()
    try:
      # First thing is to get an arbitrary url. This also ensures the network is
      # up and running, which is necessary before trying to get the FQDN below.
      resp = net.url_read(config['server'] + '/swarming/api/v1/bot/server_ping')
      if resp is None:
        logging.error('No response from server_ping')
    except Exception as e:
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

    # Next we make sure the bot can make authenticated calls by grabbing
    # the auth headers, retrying on errors a bunch of times. We don't give up
    # if it fails though (maybe the bot will "fix itself" later).
    botobj = get_bot()
    try:
      botobj.remote.initialize(quit_bit)
    except remote_client.InitializationError as exc:
      botobj.post_error('failed to grab auth headers: %s' % exc.last_error)
      logging.error('Can\'t grab auth headers, continuing anyway...')

    if arg_error:
      botobj.post_error('Bootstrapping error: %s' % arg_error)

    if quit_bit.is_set():
      logging.info('Early quit 2')
      return 0

    call_hook(botobj, 'on_bot_startup')

    # Initial attributes passed to bot.Bot in get_bot above were constructed for
    # 'fake' bot ID ('none'). Refresh them to match the real bot ID, now that we
    # have fully initialize bot.Bot object. Note that 'get_dimensions' and
    # 'get_state' may depend on actions done by 'on_bot_startup' hook, that's
    # why we do it here and not in 'get_bot'.
    botobj.update_dimensions(get_dimensions(botobj))
    botobj.update_state(get_state(botobj, 0))

    if quit_bit.is_set():
      logging.info('Early quit 3')
      return 0

    # This is the first authenticated request to the server. If the bot is
    # misconfigured, the request may fail with HTTP 401 or HTTP 403. Instead of
    # dying right away, spin in a loop, hoping the bot will "fix itself"
    # eventually. Authentication errors in /handshake are logged on the server
    # and generate error reports, so bots stuck in this state are discoverable.
    sleep_time = 5
    while not quit_bit.is_set():
      resp = botobj.remote.url_read_json(
          '/swarming/api/v1/bot/handshake', data=botobj._attributes)
      if resp:
        logging.info('Connected to %s', resp.get('server_version'))
        if resp.get('bot_version') != botobj._attributes['version']:
          logging.warning(
              'Found out we\'ll need to update: server said %s; we\'re %s',
              resp.get('bot_version'), botobj._attributes['version'])
        break
      logging.error(
          'Failed to contact for handshake, retrying in %d sec...', sleep_time)
      quit_bit.wait(sleep_time)
      sleep_time = min(300, sleep_time * 2)

    if quit_bit.is_set():
      logging.info('Early quit 4')
      return 0

    cleanup_bot_directory(botobj)
    clean_isolated_cache(botobj)

    if quit_bit.is_set():
      logging.info('Early quit 5')
      return 0

    # This environment variable is accessible to the tasks executed by this bot.
    os.environ['SWARMING_BOT_ID'] = botobj.id.encode('utf-8')

    consecutive_sleeps = 0
    last_action = time.time()
    while not quit_bit.is_set():
      try:
        botobj.update_dimensions(get_dimensions(botobj))
        botobj.update_state(get_state(botobj, consecutive_sleeps))
        did_something = poll_server(botobj, quit_bit, last_action)
        if did_something:
          last_action = time.time()
          consecutive_sleeps = 0
        else:
          consecutive_sleeps += 1
      except Exception as e:
        logging.exception('poll_server failed')
        msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
        botobj.post_error(msg)
        consecutive_sleeps = 0
    logging.info('Quitting')

  # Tell the server we are going away.
  botobj.post_event('bot_shutdown', 'Signal was received')
  botobj.cancel_all_timers()
  return 0


def poll_server(botobj, quit_bit, last_action):
  """Polls the server to run one loop.

  Returns True if executed some action, False if server asked the bot to sleep.
  """
  # Access to a protected member _XXX of a client class - pylint: disable=W0212
  start = time.time()
  resp = botobj.remote.url_read_json(
      '/swarming/api/v1/bot/poll', data=botobj._attributes)
  if not resp:
    # Back off on failure.
    time.sleep(max(1, min(60, botobj.state.get('sleep_streak', 10) * 2)))
    return False
  logging.debug('Server response:\n%s', resp)

  cmd = resp['cmd']
  if cmd == 'sleep':
    call_hook(botobj, 'on_bot_idle', max(0, time.time() - last_action))
    quit_bit.wait(resp['duration'])
    return False

  if cmd == 'terminate':
    quit_bit.set()
    # This is similar to post_update() in task_runner.py.
    params = {
      'cost_usd': 0,
      'duration': 0,
      'exit_code': 0,
      'hard_timeout': False,
      'id': botobj.id,
      'io_timeout': False,
      'output': '',
      'output_chunk_start': 0,
      'task_id': resp['task_id'],
    }
    botobj.remote.url_read_json(
        '/swarming/api/v1/bot/task_update/%s' % resp['task_id'],
        data=params)
    return False

  if cmd == 'run':
    if run_manifest(botobj, resp['manifest'], start):
      # Completed a task successfully so update swarming_bot.zip if necessary.
      update_lkgbc(botobj)
    # Clean up cache after a task
    clean_isolated_cache(botobj)
    # TODO(maruel): Handle the case where quit_bit.is_set() happens here. This
    # is concerning as this means a signal (often SIGTERM) was received while
    # running the task. Make sure the host is properly restarting.
  elif cmd == 'update':
    update_bot(botobj, resp['version'])
  elif cmd == 'restart':
    if _in_load_test_mode():
      logging.warning('Would have restarted: %s' % resp['message'])
    else:
      botobj.restart(resp['message'])
  else:
    raise ValueError('Unexpected command: %s\n%s' % (cmd, resp))

  return True


def run_manifest(botobj, manifest, start):
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
  hard_timeout = manifest['hard_timeout'] or None
  # Default the grace period to 30s here, this doesn't affect the grace period
  # for the actual task.
  grace_period = manifest['grace_period'] or 30
  if manifest['hard_timeout']:
    # One for the child process, one for run_isolated, one for task_runner.
    hard_timeout += 3 * manifest['grace_period']
    # For isolated task, download time is not counted for hard timeout so add
    # more time.
    if not manifest['command']:
      hard_timeout += manifest['io_timeout'] or 600

  url = manifest.get('host', botobj.server)
  task_dimensions = manifest['dimensions']
  task_result = {}

  failure = False
  internal_failure = False
  msg = None
  auth_params_dumper = None
  # Use 'w' instead of 'work' because path length is precious on Windows.
  work_dir = os.path.join(botobj.base_dir, 'w')
  try:
    try:
      if os.path.isdir(work_dir):
        file_path.rmtree(work_dir)
    except OSError:
      # If a previous task created an undeleteable file/directory inside 'w',
      # make sure that following tasks are not affected. This is done by working
      # around the undeleteable directory by creating a temporary directory
      # instead. This is not normal behavior. The bot will report a failure on
      # start.
      work_dir = tempfile.mkdtemp(dir=botobj.base_dir, prefix='w')
    else:
      os.makedirs(work_dir)

    env = os.environ.copy()
    # Windows in particular does not tolerate unicode strings in environment
    # variables.
    env['SWARMING_TASK_ID'] = task_id.encode('ascii')
    env['SWARMING_SERVER'] = botobj.server.encode('ascii')

    task_in_file = os.path.join(work_dir, 'task_runner_in.json')
    with open(task_in_file, 'wb') as f:
      f.write(json.dumps(manifest))
    handle, bot_file = tempfile.mkstemp(
        prefix='bot_file', suffix='.json', dir=work_dir)
    os.close(handle)
    call_hook(botobj, 'on_before_task', bot_file)
    task_result_file = os.path.join(work_dir, 'task_runner_out.json')
    if os.path.exists(task_result_file):
      os.remove(task_result_file)

    # Start a thread that periodically puts authentication headers and other
    # authentication related information to a file on disk. task_runner and its
    # subprocesses read it from there before making authenticated HTTP calls.
    auth_params_file = os.path.join(work_dir, 'bot_auth_params.json')
    if botobj.remote.uses_auth:
      env['SWARMING_AUTH_PARAMS'] = str(auth_params_file)
      auth_params_dumper = file_refresher.FileRefresherThread(
          auth_params_file, lambda: bot_auth.prepare_auth_params_json(botobj))
      auth_params_dumper.start()
    else:
      env.pop('SWARMING_AUTH_PARAMS', None)
      if os.path.exists(auth_params_file):
        os.remove(auth_params_file)

    command = [
      sys.executable, THIS_FILE, 'task_runner',
      '--swarming-server', url,
      '--in-file', task_in_file,
      '--out-file', task_result_file,
      '--cost-usd-hour', str(botobj.state.get('cost_usd_hour') or 0.),
      # Include the time taken to poll the task in the cost.
      '--start', str(start),
      '--min-free-space', str(get_min_free_space()),
      '--bot-file', bot_file,
    ]
    logging.debug('Running command: %s', command)

    # Put the output file into the current working directory, which should be
    # the one containing swarming_bot.zip.
    log_path = os.path.join(botobj.base_dir, 'logs', 'task_runner_stdout.log')
    os_utilities.roll_log(log_path)
    os_utilities.trim_rolled_log(log_path)
    with open(log_path, 'a+b') as f:
      proc = subprocess42.Popen(
          command,
          detached=True,
          cwd=botobj.base_dir,
          env=env,
          stdin=subprocess42.PIPE,
          stdout=f,
          stderr=subprocess42.STDOUT,
          close_fds=sys.platform != 'win32')
      try:
        proc.wait(hard_timeout)
      except subprocess42.TimeoutExpired:
        # That's the last ditch effort; as task_runner should have completed a
        # while ago and had enforced the timeout itself (or run_isolated for
        # hard_timeout for isolated task).
        logging.error('Sending SIGTERM to task_runner')
        proc.terminate()
        internal_failure = True
        msg = 'task_runner hung'
        try:
          proc.wait(grace_period)
        except subprocess42.TimeoutExpired:
          logging.error('Sending SIGKILL to task_runner')
          proc.kill()
        proc.wait()
        return False

    logging.info('task_runner exit: %d', proc.returncode)
    if os.path.exists(task_result_file):
      with open(task_result_file, 'rb') as fd:
        task_result = json.load(fd)

    if proc.returncode:
      msg = 'Execution failed: internal error (%d).' % proc.returncode
      internal_failure = True
    elif not task_result:
      logging.warning('task_runner failed to write metadata')
      msg = 'Execution failed: internal error (no metadata).'
      internal_failure = True
    elif task_result[u'must_signal_internal_failure']:
      msg = (
        'Execution failed: %s' % task_result[u'must_signal_internal_failure'])
      internal_failure = True

    failure = bool(task_result.get('exit_code')) if task_result else False
    return not internal_failure and not failure
  except Exception as e:
    # Failures include IOError when writing if the disk is full, OSError if
    # swarming_bot.zip doesn't exist anymore, etc.
    logging.exception('run_manifest failed')
    msg = 'Internal exception occured: %s\n%s' % (
        e, traceback.format_exc()[-2048:])
    internal_failure = True
  finally:
    if auth_params_dumper:
      auth_params_dumper.stop()
    if internal_failure:
      post_error_task(botobj, msg, task_id)
    call_hook(
        botobj, 'on_after_task', failure, internal_failure, task_dimensions,
        task_result)
    if os.path.isdir(work_dir):
      try:
        file_path.rmtree(work_dir)
      except Exception as e:
        botobj.post_error(
            'Failed to delete work directory %s: %s' % (work_dir, e))


def update_bot(botobj, version):
  """Downloads the new version of the bot code and then runs it.

  Use alternating files; first load swarming_bot.1.zip, then swarming_bot.2.zip,
  never touching swarming_bot.zip which was the originally bootstrapped file.

  LKGBC is handled by update_lkgbc().

  Does not return.
  """
  # Alternate between .1.zip and .2.zip.
  new_zip = 'swarming_bot.1.zip'
  if os.path.basename(THIS_FILE) == new_zip:
    new_zip = 'swarming_bot.2.zip'
  new_zip = os.path.join(os.path.dirname(THIS_FILE), new_zip)

  # Download as a new file.
  url_path = '/swarming/api/v1/bot/bot_code/%s?bot_id=%s' % (
      version, urllib.quote_plus(botobj.id))
  if not botobj.remote.url_retrieve(new_zip, url_path):
    # It can happen when a server is rapidly updated multiple times in a row.
    botobj.post_error(
        'Unable to download %s from %s; first tried version %s' %
        (new_zip, botobj.server + url_path, version))
    # Poll again, this may work next time. To prevent busy-loop, sleep a little.
    time.sleep(2)
    return

  s = os.stat(new_zip)
  logging.info('Restarting to %s; %d bytes.', new_zip, s.st_size)
  sys.stdout.flush()
  sys.stderr.flush()

  proc = subprocess42.Popen(
     [sys.executable, new_zip, 'is_fine'],
     stdout=subprocess42.PIPE, stderr=subprocess42.STDOUT)
  output, _ = proc.communicate()
  if proc.returncode:
    botobj.post_error(
        'New bot code is bad: proc exit = %s. stdout:\n%s' %
        (proc.returncode, output))
    # Poll again, the server may have better code next time. To prevent
    # busy-loop, sleep a little.
    time.sleep(2)
    return

  # Don't forget to release the singleton before restarting itself.
  SINGLETON.release()

  # Do not call on_bot_shutdown.
  # On OSX, launchd will be unhappy if we quit so the old code bot process has
  # to outlive the new code child process. Launchd really wants the main process
  # to survive, and it'll restart it if it disappears. os.exec*() replaces the
  # process so this is fine.
  ret = common.exec_python([new_zip, 'start_slave', '--survive'])
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


def update_lkgbc(botobj):
  """Updates the Last Known Good Bot Code if necessary."""
  try:
    if not os.path.isfile(THIS_FILE):
      botobj.post_error('Missing file %s for LKGBC' % THIS_FILE)
      return

    golden = os.path.join(os.path.dirname(THIS_FILE), 'swarming_bot.zip')
    if os.path.isfile(golden):
      org = os.stat(golden)
      cur = os.stat(THIS_FILE)
      if org.st_size == org.st_size and org.st_mtime >= cur.st_mtime:
        return

    # Copy the file back.
    shutil.copy(THIS_FILE, golden)
  except Exception as e:
    botobj.post_error('Failed to update LKGBC: %s' % e)


def get_config():
  """Returns the data from config.json."""
  global _ERROR_HANDLER_WAS_REGISTERED

  with contextlib.closing(zipfile.ZipFile(THIS_FILE, 'r')) as f:
    config = json.load(f.open('config/config.json', 'r'))

  server = config.get('server', '')
  if not _ERROR_HANDLER_WAS_REGISTERED and server:
    on_error.report_on_exception_exit(server)
    _ERROR_HANDLER_WAS_REGISTERED = True
  return config


def main(args):
  subprocess42.inhibit_os_error_reporting()
  # Add SWARMING_HEADLESS into environ so subcommands know that they are running
  # in a headless (non-interactive) mode.
  os.environ['SWARMING_HEADLESS'] = '1'

  # The only reason this is kept is to enable the unit test to use --help to
  # quit the process.
  parser = optparse.OptionParser(description=sys.modules[__name__].__doc__)
  _, args = parser.parse_args(args)

  # Enforces that only one process with a bot in this directory can be run on
  # this host at once.
  if not SINGLETON.acquire():
    if sys.platform == 'darwin':
      msg = (
          'Found a previous bot, %d rebooting as a workaround for '
          'https://crbug.com/569610.') % os.getpid()
      print >> sys.stderr, msg
      os_utilities.restart(msg)
    else:
      print >> sys.stderr, 'Found a previous bot, %d exiting.' % os.getpid()
    return 1

  for t in ('out', 'err'):
    log_path = os.path.join(
        os.path.dirname(THIS_FILE), 'logs', 'bot_std%s.log' % t)
    os_utilities.roll_log(log_path)
    os_utilities.trim_rolled_log(log_path)

  error = None
  if len(args) != 0:
    error = 'Unexpected arguments: %s' % args
  try:
    return run_bot(error)
  finally:
    call_hook(bot.Bot(None, None, None, None, os.path.dirname(THIS_FILE), None),
              'on_bot_shutdown')
    logging.info('main() returning')
