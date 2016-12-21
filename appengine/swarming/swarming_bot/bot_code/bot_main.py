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
import zipfile

# Import _strptime before threaded code. datetime.datetime.strptime is
# threadsafe except for the initial import of the _strptime module.
# See https://bugs.python.org/issue7980.
import _strptime  # pylint: disable=unused-import

import bot_auth
import common
import file_refresher
import remote_client
import remote_client_errors
import singleton
from api import bot
from api import os_utilities
from api import platforms
from utils import file_path
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


def log_call(name=None):
  def gen(fn):
    def hook(*args, **kwargs):
      start = time.time()
      fnname = name(*args) if name else fn.__name__
      logging.info('%s()', fnname)
      try:
        return fn(*args, **kwargs)
      finally:
        logging.info('%s() took %.1fs', fnname, round(time.time() - start, 1))
    return hook
  return gen


@log_call()
def get_dimensions(botobj):
  """Returns bot_config.py's get_dimensions() dict."""
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
      logging.exception('os.utilities.get_dimensions() failed')
      try:
        botid = os_utilities.get_hostname_short()
      except Exception as e2:
        logging.exception('os.utilities.get_hostname_short() failed')
        botid = 'error_%s' % str(e2)
      return {
          'id': [botid],
          'error': ['%s\n%s' % (e, traceback.format_exc()[-2048:])],
          'quarantined': ['1'],
        }


@log_call()
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

  if not state.get('quarantined') and not is_base_dir_ok(botobj):
    # Use super hammer in case of dangerous environment.
    state['quarantined'] = 'Can\'t run from blacklisted directory'

  state['sleep_streak'] = sleep_streak
  return state


@log_call(lambda _, name, *args: name)
def call_hook(botobj, name, *args):
  """Calls a hook function in bot_config.py."""
  try:
    if _in_load_test_mode():
      return

    from config import bot_config
    hook = getattr(bot_config, name, None)
    if hook:
      return hook(botobj, *args)
  except Exception as e:
    logging.exception('%s() threw', name)
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


def is_base_dir_ok(botobj):
  """Returns False if the bot must be quarantined at all cost."""
  if not botobj:
    # This can happen very early in the process lifetime.
    return os.path.dirname(THIS_FILE) != os.path.expanduser('~')
  return botobj.base_dir != os.path.expanduser('~')


def get_min_free_space(botobj):
  """Returns free disk space needed.

  Add a "250 MiB slack space" for logs, temporary files and whatever other leak.
  """
  return int(
      (os_utilities.get_min_free_space(botobj.base_dir) + 250.) * 1024 * 1024)


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
  return botobj.remote.post_task_error(task_id, botobj.id, error)


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

  base_dir = os.path.dirname(THIS_FILE)
  # Use temporary Bot object to call get_attributes. Attributes are needed to
  # construct the "real" bot.Bot.
  attributes = get_attributes(
    bot.Bot(
      remote_client.createRemoteClient(config['server'],
                                       None, config['is_grpc']),
      attributes,
      config['server'],
      config['server_version'],
      base_dir,
      on_shutdown_hook))

  # Make remote client callback use the returned bot object. We assume here
  # RemoteClient doesn't call its callback in the constructor (since 'botobj' is
  # undefined during the construction).
  botobj = bot.Bot(
      remote_client.createRemoteClient(
          config['server'],
          lambda: get_authentication_headers(botobj),
          config['is_grpc']),
      attributes,
      config['server'],
      config['server_version'],
      base_dir,
      on_shutdown_hook)
  return botobj


def cleanup_bot_directory(botobj):
  """Delete anything not expected in the swarming bot directory.

  This helps with stale work directory or any unexpected junk that could cause
  this bot to self-quarantine. Do only this when running from the zip.
  """
  if not is_base_dir_ok(botobj):
    # That's an important one-off check as cleaning the $HOME directory has
    # really bad effects on normal host.
    logging.error('Not cleaning root directory because of bad base directory')
    return
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


def clean_cache(botobj):
  """Asks run_isolated to clean its cache.

  This may take a while but it ensures that in the case of a run_isolated run
  failed and it temporarily used more space than min_free_disk, it can cleans up
  the mess properly.

  It will remove unexpected files, remove corrupted files, trim the cache size
  based on the policies and update state.json.
  """
  cmd = [
    sys.executable, THIS_FILE, 'run_isolated',
    '--clean',
    '--log-file', os.path.join(botobj.base_dir, 'logs', 'run_isolated.log'),
    '--cache', os.path.join(botobj.base_dir, 'isolated_cache'),
    '--named-cache-root', os.path.join(botobj.base_dir, 'c'),
    '--min-free-space', str(get_min_free_space(botobj)),
  ]
  logging.info('Running: %s', cmd)
  try:
    # Intentionally do not use a timeout, it can take a while to hash 50gb but
    # better be safe than sorry.
    proc = subprocess42.Popen(
        cmd,
        stdin=subprocess42.PIPE,
        stdout=subprocess42.PIPE, stderr=subprocess42.STDOUT,
        cwd=botobj.base_dir,
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
      # There's no need to do error handling here - the "ping" is just to "wake
      # up" the network; if there's something seriously wrong, the handshake
      # will fail and we'll handle it there.
      remote = remote_client.createRemoteClient(config['server'], None,
                                                config['is_grpc'])
      remote.ping()
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
    botobj._update_dimensions(get_dimensions(botobj))
    botobj._update_state(get_state(botobj, 0))

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
      resp = botobj.remote.do_handshake(botobj._attributes)
      if resp:
        logging.info('Connected to %s', resp.get('server_version'))
        if resp.get('bot_version') != botobj._attributes['version']:
          logging.warning(
              'Found out we\'ll need to update: server said %s; we\'re %s',
              resp.get('bot_version'), botobj._attributes['version'])
        # Remember the server-provided per-bot configuration. '/handshake' is
        # the only place where the server returns it. The bot will be sending
        # the 'bot_group_cfg_version' back in each /poll (as part of 'state'),
        # so that the server can instruct the bot to restart itself when
        # config changes.
        cfg_version = resp.get('bot_group_cfg_version')
        if cfg_version:
          botobj._update_bot_group_cfg(cfg_version, resp.get('bot_group_cfg'))
        break
      logging.error(
          'Failed to contact for handshake, retrying in %d sec...', sleep_time)
      quit_bit.wait(sleep_time)
      sleep_time = min(300, sleep_time * 2)

    if quit_bit.is_set():
      logging.info('Early quit 4')
      return 0

    # Let the bot to finish the initialization, now that it knows its server
    # defined dimensions.
    call_hook(botobj, 'on_handshake')

    cleanup_bot_directory(botobj)
    clean_cache(botobj)

    if quit_bit.is_set():
      logging.info('Early quit 5')
      return 0

    # This environment variable is accessible to the tasks executed by this bot.
    os.environ['SWARMING_BOT_ID'] = botobj.id.encode('utf-8')

    consecutive_sleeps = 0
    last_action = time.time()
    while not quit_bit.is_set():
      try:
        botobj._update_dimensions(get_dimensions(botobj))
        botobj._update_state(get_state(botobj, consecutive_sleeps))
        did_something = poll_server(botobj, quit_bit, last_action)
        if did_something:
          last_action = time.time()
          consecutive_sleeps = 0
        else:
          consecutive_sleeps += 1
      except Exception as e:
        logging.exception('poll_server failed in a completely unexpected way')
        msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
        botobj.post_error(msg)
        consecutive_sleeps = 0
        # Sleep a bit as a precaution to avoid hammering the server.
        quit_bit.wait(10)
    logging.info('Quitting')

  # Tell the server we are going away.
  botobj.post_event('bot_shutdown', 'Signal was received')
  return 0


def poll_server(botobj, quit_bit, last_action):
  """Polls the server to run one loop.

  Returns True if executed some action, False if server asked the bot to sleep.
  """
  start = time.time()
  try:
    cmd, value = botobj.remote.poll(botobj._attributes)
  except remote_client_errors.PollError as e:
    # Back off on failure.
    delay = max(1, min(60, botobj.state.get('sleep_streak', 10) * 2))
    logging.warning('Poll failed (%s), sleeping %.1f sec', e, delay)
    quit_bit.wait(delay)
    return False
  logging.debug('Server response:\n%s: %s', cmd, value)

  if cmd == 'sleep':
    # Value is duration
    call_hook(botobj, 'on_bot_idle', max(0, time.time() - last_action))
    quit_bit.wait(value)
    return False

  if cmd == 'terminate':
    # The value is the task ID to serve as the special termination command.
    quit_bit.set()
    try:
      # Duration must be set or server IEs. For that matter, we've never cared
      # if there's an error here before, so let's preserve that behaviour
      # (though anything that's not a remote_client.InternalError will make
      # it through, again preserving prior behaviour).
      botobj.remote.post_task_update(value, botobj.id, {'duration':0}, None, 0)
    except remote_client_errors.InternalError:
      pass
    return False

  if cmd == 'run':
    # Value is the manifest
    if run_manifest(botobj, value, start):
      # Completed a task successfully so update swarming_bot.zip if necessary.
      update_lkgbc(botobj)
    # Clean up cache after a task
    clean_cache(botobj)
    # TODO(maruel): Handle the case where quit_bit.is_set() happens here. This
    # is concerning as this means a signal (often SIGTERM) was received while
    # running the task. Make sure the host is properly restarting.
  elif cmd == 'update':
    # Value is the version
    update_bot(botobj, value)
  elif cmd == 'restart':
    # Value is the message to display while restarting
    if _in_load_test_mode():
      logging.warning('Would have restarted: %s' % value)
    else:
      botobj.restart(value)
  else:
    raise ValueError('Unexpected command: %s\n%s' % (cmd, value))

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

  # Get the server info to pass to the task runner so it can provide updates.
  url = botobj.server
  is_grpc = botobj.remote.is_grpc()
  if not is_grpc and 'host' in manifest:
    # The URL in the manifest includes the version - eg not https://chromium-
    # swarm-dev.appspot.com, but https://<some-version>-dot-chromiium-swarm-
    # dev.appspot.com. That way, if a new server version becomes the default,
    # old bots will continue to work with a server version that can manipulate
    # the old data (the new server will only ever have to read it, which is
    # much simpler) while new bots won't accidentally contact an old server
    # which the GAE engine hasn't gotten around to updating yet.
    #
    # With a gRPC proxy, we could theoretically run into the same problem
    # if we change the meaning of some data without changing the protos.
    # However, if we *do* change the protos, we already need to make the
    # change in a few steps:
    #    1. Modify the Swarming server to accept the new data
    #    2. Modify the protos and the proxy to accept the new data
    #       in gRPC calls and translate it to "native" Swarming calls.
    #    3. Update the bots to transmit the new protos.
    # Throughout all this, the proto format itself irons out minor differences
    # and additions. But because we deploy in three steps, the odds of a
    # newer bot contacting an older server is very low.
    #
    # None of this applies if we don't actually update the protos but just
    # change the semantics. If this becomes a significant problem, we could
    # start transmitting the expected server version using gRPC metadata.
    #    - aludwin, Nov 2016
    url = manifest['host']

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
    # authentication related information to a file on disk. task_runner reads it
    # from there before making authenticated HTTP calls.
    auth_params_file = os.path.join(work_dir, 'bot_auth_params.json')
    if botobj.remote.uses_auth:
      auth_params_dumper = file_refresher.FileRefresherThread(
          auth_params_file,
          lambda: bot_auth.prepare_auth_params_json(botobj, manifest))
      auth_params_dumper.start()

    command = [
      sys.executable, THIS_FILE, 'task_runner',
      '--swarming-server', url,
      '--in-file', task_in_file,
      '--out-file', task_result_file,
      '--cost-usd-hour', str(botobj.state.get('cost_usd_hour') or 0.),
      # Include the time taken to poll the task in the cost.
      '--start', str(start),
      '--min-free-space', str(get_min_free_space(botobj)),
      '--bot-file', bot_file,
    ]
    if botobj.remote.uses_auth:
      command.extend(['--auth-params-file', auth_params_file])
    if is_grpc:
      command.append('--is-grpc')
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
  new_zip = os.path.join(botobj.base_dir, new_zip)

  # Download as a new file.
  try:
    botobj.remote.get_bot_code(new_zip, version, botobj.id)
  except remote_client.BotCodeError as e:
    botobj.post_error(str(e))
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

    golden = os.path.join(botobj.base_dir, 'swarming_bot.zip')
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

  base_dir = os.path.dirname(THIS_FILE)
  for t in ('out', 'err'):
    log_path = os.path.join(base_dir, 'logs', 'bot_std%s.log' % t)
    os_utilities.roll_log(log_path)
    os_utilities.trim_rolled_log(log_path)

  error = None
  if len(args) != 0:
    error = 'Unexpected arguments: %s' % args
  try:
    return run_bot(error)
  finally:
    call_hook(
        bot.Bot(None, None, None, None, base_dir, None), 'on_bot_shutdown')
    logging.info('main() returning')
