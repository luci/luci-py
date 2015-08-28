# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot main process.

This is the program that communicates with the Swarming server, ensures the code
is always up to date and executes a child process to run tasks and upload
results back.

It manages self-update and rebooting the host in case of problems.

Set the environment variable SWARMING_LOAD_TEST=1 to disable the use of
server-provided bot_config.py. This permits safe load testing.
"""

import contextlib
import json
import logging
import optparse
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile

import common
import xsrf_client
from api import bot
from api import os_utilities
from third_party.depot_tools import fix_encoding
from utils import logging_utils
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


### bot_config handler part.


def _in_load_test_mode():
  """Returns True if the default values should be used instead of the server
  provided bot_config.py.

  This also disables server telling the bot to restart.
  """
  return os.environ.get('SWARMING_LOAD_TEST') == '1'


def get_dimensions():
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
    out = bot_config.get_dimensions()
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


def get_state(sleep_streak):
  """Returns dict with a state of the bot reported to the server with each poll.
  """
  try:
    if _in_load_test_mode():
      state = os_utilities.get_state()
      state['dimensions'] = os_utilities.get_dimensions()
    else:
      from config import bot_config
      state = bot_config.get_state()
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

    from config import bot_config
    hook = getattr(bot_config, name, None)
    if hook:
      return hook(botobj, *args)
  except Exception as e:
    msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
    botobj.post_error('Failed to call hook %s(): %s' % (name, msg))


def setup_bot(skip_reboot):
  """Calls bot_config.setup_bot() to have the bot self-configure itself.

  Reboot the host if bot_config.setup_bot() returns False, unless skip_reboot is
  also true.
  """
  if _in_load_test_mode():
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


### end of bot_config handler part.


def generate_version():
  """Returns the bot's code version."""
  try:
    return zip_package.generate_version()
  except Exception as e:
    return 'Error: %s' % e


def get_attributes():
  """Returns the attributes sent to the server.

  Each called function catches all exceptions so the bot doesn't die on startup,
  which is annoying to recover. In that case, we set a special property to catch
  these and help the admin fix the swarming_bot code more quickly.
  """
  return {
    'dimensions': get_dimensions(),
    'state': get_state(0),
    'version': generate_version(),
  }


def get_remote():
  """Return a XsrfRemote instance to the preconfigured server."""
  global _ERROR_HANDLER_WAS_REGISTERED
  config = get_config()
  server = config['server']
  if not _ERROR_HANDLER_WAS_REGISTERED:
    on_error.report_on_exception_exit(server)
    _ERROR_HANDLER_WAS_REGISTERED = True
  return xsrf_client.XsrfRemote(server, '/swarming/api/v1/bot/handshake')


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
  # TODO(maruel): It could be good to send a signal when the task hadn't started
  # at all. In this case the server could retry the task even if it doesn't have
  # 'idempotent' set. See
  # https://code.google.com/p/swarming/issues/detail?id=108.
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
  while True:
    try:
      attributes = get_attributes()

      # Handshake to get an XSRF token even if there were errors.
      remote = get_remote()
      remote.xsrf_request_params = attributes.copy()
      break
    except Exception:
      # Continue looping. The main reason to get into this situation is when the
      # network is down for > 20 minutes. It's worth continuing to loop until
      # the server is reachable again.
      logging.exception('Catastrophic failure')

  config = get_config()
  return bot.Bot(
      remote,
      attributes,
      config['server'],
      config['server_version'],
      os.path.dirname(THIS_FILE),
      on_shutdown_hook)


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
    try:
      # First thing is to get an arbitrary url. This also ensures the network is
      # up and running, which is necessary before trying to get the FQDN below.
      resp = get_remote().url_read('/swarming/api/v1/bot/server_ping')
      if resp is None:
        logging.error('No response from server_ping')
    except Exception as e:
      # url_read() already traps pretty much every exceptions. This except
      # clause is kept there "just in case".
      logging.exception('server_ping threw')

    if quit_bit.is_set():
      return 0

    # If this fails, there's hardly anything that can be done, the bot can't
    # even get to the point to be able to self-update.
    botobj = get_bot()
    if arg_error:
      botobj.post_error('Argument error: %s' % arg_error)

    if quit_bit.is_set():
      return 0

    call_hook(botobj, 'on_bot_startup')

    if quit_bit.is_set():
      return 0

    # This environment variable is accessible to the tasks executed by this bot.
    os.environ['SWARMING_BOT_ID'] = botobj.id.encode('utf-8')

    # TODO(maruel): Run 'health check' on startup.
    # https://code.google.com/p/swarming/issues/detail?id=112
    consecutive_sleeps = 0
    while not quit_bit.is_set():
      try:
        botobj.update_dimensions(get_dimensions())
        botobj.update_state(get_state(consecutive_sleeps))
        did_something = poll_server(botobj, quit_bit)
        if did_something:
          consecutive_sleeps = 0
        else:
          consecutive_sleeps += 1
      except Exception as e:
        logging.exception('poll_server failed')
        msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
        botobj.post_error(msg)
        consecutive_sleeps = 0

  # Tell the server we are going away.
  botobj.post_event('bot_shutdown', 'Signal was received')
  botobj.cancel_all_timers()
  return 0


def poll_server(botobj, quit_bit):
  """Polls the server to run one loop.

  Returns True if executed some action, False if server asked the bot to sleep.
  """
  # Access to a protected member _XXX of a client class - pylint: disable=W0212
  start = time.time()
  resp = botobj.remote.url_read_json(
      '/swarming/api/v1/bot/poll', data=botobj._attributes)
  logging.debug('Server response:\n%s', resp)

  cmd = resp['cmd']
  if cmd == 'sleep':
    quit_bit.wait(resp['duration'])
    return False

  if cmd == 'run':
    if run_manifest(botobj, resp['manifest'], start):
      # Completed a task successfully so update swarming_bot.zip if necessary.
      update_lkgbc(botobj)
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
  # Sets an hard timeout of task's hard_time + 5 minutes to notify the server.
  # TODO(maruel): Add grace period.
  hard_timeout = manifest['hard_timeout'] + 5*60.
  url = manifest.get('host', botobj.remote.url)
  task_dimensions = manifest['dimensions']
  task_result = {}

  failure = False
  internal_failure = False
  msg = None
  try:
    # We currently do not clean up the 'work' directory now is it compartmented.
    # TODO(maruel): Compartmentation should be done via tag. It is important to
    # not be too aggressive about deletion because running a task with a warm
    # cache has important performance benefit.
    # https://code.google.com/p/swarming/issues/detail?id=149
    work_dir = os.path.join(botobj.base_dir, 'work')
    if not os.path.isdir(work_dir):
      os.makedirs(work_dir)

    env = os.environ.copy()
    # Windows in particular does not tolerate unicode strings in environment
    # variables.
    env['SWARMING_TASK_ID'] = task_id.encode('ascii')

    task_in_file = os.path.join(work_dir, 'task_runner_in.json')
    with open(task_in_file, 'wb') as f:
      f.write(json.dumps(manifest))
    call_hook(botobj, 'on_before_task')
    task_result_file = os.path.join(work_dir, 'task_runner_out.json')
    if os.path.exists(task_result_file):
      os.remove(task_result_file)
    command = [
      sys.executable, THIS_FILE, 'task_runner',
      '--swarming-server', url,
      '--in-file', task_in_file,
      '--out-file', task_result_file,
      '--cost-usd-hour', str(botobj.state.get('cost_usd_hour') or 0.),
      # Include the time taken to poll the task in the cost.
      '--start', str(start),
    ]
    logging.debug('Running command: %s', command)
    # Put the output file into the current working directory, which should be
    # the one containing swarming_bot.zip.
    with open('task_runner_stdout.log', 'wb') as f:
      proc = subprocess.Popen(
          command,
          cwd=os.path.dirname(THIS_FILE),
          env=env,
          stdout=f,
          stderr=subprocess.STDOUT)
    while proc.poll() is None:
      if time.time() - start >= hard_timeout:
        proc.kill()
        internal_failure = True
        msg = 'task_runner hung'
        return False
      # Busy loop.
      time.sleep(0.5)

    if os.path.exists(task_result_file):
      with open(task_result_file, 'rb') as fd:
        task_result = json.load(fd)

    if proc.returncode:
      logging.warning('task_runner died')
      msg = 'Execution failed, internal error (%d).' % proc.returncode
      internal_failure = True
    elif not task_result:
      logging.warning('task_runner failed to write metadata')
      msg = 'Execution failed, internal error (no metadata).'
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
    if internal_failure:
      post_error_task(botobj, msg, task_id)
    call_hook(
        botobj, 'on_after_task', failure, internal_failure, task_dimensions,
        task_result)


def update_bot(botobj, version):
  """Downloads the new version of the bot code and then runs it.

  Use alternating files; first load swarming_bot.1.zip, then swarming_bot.2.zip,
  never touching swarming_bot.zip which was the originally bootstrapped file.

  Does not return.

  TODO(maruel): Create LKGBC:
  https://code.google.com/p/swarming/issues/detail?id=112
  """
  # Alternate between .1.zip and .2.zip.
  new_zip = 'swarming_bot.1.zip'
  if os.path.basename(THIS_FILE) == new_zip:
    new_zip = 'swarming_bot.2.zip'
  new_zip = os.path.join(os.path.dirname(THIS_FILE), new_zip)

  # Download as a new file.
  url = botobj.remote.url + '/swarming/api/v1/bot/bot_code/%s' % version
  if not net.url_retrieve(new_zip, url):
    # Try without a specific version. It can happen when a server is rapidly
    # updated multiple times in a row.
    botobj.post_error(
        'Unable to download %s from %s; first tried version %s' %
        (new_zip, url, version))
    # Poll again, this may work next time. To prevent busy-loop, sleep a little.
    time.sleep(2)
    return

  logging.info('Restarting to %s.', new_zip)
  sys.stdout.flush()
  sys.stderr.flush()

  # Do not call on_bot_shutdown.
  # On OSX, launchd will be unhappy if we quit so the old code bot process has
  # to outlive the new code child process. Launchd really wants the main process
  # to survive, and it'll restart it if it disappears. os.exec*() replaces the
  # process so this is fine.
  ret = common.exec_python([new_zip, 'start_slave', '--survive'])
  if ret:
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
  with contextlib.closing(zipfile.ZipFile(THIS_FILE, 'r')) as f:
    return json.load(f.open('config/config.json', 'r'))


def main(args):
  # This is necessary so os.path.join() works with unicode path. No kidding.
  fix_encoding.fix_encoding()

  # Add SWARMING_HEADLESS into environ so subcommands know that they are running
  # in a headless (non-interactive) mode.
  os.environ['SWARMING_HEADLESS'] = '1'

  # TODO(maruel): Get rid of all flags and support no option at all.
  # https://code.google.com/p/swarming/issues/detail?id=111
  parser = optparse.OptionParser(
      usage='%prog [options]',
      description=sys.modules[__name__].__doc__)
  # TODO(maruel): Always True.
  parser.add_option('-v', '--verbose', action='count', default=0,
                    help='Set logging level to INFO, twice for DEBUG.')

  error = None
  try:
    # Do this late so an error is reported. It could happen when a flag is
    # removed but the auto-update script was not upgraded properly.
    options, args = parser.parse_args(args)
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    logging_utils.set_console_level(levels[min(options.verbose, len(levels)-1)])
    if args:
      parser.error('Unsupported args.')
  except Exception as e:
    # Do not reboot here, because it would just cause a reboot loop.
    error = str(e)
  try:
    return run_bot(error)
  finally:
    call_hook(bot.Bot(None, None, None, None, os.path.dirname(THIS_FILE), None),
              'on_bot_shutdown')
