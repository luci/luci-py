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
import Queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile

import bot
import logging_utils
import os_utilities
import xsrf_client
from utils import net
from utils import on_error
from utils import zip_package


# Path to this file or the zip containing this file.
THIS_FILE = os.path.abspath(zip_package.get_main_script_path())

# Root directory containing this file or the zip containing this file.
ROOT_DIR = os.path.dirname(THIS_FILE)


# See task_runner.py for documentation.
TASK_FAILED = 89


_ERROR_HANDLER_WAS_REGISTERED = False


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

    import bot_config
    out = bot_config.get_dimensions()
    if not isinstance(out, dict):
      raise ValueError('Unexpected type %s' % out.__class__)
    return out
  except Exception as e:
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
      import bot_config
      state = bot_config.get_state()
      if not isinstance(state, dict):
        state = {'error': state}
  except Exception as e:
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

    import bot_config
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
    import bot_config
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
      remote, attributes, config['server_version'], ROOT_DIR,
      lambda b: call_hook(b, 'on_bot_shutdown'))

task_queue = Queue.Queue()

def run_bot(arg_error):
  """Runs the bot until it reboots or self-update."""
  try:
    # First thing is to get an arbitrary url. This also ensures the network is
    # up and running, which is necessary before trying to get the FQDN below.
    resp = get_remote().url_read('/swarming/api/v1/bot/server_ping')
    if resp is None:
      logging.error('No response from server_ping')
  except Exception as e:
    # url_read() already traps pretty much every exceptions. This except clause
    # is kept there "just in case".
    logging.exception('server_ping threw')

  # If this fails, there's hardly anything that can be done, the bot can't even
  # get to the point to be able to self-update.
  botobj = get_bot()
  if arg_error:
    botobj.post_error('Argument error: %s' % arg_error)

  call_hook(botobj, 'on_bot_startup')

  # This environment variable is accessible to the tasks executed by this bot.
  os.environ['SWARMING_BOT_ID'] = botobj.id.encode('utf-8')

  # TODO(maruel): Run 'health check' on startup.
  # https://code.google.com/p/swarming/issues/detail?id=112
  consecutive_sleeps = 0

  thread_pool = []
  pool_size = 10

  for i in range(pool_size):
    t = threading.Thread(target=task_worker)
    t.setDaemon(True)
    thread_pool.append(t)
    t.start()

  while True:
    try:
      botobj.update_dimensions(get_dimensions())
      botobj.update_state(get_state(consecutive_sleeps))
      # Pull a task
      resp = botobj.remote.url_read_json(
          '/swarming/api/v1/bot/poll', data=botobj._attributes)
      logging.debug('Server response:\n%s', resp)
      if resp['cmd'] == 'run':
        # Put response to process in the task queue
        task_queue.put((botobj, resp), block=True)
      else:
        process_main_thread_task(botobj, resp)

      has_todo_tasks = task_queue.empty()
      if has_todo_tasks:
        consecutive_sleeps = 0
      else:
        consecutive_sleeps += 1
    except Exception as e:
      logging.exception('poll_server failed')
      msg = '%s\n%s' % (e, traceback.format_exc()[-2048:])
      botobj.post_error(msg)
      consecutive_sleeps = 0

def process_main_thread_task(botobj, resp):
    cmd = resp['cmd']
    if cmd == 'sleep':
      time.sleep(resp['duration'])
    elif cmd == 'update':
      update_bot(botobj, resp['version'])
    elif cmd == 'restart':
      if _in_load_test_mode():
        logging.warning('Would have restarted: %s' % resp['message'])
      else:
        botobj.restart(resp['message'])
    else:
      raise ValueError('Unexpected command: %s\n%s' % (cmd, resp))
    # Handle auto-update of swarming_bot.zip
    # update_lkgbc()


def task_worker():
  thread_name = threading.current_thread().name
  logging.info(thread_name + ' has started')
  while True:
    botobj, resp = task_queue.get(block=True)
    logging.info(thread_name + ' has picked up %s', str(resp))
    # Access to a protected member _XXX of a client class - pylint: disable=W0212
    start = time.time()
    run_manifest(botobj, resp['manifest'], start)
    logging.info(threading.current_thread().name + ' done')
    task_queue.task_done()


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
  hard_timeout = manifest['hard_timeout'] + 5*60.
  url = manifest.get('host', botobj.remote.url)
  task_dimensions = manifest['dimensions']
  task_summary = {}

  failure = False
  internal_failure = False
  msg = None
  try:
    # We currently do not clean up the 'work' directory now is it compartmented.
    # TODO(maruel): Compartmentation should be done via tag. It is important to
    # not be too aggressive about deletion because running a task with a warm
    # cache has important performance benefit.
    # https://code.google.com/p/swarming/issues/detail?id=149
    thread_name = threading.current_thread().name
    work_dir = os.path.join(botobj.base_dir, 'work', thread_name)
    if not os.path.isdir(work_dir):
      os.makedirs(work_dir)

    env = os.environ.copy()
    # Windows in particular does not tolerate unicode strings in environment
    # variables.
    env['SWARMING_TASK_ID'] = task_id.encode('ascii')

    path = os.path.join(work_dir, 'task_run.json')
    with open(path, 'wb') as f:
      f.write(json.dumps(manifest))
    call_hook(botobj, 'on_before_task')
    task_summary_file = os.path.join(work_dir, 'task_summary.json')
    if os.path.exists(task_summary_file):
      os.remove(task_summary_file)
    command = [
      sys.executable, THIS_FILE, 'task_runner',
      '--swarming-server', url,
      '--file', path,
      '--cost-usd-hour', str(botobj.state.get('cost_usd_hour') or 0.),
      # Include the time taken to poll the task in the cost.
      '--start', str(start),
      '--json-file', task_summary_file,
    ]
    logging.debug('Running command: %s', command)
    proc = subprocess.Popen(command, cwd=ROOT_DIR, env=env)
    while proc.poll() is None:
      if time.time() - start >= hard_timeout:
        proc.kill()
        failure = False
        internal_failure = True
        msg = 'task_runner hung'
        return False
      # Busy loop.
      time.sleep(0.5)

    failure = proc.returncode == TASK_FAILED
    internal_failure = not failure and bool(proc.returncode)
    if internal_failure:
      msg = 'Execution failed, internal error.'
    if os.path.exists(task_summary_file):
      with open(task_summary_file) as fd:
        task_summary = json.load(fd)
    return not bool(proc.returncode)
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
      task_summary)


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

  cmd = [sys.executable, new_zip, 'start_slave', '--survive']
  # Do not call on_bot_shutdown.
  if sys.platform in ('cygwin', 'win32'):
    # (Tentative) It is expected that subprocess.Popen() behaves a tad better
    # on Windows than os.exec*(), which has to be emulated since there's no OS
    # provided implementation. This means processes will accumulate as the bot
    # is restarted, which could be a problem long term.
    try:
      subprocess.Popen(cmd)
    except Exception as e:
      logging.exception('failed to respawn: %s', e)
    else:
      sys.exit(0)
  else:
    # On OSX, launchd will be unhappy if we quit so the old code bot process
    # has to outlive the new code child process. Launchd really wants the main
    # process to survive, and it'll restart it if it disappears. os.exec*()
    # replaces the process so this is fine.
    os.execv(sys.executable, cmd)

  # This code runs only if bot failed to respawn itself.
  botobj.post_error('Bot failed to respawn after update')


def update_lkgbc():
  """Updates the Last Known Good Bot Code if necessary."""
  try:
    org = os.stat('swarming_bot.zip')
    cur = os.stat(THIS_FILE)
    if org.st_size != org.st_size or org.st_mtime < cur.st_mtime:
      shutil.copy(THIS_FILE, 'swarming_bot.zip')
  except Exception as e:
    logging.exception('failed to update lkgbc: %s', e)


def get_config():
  """Returns the data from config.json.

  First try the config.json inside the zip. If not present or not running inside
  swarming_bot.zip, use the one beside the file.
  """
  if THIS_FILE.endswith('.zip'):
    # Can't use with statement here as it has to work with python 2.6 due to
    # obscure reasons relating to old cygwin installs.
    with contextlib.closing(zipfile.ZipFile(THIS_FILE, 'r')) as f:
      return json.load(f.open('config.json'))

  with open(os.path.join(ROOT_DIR, 'config.json'), 'r') as f:
    return json.load(f)


def main(args):
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
    call_hook(bot.Bot(None, None, None, ROOT_DIR, None), 'on_bot_shutdown')
