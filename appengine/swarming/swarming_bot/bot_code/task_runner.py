# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Runs a Swarming task.

Downloads all the necessary files to run the task, executes the command and
streams results back to the Swarming server.

The process exit code is 0 when the task was executed, even if the task itself
failed. If there's any failure in the setup or teardown, like invalid packet
response, failure to contact the server, etc, a non zero exit code is used. It's
up to the calling process (bot_main.py) to signal that there was an internal
failure and to cancel this task run and ask the server to retry it.
"""

import base64
import json
import logging
import optparse
import os
import signal
import sys
import time

from utils import file_path
from utils import net
from utils import on_error
from utils import subprocess42
from utils import zip_package

import bot_auth
import file_reader


# Path to this file or the zip containing this file.
THIS_FILE = os.path.abspath(zip_package.get_main_script_path())


# Sends a maximum of 100kb of stdout per task_update packet.
MAX_CHUNK_SIZE = 102400


# Maximum wait between task_update packet when there's no output.
MAX_PACKET_INTERVAL = 30


# Minimum wait between task_update packet when there's output.
MIN_PACKET_INTERNAL = 10


# Current task_runner_out version.
OUT_VERSION = 3


# On Windows, SIGTERM is actually sent as SIGBREAK since there's no real
# SIGTERM.  SIGBREAK is not defined on posix since it's a pure Windows concept.
SIG_BREAK_OR_TERM = (
    signal.SIGBREAK if sys.platform == 'win32' else signal.SIGTERM)


# Used to implement monotonic_time for a clock that never goes backward.
_last_now = 0


def monotonic_time():
  """Returns monotonically increasing time."""
  global _last_now
  now = time.time()
  if now > _last_now:
    # TODO(maruel): If delta is large, probably worth alerting via ereporter2.
    _last_now = now
  return _last_now


def get_run_isolated():
  """Returns the path to itself to run run_isolated.

  Mocked in test to point to the real run_isolated.py script.
  """
  return [sys.executable, THIS_FILE, 'run_isolated']


def get_isolated_cmd(
    work_dir, task_details, isolated_result, bot_file, min_free_space):
  """Returns the command to call run_isolated. Mocked in tests."""
  assert (bool(task_details.command) !=
          bool(task_details.isolated and task_details.isolated.get('input')))
  bot_dir = os.path.dirname(work_dir)
  if os.path.isfile(isolated_result):
    os.remove(isolated_result)
  cmd = get_run_isolated()

  if task_details.isolated:
    cmd.extend(
        [
          '-I', task_details.isolated['server'].encode('utf-8'),
          '--namespace', task_details.isolated['namespace'].encode('utf-8'),
        ])
    isolated_input = task_details.isolated.get('input')
    if isolated_input:
      cmd.extend(
          [
            '--isolated', isolated_input,
          ])

  if task_details.cipd_input and task_details.cipd_input.get('packages'):
    for pkg in task_details.cipd_input.get('packages'):
      cmd.extend([
        '--cipd-package',
        '%s:%s:%s' % (pkg['path'], pkg['package_name'], pkg['version'])])
    cmd.extend(
        [
          '--cipd-cache', os.path.join(bot_dir, 'cipd_cache'),
          '--cipd-client-package',
          task_details.cipd_input['client_package']['package_name'],
          '--cipd-client-version',
          task_details.cipd_input['client_package']['version'],
          '--cipd-server', task_details.cipd_input.get('server'),
        ])

  cmd.extend(
      [
        '--json', isolated_result,
        '--log-file', os.path.join(bot_dir, 'logs', 'run_isolated.log'),
        '--cache', os.path.join(bot_dir, 'isolated_cache'),
        '--root-dir', work_dir,
      ])
  if min_free_space:
    cmd.extend(('--min-free-space', str(min_free_space)))
  if bot_file:
    cmd.extend(('--bot-file', bot_file))

  if task_details.hard_timeout:
    cmd.extend(('--hard-timeout', str(task_details.hard_timeout)))
  if task_details.grace_period:
    cmd.extend(('--grace-period', str(task_details.grace_period)))

  # TODO(nodir): Pass the command line arguments via a response file.
  cmd.append('--')
  if task_details.command:
    cmd.extend(task_details.command)
  elif task_details.extra_args:
    cmd.extend(task_details.extra_args)
  return cmd


class TaskDetails(object):
  def __init__(self, data):
    """Loads the raw data from a manifest file specified by --in-file."""
    logging.info('TaskDetails(%s)', data)
    if not isinstance(data, dict):
      raise ValueError('Expected dict, got %r' % data)

    # Get all the data first so it fails early if the task details is invalid.
    self.bot_id = data['bot_id']

    # Raw command. Only self.command or self.isolated.input can be set.
    self.command = data['command'] or []

    # Isolated command. Is a serialized version of task_request.FilesRef.
    self.isolated = data['isolated']
    self.extra_args = data['extra_args']

    self.cipd_input = data.get('cipd_input')

    self.env = {
      k.encode('utf-8'): v.encode('utf-8') for k, v in data['env'].iteritems()
    }
    self.grace_period = data['grace_period']
    self.hard_timeout = data['hard_timeout']
    self.io_timeout = data['io_timeout']
    self.task_id = data['task_id']


class MustExit(Exception):
  """Raised on signal that the process must exit immediately."""
  def __init__(self, sig):
    super(MustExit, self).__init__()
    self.signal = sig


def load_and_run(
    in_file, swarming_server, cost_usd_hour, start, out_file, min_free_space,
    bot_file):
  """Loads the task's metadata and execute it.

  This may throw all sorts of exceptions in case of failure. It's up to the
  caller to trap them. These shall be considered 'internal_failure' instead of
  'failure' from a TaskRunResult standpoint.
  """
  # The work directory is guaranteed to exist since it was created by
  # bot_main.py and contains the manifest. Temporary files will be downloaded
  # there. It's bot_main.py that will delete the directory afterward. Tests are
  # not run from there.
  task_result = None
  def handler(sig, _):
    logging.info('Got signal %s', sig)
    raise MustExit(sig)
  work_dir = os.path.dirname(out_file)
  try:
    with subprocess42.set_signal_handler([SIG_BREAK_OR_TERM], handler):
      if not os.path.isdir(work_dir):
        raise ValueError('%s expected to exist' % work_dir)

      with open(in_file, 'rb') as f:
        task_details = TaskDetails(json.load(f))

      task_result = run_command(
          swarming_server, task_details, work_dir,
          cost_usd_hour, start, min_free_space, bot_file)
  except MustExit as e:
    # This normally means run_command() didn't get the chance to run, as it
    # itself trap MustExit and will report accordingly. In this case, we want
    # the parent process to send the message instead.
    if not task_result:
      task_result = {
        u'exit_code': None,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'must_signal_internal_failure':
            u'task_runner received signal %s' % e.signal,
        u'version': OUT_VERSION,
      }
  finally:
    # We've found tests to delete the working directory work_dir when quitting,
    # causing an exception here. Try to recreate the directory if necessary.
    if not os.path.isdir(work_dir):
      os.mkdir(work_dir)
    with open(out_file, 'wb') as f:
      json.dump(task_result, f)


def post_update(
    swarming_server, auth_headers, params, exit_code,
    stdout, output_chunk_start):
  """Posts task update to task_update.

  Arguments:
    swarming_server: Base URL to Swarming server.
    auth_headers: dict with HTTP authentication headers.
    params: Default JSON parameters for the POST.
    exit_code: Process exit code, only when a command completed.
    stdout: Incremental output since last call, if any.
    output_chunk_start: Total number of stdout previously sent, for coherency
        with the server.

  Returns:
    False if the task should stop.
  """
  params = params.copy()
  if exit_code is not None:
    params['exit_code'] = exit_code
  if stdout:
    # The output_chunk_start is used by the server to make sure that the stdout
    # chunks are processed and saved in the DB in order.
    params['output'] = base64.b64encode(stdout)
    params['output_chunk_start'] = output_chunk_start
  # TODO(maruel): Support early cancellation.
  # https://code.google.com/p/swarming/issues/detail?id=62
  resp = net.url_read_json(
      swarming_server+'/swarming/api/v1/bot/task_update/%s' % params['task_id'],
      data=params,
      headers=auth_headers,
      follow_redirects=False)
  logging.debug('post_update() = %s', resp)
  if not resp or resp.get('error'):
    # Abandon it. This will force a process exit.
    raise ValueError(resp.get('error') if resp else 'Failed to contact server')
  return not resp.get('must_stop', False)


def should_post_update(stdout, now, last_packet):
  """Returns True if it's time to send a task_update packet via post_update().

  Sends a packet when one of this condition is met:
  - more than MAX_CHUNK_SIZE of stdout is buffered.
  - last packet was sent more than MIN_PACKET_INTERNAL seconds ago and there was
    stdout.
  - last packet was sent more than MAX_PACKET_INTERVAL seconds ago.
  """
  packet_interval = MIN_PACKET_INTERNAL if stdout else MAX_PACKET_INTERVAL
  return len(stdout) >= MAX_CHUNK_SIZE or (now - last_packet) > packet_interval


def calc_yield_wait(task_details, start, last_io, timed_out, stdout):
  """Calculates the maximum number of seconds to wait in yield_any()."""
  now = monotonic_time()
  if timed_out:
    # Give a |grace_period| seconds delay.
    if task_details.grace_period:
      return max(now - timed_out - task_details.grace_period, 0.)
    return 0.

  out = MIN_PACKET_INTERNAL if stdout else MAX_PACKET_INTERVAL
  if task_details.hard_timeout:
    out = min(out, start + task_details.hard_timeout - now)
  if task_details.io_timeout:
    out = min(out, last_io + task_details.io_timeout - now)
  out = max(out, 0)
  logging.debug('calc_yield_wait() = %d', out)
  return out


def kill_and_wait(proc, grace_period, reason):
  logging.warning('SIGTERM finally due to %s', reason)
  proc.terminate()
  try:
    proc.wait(grace_period)
  except subprocess42.TimeoutError:
    logging.warning('SIGKILL finally due to %s', reason)
    proc.kill()
  exit_code = proc.wait()
  logging.info('Waiting for proces exit in finally - done')
  return exit_code


def start_reading_headers(auth_params_file):
  """Spawns a thread that rereads headers from SWARMING_AUTH_PARAMS file.

  Returns:
    Tuple (callback that returns the last known headers, stop callback).

  Raises:
    file_reader.FatalReadError if headers file can't be read.
    ValueError if it can be read, but its body is invalid.
  """
  # Read headers more often than bot_main writes them, to reduce maximum
  # possible latency between headers are updated and read.
  reader = file_reader.FileReaderThread(auth_params_file, interval_sec=30)

  def read_and_validate_headers():
    val = bot_auth.process_auth_params_json(reader.last_value or {})
    return val.swarming_http_headers

  reader.start()
  read_and_validate_headers() # initial validation, may raise ValueError
  return read_and_validate_headers, reader.stop


def run_command(
    swarming_server, task_details, work_dir, cost_usd_hour,
    task_start, min_free_space, bot_file):
  """Runs a command and sends packets to the server to stream results back.

  Implements both I/O and hard timeouts. Sends the packets numbered, so the
  server can ensure they are processed in order.

  Returns:
    Metadata about the command.
  """
  # TODO(maruel): This function is incomprehensible, split and refactor.

  # Grab initial auth headers and start rereading them in parallel thread. They
  # MUST be there already. It's fatal internal error if they are not.
  headers_cb = lambda: {}
  stop_headers_reader = lambda: None
  auth_params_file = os.environ.get('SWARMING_AUTH_PARAMS')
  if auth_params_file:
    try:
      headers_cb, stop_headers_reader = start_reading_headers(auth_params_file)
    except (ValueError, file_reader.FatalReadError) as e:
      return {
        u'exit_code': 1,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'must_signal_internal_failure': str(e),
        u'version': OUT_VERSION,
      }

  # Signal the command is about to be started.
  last_packet = start = now = monotonic_time()
  params = {
    'cost_usd': cost_usd_hour * (now - task_start) / 60. / 60.,
    'id': task_details.bot_id,
    'task_id': task_details.task_id,
  }
  if not post_update(swarming_server, headers_cb(), params, None, '', 0):
    # Don't even bother, the task was already canceled.
    return {
      u'exit_code': -1,
      u'hard_timeout': False,
      u'io_timeout': False,
      u'must_signal_internal_failure': None,
      u'version': OUT_VERSION,
    }

  isolated_result = os.path.join(work_dir, 'isolated_result.json')
  cmd = get_isolated_cmd(
      work_dir, task_details, isolated_result, bot_file, min_free_space)
  # Hard timeout enforcement is deferred to run_isolated. Grace is doubled to
  # give one 'grace_period' slot to the child process and one slot to upload
  # the results back.
  task_details.hard_timeout = 0
  if task_details.grace_period:
    task_details.grace_period *= 2

  try:
    # TODO(maruel): Support both channels independently and display stderr in
    # red.
    env = None
    if task_details.env:
      env = os.environ.copy()
      for key, value in task_details.env.iteritems():
        if not value:
          env.pop(key, None)
        else:
          env[key] = value
    logging.info('cmd=%s', cmd)
    logging.info('cwd=%s', work_dir)
    logging.info('env=%s', env)
    try:
      assert cmd and all(isinstance(a, basestring) for a in cmd)
      proc = subprocess42.Popen(
          cmd,
          env=env,
          cwd=work_dir,
          detached=True,
          stdout=subprocess42.PIPE,
          stderr=subprocess42.STDOUT,
          stdin=subprocess42.PIPE)
    except OSError as e:
      stdout = 'Command "%s" failed to start.\nError: %s' % (' '.join(cmd), e)
      now = monotonic_time()
      params['cost_usd'] = cost_usd_hour * (now - task_start) / 60. / 60.
      params['duration'] = now - start
      params['io_timeout'] = False
      params['hard_timeout'] = False
      # Ignore server reply to stop.
      post_update(swarming_server, headers_cb(), params, 1, stdout, 0)
      return {
        u'exit_code': 1,
        u'hard_timeout': False,
        u'io_timeout': False,
        u'must_signal_internal_failure': None,
        u'version': OUT_VERSION,
      }

    output_chunk_start = 0
    stdout = ''
    exit_code = None
    had_io_timeout = False
    must_signal_internal_failure = None
    kill_sent = False
    timed_out = None
    try:
      calc = lambda: calc_yield_wait(
          task_details, start, last_io, timed_out, stdout)
      maxsize = lambda: MAX_CHUNK_SIZE - len(stdout)
      last_io = monotonic_time()
      for _, new_data in proc.yield_any(maxsize=maxsize, timeout=calc):
        now = monotonic_time()
        if new_data:
          stdout += new_data
          last_io = now

        # Post update if necessary.
        if should_post_update(stdout, now, last_packet):
          last_packet = monotonic_time()
          params['cost_usd'] = (
              cost_usd_hour * (last_packet - task_start) / 60. / 60.)
          if not post_update(
              swarming_server, headers_cb(), params, None, stdout,
              output_chunk_start):
            # Server is telling us to stop. Normally task cancelation.
            if not kill_sent:
              logging.warning('Server induced stop; sending SIGKILL')
              proc.kill()
              kill_sent = True

          output_chunk_start += len(stdout)
          stdout = ''

        # Send signal on timeout if necessary. Both are failures, not
        # internal_failures.
        # Eventually kill but return 0 so bot_main.py doesn't cancel the task.
        if not timed_out:
          if (task_details.io_timeout and
              now - last_io > task_details.io_timeout):
            had_io_timeout = True
            logging.warning(
                'I/O timeout is %.3fs; no update for %.3fs sending SIGTERM',
                task_details.io_timeout, now - last_io)
            proc.terminate()
            timed_out = monotonic_time()
        else:
          # During grace period.
          if not kill_sent and now - timed_out >= task_details.grace_period:
            # Now kill for real. The user can distinguish between the following
            # states:
            # - signal but process exited within grace period,
            #   (hard_|io_)_timed_out will be set but the process exit code will
            #   be script provided.
            # - processed exited late, exit code will be -9 on posix.
            logging.warning(
                'Grace of %.3fs exhausted at %.3fs; sending SIGKILL',
                task_details.grace_period, now - timed_out)
            proc.kill()
            kill_sent = True
      logging.info('Waiting for proces exit')
      exit_code = proc.wait()
    except MustExit as e:
      # TODO(maruel): Do the send SIGTERM to child process and give it
      # task_details.grace_period to terminate.
      must_signal_internal_failure = (
          u'task_runner received signal %s' % e.signal)
      exit_code = kill_and_wait(
          proc, task_details.grace_period, 'signal %d' % e.signal)
    except (IOError, OSError):
      # Something wrong happened, try to kill the child process.
      exit_code = kill_and_wait(
          proc, task_details.grace_period, 'exception %s' % e)

    # This is the very last packet for this command. It if was an isolated task,
    # include the output reference to the archived .isolated file.
    now = monotonic_time()
    params['cost_usd'] = cost_usd_hour * (now - task_start) / 60. / 60.
    params['duration'] = now - start
    params['io_timeout'] = had_io_timeout
    had_hard_timeout = False
    try:
      if not os.path.isfile(isolated_result):
        # It's possible if
        # - run_isolated.py did not start
        # - run_isolated.py started, but arguments were invalid
        # - host in a situation unable to fork
        # - grand child process outliving the child process deleting everything
        #   it can
        # Do not create an internal error, just send back the (partial)
        # view as task_runner saw it, for example the real exit_code is
        # unknown.
        logging.warning('there\'s no result file')
        if exit_code is None:
          exit_code = -1
      else:
        # See run_isolated.py for the format.
        with open(isolated_result, 'rb') as f:
          run_isolated_result = json.load(f)
        logging.debug('run_isolated:\n%s', run_isolated_result)
        # TODO(maruel): Grab statistics (cache hit rate, data downloaded,
        # mapping time, etc) from run_isolated and push them to the server.
        if run_isolated_result['outputs_ref']:
          params['outputs_ref'] = run_isolated_result['outputs_ref']
        had_hard_timeout = run_isolated_result['had_hard_timeout']
        if not had_io_timeout and not had_hard_timeout:
          if run_isolated_result['internal_failure']:
            must_signal_internal_failure = (
                run_isolated_result['internal_failure'])
            logging.error('%s', must_signal_internal_failure)
          elif exit_code:
            # TODO(maruel): Grab stdout from run_isolated.
            must_signal_internal_failure = (
                'run_isolated internal failure %d' % exit_code)
            logging.error('%s', must_signal_internal_failure)
        exit_code = run_isolated_result['exit_code']
        params['bot_overhead'] = 0.
        if run_isolated_result.get('duration') is not None:
          # Calculate the real task duration as measured by run_isolated and
          # calculate the remaining overhead.
          params['bot_overhead'] = params['duration']
          params['duration'] = run_isolated_result['duration']
          params['bot_overhead'] -= params['duration']
          params['bot_overhead'] -= run_isolated_result.get(
              'download', {}).get('duration', 0)
          params['bot_overhead'] -= run_isolated_result.get(
              'upload', {}).get('duration', 0)
          params['bot_overhead'] -= run_isolated_result.get(
              'cipd', {}).get('duration', 0)
          if params['bot_overhead'] < 0:
            params['bot_overhead'] = 0
        isolated_stats = run_isolated_result.get('stats', {}).get('isolated')
        if isolated_stats:
          params['isolated_stats'] = isolated_stats
        cipd_stats = run_isolated_result.get('stats', {}).get('cipd')
        if cipd_stats:
          params['cipd_stats'] = cipd_stats
    except (IOError, OSError, ValueError) as e:
      logging.error('Swallowing error: %s', e)
      if not must_signal_internal_failure:
        must_signal_internal_failure = str(e)
    # TODO(maruel): Send the internal failure here instead of sending it through
    # bot_main, this causes a race condition.
    if exit_code is None:
      exit_code = -1
    params['hard_timeout'] = had_hard_timeout
    # Ignore server reply to stop.
    post_update(
        swarming_server, headers_cb(), params, exit_code,
        stdout, output_chunk_start)
    return {
      u'exit_code': exit_code,
      u'hard_timeout': had_hard_timeout,
      u'io_timeout': had_io_timeout,
      u'must_signal_internal_failure': must_signal_internal_failure,
      u'version': OUT_VERSION,
    }
  finally:
    file_path.try_remove(unicode(isolated_result))
    stop_headers_reader()


def main(args):
  subprocess42.inhibit_os_error_reporting()
  parser = optparse.OptionParser(description=sys.modules[__name__].__doc__)
  parser.add_option('--in-file', help='Name of the request file')
  parser.add_option(
      '--out-file', help='Name of the JSON file to write a task summary to')
  parser.add_option(
      '--swarming-server', help='Swarming server to send data back')
  parser.add_option(
      '--cost-usd-hour', type='float', help='Cost of this VM in $/h')
  parser.add_option('--start', type='float', help='Time this task was started')
  parser.add_option(
      '--min-free-space', type='int',
      help='Value to send down to run_isolated')
  parser.add_option(
      '--bot-file', help='Path to a file describing the state of the host.')

  options, args = parser.parse_args(args)
  if not options.in_file or not options.out_file or args:
    parser.error('task_runner is meant to be used by swarming_bot.')

  on_error.report_on_exception_exit(options.swarming_server)

  logging.info('starting')
  now = monotonic_time()
  if options.start > now:
    options.start = now

  try:
    load_and_run(
        options.in_file, options.swarming_server, options.cost_usd_hour,
        options.start, options.out_file, options.min_free_space,
        options.bot_file)
    return 0
  finally:
    logging.info('quitting')
