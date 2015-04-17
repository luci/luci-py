# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs a Swarming task.

Downloads all the necessary files to run the task, executes the command and
streams results back to the Swarming server.

The process exit code is 0 when the task was executed, even if the task itself
failed. If there's any failure in the setup or teardown, like invalid packet
response, failure to contact the server, etc, a non zero exit code is used. It's
up to the calling process (bot_main.py) to signal that there was an internal
failure and to cancel this task run and ask the server to retry it.
"""

__version__ = '0.4'

import StringIO
import base64
import json
import logging
import optparse
import os
import subprocess
import sys
import time
import zipfile

import xsrf_client
from utils import net
from utils import on_error
from utils import subprocess42


# Sends a maximum of 100kb of stdout per task_update packet.
MAX_CHUNK_SIZE = 102400


# Maximum wait between task_update packet when there's no output.
MAX_PACKET_INTERVAL = 30


# Minimum wait between task_update packet when there's output.
MIN_PACKET_INTERNAL = 10


# Exit code used to indicate the task failed. Keep in sync with bot_main.py. The
# reason for its existance is that if an exception occurs, task_runner's exit
# code will be 1. If the process is killed, it'll likely be -9. In these cases,
# we want the task to be marked as internal_failure, not as a failure.
TASK_FAILED = 89


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


def download_data(root_dir, files):
  """Downloads and expands the zip files enumerated in the test run data."""
  for data_url, _ in files:
    logging.info('Downloading: %s', data_url)
    content = net.url_read(data_url)
    if content is None:
      raise Exception('Failed to download %s' % data_url)
    with zipfile.ZipFile(StringIO.StringIO(content)) as zip_file:
      zip_file.extractall(root_dir)


class TaskDetails(object):
  def __init__(self, data):
    """Loads the raw data.

    It is expected to have at least:
     - bot_id
     - command as a list of str
     - data as a list of urls
     - env as a dict
     - hard_timeout
     - io_timeout
     - task_id
    """
    if not isinstance(data, dict):
      raise ValueError('Expected dict, got %r' % data)

    # Get all the data first so it fails early if the task details is invalid.
    self.bot_id = data['bot_id']
    self.command = data['command']
    self.data = data['data']
    self.env = {
      k.encode('utf-8'): v.encode('utf-8') for k, v in data['env'].iteritems()
    }
    self.grace_period = data['grace_period']
    self.hard_timeout = data['hard_timeout']
    self.io_timeout = data['io_timeout']
    self.task_id = data['task_id']


def load_and_run(filename, swarming_server, cost_usd_hour, start, json_file):
  """Loads the task's metadata and execute it.

  This may throw all sorts of exceptions in case of failure. It's up to the
  caller to trap them. These shall be considered 'internal_failure' instead of
  'failure' from a TaskRunResult standpoint.

  Return:
    True on success, False if the task failed.
  """
  # The work directory is guaranteed to exist since it was created by
  # bot_main.py and contains the manifest. Temporary files will be downloaded
  # there. It's bot_main.py that will delete the directory afterward. Tests are
  # not run from there.
  root_dir = os.path.abspath('work')
  if not os.path.isdir(root_dir):
    raise ValueError('%s expected to exist' % root_dir)

  with open(filename, 'rb') as f:
    task_details = TaskDetails(json.load(f))

  # Download the script to run in the temporary directory.
  download_data(root_dir, task_details.data)

  exit_code = run_command(
      swarming_server, task_details, root_dir, cost_usd_hour, start, json_file)
  return not bool(exit_code)


def post_update(swarming_server, params, exit_code, stdout, output_chunk_start):
  """Posts task update to task_update.

  Arguments:
    swarming_server: XsrfRemote instance.
    params: Default JSON parameters for the POST.
    exit_code: Process exit code, only when a command completed.
    stdout: Incremental output since last call, if any.
    output_chunk_start: Total number of stdout previously sent, for coherency
        with the server.
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
  resp = swarming_server.url_read_json(
      '/swarming/api/v1/bot/task_update/%s' % params['task_id'], data=params)
  logging.debug('post_update() = %s', resp)
  if resp.get('error'):
    # Abandon it. This will force a process exit.
    raise ValueError(resp.get('error'))


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
    return max(now - timed_out - task_details.grace_period, 0.)

  packet_interval = MIN_PACKET_INTERNAL if stdout else MAX_PACKET_INTERVAL
  hard_timeout = start + task_details.hard_timeout - now
  io_timeout = last_io + task_details.io_timeout - now
  out = max(min(min(packet_interval, hard_timeout), io_timeout), 0)
  logging.debug('calc_yield_wait() = %d', out)
  return out


def run_command(
    swarming_server, task_details, root_dir, cost_usd_hour, task_start,
    json_file):
  """Runs a command and sends packets to the server to stream results back.

  Implements both I/O and hard timeouts. Sends the packets numbered, so the
  server can ensure they are processed in order.

  Returns:
    Child process exit code.
  """
  # Signal the command is about to be started.
  last_packet = start = now = monotonic_time()
  params = {
    'cost_usd': cost_usd_hour * (now - task_start) / 60. / 60.,
    'id': task_details.bot_id,
    'task_id': task_details.task_id,
  }
  post_update(swarming_server, params, None, '', 0)

  logging.info('Executing: %s', task_details.command)
  # TODO(maruel): Support both channels independently and display stderr in red.
  env = None
  if task_details.env:
    env = os.environ.copy()
    env.update(task_details.env)
  try:
    proc = subprocess42.Popen(
        task_details.command,
        env=env,
        cwd=root_dir,
        detached=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE)
  except OSError as e:
    stdout = 'Command "%s" failed to start.\nError: %s' % (
        ' '.join(task_details.command), e)
    now = monotonic_time()
    params['cost_usd'] = cost_usd_hour * (now - task_start) / 60. / 60.
    params['duration'] = now - start
    params['io_timeout'] = False
    params['hard_timeout'] = False
    post_update(swarming_server, params, 1, stdout, 0)
    return 1

  output_chunk_start = 0
  stdout = ''
  exit_code = None
  had_hard_timeout = False
  had_io_timeout = False
  timed_out = None
  try:
    calc = lambda: calc_yield_wait(
        task_details, start, last_io, timed_out, stdout)
    maxsize = lambda: MAX_CHUNK_SIZE - len(stdout)
    last_io = monotonic_time()
    for _, new_data in proc.yield_any(maxsize=maxsize, soft_timeout=calc):
      now = monotonic_time()
      if new_data:
        stdout += new_data
        last_io = now

      # Post update if necessary.
      if should_post_update(stdout, now, last_packet):
        last_packet = monotonic_time()
        params['cost_usd'] = (
            cost_usd_hour * (last_packet - task_start) / 60. / 60.)
        post_update(swarming_server, params, None, stdout, output_chunk_start)
        output_chunk_start += len(stdout)
        stdout = ''

      # Send signal on timeout if necessary. Both are failures, not
      # internal_failures.
      # Eventually kill but return 0 so bot_main.py doesn't cancel the task.
      if not timed_out:
        if now - last_io > task_details.io_timeout:
          had_io_timeout = True
          logging.warning('I/O timeout')
          proc.terminate()
          timed_out = monotonic_time()
        elif now - start > task_details.hard_timeout:
          had_hard_timeout = True
          logging.warning('Hard timeout')
          proc.terminate()
          timed_out = monotonic_time()
      else:
        # During grace period.
        if now >= timed_out + task_details.grace_period:
          # Now kill for real. The user can distinguish between the following
          # states:
          # - signal but process exited within grace period,
          #   (hard_|io_)_timed_out will be set but the process exit code will
          #   be script provided.
          # - processed exited late, exit code will be -9 on posix.
          try:
            logging.warning('proc.kill() after grace')
            proc.kill()
          except OSError:
            pass
    logging.info('Waiting for proces exit')
    exit_code = proc.wait()
    logging.info('Waiting for proces exit - done')
  finally:
    # Something wrong happened, try to kill the child process.
    if exit_code is None:
      had_hard_timeout = True
      try:
        logging.warning('proc.kill() in finally')
        proc.kill()
      except OSError:
        # The process has already exited.
        pass

      # TODO(maruel): We'd wait only for X seconds.
      logging.info('Waiting for proces exit in finally')
      exit_code = proc.wait()
      logging.info('Waiting for proces exit in finally - done')

    # This is the very last packet for this command.
    now = monotonic_time()
    params['cost_usd'] = cost_usd_hour * (now - task_start) / 60. / 60.
    params['duration'] = now - start
    params['io_timeout'] = had_io_timeout
    params['hard_timeout'] = had_hard_timeout
    # At worst, it'll re-throw, which will be caught by bot_main.py.
    post_update(swarming_server, params, exit_code, stdout, output_chunk_start)
    output_chunk_start += len(stdout)
    stdout = ''

    with open(json_file, 'w') as fd:
      json.dump({'exit_code': exit_code, 'version': 1}, fd)

  logging.info('run_command() = %s', exit_code)
  assert not stdout
  return exit_code


def main(args):
  parser = optparse.OptionParser(
      description=sys.modules[__name__].__doc__,
      version=__version__)
  parser.add_option('--file', help='Name of the request file')
  parser.add_option(
      '--swarming-server', help='Swarming server to send data back')
  parser.add_option(
      '--cost-usd-hour', type='float', help='Cost of this VM in $/h')
  parser.add_option('--start', type='float', help='Time this task was started')
  parser.add_option(
      '--json-file', help='Name of the JSON file to write a task summary to')

  options, args = parser.parse_args(args)
  if not options.file:
    parser.error('You must provide the request file name.')
  if args:
    parser.error('Unknown args: %s' % args)

  on_error.report_on_exception_exit(options.swarming_server)

  logging.info('starting')
  remote = xsrf_client.XsrfRemote(options.swarming_server)

  now = monotonic_time()
  if options.start > now:
    options.start = now

  try:
    if not load_and_run(
        options.file, remote, options.cost_usd_hour, options.start,
        options.json_file):
      return TASK_FAILED
    return 0
  finally:
    logging.info('quitting')
