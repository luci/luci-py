#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Runs a Swarming task.

Downloads all the necessary files to run the task, executes the commands and
streams results back to the Swarming server.

The process exit code is 0 when the task was executed, even if the task itself
failed. If there's any failure in the setup or teardown, like invalid packet
response, failure to contact the server, etc, a non zero exit code is used. It's
up to the calling process (slave_machine.py) to signal that there was an
internal failure and to cancel this task run and ask the server to retry it.
"""

__version__ = '0.3'

import StringIO
import json
import logging
import optparse
import os
import subprocess
import sys
import time
import zipfile

# pylint: disable-msg=W0403
import logging_utils
import url_helper
from utils import net
from utils import on_error
from utils import subprocess42


# Sends a maximum of 100kb of stdout per task_update packet.
MAX_CHUNK_SIZE = 102400


# Maximum wait between task_update packet when there's no output.
MAX_PACKET_INTERVAL = 30


# Minimum wait between task_update packet when there's output.
MIN_PACKET_INTERNAL = 10


# Exit code used to restart the host. Keep in sync with slave_machine.py. The
# reason for its existance is that if an exception occurs, local_test_runner's
# exit code will be 1. If the process is killed, it'll likely be -9. In these
# cases, we want the task to be marked as error. But if local_test_runner wants
# to reboot without marking the task as an internal failure, a special code must
# be used.
RESTART_CODE = 89


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
     - commands as a list of lists
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
    self.commands = data['commands']
    self.data = data['data']
    self.env = os.environ.copy()
    self.env.update(
        (k.encode('utf-8'), v.encode('utf-8'))
        for k, v in data['env'].iteritems())
    self.hard_timeout = data['hard_timeout']
    self.io_timeout = data['io_timeout']
    self.task_id = data['task_id']


def load_and_run(filename, swarming_server):
  """Loads the task's metadata and execute it.

  This may throw all sorts of exceptions in case of failure. It's up to the
  caller to trap them. These shall be considered 'internal_failure' instead of
  'failure' from a TaskRunResult standpoint.

  Return:
    True on success, False if the task failed.
  """
  # The work directory is guaranteed to exist since it was created by
  # slave_machine.py and contains the manifest. Temporary files will be
  # downloaded there. It's slave_machine.py that will delete the directory
  # afterward. Tests are not run from there.
  root_dir = os.path.abspath('work')
  if not os.path.isdir(root_dir):
    raise ValueError('%s expected to exist' % root_dir)

  with open(filename, 'rb') as f:
    task_details = TaskDetails(json.load(f))

  # Download the script to run in the temporary directory.
  download_data(root_dir, task_details.data)

  out = True
  for index in xrange(len(task_details.commands)):
    out = out and not bool(
        run_command(swarming_server, index, task_details, root_dir))
  return out


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
    params['output'] = stdout
    params['output_chunk_start'] = output_chunk_start
  # TODO(maruel): Support early cancellation.
  # https://code.google.com/p/swarming/issues/detail?id=62
  resp = swarming_server.url_read_json(
      '/swarming/api/v1/bot/task_update/%s' % params['task_id'], data=params)
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


def calc_yield_wait(task_details, start, last_io, stdout):
  """Calculates the maximum number of seconds to wait in yield_any()."""
  packet_interval = MIN_PACKET_INTERNAL if stdout else MAX_PACKET_INTERVAL
  now = time.time()
  hard_timeout = start + task_details.hard_timeout - now
  io_timeout = last_io + task_details.io_timeout - now
  return min(min(packet_interval, hard_timeout), io_timeout)


def run_command(swarming_server, index, task_details, root_dir):
  """Runs a command and sends packets to the server to stream results back.

  Implements both I/O and hard timeouts. Sends the packets numbered, so the
  server can ensure they are processed in order.

  Returns:
    Child process exit code.
  """
  # Signal the command is about to be started.
  params = {
    'command_index': index,
    'id': task_details.bot_id,
    'task_id': task_details.task_id,
  }
  last_packet = start = time.time()
  post_update(swarming_server, params, None, '', 0)

  logging.info('Executing: %s', task_details.commands[index])
  # TODO(maruel): Support both channels independently and display stderr in red.
  env = None
  if task_details.env:
    env = os.environ.copy()
    env.update(task_details.env)
  proc = subprocess42.Popen(
      task_details.commands[index],
      env=env,
      cwd=root_dir,
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
      stdin=subprocess.PIPE)

  output_chunk_start = 0
  stdout = ''
  exit_code = None
  had_hard_timeout = False
  had_io_timeout = False
  try:
    now = last_io = time.time()
    for _, new_data in proc.yield_any(
          maxsize=MAX_CHUNK_SIZE - len(stdout),
          soft_timeout=calc_yield_wait(task_details, start, last_io, stdout)):
      now = time.time()
      if new_data:
        stdout += new_data
        last_io = now
      elif proc.poll() != None:
        break

      # Post update if necessary.
      if should_post_update(stdout, now, last_packet):
        last_packet = time.time()
        post_update(swarming_server, params, None, stdout, output_chunk_start)
        output_chunk_start += len(stdout)
        stdout = ''

      # Kill on timeout if necessary. Both are failures, not internal_failures.
      # Kill but return 0 so slave_machine.py doesn't cancel the task.
      if not had_hard_timeout and not had_io_timeout:
        if now - last_io > task_details.io_timeout:
          had_io_timeout = True
          logging.warning('I/O timeout')
          proc.kill()
        elif now - start > task_details.hard_timeout:
          had_hard_timeout = True
          logging.warning('Hard timeout')
          proc.kill()

    exit_code = proc.wait()
  finally:
    # Something wrong happened, try to kill the child process.
    if exit_code is None:
      had_hard_timeout = True
      proc.kill()
      # TODO(maruel): We'd wait only for X seconds.
      exit_code = proc.wait()

    # This is the very last packet for this command.
    params['duration'] = time.time() - start
    params['io_timeout'] = had_io_timeout
    params['hard_timeout'] = had_hard_timeout
    # At worst, it'll re-throw.
    post_update(swarming_server, params, exit_code, stdout, output_chunk_start)
    output_chunk_start += len(stdout)
    stdout = ''

  assert not stdout
  return exit_code


def main(args):
  parser = optparse.OptionParser(
      description=sys.modules[__name__].__doc__,
      version=__version__)
  parser.add_option(
      '-f', '--request_file_name',
      help='name of the request file')
  parser.add_option(
      '-S', '--swarming-server', help='Swarming server to send data back')

  options, args = parser.parse_args(args)
  if not options.request_file_name:
    parser.error('You must provide the request file name.')
  if args:
    parser.error('Unknown args: %s' % args)

  on_error.report_on_exception_exit(options.swarming_server)

  remote = url_helper.XsrfRemote(options.swarming_server)
  if not load_and_run(options.request_file_name, remote):
    # This means it's time for the bot to reboot but it's not task_error worthy.
    return RESTART_CODE
  return 0


if __name__ == '__main__':
  logging_utils.prepare_logging('local_test_runner.log')
  # Setup the logger for the console ouput. This will be used by
  # slave_machine.py in case of internal_failure.
  logging_utils.set_console_level(logging.DEBUG)
  sys.exit(main(None))
