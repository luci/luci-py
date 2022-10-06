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

import json
import logging
import optparse
import os
import signal
import sys
import time
import traceback

from api import os_utilities
from bot_code import bot_auth
from bot_code import remote_client
from libs import luci_context
from utils import file_path
from utils import net
from utils import on_error
from utils import subprocess42
from utils import zip_package

# Path to this file or the zip containing this file.
THIS_FILE = os.path.abspath(zip_package.get_main_script_path())


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

  Use -u to disable stdout buffering by python.

  Mocked in test to point to the real run_isolated.py script.
  """
  return [sys.executable, '-u', THIS_FILE, 'run_isolated']


def get_isolated_args(work_dir, task_details, isolated_result,
                      bot_file, run_isolated_flags):
  """Returns the command to call run_isolated. Mocked in tests."""
  bot_dir = os.path.dirname(work_dir)
  if os.path.isfile(isolated_result):
    os.remove(isolated_result)
  cmd = []

  # CAS options.
  if task_details.cas_input_root:
    input_root = task_details.cas_input_root
    cmd.extend([
        '--cas-instance',
        input_root['cas_instance'],
        '--cas-digest',
        '%s/%s' %
        (input_root['digest']['hash'], input_root['digest']['size_bytes']),
    ])

  # Named caches options.
  # Specify --named-cache-root unconditionally so run_isolated.py never creates
  # "named_caches" dir and always operats in "c" dir.
  cmd.extend(['--named-cache-root', os.path.join(bot_dir, 'c')])
  if task_details.caches:
    for c in task_details.caches:
      cmd.extend([
          '--named-cache',
          c['name'],
          c['path'].replace('/', os.sep),
          c['hint'],
      ])

  # Expected output files:
  for output in task_details.outputs:
    cmd.extend(['--output', output])

  # CIPD options. Empty 'packages' list is fine. It means the task needs
  # a bootstrapped CIPD client only.
  if task_details.cipd_input:
    for pkg in task_details.cipd_input.get('packages', []):
      cmd.extend([
          '--cipd-package',
          '%s:%s:%s' % (pkg['path'], pkg['package_name'], pkg['version'])
      ])
    cmd.extend([
        '--cipd-cache',
        os.path.join(bot_dir, 'cipd_cache'),
        '--cipd-client-package',
        task_details.cipd_input['client_package']['package_name'],
        '--cipd-client-version',
        task_details.cipd_input['client_package']['version'],
        '--cipd-server',
        task_details.cipd_input.get('server'),
    ])

  cmd.extend([
      # Switch to 'task' logical account, if it is set.
      '--switch-to-account',
      'task',
      '--json',
      isolated_result,
      '--log-file',
      os.path.join(bot_dir, 'logs', 'run_isolated.log'),
      '--root-dir',
      work_dir,
  ])
  if bot_file:
    cmd.extend(('--bot-file', bot_file))

  if task_details.relative_cwd:
    cmd.extend(('--relative-cwd', task_details.relative_cwd))

  if task_details.hard_timeout:
    cmd.extend(('--hard-timeout', str(task_details.hard_timeout)))
  if task_details.grace_period:
    cmd.extend(('--grace-period', str(task_details.grace_period)))

  for key, value in (task_details.env or {}).items():
    cmd.extend(('--env', '%s=%s' % (key, value)))

  cmd.extend(task_details.containment.flags())
  cmd.extend(run_isolated_flags)

  for key, values in task_details.env_prefixes.items():
    for v in values:
      cmd.extend(('--env-prefix', '%s=%s' % (key, v)))

  # TODO(nodir): Pass the command line arguments via a response file.
  cmd.append('--')
  cmd.extend(task_details.command)
  return cmd


class Containment(object):
  """Containment details."""
  _EXPECTED = frozenset((
      'containment_type',
  ))

  def __init__(self, data):
    if set(data) != self._EXPECTED:
      raise InternalError(
          'Unexpected keys: %s != %s' % (sorted(data), sorted(self._EXPECTED)))
    self.containment_type = data['containment_type']

  def flags(self):
    """Returns flags to use for run_isolated.py."""
    out = []
    if self.containment_type != 'NONE':
      pass
    return out


class TaskDetails(object):
  """A task_runner specific view of the server's TaskProperties.

  It only contains what the bot needs to know.
  """
  _EXPECTED = frozenset((
      'bot_authenticated_as',
      'bot_dimensions',
      'bot_id',
      'caches',
      'cas_input_root',
      'cipd_input',
      'command',
      'containment',
      'dimensions',
      'env',
      'env_prefixes',
      'grace_period',
      'hard_timeout',
      'host',
      'io_timeout',
      'outputs',
      'realm',
      'relative_cwd',
      'resultdb',
      'secret_bytes',
      'service_accounts',
      'task_id',
  ))

  def __init__(self, data):
    logging.info('TaskDetails(%s)', data)
    if not isinstance(data, dict):
      raise InternalError('Expected dict in task_runner_in.json, got %r' % data)
    if set(data) != self._EXPECTED:
      raise InternalError(
          'Unexpected keys: %s != %s' % (sorted(data), sorted(self._EXPECTED)))

    # Get all the data first so it fails early if the task details is invalid.
    self.bot_dimensions = data['bot_dimensions']
    self.bot_id = data['bot_id']

    # Raw command.
    self.command = data['command'] or []
    self.relative_cwd = data['relative_cwd']
    self.cas_input_root = data['cas_input_root']
    self.cipd_input = data['cipd_input']
    self.caches = data['caches']
    self.env = data['env']
    self.env_prefixes = data.get('env_prefixes') or {}
    self.grace_period = data['grace_period']
    self.hard_timeout = data['hard_timeout']
    self.io_timeout = data['io_timeout']
    self.task_id = data['task_id']
    self.outputs = data['outputs']
    self.secret_bytes = data['secret_bytes']
    self.resultdb = data['resultdb']
    self.realm = data['realm']
    self.containment = Containment(data['containment'])

  @staticmethod
  def load(path):
    """Loads the TaskDetails from a file on disk (specified via --in-file).

    Raises InternalError if the file can't be read or parsed.
    """
    try:
      with open(path, 'rb') as f:
        return TaskDetails(json.load(f))
    except (IOError, ValueError) as e:
      raise InternalError('Cannot load task_runner_in.json: %s' % e)


class ExitSignal(Exception):
  """Raised on a signal that the process must exit immediately."""

  def __init__(self, sig):
    super(ExitSignal, self).__init__('task_runner received signal %s' % sig)
    self.signal = sig


class InternalError(Exception):
  """Raised on unrecoverable errors that abort task with 'internal error'."""


def load_and_run(in_file, swarming_server, cost_usd_hour, start, out_file,
                 run_isolated_flags, bot_file, auth_params_file):
  """Loads the task's metadata, prepares auth environment and executes the task.

  This may throw all sorts of exceptions in case of failure. It's up to the
  caller to trap them. These shall be considered 'internal_failure' instead of
  'failure' from a TaskRunResult standpoint.
  """
  auth_system = None
  local_auth_context = None
  task_result = None
  work_dir = os.path.dirname(out_file)

  def handler(sig, _):
    logging.info('Got signal %s', sig)
    raise ExitSignal(sig)

  try:
    with subprocess42.set_signal_handler([SIG_BREAK_OR_TERM], handler):
      # The work directory is guaranteed to exist since it was created by
      # bot_main.py and contains the manifest. Temporary files will be
      # downloaded there. It's bot_main.py that will delete the directory
      # afterward. Tests are not run from there.
      if not os.path.isdir(work_dir):
        raise InternalError('%s expected to exist' % work_dir)

      # Raises InternalError on errors.
      task_details = TaskDetails.load(in_file)

      # This will start a thread that occasionally reads bot authentication
      # headers from 'auth_params_file'. It will also optionally launch local
      # HTTP server that serves OAuth tokens to the task processes. We put
      # location of this service into a file referenced by LUCI_CONTEXT env var
      # below.
      if auth_params_file:
        try:
          auth_system = bot_auth.AuthSystem(auth_params_file)
          local_auth_context = auth_system.start()
        except bot_auth.AuthSystemError as e:
          raise InternalError('Failed to init auth: %s' % e)
      else:
        logging.warning(
            'Disabling auth subsystem, --auth-params-file wasn\'t provided')

      # Override LUCI_CONTEXT['local_auth']. If the task is not using auth,
      # do NOT inherit existing local_auth (if its there). Kick it out by
      # passing None.
      context_edits = {
        'local_auth': local_auth_context,
        'deadline': {
          # Ironically, "hard_timeout" is actually the soft timeout.
          'soft_deadline': monotonic_time() + task_details.hard_timeout,
          'grace_period': task_details.grace_period,
        },
      }

      # Override LUCI_CONTEXT['swarming'].
      bot_dimensions = []
      for k, vs in task_details.bot_dimensions.items():
        bot_dimensions.extend('%s:%s' % (k, v) for v in vs)
      bot_dimensions.sort()
      swarming = {
          'task': {
              'server': swarming_server,
              # Uses the task_id instead of run_id in the context.
              'task_id': task_details.task_id[:-1] + '0',
              'bot_dimensions': bot_dimensions,
          },
      }
      if task_details.secret_bytes is not None:
        swarming['secret_bytes'] = task_details.secret_bytes
      context_edits['swarming'] = swarming

      # Extend existing LUCI_CONTEXT['realm'], if any.
      if task_details.realm is not None:
        realm = luci_context.read('realm') or {}
        realm.update(task_details.realm)
        context_edits['realm'] = realm

      # Extend existing LUCI_CONTEXT['resultdb'], if any.
      if task_details.resultdb is not None:
        resultdb = luci_context.read('resultdb') or {}
        resultdb.update(task_details.resultdb)
        context_edits['resultdb'] = resultdb


      # Returns bot authentication headers dict or raises InternalError.
      def headers_cb():
        try:
          if auth_system:
            return auth_system.get_bot_headers()
          return (None, None) # A timeout of "None" means "don't use auth"
        except bot_auth.AuthSystemError as e:
          raise InternalError('Failed to grab bot auth headers: %s' % e)

      # The hostname and work dir provided here don't really matter, since the
      # task runner is always called with a specific versioned URL.
      remote = remote_client.createRemoteClient(
          swarming_server, headers_cb, os_utilities.get_hostname_short(),
          work_dir)
      remote.initialize()
      remote.bot_id = task_details.bot_id

      # Let AuthSystem know it can now send RPCs to Swarming (to grab OAuth
      # tokens). There's a circular dependency here! AuthSystem will be
      # indirectly relying on its own 'get_bot_headers' method to authenticate
      # RPCs it sends through the provided client.
      if auth_system:
        auth_system.set_remote_client(remote)

      # Auth environment is up, start the command. task_result is dumped to
      # disk in 'finally' block.
      with luci_context.stage(_tmpdir=work_dir, **context_edits) as ctx_file:
        task_result = run_command(
            remote, task_details, work_dir, cost_usd_hour,
            start, run_isolated_flags, bot_file, ctx_file)
  except (ExitSignal, InternalError, remote_client.InternalError) as e:
    # This normally means run_command() didn't get the chance to run, as it
    # itself traps exceptions and will report accordingly. In this case, we want
    # the parent process to send the message instead.
    logging.exception('Exception caught in run_command().')
    if not task_result:
      task_result = {
          'exit_code': -1,
          'hard_timeout': False,
          'io_timeout': False,
          'must_signal_internal_failure': str(e) or 'unknown error',
          'version': OUT_VERSION,
      }
  finally:
    # We've found tests to delete the working directory work_dir when quitting,
    # causing an exception here. Try to recreate the directory if necessary.
    if not os.path.isdir(work_dir):
      os.mkdir(work_dir)
    if auth_system:
      auth_system.stop()
    with open(out_file, 'w') as f:
      json.dump(task_result, f)


def kill_and_wait(proc, grace_period, reason):
  logging.warning('SIGTERM finally due to %s', reason)
  proc.terminate()
  try:
    proc.wait(grace_period)
  except subprocess42.TimeoutExpired:
    logging.warning('SIGKILL finally due to %s', reason)
    proc.kill()
  exit_code = proc.wait()
  logging.info('Process exited with: %d', exit_code)
  return exit_code


def fail_without_command(remote, task_id, params, cost_usd_hour, task_start,
                         exit_code, stdout):
  now = monotonic_time()
  params['cost_usd'] = cost_usd_hour * (now - task_start) / 60. / 60.
  params['duration'] = now - task_start
  params['io_timeout'] = False
  params['hard_timeout'] = False
  # Ignore server reply to stop.
  remote.post_task_update(task_id, params, (stdout, 0), 1)
  return {
      'exit_code': exit_code,
      'hard_timeout': False,
      'io_timeout': False,
      'must_signal_internal_failure': None,
      'version': OUT_VERSION,
  }


class _FailureOnStart(Exception):
  """Process run_isolated couldn't be started."""
  def __init__(self, exit_code, stdout):
    super(_FailureOnStart, self).__init__(stdout)
    self.exit_code = exit_code
    self.stdout = stdout


def _start_task_runner(args, work_dir, ctx_file):
  """Starts task_runner process and returns its handle.

  Raises:
    _FailureOnStart if a problem occurred.
  """
  cmd = get_run_isolated()
  args_path = os.path.join(work_dir, 'run_isolated_args.json')
  cmd.extend(['-a', args_path])
  env = os.environ.copy()
  if ctx_file:
    env['LUCI_CONTEXT'] = ctx_file
  logging.info('cmd=%s', cmd)
  logging.info('cwd=%s', work_dir)
  logging.info('args=%s', args)

  # We write args to a file since there may be more of them than the OS
  # can handle. Since it is inside work_dir, it'll be automatically be deleted
  # upon task termination.
  try:
    with open(args_path, 'w') as f:
      json.dump(args, f)
  except (IOError, OSError) as e:
    raise _FailureOnStart(
        e.errno or -1,
        'Could not write args to %s: %s' % (args_path, e))

  try:
    # TODO(maruel): Support separate streams for stdout and stderr.
    proc = subprocess42.Popen(
        cmd,
        env=env,
        cwd=work_dir,
        detached=True,
        stdout=subprocess42.PIPE,
        stderr=subprocess42.STDOUT,
        stdin=subprocess42.PIPE)
  except OSError as e:
    raise _FailureOnStart(
        e.errno or -1,
        'Command "%s" failed to start.\nError: %s' % (' '.join(cmd), e))
  return proc


class _OutputBuffer(object):
  """_OutputBuffer implements stdout (and eventually stderr) buffering.

  This data is buffered and must be sent to the Swarming server when
  self.should_post_update() is True.
  """
  # To be mocked in tests.
  _MIN_PACKET_INTERVAL = 4
  _MAX_PACKET_INTERVAL = 10

  def __init__(self, task_details, start):
    self._task_details = task_details
    self._start = start
    # Sends a maximum of 250kb of stdout per task_update packet.
    self._max_chunk_size = 250000
    # Minimum wait between task_update packet when there's output.
    self._min_packet_interval = self._MIN_PACKET_INTERVAL
    # Maximum wait between task_update packet when there's no output.
    self._max_packet_interval = self._MAX_PACKET_INTERVAL

    # Mutable:
    # Buffered data to send to the server.
    self._stdout = b''
    # Offset at which the buffered data shall be sent to the server.
    self._output_chunk_start = 0
    # Last time proc.yield_any() yielded.
    self._last_loop = monotonic_time()
    # Last time proc.yield_any() yielded data.
    self._last_io = self._last_loop
    # Last time data was poped and (we assume) sent to the server.
    self._last_pop = self._last_loop

  @property
  def last_loop(self):
    return self._last_loop

  @property
  def since_last_io(self):
    """Returns the number of seconds since the last stdout/stderr data was
    received from the child process.

    This is used to implement I/O timeout.
    """
    return self._last_loop - self._last_io

  def add(self, _channel, data):
    """Buffers more data, that will eventually be sent to the server."""
    self._last_loop = monotonic_time()
    if data:
      self._last_io = self._last_loop
      self._stdout += data

  def pop(self):
    """Pops the buffered data to send it to the server."""
    o = self._output_chunk_start
    s = self._stdout
    self._output_chunk_start += len(self._stdout)
    self._stdout = b''
    self._last_pop = monotonic_time()
    return (s, o)

  def maxsize(self):
    """Returns the maximum number of bytes proc.yield_any() can return."""
    return self._max_chunk_size - len(self._stdout)

  def should_post_update(self):
    """Returns True if it's time to send a task_update packet via post_update().

    Sends a packet when one of this condition is met:
    - more than self._max_chunk_size of stdout is buffered.
    - self._last_pop was more than "self._min_packet_interval seconds ago"
      and there was stdout.
    - self._last_pop was more than "self._max_packet_interval seconds ago".
    """
    packet_interval = (
        self._min_packet_interval
        if self._stdout else self._max_packet_interval)
    return (
        len(self._stdout) >= self._max_chunk_size or
        (self._last_loop - self._last_pop) > packet_interval)

  def calc_yield_wait(self, timed_out):
    """Calculates the maximum number of seconds to wait in yield_any().

    This is necessary as task_runner must send keep-alive to the server to tell
    it that it is not hung, even if the subprocess doesn't output any data.
    """
    if timed_out:
      # Give a |grace_period| seconds delay.
      if self._task_details.grace_period:
        return max(
            self._last_loop - timed_out - self._task_details.grace_period, 0.)
      return 0.

    out = (
        self._min_packet_interval if self._stdout
        else self._max_packet_interval)
    now = monotonic_time()
    if self._task_details.hard_timeout:
      out = min(out, self._start + self._task_details.hard_timeout - now)
    if self._task_details.io_timeout:
      out = min(out, self._last_loop + self._task_details.io_timeout - now)
    return max(out, 0)


def run_command(remote, task_details, work_dir, cost_usd_hour,
                task_start, run_isolated_flags, bot_file, ctx_file):
  """Runs a command and sends packets to the server to stream results back.

  Implements both I/O and hard timeouts. Sends the packets numbered, so the
  server can ensure they are processed in order.

  Returns:
    Metadata dict with the execution result.

  Raises:
    ExitSignal if caught some signal when starting or stopping.
    InternalError on unexpected internal errors.
  """
  # Signal the command is about to be started. It is important to post a task
  # update *BEFORE* starting any user code to signify the server that the bot
  # correctly started processing the task. In the case of non-idempotent task,
  # this signal is used to know if it is safe to retry the task or not. See
  # _reap_task() in task_scheduler.py for more information.
  start = monotonic_time()
  params = {
      'cost_usd': cost_usd_hour * (start - task_start) / 60. / 60.,
  }
  if not remote.post_task_update(task_details.task_id, params):
    # Don't even bother, the task was already canceled.
    logging.debug('Task has been already canceled. Won\'t start it. '
                  'task_id: %s. Sending update...', task_details.task_id)
    # crbug.com/1052208:
    # Send task update again for the server to know that the task has stopped.
    # Sending 'canceled' signal to the server for the task to be 'CANCELED'
    # instead of 'KILLED'.
    params['canceled'] = True
    remote.post_task_update(task_details.task_id, params, exit_code=-1)
    return {
        'exit_code': -1,
        'hard_timeout': False,
        'io_timeout': False,
        'must_signal_internal_failure': None,
        'version': OUT_VERSION,
    }

  isolated_result = os.path.join(work_dir, 'isolated_result.json')
  args = get_isolated_args(work_dir, task_details,
                           isolated_result, bot_file, run_isolated_flags)
  # Hard timeout enforcement is deferred to run_isolated. Grace is doubled to
  # give one 'grace_period' slot to the child process and one slot to upload
  # the results back.
  task_details.hard_timeout = 0
  if task_details.grace_period:
    task_details.grace_period *= 2

  try:
    proc = _start_task_runner(args, work_dir, ctx_file)
    logging.info('Subprocess for run_isolated started')
  except _FailureOnStart as e:
    return fail_without_command(remote, task_details.task_id, params,
                                cost_usd_hour, task_start, e.exit_code,
                                e.stdout)

  buf = _OutputBuffer(task_details, start)
  try:
    # Monitor the task
    exit_code = None
    had_io_timeout = False
    must_signal_internal_failure = None
    missing_cas = []
    missing_cipd = []
    term_sent = False
    kill_sent = False
    timed_out = None
    try:
      for channel, new_data in proc.yield_any(
          maxsize=buf.maxsize, timeout=lambda: buf.calc_yield_wait(timed_out)):
        buf.add(channel, new_data)

        # Post update if necessary.
        if buf.should_post_update():
          params['cost_usd'] = (
              cost_usd_hour * (monotonic_time() - task_start) / 60. / 60.)
          if not remote.post_task_update(task_details.task_id, params,
                                         buf.pop()):
            logging.debug('Server induced stop; kill_sent: %s, term_sent: %s',
                          kill_sent, term_sent)
            # Server is telling us to stop. Normally task cancellation.
            if not kill_sent and not term_sent:
              logging.warning('Server induced stop; sending SIGTERM')
              term_sent = True
              proc.terminate()
              timed_out = monotonic_time()

        # Send signal on timeout if necessary. Both are failures, not
        # internal_failures.
        # Eventually kill but return 0 so bot_main.py doesn't cancel the task.
        if not timed_out:
          if (task_details.io_timeout and
              buf.since_last_io > task_details.io_timeout):
            had_io_timeout = True
            if not term_sent:
              logging.warning(
                  'I/O timeout is %.3fs; no update for %.3fs sending SIGTERM',
                  task_details.io_timeout, buf.since_last_io)
              proc.terminate()
              timed_out = monotonic_time()
        else:
          # During grace period.
          if (not kill_sent and
              buf.last_loop - timed_out >= task_details.grace_period):
            # Now kill for real. The user can distinguish between the following
            # states:
            # - signal but process exited within grace period,
            #   (hard_|io_)_timed_out will be set but the process exit code will
            #   be script provided.
            # - processed exited late, exit code will be -9 on posix.
            logging.warning(
                'Grace of %.3fs exhausted at %.3fs; sending SIGKILL',
                task_details.grace_period, buf.last_loop - timed_out)
            proc.kill()
            kill_sent = True
      logging.info('Waiting for process exit')
      exit_code = proc.wait()

      # the process group / job object may be dangling so if we didn't kill
      # it already, give it a poke now.
      if not kill_sent:
        logging.info('got exit code %d, but kill whole process group',
                     exit_code)
        proc.kill()
    except (
        ExitSignal, InternalError, IOError,
        OSError, remote_client.InternalError) as e:
      logging.exception('got some exception, killing process')
      # Something wrong happened, try to kill the child process.
      must_signal_internal_failure = str(e) or 'unknown error'
      exit_code = kill_and_wait(proc, task_details.grace_period, str(e))

    logging.info('Subprocess for run_isolated was completed or killed')

    # This is the very last packet for this command. If it was a task with CAS,
    # include the output reference to the archived CAS digest.
    now = monotonic_time()
    params['cost_usd'] = cost_usd_hour * (now - task_start) / 60. / 60.
    duration = now - start
    params['duration'] = duration
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
        missing_cas = run_isolated_result.get('missing_cas', [])
        missing_cipd = run_isolated_result.get('missing_cipd', [])
        if missing_cipd or missing_cas:
          must_signal_internal_failure = run_isolated_result['internal_failure']
        else:
          if run_isolated_result['cas_output_root']:
            params['cas_output_root'] = run_isolated_result['cas_output_root']
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
            # Store the real task duration as measured by run_isolated and
            # calculate the total overhead.
            params['duration'] = run_isolated_result['duration']
            params['bot_overhead'] = duration - run_isolated_result['duration']
            if params['bot_overhead'] < 0:
              params['bot_overhead'] = 0

          run_isolated_stats = run_isolated_result.get('stats', {})
          isolated_stats = run_isolated_stats.get('isolated')
          if isolated_stats:
            params['isolated_stats'] = isolated_stats
          cipd_stats = run_isolated_stats.get('cipd')
          if cipd_stats:
            params['cipd_stats'] = cipd_stats
          cipd_pins = run_isolated_result.get('cipd_pins')
          if cipd_pins:
            params['cipd_pins'] = cipd_pins
          named_caches_stats = run_isolated_stats.get('named_caches')
          if named_caches_stats:
            params['named_caches_stats'] = named_caches_stats
          cache_trim_stats = run_isolated_stats.get('trim_caches')
          if cache_trim_stats:
            params['cache_trim_stats'] = cache_trim_stats
          cleanup_stats = run_isolated_stats.get('cleanup')
          if cleanup_stats:
            params['cleanup_stats'] = cleanup_stats
    except (IOError, OSError, ValueError) as e:
      logging.error('Swallowing error: %s', e)
      if not must_signal_internal_failure:
        must_signal_internal_failure = '%s\n%s' % (
            e, traceback.format_exc()[-2048:])

    # If no exit code has been set, something went wrong with run_isolated.py.
    # Set exit code to -1 to indicate a generic error occurred.
    if exit_code is None:
      exit_code = -1
    params['hard_timeout'] = had_hard_timeout

    # Ignore server reply to stop. Also ignore internal errors here if we are
    # already handling some.
    try:
      if must_signal_internal_failure:
        # We need to update the task and then send task error. However, we
        # should *not* send the exit_code since doing so would cause the task
        # to be marked as COMPLETED until the subsequent post_task_error call
        # finished, which would cause any query made between these two calls to
        # get the wrong task status. We also clear out the duration and various
        # stats as the server prints errors if either are set in this case.
        # TODO(sethkoehler): Come up with some way to still send the exit_code
        # (and thus also duration/stats) without marking the task COMPLETED.
        logging.debug('must_signal_internal_failure: True. '
                      'Resetting exit_code and params.')
        exit_code = None
        params.pop('duration', None)
        params.pop('bot_overhead', None)
        params.pop('isolated_stats', None)
        params.pop('cipd_stats', None)
        params.pop('cipd_pins', None)
        params.pop('named_caches_stats', None)
        params.pop('cache_trim_stats', None)
        params.pop('cleanup_stats', None)
      remote.post_task_update(task_details.task_id, params, buf.pop(),
                              exit_code)
      logging.debug('Last task update finished. task_id: %s, exit_code: %s, '
                    'params: %s.', task_details.task_id, exit_code, params)
      if must_signal_internal_failure:
        remote.post_task_error(task_details.task_id,
                               must_signal_internal_failure,
                               missing_cas=missing_cas,
                               missing_cipd=missing_cipd)
        # Clear out this error as we've posted it now (we already cleared out
        # exit_code above). Note: another error could arise after this point,
        # which is fine, since bot_main.py will post it).
        must_signal_internal_failure = ''
    except remote_client.InternalError as e:
      logging.error('Internal error while finishing the task: %s', e)
      if not must_signal_internal_failure:
        must_signal_internal_failure = str(e) or 'unknown error'

    return {
        'exit_code': exit_code,
        'hard_timeout': had_hard_timeout,
        'io_timeout': had_io_timeout,
        'must_signal_internal_failure': must_signal_internal_failure,
        'version': OUT_VERSION,
    }
  finally:
    file_path.try_remove(isolated_result)


def main(args):
  logging.info('Starting task_runner script')
  subprocess42.inhibit_os_error_reporting()

  # Disable magical auto-detection of OAuth config. See main() in bot_main.py
  # for detailed explanation why.
  net.disable_oauth_config()

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
      '--bot-file', help='Path to a file describing the state of the host.')
  parser.add_option(
      '--auth-params-file',
      help='Path to a file with bot authentication parameters')

  options, args = parser.parse_args(args)
  if not options.in_file or not options.out_file:
    parser.error('task_runner is meant to be used by swarming_bot.')

  on_error.report_on_exception_exit(options.swarming_server)

  logging.info('starting')
  now = monotonic_time()
  if options.start > now:
    options.start = now

  try:
    load_and_run(options.in_file, options.swarming_server,
                 options.cost_usd_hour, options.start, options.out_file, args,
                 options.bot_file, options.auth_params_file)
    return 0
  finally:
    logging.info('quitting')
