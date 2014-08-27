#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Swarming bot.

This is the program that communicates with the Swarming server, ensures the code
is always up to date and executes a child process to run tasks and upload
results back.

It manages self-update and rebooting the host in case of problems.
"""

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
import os_utilities
import url_helper
import zipped_archive
from utils import on_error


# Path to this file or the zip containing this file.
THIS_FILE = os.path.abspath(zipped_archive.get_main_script_path())

# Root directory containing this file or the zip containing this file.
ROOT_DIR = os.path.dirname(THIS_FILE)


def get_attributes():
  """Returns start_slave.py's get_attributes() dict."""
  # Importing this administrator provided script could have side-effects on
  # startup. That is why it is imported late.
  import start_slave
  return start_slave.get_attributes()


def get_attributes_failsafe():
  """Returns fail-safe default attributes."""
  try:
    hostname = os_utilities.get_hostname_short()
  except Exception as e:
    hostname = str(e)
  try:
    ip = os_utilities.get_ip()
  except Exception as e:
    ip = str(e)

  return {
    'dimensions': {},
    'id': hostname,
    'ip': ip,
  }


def post_error(remote, attributes, error):
  """Posts given string as a failure.

  This is used in case of internal code error, and this causes the bot to become
  quarantined. https://code.google.com/p/swarming/issues/detail?id=115

  Arguments:
    remote: An XrsfRemote instance.
    attributes: This bot's attributes.
    error: String representing the problem.
  """
  logging.error('Error: %s\n%s', attributes, error)
  data = {
    'id': attributes['id'],
    'message': error,
  }
  return remote.url_read_json('/swarming/api/v1/bot/error', data=data)


def post_error_task(remote, attributes, error, task_id):
  """Posts given error as failure cause for the task.

  This is used in case of internal code error, and this causes the task to
  become BOT_DIED.

  Arguments:
    remote: An XrsfRemote instance.
    attributes: This bot's attributes.
    error: String representing the problem.
    task_id: Task that had an internal error. When the Swarming server sends
        commands to a slave machine, even though they could be completely wrong,
        the server assumes the job as running. Thus this function acts as the
        exception handler for incoming commands from the Swarming server. If for
        any reason the local test runner script can not be run successfully,
        this function is invoked.
  """
  # TODO(maruel): It could be good to send a signal when the task hadn't started
  # at all. In this case the server could retry the task even if it doesn't have
  # 'idempotent' set. See
  # https://code.google.com/p/swarming/issues/detail?id=108.
  logging.error('Error: %s\n%s', attributes, error)
  data = {
    'id': attributes.get('id'),
    'message': error,
    'task_id': task_id,
  }
  return remote.url_read_json('/swarming/api/v1/bot/task_error', data=data)


def run_bot(remote, error):
  """Runs the bot until it reboots or self-update."""
  # TODO(maruel): This should be part of the 'health check' and the bot
  # shouldn't allow itself to upgrade in this condition.
  # https://code.google.com/p/swarming/issues/detail?id=112
  # Catch all exceptions here so the bot doesn't die on startup, which is
  # annoying to recover. In that case, we set a special property to catch these
  # and help the admin fix the swarming_bot code more quickly.
  attributes = {}
  try:
    # If zipped_archive.generate_version() fails, we still want the server to do
    # the /server_ping before calculating the attributes.
    attributes['version'] = zipped_archive.generate_version()
  except Exception as e:
    error = str(e)

  try:
    # First thing is to get an arbitrary url. This also ensures the network is
    # up and running, which is necessary before trying to get the FQDN below.
    remote.url_read('/server_ping')
  except Exception as e:
    # url_read() already traps pretty much every exceptions. This except clause
    # is kept there "just in case".
    error = str(e)

  try:
    # The fully qualified domain name will uniquely identify this machine to the
    # server, so we can use it to give a deterministic id for this slave. Also
    # store as lower case, since it is already case-insensitive.
    attributes.update(get_attributes())
  except Exception as e:
    attributes.update(get_attributes_failsafe())
    error = str(e)

  logging.info('Attributes: %s', attributes)

  if error:
    post_error(remote, attributes, 'Startup failure: %s' % error)

  # Handshake to get an XSRF token.
  remote.xsrf_request_params = {'attributes': attributes.copy()}
  remote.refresh_token()

  consecutive_sleeps = 0
  while True:
    try:
      consecutive_sleeps = poll_server(remote, attributes, consecutive_sleeps)
    except Exception as e:
      post_error(remote, attributes, str(e))
      consecutive_sleeps = 0


def poll_server(remote, attributes, consecutive_sleeps):
  """Polls the server to run one loop."""
  data = {
    'attributes': attributes,
    'sleep_streak': str(consecutive_sleeps),
  }
  resp = remote.url_read_json('/swarming/api/v1/bot/poll', data=data)
  logging.debug('Server response:\n%s', resp)

  cmd = resp['cmd']
  if cmd == 'sleep':
    time.sleep(resp['duration'])
    consecutive_sleeps += 1

  elif cmd == 'run':
    run_manifest(remote, attributes, resp['manifest'])
    consecutive_sleeps = 0

  elif cmd == 'update':
    update_bot(remote, resp['version'])
    consecutive_sleeps = 0

  else:
    raise ValueError('Unexpected command: %s\n%s' % (cmd, resp))
  return consecutive_sleeps


def run_manifest(remote, attributes, manifest):
  """Defers to local_test_runner.py."""
  # Ensure the manifest is valid. This can throw a json decoding error. Also
  # raise if it is empty.
  if not manifest:
    raise ValueError('Empty manifest')

  # Necessary to signal an internal_failure. This occurs when local_test_runner
  # fails to execute the command. It is important to note that this data is
  # extracted before any I/O is done, like writting the manifest to disk.
  task_id = manifest['task_id']

  try:
    # We currently do not clean up the 'work' directory now is it compartmented.
    # TODO(maruel): Compartmentation should be done via tag. It is important to
    # not be too aggressive about deletion because running a task with a warm
    # cache has important performance benefit.
    # https://code.google.com/p/swarming/issues/detail?id=149
    if not os.path.isdir('work'):
      os.makedirs('work')

    path = os.path.join('work', 'test_run.json')
    with open(path, 'wb') as f:
      f.write(json.dumps(manifest))
    command = [
      sys.executable, THIS_FILE, 'local_test_runner',
      '-S', remote.url,
      '-f', path,
    ]
    logging.debug('Running command: %s', command)
    proc = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=ROOT_DIR)
    out = proc.communicate()[0]

    if proc.returncode:
      # TODO(maruel): manifest/task number.
      raise ValueError('Execution failed, internal error:\n%s', out)

    # At this point the script called by subprocess has handled any further
    # communication with the swarming server.
    logging.debug('done!')
  except Exception as e:
    # Cancel task_id with internal_failure.
    post_error_task(remote, attributes, str(e), task_id)


def update_bot(remote, version):
  """Downloads the new version of the bot code and then runs it.

  Use alterning files; first load swarming_bot.1.zip, then swarming_bot.2.zip,
  never touching swarming_bot.zip which was the originally bootstrapped file.

  TODO(maruel): Create LKGBC:
  https://code.google.com/p/swarming/issues/detail?id=112
  """
  # Alternate between .1.zip and .2.zip.
  new_zip = 'swarming_bot.1.zip'
  if os.path.basename(THIS_FILE) == new_zip:
    new_zip = 'swarming_bot.2.zip'

  # Download as a new file.
  url = remote.url + '/get_slave_code/%s' % version
  if not url_helper.DownloadFile(new_zip, url):
    raise Exception('Unable to download %s from %s.' % (new_zip, url))

  logging.info('Restarting to %s.', new_zip)
  sys.stdout.flush()
  sys.stderr.flush()

  cmd = [sys.executable, new_zip, 'start_slave', '--survive']
  if sys.platform in ('cygwin', 'win32'):
    # (Tentative) It is expected that subprocess.Popen() behaves a tad better
    # on Windows than os.exec*(), which has to be emulated since there's no OS
    # provided implementation. This means processes will accumulate as the bot
    # is restarted, which could be a problem long term.
    subprocess.call(cmd)
  else:
    # On OSX, launchd will be unhappy if we quit so the old code bot process
    # has to outlive the new code child process. Launchd really wants the main
    # process to survive, and it'll restart it if it disapears. os.exec*()
    # replaces the process so this is fine.
    os.execv(sys.executable, cmd)


def get_config():
  """Returns the data from config.json.

  First try the config.json inside the zip. If not present or not running inside
  swarming_bot.zip, use the one beside the file.
  """
  if THIS_FILE.endswith('.zip'):
    with zipfile.ZipFile(THIS_FILE, 'r') as f:
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

  config = get_config()
  server = config['server']
  on_error.report_on_exception_exit(server)
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
  remote = url_helper.XsrfRemote(server, '/swarming/api/v1/bot/handshake')
  # run_bot() normally doesn't return, except in unit tests, so keep the return
  # statement here.
  return run_bot(remote, error)


if __name__ == '__main__':
  logging_utils.prepare_logging('swarming_bot.log')
  os.chdir(ROOT_DIR)
  sys.exit(main(None))
