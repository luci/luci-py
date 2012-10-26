#!/usr/bin/python2.7
#
# Copyright 2010 Google Inc. All Rights Reserved.

"""Sends test run to a remote machine."""




import logging
import optparse
import os.path
import sys

from common import test_request_message
from test_runner import remote_test_runner


DESCRIPTION = """This script sends a test run to a remote machine.  The test run
is taken from a file as specified on the command line and must be formatted
as explained in http://code.google.com/p/swarming/wiki/SwarmFileFormat.

The remote machine must also be specified by providing a URL that has access to
the xmlrpc server that the remote test runner needs to use to send tests
to the machine. For example: http://<ip_address>:7399 for a hive instance.

You can also specify that you want to wait and capture the output of the tests
to display them in the stdout of this script.
"""


def ParseRequestFile(request_file_name):
  """Parse and validate the given request file and store the result test_run.

  Args:
    request_file_name: The name of the request file to parse and validate.

  Returns:
    True if the parsed request file was validated, False othewise.
  """
  try:
    request_file = None
    try:
      request_file = open(request_file_name, 'r')
      request_data = request_file.read()
    except IOError as e:
      logging.exception('Failed to open file %s.\nException: %s',
                        request_file_name, e)
      return False
  finally:
    if request_file:
      request_file.close()
  try:
    test_run = test_request_message.TestRun()
    errors = []
    if not test_run.ParseTestRequestMessageText(request_data, errors):
      logging.error('Errors while parsing text file: %s', errors)
      return False
  except test_request_message.Error as e:
    logging.exception('Failed to evaluate %s\'s file content.\nException: %s',
                      request_file_name, e)
    return False
  return True


def main():
  parser = optparse.OptionParser(usage='%prog [options] [filename]',
                                 description=DESCRIPTION)
  parser.add_option('-s', '--server', dest='url',
                    help='The URL of the machine to run tests on.')
  parser.add_option('-w', '--wait_and_capture', dest='wait_and_capture',
                    action='store_true', help=
                    'Specify if you want to wait for the commands to '
                    'complete and capture their output. Defaults to no.')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to INFO. Optional. Defaults to '
                    'ERROR level.')
  parser.set_default('wait_and_capture', False)

  (options, args) = parser.parse_args()
  if not options.url:
    parser.error('You must specify the URL of the machine to run tests on.')
  if not args:
    parser.error('You must specify at least one filename.')
  elif len(args) > 1:
    parser.error('You must specify only one filename.')

  if options.verbose:
    logging.getLogger().setLevel(logging.INFO)
  else:
    logging.getLogger().setLevel(logging.ERROR)

  # Validate the text file before trying to send it to the server.
  test_run_local_filename = args[0]
  if not ParseRequestFile(test_run_local_filename):
    return 1

  test_run_dest_filename = os.path.basename(test_run_local_filename)
  dest_runner_script = 'local_test_runner.py'
  local_runner_script = os.path.join('test_runner', dest_runner_script)
  downloader_script = os.path.join('test_runner', 'downloader.py')
  trm_script = os.path.join('common', 'test_request_message.py')
  test_runner_init = os.path.join('test_runner', '__init__.py')
  common_init = os.path.join('common', '__init__.py')
  file_pairs_to_upload = [(test_run_local_filename, test_run_dest_filename),
                          (local_runner_script, dest_runner_script),
                          (downloader_script, downloader_script),
                          (trm_script, trm_script),
                          (test_runner_init, test_runner_init),
                          (common_init, common_init)]

  remote_root = 'c:\\tests'
  command_to_execute = ['c:\\python26\\python.exe',
                        '%s\\local_test_runner.py' % remote_root,
                        '-f', '%s\\%s' % (remote_root,
                                          test_run_dest_filename)]
  if options.verbose:
    command_to_execute.append('-v')
  logging.info('Creating the remote test runner with URL: %s', options.url)
  test_runner = remote_test_runner.RemoteTestRunner(
      server_url=options.url,
      remote_root=remote_root,
      file_pairs_to_upload=file_pairs_to_upload,
      commands_to_execute=[command_to_execute])
  if test_runner.UploadFiles():
    run_results = test_runner.RunCommands(options.wait_and_capture)
    if not run_results:
      logging.error('Failed to run commands')
      return 1
    elif options.wait_and_capture:
      logging.info('RunCommands returned:\n%s',
                   '\n'.join(['%d: %s' % (r[0], r[1]) for r in run_results]))
    else:
      logging.info('RunCommands returned:\n%s',
                   '\n'.join([str(r) for r in run_results]))
  else:
    logging.error('Failed to upload files.')
    return 1
  return 0


if __name__ == '__main__':
  sys.exit(main())
