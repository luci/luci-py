#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""A command line script to start the server and setup logging."""




import logging
import optparse
import os.path
import subprocess
import sys
from twisted.python import logfile


def main():
  parser = optparse.OptionParser()
  parser.add_option('-p', '--logfiles_path', dest='logfiles_path', default='.',
                    help='The absolute path where the logfiles will be saved. '
                    'Optional. Defaults to current working folder.')
  parser.add_option('-f', '--logfiles_basename', dest='logfiles_basename',
                    help='The base for the name of the logfiles. Optional. '
                    'Defaults to swarm_logs.', default='swarm_logs')
  parser.add_option('-n', '--num_logfiles', dest='num_logfiles', type=int,
                    help='The number of log files to ring through. '
                    'Optional. Defaults to 10.', default=10)
  parser.add_option('-a', '--appengine', dest='appengine_cmds',
                    action='append', help='The command(s) to start the '
                    'AppEngine launcher. The --skip_sdk_update_check '
                    'argument will be added to the command(s) you specify.')
  parser.add_option('-s', '--swarm', dest='swarm_path', default='.',
                    help='The root path of the Swarm server code.'
                    'Optional. Defaults to the current working folder.')
  parser.add_option('-o', '--swarm_server_start_timeout', default=60,
                    dest='swarm_server_start_timeout', type=int,
                    help='How long should we wait (in seconds) for the '
                    'Swarm server to start? Optional. Defaults to 60 seconds.')
  parser.add_option('-v', '--verbose', action='store_true',
                    help='Set logging level to DEBUG. Optional. Defaults to '
                    'ERROR level.', default=False)

  (options, args) = parser.parse_args()
  if not options.appengine_cmds:
    parser.print_help()
    parser.error('You must specify the command(s) to start the '
                 'AppEngine launcher.')

  if (not os.path.isdir(options.logfiles_path) or
      not os.path.exists(options.logfiles_path)):
    parser.print_help()
    parser.error('%s Doesn\'t exist!\nYou must specify an existing folder for '
                 'the logfiles_path.' % options.logfiles_path)

  if options.verbose:
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.getLogger().setLevel(logging.ERROR)

  if args:
    logging.warning('These unknown arguments will be ignored: %s', args)

  logging.debug('Creating LogFile object under: %s', options.logfiles_path)
  log_file = logfile.LogFile(directory=options.logfiles_path,
                             name=options.logfiles_basename,
                             rotateLength=10000000)

  options.appengine_cmds.extend(['--skip_sdk_update_check',
                                 options.swarm_path])
  logging.debug('Starting execution of command: %s',
                ', '.join(options.appengine_cmds))
  app_engine_process = subprocess.Popen(options.appengine_cmds,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
  while not app_engine_process.poll():
    line = app_engine_process.stdout.readline()
    if line:
      print line,
      log_file.write(line)
  logging.debug('Returning code %d.', app_engine_process.returncode)
  return app_engine_process.returncode


if __name__ == '__main__':
  sys.exit(main())
