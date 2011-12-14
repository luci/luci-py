#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""A simple script printing lines to test the XMLRPC server."""

import optparse
import sys
import time

parser = optparse.OptionParser()
parser.add_option('-n', '--num_lines', dest='num_lines', default=100, type=int,
                  help='The number of lines to print. Defaults to 100.')
parser.add_option('-t', '--sleep_time', dest='sleep_time', default=5,
                  help='The number of seconds to sleep between each loop turn. '
                  'Defaults to 5.', type=float)
parser.add_option('-x', '--exit_code', dest='exit_code', default=0, type=int,
                  help='The exit code to return to the system. Defaults to 0.')

(options, args) = parser.parse_args()

for i in range(options.num_lines):
  print 'line number', i
  time.sleep(options.sleep_time)

sys.exit(options.exit_code)
