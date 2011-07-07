#!/usr/bin/python2.6
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Sends test requests to the TRS."""

import json
import optparse
import os.path
import sys
import time
import urllib2


DESCRIPTION = """This script sends a test request to a TRS server.  The request
is taken from a file as specified on the command line and must be formatted
as explained in http://code.google.com/p/swarming/wiki/SwarmFileFormat.

If no filename is specified, or if - is specified as the filename, standard
input is used to read the request.

The results of the test will be visible on the TRS web interface.
"""


def main():
  parser = optparse.OptionParser(usage='%prog [options] [filename]',
                                 description=DESCRIPTION)
  parser.add_option('-w', '--wait', dest='wait_for_results',
                    action='store_true',
                    help='Wait for all test to complete and print their output')
  parser.add_option('-t', '--sleep_time', dest='sleep_time', type='int',
                    default=60, help='The time, in seconds, to wait between '
                    'each poll. Defaults to 60 seconds.')
  parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
                    help='Print verbose logging')
  parser.add_option('-n', '--hostname', dest='hostname', default='localhost',
                    help='Specify the hostname of the Swarm server. Optional. '
                    'Defaults to Localhost')
  parser.add_option('-p', '--port', dest='port', type='int', default=8080,
                    help='Specify the port of the Swarm server. Optional. '
                    'Defaults to 8080')

  (options, args) = parser.parse_args()
  if not args:
    args.append('-')
  elif len(args) > 1:
    parser.error('Must specify only one filename')

  # Build the URL for sending the request.
  base_url = 'http://%s:%d' % (options.hostname, options.port)
  test_url = base_url + '/test'
  filename = args[0]

  # Open the specified file, or stdin.
  f = sys.stdin
  if filename != '-':
    f = open(filename)

  output = None
  if options.verbose:
    print 'Sending %s to %s' % (os.path.basename(filename), test_url)
  try:
    output = urllib2.urlopen(test_url, data=f.read()).read()
  except urllib2.URLError, ex:
    print 'Error: %s' % str(ex)
    return 1

  # Check that we can read the output as a JSON string
  try:
    test_keys = json.loads(output)
  except (ValueError, TypeError), e:
    print 'Request failed:'
    print output
    return 1

  if options.verbose:
    print ('Test case: %s sucessfully sent %s tests to these configurations:' %
           (test_keys['test_case_name'], len(test_keys['test_keys'])))
  running_test_keys = []
  for test_key in test_keys['test_keys']:
    running_test_keys.append(test_key)
    if options.verbose:
      print 'Config: %s, index: %s/%s, test key: %s' % (
          test_key['config_name'], int(test_key['instance_index']) + 1,
          test_key['num_instances'], test_key['test_key'])

  if options.wait_for_results:
    test_result_output = ''
    test_all_succeeded = True
    while running_test_keys:
      for running_test_key in running_test_keys[:]:
        try:
          key_url = '%s/get_result?r=%s' % (base_url,
                                            running_test_key['test_key'])
          output = urllib2.urlopen(key_url).read()
          if output:
            if options.verbose:
              test_result_output = (
                  '%s\n=======================\nConfig: %s\n%s' %
                  (test_result_output, running_test_key['config_name'], output))
            if test_all_succeeded and '0 FAILED TESTS' not in output:
              test_all_succeeded = False
            running_test_keys.remove(running_test_key)
            if options.verbose:
              print 'Test done for %s' % running_test_key['config_name']
          else:
            if options.verbose:
              print running_test_key['config_name'] + ' isn\'t done yet'
        except urllib2.HTTPError, e:
          print 'Calling %s threw %s' % (key_url, e)
      time.sleep(options.sleep_time)
    if options.verbose:
      print test_result_output
      print '======================='
    if test_all_succeeded:
      print 'All test succeeded'
      return 0
    else:
      print 'At least one test failed'
      return 42

if __name__ == '__main__':
  sys.exit(main())
