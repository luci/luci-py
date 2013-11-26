#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Sends test results to the TRS.

This is a useful debugging tool to fake sending results to the TRS.
"""

import optparse
import sys
import urllib
import urllib2


def main():
  parser = optparse.OptionParser(usage='%prog [options] [filename]')
  parser.add_option('-k', '--key', dest='key',
                    help='Specify the key for the result.')
  parser.add_option('-r', '--result', dest='result',
                    help='The [optional] result to be posted.')
  parser.add_option('-u', '--url', dest='url',
                    help='The [optional] url of the server (without '
                    'the /result?k=).\nDefaults to http://swarm.hot:8080.')

  (options, _) = parser.parse_args()
  if not options.key:
    print 'You must provide a test runner key!'
    return 1

  if not options.url:
    options.url = 'http://swarm.hot:8080'

  # Build the URL for sending the request.
  url = '%s/result?k=%s' % (options.url, options.key)
  if not options.result:
    options.result = 'Posted from post_results.py'

  try:
    urllib2.urlopen(url, urllib.urlencode((('s', False),
                                           ('r', options.result))))
  except urllib2.URLError as ex:
    print 'Error: %s' % str(ex)
    return 1

  print 'Sucessfully sent "%s"\nTo: %s.' % (options.result, url)


if __name__ == '__main__':
  sys.exit(main())
