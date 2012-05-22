#!/usr/bin/python2.4
#
# Copyright 2010 Google Inc. All Rights Reserved.
"""A simple command line script/helper function to download files from a URL.

As a command line script, you specify the local file path and the remote url
as command line arguments.

When used as an imported helper function, both the local file path and url
are passed as function arguments.

When executed, this function simply download the content at the given url and
saves it in the local file.

If the local file already exists, it will simply be overwritten.

If an exception is raised by urllib2.urlopen, read, open, write or close,
the error will be logged and the exception raised again. We currently look for
and log urllib2.URLError & IOError, we let the others through.

"""



import logging
from os import path
import urllib
import urllib2


def DownloadFile(local_file, url):
  """Downloads the data from the given url and saves it in the local_file.

  Args:
    local_file: Where to save the data downloaded from the url.
    url: Where to fetch the data from.

  Raises:
    Any exception that would have been raised by calls to open, write, close.
  """

  if not path.isabs(local_file):
    local_file = path.abspath(local_file)

  # Don't quote ':' or '/' as that will break the url format.
  # Don't quote '%' because it can be handled as is and could
  # represent an element that has already been quoted.
  url = urllib.quote(url, ':/%')

  try:
    url_stream = None
    try:
      url_stream = urllib2.urlopen(urllib2.Request(url))
      url_data = url_stream.read()
    except (urllib2.URLError, IOError), e:
      logging.exception('Failed to read from %s\n%s', url, e)
      raise
  finally:
    if url_stream:
      url_stream.close()

  try:
    file_stream = None
    try:
      file_stream = open(local_file, 'wb')
      file_stream.write(url_data)
    except IOError, e:
      logging.exception('Failed to write to %s\n%s', local_file, e)
      raise
  finally:
    if file_stream:
      file_stream.close()


def main():
  """The main entry point when this file is used as command line script."""
  # Here so that it isn't imported for nothing if we are imported as a module.
  # pylint: disable-msg=C6204
  import optparse
  # pylint: enable-msg=C6204
  parser = optparse.OptionParser()
  parser.add_option('-f', '--file', dest='local_file',
                    help='Local file where to download the data.')
  parser.add_option('-u', '--url', dest='url',
                    help='url of the file to download.')

  (options, args) = parser.parse_args()
  if not options.local_file:
    parser.error('You must provide a local file path.')
  if not options.url:
    parser.error('You must provide the url of the file to download.')
  if args:
    print 'Ignoring unknow args:', args

  DownloadFile(options.local_file.strip(), options.url.strip())

if __name__ == '__main__':
  # Change the argument to logging.DEBUG for more info when debugging.
  logging.getLogger().setLevel(logging.ERROR)
  main()
