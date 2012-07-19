#/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""A helper script for wrapping url calls."""





import logging
import math
import random
import time
import urllib
import urllib2


# pylint: disable-msg=W0102
def UrlOpen(url, data={}, max_tries=1, wait_duration=None):
  """Attempts to open the given url multiple times.

  Args:
    url: The url to open.
    data: The unencoded data to send to the url. This must be a mapping object
        or a sequence of two-element tuples.
    max_tries: The maximum number of times to try sending this data. Must be
        greater than 0.
    wait_duration: The number of seconds to wait between successive attempts.
        This must be greater than or equal to 0. If no value is given then a
        random value between 0.1 and 10 will be chosen each time (with
        exponential back off to give later retries a longer wait).

  Returns:
    The reponse from the url contacted. If it failed to connect or is given
    invalid arguments, then it returns None.
  """

  if max_tries <= 0:
    logging.error('Invalid number of tries, %d, passed in.', max_tries)
    return None

  if wait_duration and wait_duration < 0:
    logging.error('Invalid wait duration, %d, passed in.', wait_duration)
    return None

  url_response = None
  encoded_data = urllib.urlencode(data)
  for attempt in range(max_tries):
    try:
      # Simply specifying data to urlopen makes it a POST.
      url_response = urllib2.urlopen(url, encoded_data).read()
    except urllib2.HTTPError as e:
      # An HTTPError means we reached the server, so don't retry.
      logging.error('Able to connect to %s but an exception was thrown.\n%s',
                    url, e)
      return None
    except urllib2.URLError as e:
      logging.info(
          'Unable to open url %s on attempt %d.\nException: %s',
          url, attempt, e)

      if wait_duration is None:
        duration = random.random() * 3 + math.pow(1.5, (attempt + 1))
        duration = min(10, max(0.1, duration))
      else:
        duration = wait_duration

      time.sleep(duration)

    if url_response is not None:
      return url_response

  logging.error('Unable to open given url, %s, after %d attempts.',
                url, max_tries)
  return None
