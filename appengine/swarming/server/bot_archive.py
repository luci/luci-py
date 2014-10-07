#!/usr/bin/env python
# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Generates the archive for the bot.

The hash of the archive is used to define the current version of the swarm bot
code.
"""

import hashlib
import json
import logging
import os
import StringIO
import sys
import zipfile


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# List of files needed by the swarming bot.
# TODO(maruel): Make the list automatically generated?
FILES = (
    '__main__.py',
    'bot_main.py',
    'common/__init__.py',
    'common/test_request_message.py',
    'logging_utils.py',
    'os_utilities.py',
    'task_runner.py',
    'third_party/__init__.py',
    'third_party/httplib2/__init__.py',
    'third_party/httplib2/cacerts.txt',
    'third_party/httplib2/iri2uri.py',
    'third_party/httplib2/socks.py',
    'third_party/oauth2client/__init__.py',
    'third_party/oauth2client/anyjson.py',
    'third_party/oauth2client/client.py',
    'third_party/oauth2client/clientsecrets.py',
    'third_party/oauth2client/crypt.py',
    'third_party/oauth2client/file.py',
    'third_party/oauth2client/gce.py',
    'third_party/oauth2client/keyring_storage.py',
    'third_party/oauth2client/locked_file.py',
    'third_party/oauth2client/multistore_file.py',
    'third_party/oauth2client/old_run.py',
    'third_party/oauth2client/tools.py',
    'third_party/oauth2client/util.py',
    'third_party/oauth2client/xsrfutil.py',
    'third_party/requests/__init__.py',
    'third_party/requests/adapters.py',
    'third_party/requests/api.py',
    'third_party/requests/auth.py',
    'third_party/requests/certs.py',
    'third_party/requests/compat.py',
    'third_party/requests/cookies.py',
    'third_party/requests/exceptions.py',
    'third_party/requests/hooks.py',
    'third_party/requests/models.py',
    'third_party/requests/packages/__init__.py',
    'third_party/requests/packages/urllib3/__init__.py',
    'third_party/requests/packages/urllib3/_collections.py',
    'third_party/requests/packages/urllib3/connectionpool.py',
    'third_party/requests/packages/urllib3/contrib/__init__.py',
    'third_party/requests/packages/urllib3/contrib/ntlmpool.py',
    'third_party/requests/packages/urllib3/contrib/pyopenssl.py',
    'third_party/requests/packages/urllib3/exceptions.py',
    'third_party/requests/packages/urllib3/fields.py',
    'third_party/requests/packages/urllib3/filepost.py',
    'third_party/requests/packages/urllib3/packages/__init__.py',
    'third_party/requests/packages/urllib3/packages/ordered_dict.py',
    'third_party/requests/packages/urllib3/packages/six.py',
    'third_party/requests/packages/urllib3/packages/ssl_match_hostname/'
        '__init__.py',
    'third_party/requests/packages/urllib3/poolmanager.py',
    'third_party/requests/packages/urllib3/request.py',
    'third_party/requests/packages/urllib3/response.py',
    'third_party/requests/packages/urllib3/util.py',
    'third_party/requests/sessions.py',
    'third_party/requests/status_codes.py',
    'third_party/requests/structures.py',
    'third_party/requests/utils.py',
    'utils/__init__.py',
    'utils/cacert.pem',
    'utils/net.py',
    'utils/oauth.py',
    'utils/on_error.py',
    'utils/subprocess42.py',
    'utils/tools.py',
    'utils/zip_package.py',
    'xsrf_client.py',
)


def yield_swarming_bot_files(root_dir, host, additionals):
  """Yields all the files to map as tuple(filename, content).

  config.json is injected with json data about the server.
  """
  items = {i: None for i in FILES}
  items.update(additionals)
  items['config.json'] = json.dumps({'server': host.rstrip('/')})
  for item, content in sorted(items.iteritems()):
    if content is not None:
      yield item, content
    else:
      with open(os.path.join(root_dir, item), 'rb') as f:
        yield item, f.read()


def get_swarming_bot_zip(root_dir, host, additionals):
  """Returns a zipped file of all the files a slave needs to run.

  Arguments:
    root_dir: directory swarming_bot.
    additionals: dict(filepath: content) of additional items to put into the zip
        file, in addition to FILES and MAPPED. In practice, it's going to be a
        custom bot_config.py.
  Returns:
    A string representing the zipped file's contents.
  """
  zip_memory_file = StringIO.StringIO()
  # TODO(maruel): Use client/utils/zipped_archive.py.
  with zipfile.ZipFile(zip_memory_file, 'w', zipfile.ZIP_DEFLATED) as zip_file:
    for item, content in yield_swarming_bot_files(root_dir, host, additionals):
      zip_file.writestr(item, content)

  data = zip_memory_file.getvalue()
  logging.info('get_swarming_bot_zip(%s) is %d bytes', additionals, len(data))
  return data


def get_swarming_bot_version(root_dir, host, additionals):
  """Returns the SHA1 hash of the slave code, representing the version.

  Arguments:
    root_dir: directory swarming_bot.
    additionals: See get_swarming_bot_zip's doc.

  Returns:
    The SHA1 hash of the slave code.
  """
  result = hashlib.sha1()
  try:
    # TODO(maruel): Deduplicate from zip_package.genereate_version().
    for item, content in yield_swarming_bot_files(root_dir, host, additionals):
      result.update(item)
      result.update('\x00')
      result.update(content)
      result.update('\x00')
  except IOError:
    logging.warning('Missing expected file. Hash will be invalid.')
  out = result.hexdigest()
  logging.info('get_swarming_bot_version(%s) = %s', sorted(additionals), out)
  return out


def main():
  if len(sys.argv) > 1:
    print >> sys.stderr, (
        'This script creates a swarming_bot.zip file locally in the server '
        'directory. This script doesn\'t accept any argument.')
    return 1

  with open(os.path.join(ROOT_DIR, 'swarming_bot', 'config.json'), 'rb') as f:
    config = json.load(f) or {}
  expected = ['server']
  actual = sorted(config)
  if expected != actual:
    print >> sys.stderr, 'Only expected keys \'%s\', got \'%s\'' % (
    ','.join(expected), ','.join(actual))
  swarming_bot_dir = os.path.join(ROOT_DIR, 'swarming_bot')

  zip_file = os.path.join(ROOT_DIR, 'swarming_bot.zip')
  with open(os.path.join(swarming_bot_dir, 'bot_config.py'), 'rb') as f:
    additionals = {'bot_config.py': f.read()}
  with open(zip_file, 'wb') as f:
    f.write(
        get_swarming_bot_zip(swarming_bot_dir, config['server'], additionals))
  return 0


if __name__ == '__main__':
  sys.exit(main())
