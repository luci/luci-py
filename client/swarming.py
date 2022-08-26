#!/usr/bin/env vpython3
# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""Client tool to trigger tasks or retrieve results from a Swarming server."""

# This spec is for the case swarming.py is used via
# https://chromium.googlesource.com/infra/luci/client-py
#
# [VPYTHON:BEGIN]
# wheel: <
#  name: "infra/python/wheels/pyobjc/${vpython_platform}"
#  version: "version:7.3.chromium.1"
#  match_tag: <
#    platform: "macosx_10_10_intel"
#  >
#  match_tag: <
#    platform: "macosx_11_0_arm64"
#  >
# >
# [VPYTHON:END]

__version__ = '1.0'

import json
import logging
import optparse
import os
import sys
import textwrap
import urllib.parse

from utils import tools
tools.force_local_third_party()

# third_party/
import colorama
from depot_tools import fix_encoding
from depot_tools import subcommand

# pylint: disable=ungrouped-imports
import auth
from utils import fs
from utils import logging_utils
from utils import net
from utils import subprocess42


class Failure(Exception):
  """Generic failure."""


### API management.


class APIError(Exception):
  pass


def endpoints_api_discovery_apis(host):
  """Uses Cloud Endpoints' API Discovery Service to returns metadata about all
  the APIs exposed by a host.

  https://developers.google.com/discovery/v1/reference/apis/list
  """
  # Uses the real Cloud Endpoints. This needs to be fixed once the Cloud
  # Endpoints version is turned down.
  data = net.url_read_json(host + '/_ah/api/discovery/v1/apis')
  if data is None:
    raise APIError('Failed to discover APIs on %s' % host)
  out = {}
  for api in data['items']:
    if api['id'] == 'discovery:v1':
      continue
    # URL is of the following form:
    # url = host + (
    #   '/_ah/api/discovery/v1/apis/%s/%s/rest' % (api['id'], api['version'])
    api_data = net.url_read_json(api['discoveryRestUrl'])
    if api_data is None:
      raise APIError('Failed to discover %s on %s' % (api['id'], host))
    out[api['id']] = api_data
  return out


def get_yielder(base_url, limit):
  """Returns the first query and a function that yields following items."""
  CHUNK_SIZE = 250

  url = base_url
  if limit:
    url += '%slimit=%d' % ('&' if '?' in url else '?', min(CHUNK_SIZE, limit))
  data = net.url_read_json(url)
  if data is None:
    # TODO(maruel): Do basic diagnostic.
    raise Failure('Failed to access %s' % url)
  org_cursor = data.pop('cursor', None)
  org_total = len(data.get('items') or [])
  logging.info('get_yielder(%s) returning %d items', base_url, org_total)
  if not org_cursor or not org_total:
    # This is not an iterable resource.
    return data, lambda: []

  def yielder():
    cursor = org_cursor
    total = org_total
    # Some items support cursors. Try to get automatically if cursors are needed
    # by looking at the 'cursor' items.
    while cursor and (not limit or total < limit):
      merge_char = '&' if '?' in base_url else '?'
      url = base_url + '%scursor=%s' % (merge_char, urllib.parse.quote(cursor))
      if limit:
        url += '&limit=%d' % min(CHUNK_SIZE, limit - total)
      new = net.url_read_json(url)
      if new is None:
        raise Failure('Failed to access %s' % url)
      cursor = new.get('cursor')
      new_items = new.get('items')
      nb_items = len(new_items or [])
      total += nb_items
      logging.info('get_yielder(%s) yielding %d items', base_url, nb_items)
      yield new_items

  return data, yielder


### Commands.


@subcommand.usage('[method name]')
def CMDquery(parser, args):
  """Returns raw JSON information via an URL endpoint. Use 'query-list' to
  gather the list of API methods from the server.

  Examples:
    Raw task request and results:
      swarming.py query -S server-url.com task/123456/request
      swarming.py query -S server-url.com task/123456/result

    Listing all bots:
      swarming.py query -S server-url.com bots/list

    Listing last 10 tasks on a specific bot named 'bot1':
      swarming.py query -S server-url.com --limit 10 bot/bot1/tasks

    Listing last 10 tasks with tags os:Ubuntu-14.04 and pool:Chrome. Note that
    quoting is important!:
      swarming.py query -S server-url.com --limit 10 \\
          'tasks/list?tags=os:Ubuntu-14.04&tags=pool:Chrome'
  """
  parser.add_option(
      '-L',
      '--limit',
      type='int',
      default=200,
      help='Limit to enforce on limitless items (like number of tasks); '
      'default=%default')
  parser.add_option(
      '--json', help='Path to JSON output file (otherwise prints to stdout)')
  parser.add_option(
      '--progress',
      action='store_true',
      help='Prints a dot at each request to show progress')
  options, args = parser.parse_args(args)
  if len(args) != 1:
    parser.error(
        'Must specify only method name and optionally query args properly '
        'escaped.')
  base_url = options.swarming + '/_ah/api/swarming/v1/' + args[0]
  try:
    data, yielder = get_yielder(base_url, options.limit)
    for items in yielder():
      if items:
        data['items'].extend(items)
      if options.progress:
        sys.stderr.write('.')
        sys.stderr.flush()
  except Failure as e:
    sys.stderr.write('\n%s\n' % e)
    return 1
  if options.progress:
    sys.stderr.write('\n')
    sys.stderr.flush()
  if options.json:
    options.json = os.path.abspath(options.json)
    tools.write_json(options.json, data, True)
  else:
    try:
      tools.write_json(sys.stdout, data, False)
      sys.stdout.write('\n')
    except IOError:
      pass
  return 0


def CMDquery_list(parser, args):
  """Returns list of all the Swarming APIs that can be used with command
  'query'.
  """
  parser.add_option(
      '--json', help='Path to JSON output file (otherwise prints to stdout)')
  options, args = parser.parse_args(args)
  if args:
    parser.error('No argument allowed.')

  try:
    apis = endpoints_api_discovery_apis(options.swarming)
  except APIError as e:
    parser.error(str(e))
  if options.json:
    options.json = os.path.abspath(options.json)
    with fs.open(options.json, 'wb') as f:
      json.dump(apis, f)
  else:
    help_url = (
        'https://apis-explorer.appspot.com/apis-explorer/?base=%s/_ah/api#p/' %
        options.swarming)
    for i, (api_id, api) in enumerate(sorted(apis.items())):
      if i:
        print('')
      print(api_id)
      print('  ' + api['description'].strip())
      if 'resources' in api:
        # Old.
        # TODO(maruel): Remove.
        # pylint: disable=too-many-nested-blocks
        for j, (resource_name,
                resource) in enumerate(sorted(api['resources'].items())):
          if j:
            print('')
          for method_name, method in sorted(resource['methods'].items()):
            # Only list the GET ones.
            if method['httpMethod'] != 'GET':
              continue
            print('- %s.%s: %s' % (resource_name, method_name, method['path']))
            print('\n'.join('  ' + l for l in textwrap.wrap(
                method.get('description', 'No description'), 78)))
            print('  %s%s%s' % (help_url, api['servicePath'], method['id']))
      else:
        # New.
        for method_name, method in sorted(api['methods'].items()):
          # Only list the GET ones.
          if method['httpMethod'] != 'GET':
            continue
          print('- %s: %s' % (method['id'], method['path']))
          print('\n'.join(
              '  ' + l for l in textwrap.wrap(method['description'], 78)))
          print('  %s%s%s' % (help_url, api['servicePath'], method['id']))
  return 0


class OptionParserSwarming(logging_utils.OptionParserWithLogging):

  def __init__(self, **kwargs):
    logging_utils.OptionParserWithLogging.__init__(
        self, prog='swarming.py', **kwargs)
    self.server_group = optparse.OptionGroup(self, 'Server')
    self.server_group.add_option(
        '-S',
        '--swarming',
        metavar='URL',
        default=os.environ.get('SWARMING_SERVER', ''),
        help='Swarming server to use')
    self.add_option_group(self.server_group)
    auth.add_auth_options(self)

  def parse_args(self, *args, **kwargs):
    options, args = logging_utils.OptionParserWithLogging.parse_args(
        self, *args, **kwargs)
    auth.process_auth_options(self, options)
    user = self._process_swarming(options)
    if hasattr(options, 'user') and not options.user:
      options.user = user
    return options, args

  def _process_swarming(self, options):
    """Processes the --swarming option and aborts if not specified.

    Returns the identity as determined by the server.
    """
    if not options.swarming:
      self.error('--swarming is required.')
    try:
      options.swarming = net.fix_url(options.swarming)
    except ValueError as e:
      self.error('--swarming %s' % e)

    try:
      user = auth.ensure_logged_in(options.swarming)
    except ValueError as e:
      self.error(str(e))
    return user


def main(args):
  dispatcher = subcommand.CommandDispatcher(__name__)
  return dispatcher.execute(OptionParserSwarming(version=__version__), args)


if __name__ == '__main__':
  subprocess42.inhibit_os_error_reporting()
  fix_encoding.fix_encoding()
  tools.disable_buffering()
  colorama.init()
  net.set_user_agent('swarming.py/' + __version__)
  sys.exit(main(sys.argv[1:]))
