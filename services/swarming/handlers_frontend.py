# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Main entry point for Swarming service.

This file contains the URL handlers for all the Swarming service URLs,
implemented using the webapp2 framework.
"""

import collections
import json
import logging
import time
import urllib

import webapp2

from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.api import modules
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

import handlers_backend
import handlers_common
import template
from common import rpc
from common import swarm_constants
from common import test_request_message
from components import auth
from components import auth_ui
from components import decorators
from components import ereporter2
from components import utils
from server import bot_management
from server import errors
from server import file_chunks
from server import result_helper
from server import stats
from server import stats_gviz
from server import task_common
from server import task_request
from server import task_result
from server import task_scheduler
from server import task_to_run
from server import user_manager


ACCEPTABLE_TASKS_SORTS = {
  'created_ts': 'Created',
  'done_ts': 'Ended',
  'modified_ts': 'Last updated',
  'name': 'Name',
  'user': 'User',
}


def GetModulesVersions():
  """Returns the current versions on the instance.

  TODO(maruel): Move in components/.
  """
  return [('default', i) for i in modules.get_versions()]


# Helper class for displaying the sort options in html templates.
SortOptions = collections.namedtuple('SortOptions', ['key', 'name'])


def ipv4_to_int(ip):
  values = [int(i) for i in ip.split('.')]
  factor = 256
  value = 0L
  for i in values:
    value = value * factor + i
  return value


def int_to_ipv4(integer):
  values = []
  factor = 256
  for _ in range(4):
    values.append(integer % factor)
    integer = integer / factor
  return '.'.join(str(i) for i in reversed(values))


def expand_subnet(ip, mask):
  """Returns all the IP addressed comprised in a range."""
  if mask == 32:
    return [ip]
  bit = 1 << (32 - mask)
  return [int_to_ipv4(ipv4_to_int(ip) + r) for r in range(bit)]


def ip_whitelist_authentication(request):
  """Check to see if the request is from a whitelisted machine.

  Will use the remote machine's IP.

  Args:
    request: WebAPP request sent by remote machine.

  Returns:
    auth.Identity of a machine if IP is whitelisted, None otherwise.
  """
  assert request.remote_addr
  is_whitelisted = user_manager.IsWhitelistedMachine(request.remote_addr)

  # IP v6 addresses contain ':' that is not allowed in identity name.
  if is_whitelisted:
    return auth.Identity(
        auth.IDENTITY_BOT, request.remote_addr.replace(':', '-'))

  # Log the error.
  error = errors.SwarmError(
      name='Authentication Failure', message='Handler: %s' % request.url,
      info='Remote machine address: %s' % request.remote_addr)
  error.put()

  return None


def convert_test_case(data):
  """Constructs a TaskProperties out of a test_request_message.TestCase.

  This code is kept for compatibility with the previous API. See make_request()
  for more details.

  Plan of attack:
  - Create new bot API.
  - Convert swarm_bot to use the new versioned API.
  - Deploy servers.
  - Delete old bot code.
  - Create new client API.
  - Deploy servers.
  - Switch client code to use new API.
  - Roll client code into chromium.
  - Wait 1 month.
  - Remove old client code API.
  """
  test_case = test_request_message.TestCase.FromJSON(data)
  # TODO(maruel): Add missing mapping and delete obsolete ones.
  assert len(test_case.configurations) == 1, test_case.configurations
  config = test_case.configurations[0]

  if test_case.tests:
    execution_timeout_secs = int(round(test_case.tests[0].hard_time_out))
    io_timeout_secs = int(round(test_case.tests[0].io_time_out))
  else:
    execution_timeout_secs = 2*60*60
    io_timeout_secs = 60*60

  # Ignore all the settings that are deprecated.
  return {
    'name': test_case.test_case_name,
    'user': test_case.requestor,
    'properties': {
      'commands': [c.action for c in test_case.tests],
      'data': test_case.data,
      'dimensions': config.dimensions,
      'env': test_case.env_vars,
      'execution_timeout_secs': execution_timeout_secs,
      'io_timeout_secs': io_timeout_secs,
    },
    'priority': config.priority,
    'scheduling_expiration_secs': config.deadline_to_run,
  }


def request_work_item(attributes, server_url, remote_addr):
  # TODO(maruel): Split it out a little.
  attribs = bot_management.validate_and_fix_attributes(attributes)
  response = bot_management.check_version(attributes, server_url)
  if response:
    return response

  dimensions = attribs['dimensions']
  bot_id = attribs['id'] or dimensions['hostname']
  # Note its existence at two places, one for stats at 1 minute resolution, the
  # other for the list of known bots.
  stats.add_entry(action='bot_active', bot_id=bot_id, dimensions=dimensions)

  # The TaskRunResult will be referenced on the first ping, ensuring that the
  # task was actually taken.
  bot_management.tag_bot_seen(
      bot_id,
      dimensions.get('hostname', bot_id),
      attribs['ip'],
      remote_addr,
      dimensions,
      attribs['version'])

  request, run_result = task_scheduler.bot_reap_task(dimensions, bot_id)
  if not request:
    try_count = attribs['try_count'] + 1
    return {
      'come_back': task_scheduler.exponential_backoff(try_count),
      'try_count': try_count,
    }

  assert bot_id == run_result.bot_id
  packed = task_common.pack_run_result_key(run_result.key)
  test_objects = [
    test_request_message.TestObject(
        test_name=str(i),
        action=command,
        hard_time_out=request.properties.execution_timeout_secs,
        io_time_out=request.properties.io_timeout_secs)
    for i, command in enumerate(request.properties.commands)
  ]
  result_url = '%s/result?r=%s&id=%s' % (server_url, packed, bot_id)
  ping_url = '%s/runner_ping?r=%s&id=%s' % (server_url, packed, bot_id)
  # Ask the slave to ping every 55 seconds. The main reason is to make sure that
  # the majority of the time, the stats of active shards has a ping in every
  # minute window.
  # TODO(maruel): Have the slave stream the stdout on the fly, so there won't be
  # need for ping unless there's no stdout. The slave will decide this instead
  # of the master.
  ping_delay = 55
  test_run = test_request_message.TestRun(
      test_run_name=request.name,
      env_vars=request.properties.env,
      configuration=test_request_message.TestConfiguration(
          config_name=request.name),
      result_url=result_url,
      ping_url=ping_url,
      ping_delay=ping_delay,
      data=request.properties.data,
      tests=test_objects)

  content = test_request_message.Stringize(test_run, json_readable=True)
  # The Swarming bot uses an hand rolled RPC system and 'commands' is actual the
  # custom RPC commands.
  return {
    'commands': [rpc.BuildRPC('RunManifest', content)],
    'result_url': test_run.result_url,
    'try_count': 0,
  }


### UI code that should be in jinja2 templates


class FilterParams(object):
  def __init__(
      self, status, test_name_filter, show_successfully_completed,
      machine_id_filter):
    self.status = status
    self.test_name_filter = test_name_filter
    self.show_successfully_completed = show_successfully_completed
    self.machine_id_filter = machine_id_filter

  def generate_page_url(
      self, current_page=None, sort_by=None,
      include_filters=False):
    """Generates an URL that points to the current page with similar options.

    If an option is listed as None, don't include it to allow the html page to
    ensure it can add its desired option.

    Args:
      current_page: The current page to display.
      sort_by: The value to sort the runners by.
      include_filters: True if the filters should be included in the url.

    Returns:
      query url page for the requested filtering settings.
    """
    params = {}
    if current_page:
      params['page'] = str(current_page)
    if sort_by:
      params['sort_by'] = sort_by
    if include_filters:
      if self.status is not None:
        params['status'] = self.status
      if self.show_successfully_completed is not None:
        params['show_successfully_completed'] = str(
            self.show_successfully_completed)
      if self.test_name_filter is not None:
        params['test_name_filter'] = self.test_name_filter
      if self.machine_id_filter is not None:
        params['machine_id_filter'] = self.machine_id_filter
    return '?' + urllib.urlencode(params)

  @staticmethod
  def get_shards(sort_by, ascending, limit, offset):
    """Returns a query with the given parameters, also applying the filters.

    Args:
      sort_by: The value to sort the runners by.
      ascending: True if the runners should be sorted in ascending order.
      limit: The maximum number of runners the query should return.
      offset: Number of queries to skip.

    Returns:
      A ndb.Future that will return the items in the DB with a query that is
      properly adjusted and filtered.
    """
    # TODO(maruel): Use cursors!
    # TODO(maruel): Use self.status, self.show_successfully_completed,
    # self.test_name_filter, self.machine_id_filter.
    assert sort_by in ACCEPTABLE_TASKS_SORTS, (
        'This should have been validated at a higher level')
    opts = ndb.QueryOptions(limit=limit, offset=offset)
    direction = (
        datastore_query.PropertyOrder.ASCENDING
        if ascending else datastore_query.PropertyOrder.DESCENDING)
    return task_result.TaskResultSummary.query(default_options=opts).order(
        datastore_query.PropertyOrder(sort_by, direction)).fetch_async()

  def filter_selects_as_html(self):
    """Generates the HTML filter select values, with the proper defaults set.

    Returns:
      The HTML representing the filter select options.
    """
    # TODO(maruel): Use jinja2 instead.
    html = ('Ran Successfully:'
            '<select name="show_successfully_completed" form="filter">')

    if self.show_successfully_completed:
      html += ('<option value="True" selected="True">Yes</option>'
               '<option value="False">No</option>')
    else:
      html += ('<option value="True">Yes</option>'
               '<option value="False" selected="True">No</option>')

    html += ('</select>'
             'Status:'
             '<select name="status" form="filter">')

    if self.status == 'pending':
      html += ('<option value="all">All</option>'
               '<option value="pending" selected="True">Pending Only</option>'
               '<option value="running">Running Only</option>'
               '<option value="done">Done Only</option>')
    elif self.status == 'running':
      html += ('<option value="all">All</option>'
               '<option value="pending">Pending Only</option>'
               '<option value="running" selected="True">Running Only</option>'
               '<option value="done">Done Only</option>')
    elif self.status == 'done':
      html += ('<option value="all">All</option>'
               '<option value="pending">Pending Only</option>'
               '<option value="running">Running Only</option>'
               '<option value="done" selected="True">Done Only</option>')
    else:
      html += ('<option value="all">All</option>'
               '<option value="pending">Pending Only</option>'
               '<option value="running">Running Only</option>'
               '<option value="done">Done Only</option>')

    html += '</select>'
    return html


### Admin accessible pages.

# TODO(maruel): Sort the handlers once they got their final name.


class RestrictedHandler(auth.AuthenticatingHandler):
  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    self.response.out.write(template.render('restricted.html', {}))


class BotsListHandler(auth.AuthenticatingHandler):
  """Presents the list of known bots."""
  ACCEPTABLE_BOTS_SORTS = {
    'dimensions': 'Dimensions',
    'last_seen': 'Last Seen',
    'hostname': 'Hostname',
    'id': 'ID',
  }

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    sort_by = self.request.get('sort_by', 'id')
    if sort_by not in self.ACCEPTABLE_BOTS_SORTS:
      self.abort(400, 'Invalid sort_by query parameter')

    dead_machine_cutoff = (
        task_common.utcnow() - bot_management.MACHINE_DEATH_TIMEOUT)

    def sort_bot(bot):
      if sort_by == 'id':
        return bot.key.string_id()
      return getattr(bot, sort_by)

    bots = sorted(bot_management.Bot.query().fetch(), key=sort_bot)

    sort_options = [
      SortOptions(k, v)
      for k, v in sorted(self.ACCEPTABLE_BOTS_SORTS.iteritems())
    ]
    params = {
      'bots': bots,
      # TODO(maruel): it should be the default AppEngine url version.
      'current_version':
          bot_management.get_slave_version(self.request.host_url),
      'dead_machine_cutoff': dead_machine_cutoff,
      'now': task_common.utcnow(),
      'selected_sort': sort_by,
      'sort_options': sort_options,
    }
    self.response.out.write(
        template.render('restricted_botslist.html', params))


class Ereporter2ReportHandler(auth.AuthenticatingHandler):
  """Returns all the recent errors as a web page."""

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    """Reports the errors logged and ignored.

    Arguments:
      start: epoch time to start looking at. Defaults to the messages since the
             last email.
      end: epoch time to stop looking at. Defaults to now.
    """
    request_id_url = '/restricted/ereporter2/request/'
    end = int(float(self.request.get('end', 0)) or time.time())
    start = int(
        float(self.request.get('start', 0)) or
        ereporter2.get_default_start_time() or 0)
    module_versions = GetModulesVersions()
    report, ignored = ereporter2.generate_report(
        start, end, module_versions, handlers_common.should_ignore_error_record)
    env = ereporter2.get_template_env(start, end, module_versions)
    content = ereporter2.report_to_html(
        report, ignored,
        ereporter2.REPORT_HEADER_TEMPLATE,
        ereporter2.REPORT_CONTENT_TEMPLATE,
        request_id_url, env)
    out = template.render('ereporter2_report.html', {'content': content})
    self.response.write(out)


class Ereporter2RequestHandler(auth.AuthenticatingHandler):
  """Dumps information about single logged request."""

  @auth.require(auth.READ, 'swarming/management')
  def get(self, request_id):
    # TODO(maruel): Add UI.
    data = ereporter2.log_request_id_to_dict(request_id)
    if not data:
      self.abort(404, detail='Request id was not found.')
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    json.dump(data, self.response, indent=2, sort_keys=True)


class UploadStartSlaveHandler(auth.AuthenticatingHandler):
  """Accept a new start slave script."""

  @auth.require(auth.UPDATE, 'swarming/management')
  def get(self):
    params = {
      'path': self.request.path,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.out.write(
        template.render('restricted_uploadstartslave.html', params))

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    script = self.request.get('script', '')
    if not script:
      self.abort(400, 'No script uploaded')

    bot_management.store_start_slave(script)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('%d bytes stored.' % len(script))


class UploadBootstrapHandler(auth.AuthenticatingHandler):
  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    params = {
      'path': self.request.path,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.out.write(
        template.render('restricted_uploadbootstrap.html', params))

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    script = self.request.get('script', '')
    if not script:
      self.abort(400, 'No script uploaded')

    file_chunks.StoreFile('bootstrap.py', script.encode('utf-8'))
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('%d bytes stored.' % len(script))


class WhitelistIPHandler(auth.AuthenticatingHandler):
  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    display_whitelists = sorted(
        (
          {
            'ip': w.ip,
            'key': w.key.id,
            'url': self.request.path_url,
          } for w in user_manager.MachineWhitelist().query()),
        key=lambda x: x['ip'])

    params = {
      'post_url': self.request.path_url,
      'whitelists': display_whitelists,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.out.write(
        template.render('restricted_whitelistip.html', params))

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    ip = self.request.get('i', self.request.remote_addr)
    mask = 32
    if '/' in ip:
      ip, mask = ip.split('/', 1)
      mask = int(mask)
    ips = expand_subnet(ip, mask)

    add = self.request.get('a')
    if add == 'True':
      for ip in ips:
        user_manager.AddWhitelist(ip)
    elif add == 'False':
      for ip in ips:
        user_manager.DeleteWhitelist(ip)
    else:
      self.abort(400, 'Invalid \'a\' parameter.')
    self.get()


### User accessible pages.


class UserHandler(auth.AuthenticatingHandler):
  # TODO(maruel): We want swarming/user here.
  @auth.require(auth.READ, 'swarming/clients')
  def get(self):
    self.response.out.write(template.render('user.html', {}))


class TasksHandler(auth.AuthenticatingHandler):
  """Lists all requests and allows callers to manage them."""

  def parse_filters(self):
    """Parse the filters from the request."""
    return FilterParams(
      self.request.get('status', 'all'),
      self.request.get('test_name_filter', ''),
      # Compare to 'False' so that the default value for invalid user input
      # is True.
      self.request.get('show_successfully_completed', '') != 'False',
      self.request.get('machine_id_filter', ''))

  # TODO(maruel): We want swarming/user here.
  @auth.require(auth.READ, 'swarming/clients')
  def get(self):
    # TODO(maruel): Convert to Google Graph API.
    # TODO(maruel): Once migration is complete, remove limit and offset, replace
    # with cursor.
    page_length = int(self.request.get('length', 50))
    page = int(self.request.get('page', 1))

    default_sort = 'created_ts'
    sort_by = self.request.get('sort_by', 'D' + default_sort)
    ascending = bool(sort_by[0] == 'A')
    sort_key = sort_by[1:]
    if sort_key not in ACCEPTABLE_TASKS_SORTS:
      self.abort(400, 'Invalid sort key')

    # TODO(maruel): Stop doing HTML in python.
    sorted_by_message = '<p>Currently sorted by: '
    if not ascending:
      sorted_by_message += 'Reverse '
    sorted_by_message += ACCEPTABLE_TASKS_SORTS[sort_key] + '</p>'
    sort_options = []
    # TODO(maruel): Use an order that makes sense instead of whatever the dict
    # happens to be.
    for key, value in ACCEPTABLE_TASKS_SORTS.iteritems():
      # Add 'A' for ascending and 'D' for descending order.
      sort_options.append(SortOptions('A' + key, value))
      sort_options.append(SortOptions('D' + key, 'Reverse ' + value))

    # Parse and load the filters.
    params = self.parse_filters()

    # Fire up all the queries in parallel.
    tasks_future = params.get_shards(
        sort_key,
        ascending=ascending,
        limit=page_length,
        offset=page_length * (page - 1))
    total_task_count_future = task_request.TaskRequest.query().count_async()

    opts = ndb.QueryOptions(limit=10)
    errors_found_future = errors.SwarmError.query(
        default_options=opts).order(-errors.SwarmError.created).fetch_async()

    params = {
      'current_page': page,
      'errors': errors_found_future.get_result(),
      'filter_selects': params.filter_selects_as_html(),
      'machine_id_filter': params.machine_id_filter,
      'now': task_common.utcnow(),
      'selected_sort': ('A' if ascending else 'D') + sort_key,
      'sort_options': sort_options,
      'sort_by': sort_by,
      'sorted_by_message': sorted_by_message,
      'tasks': tasks_future.get_result(),
      'test_name_filter': params.test_name_filter,
      'total_tasks': total_task_count_future.get_result(),
      'url_no_filters': params.generate_page_url(page, sort_by),
      'url_no_page': params.generate_page_url(
          sort_by=sort_by, include_filters=True),
      'url_no_sort_by_or_filters': params.generate_page_url(
          page, include_filters=False),
    }
    # TODO(maruel): If admin or if the user is task's .user, show the Cancel
    # button. Do not show otherwise.
    self.response.out.write(template.render('user_tasks.html', params))


class TaskHandler(auth.AuthenticatingHandler):
  """Show the full text of a test request."""

  # TODO(maruel): We want swarming/user here.
  @auth.require(auth.READ, 'swarming/clients')
  def get(self, key_id):
    key = None
    request_key = None
    try:
      key = task_scheduler.unpack_result_summary_key(key_id)
      request_key = task_result.result_summary_key_to_request_key(key)
    except ValueError:
      try:
        key = task_scheduler.unpack_run_result_key(key_id)
        request_key = task_result.result_summary_key_to_request_key(
            task_result.run_result_key_to_result_summary_key(key))
      except ValueError:
        self.abort(404, 'Invalid key format.')

    # It can be either a TaskRunResult or TaskResultSummary.
    result, request = ndb.get_multi([key, request_key])
    if not result:
      self.abort(404, 'Invalid key.')

    bot = (
      bot_management.get_bot_key(result.bot_id).get()
      if result.bot_id else None)
    params = {
      'bot': bot,
      'now': task_common.utcnow(),
      'request': request,
      'task': result,
    }
    self.response.out.write(template.render('user_task.html', params))


### Client APIs.


class ApiBots(auth.AuthenticatingHandler):
  """Returns the list of known swarming bots."""

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    params = {
        'machine_death_timeout':
            int(bot_management.MACHINE_DEATH_TIMEOUT.total_seconds()),
        'machines': sorted(m.to_dict() for m in bot_management.Bot.query()),
    }
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.headers['Cache-Control'] = 'no-cache, no-store'
    self.response.write(utils.encode_to_json(params))


class DeleteMachineStatsHandler(auth.AuthenticatingHandler):
  """Handler to delete a machine assignment."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/clients')
  def post(self):
    bot_key = bot_management.get_bot_key(self.request.get('r', ''))
    if bot_key.get():
      bot_key.delete()
      self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
      self.response.out.write('Machine Assignment removed.')
    else:
      self.response.set_status(204)


class TestRequestHandler(auth.AuthenticatingHandler):
  """Handles test requests from clients."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/clients')
  def post(self):
    # Validate the request.
    if not self.request.get('request'):
      self.abort(400, 'No request parameter found.')

    # TODO(vadimsh): Store identity of a user that posted the request.
    test_case = self.request.get('request')
    try:
      request_properties = convert_test_case(test_case)
    except test_request_message.Error as e:
      message = str(e)
      logging.error(message)
      self.abort(400, message)

    _, result_summary = task_scheduler.make_request(request_properties)
    out = {
      'test_case_name': result_summary.name,
      'test_keys': [
        {
          'config_name': result_summary.name,
          # TODO(maruel): Remove this.
          'instance_index': 0,
          'num_instances': 1,
          'test_key': task_common.pack_result_summary_key(result_summary.key),
        }
      ],
    }
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.out.write(json.dumps(out))


class GetMatchingTestCasesHandler(auth.AuthenticatingHandler):
  """Get all the keys for any test runners that match a given test case name."""

  @auth.require(auth.READ, 'swarming/clients')
  def get(self):
    """Returns a list of TaskResultSummary ndb.Key."""
    test_case_name = self.request.get('name', '')
    q = task_result.TaskResultSummary.query().filter(
        task_result.TaskResultSummary.name == test_case_name)
    # Returns all the relevant task_ids.
    keys = [
      task_common.pack_result_summary_key(result_summary)
      for result_summary in q.iter(keys_only=True)
    ]
    logging.info('Found %d keys', len(keys))
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    if keys:
      self.response.write(utils.encode_to_json(keys))
    else:
      # TODO(maruel): This is semantically incorrect if you think about this API
      # as a search API.
      self.response.set_status(404)
      self.response.write('[]')


# TODO(vadimsh): Remove once final ACLs structure is in place.
class SecureGetResultHandler(auth.AuthenticatingHandler):
  """Show the full result string from a test runner."""

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    SendRunnerResults(self.response, self.request.get('r', ''))


class GetResultHandler(auth.AuthenticatingHandler):
  """Show the full result string from a test runner."""

  @auth.require(auth.READ, 'swarming/clients')
  def get(self):
    SendRunnerResults(self.response, self.request.get('r', ''))


def SendRunnerResults(response, key_id):
  """Sends the results of the runner specified by key.

  Args:
    response: Response to be sent to remote machine.
    key: Key identifying the runner.
  """
  # TODO(maruel): Formalize returning data for a specific try or the overall
  # results.
  key = None
  try:
    key = task_scheduler.unpack_result_summary_key(key_id)
  except ValueError:
    try:
      key = task_scheduler.unpack_run_result_key(key_id)
    except ValueError:
      response.set_status(400)
      response.out.write('Invalid key')
      return

  result = key.get()
  if not result:
    # TODO(maruel): Use 404 if not present.
    response.set_status(204)
    logging.info('Unable to provide runner results [key: %s]', key_id)
    return

  results = {
    'exit_codes': ','.join(map(str, result.exit_codes)),
    # TODO(maruel): Refactor these to make sense.
    'machine_id': result.bot_id,
    'machine_tag': result.bot_id,
    'config_instance_index': 0,
    'num_config_instances': 1,
    # TODO(maruel): Return each output independently. Figure out a way to
    # describe what is important in the steps and what should be ditched.
    'output': u'\n'.join(
        i.get().GetResults().decode('utf-8', 'replace')
        for i in result.outputs),
  }

  response.headers['Content-Type'] = 'application/json; charset=utf-8'
  response.out.write(json.dumps(results))


class CancelHandler(auth.AuthenticatingHandler):
  """Cancel a test runner that is not already running."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    """Ensures that the associated TaskToRun is canceled and update the
    TaskResultSummary accordingly.

    TODO(maruel): If a bot is running the task, mark the TaskRunResult as
    canceled and tell the bot on the next ping to reboot itself.
    """
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'

    runner_key = self.request.get('r', '')
    # Make sure found runner is not yet running.
    try:
      result_summary_key = task_scheduler.unpack_result_summary_key(runner_key)
    except ValueError:
      self.response.out.write('Unable to cancel runner')
      self.response.set_status(400)
      return

    # TODO(maruel): Move this code into task_scheduler.py.
    request_key = task_result.result_summary_key_to_request_key(
        result_summary_key)
    task_key = task_to_run.request_to_task_to_run_key(request_key.get())
    task_to_run.abort_task_to_run(task_key.get())
    result_summary = result_summary_key.get()
    result_summary.state = task_result.State.CANCELED
    result_summary.abandoned_ts = task_common.utcnow()
    result_summary.put()
    self.response.out.write('Runner canceled.')


class RetryHandler(auth.AuthenticatingHandler):
  """Retry a test runner again."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    """Duplicates the original request into a new one.

    Only change the ownership to the user that requested the retry.
    TaskProperties is unchanged.
    """
    runner_key = self.request.get('r', '')
    try:
      result_key = task_scheduler.unpack_result_summary_key(runner_key)
    except ValueError:
      self.response.set_status(400)
      self.response.out.write('Unable to retry runner')
      return

    request = task_result.result_summary_key_to_request_key(result_key).get()
    # TODO(maruel): Decide if it will be supported, and if so create a proper
    # function to 'duplicate' a TaskRequest in task_request.py.
    # TODO(maruel): Delete.
    data = {
      'name': request.name,
      'user': request.user,
      'properties': {
        'commands': request.properties.commands,
        'data': request.properties.data,
        'dimensions': request.properties.dimensions,
        'env': request.properties.env,
        'execution_timeout_secs': request.properties.execution_timeout_secs,
        'io_timeout_secs': request.properties.io_timeout_secs,
      },
      'priority': request.priority,
      'scheduling_expiration_secs': request.scheduling_expiration_secs,
    }
    # TODO(maruel): The user needs to have a way to get the new task id.
    task_scheduler.make_request(data)

    # TODO(maruel): Return the new TaskRequest key.
    # TODO(maruel): Use json encoded return values for the APIs.
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Runner set for retry.')


### Bot APIs.


class BootstrapHandler(auth.AuthenticatingHandler):
  """Returns python code to run to bootstrap a swarming bot."""
  @auth.require(auth.READ, 'swarming/bots')
  def get(self):
    content = file_chunks.RetrieveFile('bootstrap.py')
    if not content:
      # Fallback to the one embedded in the tree.
      with open('swarm_bot/bootstrap.py', 'rb') as f:
        content = f.read()

    self.response.headers['Content-Type'] = 'text/x-python'
    self.response.headers['Content-Disposition'] = (
        'attachment; filename="swarming_bot_bootstrap.py"')
    header = 'host_url = %r\n' % self.request.host_url
    self.response.out.write(header + content)


class GetSlaveCodeHandler(auth.AuthenticatingHandler):
  """Returns a zip file with all the files required by a slave.

  Optionally specify the hash version to download. If so, the returned data is
  cacheable.
  """

  @auth.require(auth.READ, 'swarming/bots')
  def get(self, version=None):
    if version:
      expected = bot_management.get_slave_version(self.request.host_url)
      if version != expected:
        logging.error('Requested Swarming bot %s, have %s', version, expected)
        self.abort(404)
      self.response.headers['Cache-Control'] = 'public, max-age=3600'
    else:
      self.response.headers['Cache-Control'] = 'no-cache, no-store'
    self.response.headers['Content-Type'] = 'application/octet-stream'
    self.response.headers['Content-Disposition'] = (
        'attachment; filename="swarming_bot.zip"')
    self.response.out.write(
        bot_management.get_swarming_bot_zip(self.request.host_url))


class ServerPingHandler(webapp2.RequestHandler):
  """Handler to ping when checking if the server is up.

  This handler should be extremely lightweight. It shouldn't do any
  computations, it should just state that the server is up.
  """

  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Server up')


class RegisterHandler(auth.AuthenticatingHandler):
  """Handler for the register_machine of the Swarm server.

  Attempt to find a matching job for the querying machine.
  """

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError)
  @auth.require(auth.UPDATE, 'swarming/bots')
  def post(self):
    # Validate the request.
    if not self.request.body:
      self.abort(400, 'Request must have a body')

    attributes_str = self.request.get('attributes')
    try:
      attributes = json.loads(attributes_str)
    except (TypeError, ValueError) as e:
      message = 'Invalid attributes: %s: %s' % (attributes_str, e)
      logging.error(message)
      self.abort(400, message)

    # TODO(vadimsh): Ensure attributes['id'] matches credentials used
    # to authenticate the request (i.e. auth.get_current_identity()).
    try:
      out = request_work_item(
          attributes, self.request.host_url, self.request.remote_addr)
      response = json.dumps(out)
    except runtime.DeadlineExceededError as e:
      # If the timeout happened before a runner was assigned there are no
      # problems. If the timeout occurred after a runner was assigned, that
      # runner will timeout (since the machine didn't get the details required
      # to run it) and it will automatically get retried when the machine
      # "timeout".
      message = str(e)
      logging.warning(message)
      self.abort(500, message)
    except test_request_message.Error as e:
      message = str(e)
      logging.error(message)
      self.abort(400, message)

    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.out.write(response)


class RunnerPingHandler(auth.AuthenticatingHandler):
  """Handler for runner pings to the server.

  The runner pings are used to let the server know a runner is still working, so
  it won't consider it stale.
  """

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/bots')
  def post(self):
    # TODO(vadimsh): Any machine can send ping on behalf of any other machine.
    # Ensure 'id' matches credentials used to authenticate the request (i.e.
    # auth.get_current_identity()).
    packed_run_result_key = self.request.get('r', '')
    bot_id = self.request.get('id', '')
    try:
      run_result_key = task_scheduler.unpack_run_result_key(
          packed_run_result_key)
      task_scheduler.bot_update_task(run_result_key, {}, bot_id)
    except ValueError as e:
      logging.error('Failed to accept value %s: %s', packed_run_result_key, e)
      self.abort(400, 'Runner failed to ping.')

    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class ResultHandler(auth.AuthenticatingHandler):
  """Handles test results from remote test runners."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/bots')
  def post(self):
    # TODO(user): Share this code between all the request handlers so we
    # can always see how often a request is being sent.
    connection_attempt = self.request.get(swarm_constants.COUNT_KEY)
    if connection_attempt:
      logging.info('This is the %s connection attempt from this machine to '
                   'POST these results', connection_attempt)

    packed = self.request.get('r', '')
    run_result_key = task_scheduler.unpack_run_result_key(packed)
    bot_id = urllib.unquote_plus(self.request.get('id'))
    run_result = run_result_key.get()
    # TODO(vadimsh): Verify bot_id matches credentials that are used for
    # current request (i.e. auth.get_current_identity()). Or shorter, just use
    # auth.get_current_identity() and have the bot stops passing id=foo at all.
    if bot_id != run_result.bot_id:
      # Check that machine that posts the result is same as machine that claimed
      # the task.
      msg = 'Expected bot id %s, got %s' % (run_result.bot_id, bot_id)
      logging.error(msg)
      self.abort(404, msg)

    exit_codes = urllib.unquote_plus(self.request.get('x'))
    exit_codes = filter(None, (i.strip() for i in exit_codes.split(',')))

    # TODO(maruel): Get rid of this: the result string should probably be in the
    # body of the request.
    result_string = urllib.unquote_plus(self.request.get(
        swarm_constants.RESULT_STRING_KEY))
    if isinstance(result_string, unicode):
      # Zap out any binary content on stdout.
      result_string = result_string.encode('utf-8', 'replace')

    results = result_helper.StoreResults(result_string)

    data = {
      'exit_codes': map(int, exit_codes),
      # TODO(maruel): Store output for each command individually.
      'outputs': [results.key],
    }
    task_scheduler.bot_update_task(run_result.key, data, bot_id)

    # TODO(maruel): Return JSON.
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Successfully update the runner results.')


class RemoteErrorHandler(auth.AuthenticatingHandler):
  """Handler to log an error reported by remote machine."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/bots')
  def post(self):
    # TODO(vadimsh): Log machine identity as well.
    error_message = self.request.get('m', '')
    error = errors.SwarmError(
        name='Remote Error Report', message=error_message,
        info='Remote machine address: %s' % self.request.remote_addr)
    error.put()

    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


### Public pages.


class RootHandler(webapp2.RequestHandler):
  def get(self):
    params = {
      'host_url': self.request.host_url,
    }
    self.response.out.write(template.render('root.html', params))


class DeadBotsCountHandler(webapp2.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    cutoff = task_common.utcnow() - bot_management.MACHINE_DEATH_TIMEOUT
    count = bot_management.Bot.query().filter(
        bot_management.Bot.last_seen < cutoff).count()
    self.response.out.write(str(count))


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    bot_management.get_swarming_bot_zip(self.request.host_url)
    utils.get_module_version_list(None, None)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def CreateApplication():
  urls = [
      # Frontend pages. They return HTML.
      # Public pages.
      ('/', RootHandler),
      ('/stats', stats_gviz.StatsSummaryHandler),
      ('/stats/dimensions/<dimensions:.+>', stats_gviz.StatsDimensionsHandler),
      ('/stats/user/<user:.+>', stats_gviz.StatsUserHandler),

      # User pages.
      ('/user', UserHandler),
      ('/user/tasks', TasksHandler),
      ('/user/task/<key_id:[0-9a-fA-F]+>', TaskHandler),

      # Admin pages.
      ('/restricted', RestrictedHandler),
      ('/restricted/bots', BotsListHandler),
      ('/restricted/ereporter2/report', Ereporter2ReportHandler),
      # TODO(maruel): This is an API, not a endpoint.
      ('/restricted/ereporter2/request/<request_id:[0-9a-fA-F]+>',
          Ereporter2RequestHandler),
      ('/restricted/whitelist_ip', WhitelistIPHandler),
      ('/restricted/upload_start_slave', UploadStartSlaveHandler),
      ('/restricted/upload_bootstrap', UploadBootstrapHandler),

      # Eventually accessible for client.
      ('/restricted/cancel', CancelHandler),
      ('/restricted/get_result', SecureGetResultHandler),
      ('/restricted/retry', RetryHandler),

      # Client API, in some cases also indirectly used by the frontend.
      ('/get_matching_test_cases', GetMatchingTestCasesHandler),
      ('/get_result', GetResultHandler),
      ('/test', TestRequestHandler),

      # Bot API.
      ('/bootstrap', BootstrapHandler),
      ('/get_slave_code', GetSlaveCodeHandler),
      ('/get_slave_code/<version:[0-9a-f]{40}>', GetSlaveCodeHandler),
      ('/poll_for_test', RegisterHandler),
      ('/remote_error', RemoteErrorHandler),
      ('/result', ResultHandler),
      ('/runner_ping', RunnerPingHandler),
      ('/server_ping', ServerPingHandler),

      # Both Client and Bot API.
      ('/delete_machine_stats', DeleteMachineStatsHandler),

      # The new APIs:
      # TODO(maruel): Move into restricted/
      ('/swarming/api/v1/bots', ApiBots),
      ('/swarming/api/v1/bots/dead/count', DeadBotsCountHandler),
      ('/swarming/api/v1/stats/summary/<resolution:[a-z]+>',
        stats_gviz.StatsGvizSummaryHandler),
      ('/swarming/api/v1/stats/dimensions/<dimensions:.+>/<resolution:[a-z]+>',
        stats_gviz.StatsGvizDimensionsHandler),
      ('/swarming/api/v1/stats/user/<user:.+>/<resolution:[a-z]+>',
        stats_gviz.StatsGvizUserHandler),

      ('/_ah/warmup', WarmupHandler),
  ]

  # Upgrade to Route objects so regexp work.
  routes = [webapp2.Route(*i) for i in urls]

  # Supported authentication mechanisms.
  auth.configure([
      auth.oauth_authentication,
      auth.cookie_authentication,
      auth.service_to_service_authentication,
      ip_whitelist_authentication,
  ])

  # Customize auth UI to show that it's running on swarming service.
  auth_ui.configure_ui(
      app_name='Swarming',
      app_version=utils.get_app_version(),
      app_revision_url=template.get_app_revision_url())

  # Add routes with Auth REST API and Auth UI.
  routes.extend(auth_ui.get_rest_api_routes())
  routes.extend(auth_ui.get_ui_routes())
  routes.extend(handlers_backend.get_routes())

  return webapp2.WSGIApplication(routes, debug=True)
