# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Main entry point for Swarming service.

This file contains the URL handlers for all the Swarming service URLs,
implemented using the webapp2 framework.
"""

import collections
import datetime
import functools
import json
import logging
import os
import time
import urllib

import webapp2

from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.api import modules
from google.appengine.api import taskqueue
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

from common import rpc
from common import swarm_constants
from common import test_request_message
from components import auth
from components import auth_ui
from components import datastore_utils
from components import decorators
from components import ereporter2
from components import utils
from server import admin_user
from server import bot_management
from server import dimension_mapping
from server import errors
from server import result_helper
from server import stats_gviz
from server import stats_new as stats
from server import task_glue
from server import task_request
from server import task_result
from server import task_scheduler
from server import task_shard_to_run
from server import test_management
from server import user_manager
from stats import machine_stats

import template


_NUM_USER_TEST_RUNNERS_PER_PAGE = 50
_NUM_RECENT_ERRORS_TO_DISPLAY = 10

_DELETE_MACHINE_STATS_URL = '/delete_machine_stats'

_SECURE_CHANGE_WHITELIST_URL = '/secure/change_whitelist'
_SECURE_GET_RESULTS_URL = '/secure/get_result'
_SECURE_MAIN_URL = '/secure/main'
_SECURE_USER_PROFILE_URL = '/secure/user_profile'


ACCEPTABLE_SORTS =  {
  'created_ts': 'Created',
  'done_ts': 'Ended',
  'modified_ts': 'Last updated',
  'name': 'Name',
  'user': 'User',
}
DEFAULT_SORT = 'created_ts'

# These sort are done on the TaskRequest.
SORT_REQUEST = frozenset(('created_ts', 'name', 'user'))

# These sort are done on the TaskResultSummary.
SORT_RESULT = frozenset(('done_ts', 'modified_ts'))

assert SORT_REQUEST | SORT_RESULT == frozenset(ACCEPTABLE_SORTS)


# Ignore these failures, there's nothing to do.
# TODO(maruel): Store them in the db and make this runtime configurable.
IGNORED_LINES = (
  # Probably originating from appstats.
  '/base/data/home/runtimes/python27/python27_lib/versions/1/google/'
      'appengine/_internal/django/template/__init__.py:729: UserWarning: '
      'api_milliseconds does not return a meaningful value',
)

# Ignore these exceptions.
IGNORED_EXCEPTIONS = (
  'DeadlineExceededError',
  # These occurs during a transaction.
  'Timeout',
)

# Function that is used to determine if an error entry should be ignored.
should_ignore_error_record = functools.partial(
    ereporter2.should_ignore_error_record, IGNORED_LINES, IGNORED_EXCEPTIONS)


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


def request_work_item(attributes, server_url):
  # TODO(maruel): Split it out a little.
  attribs = test_management.ValidateAndFixAttributes(attributes)
  response = test_management.CheckVersion(attributes, server_url)
  if response:
    return response

  dimensions = attribs['dimensions']
  bot_id = attribs['id'] or dimensions['hostname']
  stats.add_entry(action='bot_active', bot_id=bot_id, dimensions=dimensions)
  request, shard_result = task_scheduler.bot_reap_task(dimensions, bot_id)
  if not request:
    try_count = attribs['try_count'] + 1
    return {
      'come_back': task_scheduler.exponential_backoff(try_count),
      'try_count': try_count,
    }

  runner_key = task_scheduler.pack_shard_result_key(shard_result.key)
  test_objects = [
    test_request_message.TestObject(
        test_name=str(i),
        action=command,
        hard_time_out=request.properties.execution_timeout_secs,
        io_time_out=request.properties.io_timeout_secs)
    for i, command in enumerate(request.properties.commands)
  ]
  # pylint: disable=W0212
  test_run = test_request_message.TestRun(
      test_run_name=request.name,
      env_vars=request.properties.env,
      instance_index=shard_result.shard_number,
      num_instances=request.properties.number_shards,
      configuration=test_request_message.TestConfiguration(
          config_name=request.name,
          num_instances=request.properties.number_shards),
      result_url=('%s/result?r=%s&id=%s' % (server_url,
                                            runner_key,
                                            shard_result.bot_id)),
      ping_url=('%s/runner_ping?r=%s&id=%s' % (server_url,
                                              runner_key,
                                              shard_result.bot_id)),
      ping_delay=(test_management._TIMEOUT_FACTOR /
          test_management._MISSED_PINGS_BEFORE_TIMEOUT),
      data=request.properties.data,
      tests=test_objects)
  test_run.ExpandVariables({
      'instance_index': shard_result.shard_number,
      'num_instances': request.properties.number_shards,
  })
  test_run.Validate()

  # See test_management._GetTestRunnerCommands() for the expected format.
  files_to_upload = [
      (test_run.working_dir or '', test_management._TEST_RUN_SWARM_FILE_NAME,
      test_request_message.Stringize(test_run, json_readable=True))
  ]

  # Define how to run the scripts.
  swarm_file_path = test_management._TEST_RUN_SWARM_FILE_NAME
  if test_run.working_dir:
    swarm_file_path = os.path.join(
        test_run.working_dir, test_management._TEST_RUN_SWARM_FILE_NAME)
  command_to_execute = [
    os.path.join('local_test_runner.py'),
    '-f', swarm_file_path,
    '--restart_on_failure',
  ]

  rpc_commands = [
    rpc.BuildRPC('StoreFiles', files_to_upload),
    rpc.BuildRPC('RunCommands', command_to_execute),
  ]
  # The Swarming bot uses an hand rolled RPC system and 'commands' is actual the
  # custom RPC commands. See test_management._BuildTestRun()
  return {
    'commands': rpc_commands,
    'result_url': test_run.result_url,
    'try_count': 0,
  }


def ping_runner(runner_key, machine_id):
  # TODO(maruel): Delete this function.
  try:
    shard_result_key = task_scheduler.unpack_shard_result_key(runner_key)
    return task_scheduler.bot_update_task(shard_result_key, {}, machine_id)
  except ValueError as e:
    logging.error('Failed to accept value %s: %s', runner_key, e)
    return False


### UI code that should be in jinja2 templates


def generate_button_with_hidden_form(button_text, url, form_id):
  """Generates a button that when used will post to the given url.

  Args:
    button_text: The text to display on the button.
    url: The url to post to.
    form_id: The id to give the form.

  Returns:
    The html text to display the button.
  """
  button_html = '<form id="%s" method="post" action=%s>' % (form_id, url)
  button_html += (
      '<button onclick="document.getElementById(%s).submit()">%s</button>' %
      (form_id, button_text))
  button_html += '</form>'

  return button_html


def make_runner_view(runner):
  """Returns a html template friendly dict from a TestRunner."""
  # Simulate the old properties until the templates are updated.
  request, result = runner
  assert isinstance(request, task_request.TaskRequest), request
  assert isinstance(result, task_result.TaskResultSummary), result
  started = [i.started_ts for i in result.shards if i.started_ts]
  out = {
    # TODO(maruel): out['class_string'] = 'failed_test'
    'class_string': '',
    'command_string': 'TODO',
    'created': request.created_ts,
    'ended': result.done_ts,
    'key_string': '%x' % request.key.integer_id(),
    # TODO(maruel):
    # 'machine_id': ','.join(i.bot_id for i in runner.shards if i.bot_id),
    'machine_id': 'TODO',
    'name': request.name,
    'started': min(started) if started else None,
    'status_string': result.to_string(),
    'user': request.user,
  }
  return out



### Handlers


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
      A ndb.Future that will return the items in the DB from a query that is
      properly adjusted and filtered.
    """
    # TODO(maruel): Use cursors!
    # TODO(maruel): Use self.status, self.show_successfully_completed,
    # self.test_name_filter, self.machine_id_filter.
    assert sort_by in ACCEPTABLE_SORTS, (
        'This should have been validated at a higher level')
    opts = ndb.QueryOptions(limit=limit, offset=offset)
    direction = (
        datastore_query.PropertyOrder.ASCENDING
        if ascending else datastore_query.PropertyOrder.DESCENDING)

    fetch_request = bool(sort_by in SORT_REQUEST)
    if fetch_request:
      base_type = task_request.TaskRequest
    else:
      base_type = task_result.TaskResultSummary

    # The future returned to the user:
    final = ndb.Future()

    # Phase 1: initiates the fetch.
    initial_future = base_type.query(default_options=opts).order(
        datastore_query.PropertyOrder(sort_by, direction)).fetch_async()

    def phase2_fetch_complement(future):
      """Initiates the async fetch for the other object.

      If base_type is TaskRequest, it will fetch the corresponding
      TaskResultSummary.
      If base_type is TaskResultSummary, it will fetch the corresponding
      TaskRequest.
      """
      entities = future.get_result()
      if not entities:
        # Skip through if no entity was returned by the Query.
        final.set_result(entities)
        return

      if fetch_request:
        keys = (
          task_result.request_key_to_result_summary_key(i.key) for i in entities
        )
      else:
        keys = (i.request_key for i in entities)

      # Convert a list of Future into a MultiFuture.
      second_future = ndb.MultiFuture()
      map(second_future.add_dependent, ndb.get_multi_async(keys))
      second_future.complete()
      second_future.add_immediate_callback(
          phase3_merge_complementatry_models, entities, second_future)

    def phase3_merge_complementatry_models(entities, future):
      """Merge the results together to yield (TaskRequest,
      TaskResultSummary).
      """
      if fetch_request:
        out = zip(entities, future.get_result())
      else:
        # Reverse the order.
        out = zip(future.get_result(), entities)
      # Filter out inconsistent items.
      final.set_result([i for i in out if i[0] and i[1]])

    # ndb.Future.add_immediate_callback() adds a callback that is immediately
    # called when self.set_result() is called.
    initial_future.add_immediate_callback(
        phase2_fetch_complement, initial_future)
    return final

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


class MainHandler(auth.AuthenticatingHandler):
  """Handler for the main page of the web server.

  This handler lists all pending requests and allows callers to manage them.
  """
  def parse_filters(self):
    """Parse the filters from the request."""
    return FilterParams(
      self.request.get('status', 'all'),
      self.request.get('test_name_filter', ''),
      # Compare to 'False' so that the default value for invalid user input
      # is True.
      self.request.get('show_successfully_completed', '') != 'False',
      self.request.get('machine_id_filter', ''))

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    # TODO(maruel): Convert to Google Graph API.
    # TODO(maruel): Once migration is complete, remove limit and offset, replace
    # with cursor.
    page_length = int(self.request.get('length', 50))
    page = int(self.request.get('page', 1))

    sort_by = self.request.get('sort_by', 'D' + DEFAULT_SORT)
    ascending = bool(sort_by[0] == 'A')
    sort_key = sort_by[1:]
    if sort_key not in ACCEPTABLE_SORTS:
      self.abort(400, 'Invalid sort key')

    # TODO(maruel): Stop doing HTML in python.
    sorted_by_message = '<p>Currently sorted by: '
    if not ascending:
      sorted_by_message += 'Reverse '
    sorted_by_message += ACCEPTABLE_SORTS[sort_key] + '</p>'
    sort_options = []
    # TODO(maruel): Use an order that makes sense instead of whatever the dict
    # happens to be.
    for key, value in ACCEPTABLE_SORTS.iteritems():
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

    runners = [
      make_runner_view(r) for r in tasks_future.get_result()
    ]

    params = {
      'current_page': page,
      'errors': errors_found_future.get_result(),
      'filter_selects': params.filter_selects_as_html(),
      'machine_id_filter': params.machine_id_filter,
      'runners': runners,
      'selected_sort': ('A' if ascending else 'D') + sort_key,
      'sort_options': sort_options,
      'sort_by': sort_by,
      'sorted_by_message': sorted_by_message,
      'test_name_filter': params.test_name_filter,
      'total_tasks': total_task_count_future.get_result(),
      'url_no_filters': params.generate_page_url(page, sort_by),
      'url_no_page': params.generate_page_url(
          sort_by=sort_by, include_filters=True),
      'url_no_sort_by_or_filters': params.generate_page_url(
          page, include_filters=False),
    }
    self.response.out.write(template.render('index.html', params))


class RedirectToMainHandler(webapp2.RequestHandler):
  """Handler to redirect requests to base page secured main page."""

  def get(self):
    self.redirect(_SECURE_MAIN_URL)


class MachineListHandler(auth.AuthenticatingHandler):
  """Handler for the machine list page of the web server.

  This handler lists all the machines that have ever polled the server and
  some basic information about them.
  """

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    sort_by = self.request.get('sort_by', 'machine_id')
    if sort_by not in machine_stats.ACCEPTABLE_SORTS:
      self.abort(400, 'Invalid sort_by query parameter')

    dead_machine_cutoff = (
        datetime.datetime.utcnow() - machine_stats.MACHINE_DEATH_TIMEOUT)
    machines = []
    for machine in machine_stats.GetAllMachines(sort_by):
      m = machine.to_dict()
      m['html_class'] = (
          'dead_machine' if machine.last_seen < dead_machine_cutoff else '')
      # Add a delete option for each machine assignment.
      # TODO(maruel): This should not be generated via python.
      m['command_string'] = generate_button_with_hidden_form(
          'Delete',
          '%s?r=%s' % (_DELETE_MACHINE_STATS_URL, machine.key.string_id()),
          machine.key.string_id())
      machines.append(m)

    sort_options = [
        SortOptions(k, v) for k, v in machine_stats.ACCEPTABLE_SORTS.iteritems()
    ]
    params = {
        'machine_update_time': machine_stats.MACHINE_UPDATE_TIME,
        'machines': machines,
        'selected_sort': sort_by,
        'sort_options': sort_options,
    }
    self.response.out.write(template.render('machine_list.html', params))


class ApiBots(auth.AuthenticatingHandler):
  """Returns the list of known swarming bots."""

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    params = {
        'machine_death_timeout': machine_stats.MACHINE_DEATH_TIMEOUT,
        'machine_update_time': machine_stats.MACHINE_UPDATE_TIME,
        'machines': [m.to_dict() for m in machine_stats.MachineStats.query()],
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
    key = self.request.get('r')

    if key and machine_stats.DeleteMachineStats(key):
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
      request_properties = task_glue.convert_test_case(test_case)
    except test_request_message.Error as e:
      message = str(e)
      logging.error(message)
      self.abort(400, message)

    def to_packed_key(i):
      return task_scheduler.pack_shard_result_key(
          task_result.shard_to_run_key_to_shard_result_key(
              shard_runs[i].key, 1))

    request, shard_runs = task_scheduler.make_request(request_properties)
    out = {
      'test_case_name': request.name,
      'test_keys': [
        {
          'config_name': request.name,
          'instance_index': shard_index,
          'num_instances': request.properties.number_shards,
          'test_key': to_packed_key(shard_index),
        }
        for shard_index in xrange(request.properties.number_shards)
      ],
    }
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.out.write(json.dumps(out))


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

    logging.debug('Received Result: %s', self.request.url)

    # TODO(vadimsh): Check that machine that posts the result is same as
    # machine that claimed the runner.
    runner_key = self.request.get('r', '')
    shard_result_key = task_scheduler.unpack_shard_result_key(runner_key)
    runner = shard_result_key.get()

    if not runner:
      # If the runner is gone, it probably already received results from
      # a different machine and was naturally deleted. We can't do anything
      # with the results now, so just ignore them.
      msg = ('The runner, with key %s, has already been deleted, results lost.'
             % runner_key)
      logging.info(msg)
      self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
      self.response.out.write(msg)
      return

    # Find the high level success/failure from the URL. We assume failure if
    # we can't find the success parameter in the request.
    exit_codes = urllib.unquote_plus(self.request.get('x'))
    machine_id = urllib.unquote_plus(self.request.get('id'))

    # TODO(vadimsh): Verify machine_id matches credentials that are used for
    # current request (i.e. auth.get_current_identity()).
    if runner.bot_id != machine_id:
      self.abort(400, 'Expected bot id %s, got %s', runner.bot_id, machine_id)

    # TODO(user): The result string should probably be in the body of the
    # request.
    result_string = urllib.unquote_plus(self.request.get(
        swarm_constants.RESULT_STRING_KEY))
    if isinstance(result_string, unicode):
      result_string = result_string.encode('utf-8')

    # Mark the runner as pinging now to prevent it from timing out while
    # the results are getting stored.
    ping_runner(runner_key, machine_id)

    results = result_helper.StoreResults(result_string)
    # Ignore the argument 'success'.
    exit_codes = filter(None, (i.strip() for i in exit_codes.split(',')))
    data = {
      'exit_codes': map(int, exit_codes),
      # TODO(maruel): Store output for each command individually.
      'outputs': [results.key],
    }
    task_scheduler.bot_update_task(runner.key, data, machine_id)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Successfully update the runner results.')


class TaskCleanupDataHandler(webapp2.RequestHandler):
  """Deletes orphaned blobs."""

  @decorators.silence(datastore_errors.Timeout)
  @decorators.require_taskqueue('cleanup')
  def post(self):
    # All the things that need to be deleted.
    queries = [
        errors.QueryOldErrors(),
        dimension_mapping.QueryOldDimensionMapping(),
        result_helper.QueryOldResults(),
        result_helper.QueryOldResultChunks(),
    ]
    datastore_utils.incremental_map(
        queries, ndb.delete_multi_async, max_inflight=50)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronAbortBotDiedHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    task_scheduler.cron_abort_bot_died()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronAbortExpiredShardToRunHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    task_scheduler.cron_abort_expired_shard_to_run()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronSyncAllResultSummaryHandler(webapp2.RequestHandler):
  @decorators.require_cronjob
  def get(self):
    task_scheduler.cron_sync_all_result_summary()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class CronTriggerCleanupDataHandler(webapp2.RequestHandler):
  """Triggers task to delete orphaned blobs."""

  @decorators.require_cronjob
  def get(self):
    taskqueue.add(method='POST', url='/internal/taskqueue/cleanup_data',
                  queue_name='cleanup')
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class DeadBotsCountHandler(webapp2.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    cutoff = machine_stats.utcnow() - machine_stats.MACHINE_DEATH_TIMEOUT
    count = machine_stats.MachineStats.query().filter(
        machine_stats.MachineStats.last_seen < cutoff).count()
    self.response.out.write(str(count))


class CronSendEreporter2MailHandler(webapp2.RequestHandler):
  """Sends email containing the errors found in logservice."""

  @decorators.require_cronjob
  def get(self):
    request_id_url = self.request.host_url + '/secure/ereporter2/request/'
    report_url = self.request.host_url + '/secure/ereporter2/report'
    result = ereporter2.generate_and_email_report(
        None,
        should_ignore_error_record,
        admin_user.GetAdmins(),
        request_id_url,
        report_url,
        ereporter2.REPORT_TITLE_TEMPLATE,
        ereporter2.REPORT_CONTENT_TEMPLATE,
        {})
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    if result:
      self.response.write('Success.')
    else:
      # Do not HTTP 500 since we do not want it to be retried.
      self.response.write('Failed.')


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
    request_id_url = '/secure/ereporter2/request/'
    end = int(float(self.request.get('end', 0)) or time.time())
    start = int(
        float(self.request.get('start', 0)) or
        ereporter2.get_default_start_time() or 0)
    module_versions = GetModulesVersions()
    report, ignored = ereporter2.generate_report(
        start, end, module_versions, should_ignore_error_record)
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


class ShowMessageHandler(auth.AuthenticatingHandler):
  """Show the full text of a test request."""

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'

    runner_key = self.request.get('r', '')
    try:
      shard_result_key = task_scheduler.unpack_shard_result_key(runner_key)
      runner = shard_result_key.get()
    except ValueError:
      runner = None

    if runner:
      self.response.write(utils.encode_to_json(runner.GetAsDict()))
    else:
      self.response.set_status(404)
      self.response.out.write('{}')


class UploadStartSlaveHandler(auth.AuthenticatingHandler):
  """Accept a new start slave script."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    script = self.request.get('script', '')
    if not script:
      self.abort(400, 'No script uploaded')

    bot_management.StoreStartSlaveScript(script)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class GetMatchingTestCasesHandler(auth.AuthenticatingHandler):
  """Get all the keys for any test runners that match a given test case name."""

  @auth.require(auth.READ, 'swarming/clients')
  def get(self):
    test_case_name = self.request.get('name', '')

    # Find the matching TaskRequest, then return all the valid keys.
    q = task_request.TaskRequest.query().filter(
        task_request.TaskRequest.name == test_case_name)
    keys = []
    for request in q:
      # Then return all the relevant shard_ids.
      # TODO(maruel): It's hacked up. This needs to fetch TaskResultSummary to
      # get the try_numbers.
      try_number = 1
      keys.extend(
          '%x-%s' % (request.key.integer_id()+i+1, try_number)
          for i in xrange(request.properties.number_shards))

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


class GetSlaveCodeHandler(auth.AuthenticatingHandler):
  """Returns a zip file with all the files required by a slave.

  Optionally specify the hash version to download. If so, the returned data is
  cacheable.
  """

  @auth.require(auth.READ, 'swarming/bots')
  def get(self, version=None):
    if version:
      expected = bot_management.SlaveVersion()
      if version != expected:
        logging.error('Requested Swarming bot %s, have %s', version, expected)
        self.abort(404)
      self.response.headers['Cache-Control'] = 'public, max-age=3600'
    else:
      self.response.headers['Cache-Control'] = 'no-cache, no-store'
    self.response.headers['Content-Type'] = 'application/octet-stream'
    self.response.headers['Content-Disposition'] = (
        'attachment; filename="swarm_bot.zip"')
    self.response.out.write(bot_management.SlaveCodeZipped())


class CleanupResultsHandler(auth.AuthenticatingHandler):
  """Delete the Test Runner with the given key."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/bots')
  def post(self):
    # TODO(maruel): Remove this API.
    self.response.headers['Content-Type'] = 'test/plain'
    self.response.out.write('Key deleted.')


class CancelHandler(auth.AuthenticatingHandler):
  """Cancel a test runner that is not already running."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'

    runner_key = self.request.get('r', '')
    # Make sure found runner is not yet running.
    try:
      shard_result_key = task_scheduler.unpack_shard_result_key(runner_key)
    except ValueError:
      self.response.set_status(400)
      self.response.write('Unable to cancel runner')
      return

    task_shard_to_run.abort_shard_to_run(shard_result_key.parent().get())
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
      shard_result_key = task_scheduler.unpack_shard_result_key(runner_key)
    except ValueError:
      self.response.set_status(400)
      self.response.write('Unable to retry runner')
      return

    request = task_result.shard_result_key_to_request_key(
        shard_result_key).get()
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
        'number_shards': request.properties.number_shards,
        'execution_timeout_secs': request.properties.execution_timeout_secs,
        'io_timeout_secs': request.properties.io_timeout_secs,
      },
      'priority': request.priority,
      'scheduling_expiration_secs': request.scheduling_expiration_secs,
    }
    # TODO(maruel): Return the new TaskRequest key.
    task_scheduler.make_request(data)

    # TODO(maruel): Use json encoded return values for the APIs.
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Runner set for retry.')


class RegisterHandler(auth.AuthenticatingHandler):
  """Handler for the register_machine of the Swarm server.

  Attempt to find a matching job for the querying machine.
  """

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

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
      response = json.dumps(
          request_work_item(attributes, self.request.host_url))
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
    key = self.request.get('r', '')
    machine_id = self.request.get('id', '')
    if not ping_runner(key, machine_id):
      self.abort(400, 'Runner failed to ping.')

    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class ServerPingHandler(webapp2.RequestHandler):
  """Handler to ping when checking if the server is up.

  This handler should be extremely lightweight. It shouldn't do any
  computations, it should just state that the server is up.
  """

  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Server up')


class UserProfileHandler(auth.AuthenticatingHandler):
  """Handler for the user profile page of the web server.

  This handler lists user info, such as their IP whitelist and settings.
  """

  @auth.require(auth.READ, 'swarming/management')
  def get(self):
    display_whitelists = sorted(
        (
          {
            'ip': w.ip,
            'key': w.key.id,
            'password': w.password,
            'url': _SECURE_CHANGE_WHITELIST_URL,
          } for w in user_manager.MachineWhitelist().query()),
        key=lambda x: x['ip'])

    params = {
        'change_whitelist_url': _SECURE_CHANGE_WHITELIST_URL,
        'whitelists': display_whitelists,
    }
    self.response.out.write(template.render('user_profile.html', params))


class ChangeWhitelistHandler(auth.AuthenticatingHandler):
  """Handler for making changes to a user whitelist."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(auth.UPDATE, 'swarming/management')
  def post(self):
    ip = self.request.get('i', self.request.remote_addr)
    mask = 32
    if '/' in ip:
      ip, mask = ip.split('/', 1)
      mask = int(mask)
    ips = expand_subnet(ip, mask)

    # Make sure a password '' sent by the form is stored as None.
    password = self.request.get('p', None) or None

    add = self.request.get('a')
    if add == 'True':
      for ip in ips:
        user_manager.AddWhitelist(ip, password)
    elif add == 'False':
      for ip in ips:
        user_manager.DeleteWhitelist(ip)
    else:
      self.abort(400, 'Invalid \'a\' parameter.')

    self.redirect(_SECURE_USER_PROFILE_URL, permanent=True)


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


def ip_whitelist_authentication(request):
  """Check to see if the request is from a whitelisted machine.

  Will use the remote machine's IP and provided password (if any).

  Args:
    request: WebAPP request sent by remote machine.

  Returns:
    auth.Identity of a machine if IP is whitelisted, None otherwise.
  """
  assert request.remote_addr
  is_whitelisted = user_manager.IsWhitelistedMachine(
      request.remote_addr, request.get('password', None))

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


def SendRunnerResults(response, key):
  """Sends the results of the runner specified by key.

  Args:
    response: Response to be sent to remote machine.
    key: Key identifying the runner.
  """
  try:
    shard_result_key = task_scheduler.unpack_shard_result_key(key)
    shard_result = shard_result_key.get()
    if not shard_result:
      # TODO(maruel): Implement in next CL.
      results = {
        'exit_codes': '',
        'machine_id': None,
        'machine_tag': None,
        'output': '',
      }
    else:
      results = {
        'exit_codes': ','.join(map(str, shard_result.exit_codes)),
        'machine_id': shard_result.bot_id,
        # TODO(maruel): Likely unnecessary.
        'machine_tag': machine_stats.GetMachineTag(shard_result.bot_id),
        'config_instance_index': shard_result.shard_number,
        'num_config_instances':
            shard_result.request_key.get().properties.number_shards,
        # TODO(maruel): Return each output independently. Figure out a way to
        # describe what is important in the steps and what should be ditched.
        'output': '\n'.join(i.get().GetResults() for i in shard_result.outputs),
      }
  except ValueError:
    results = None

  if results:
    # Convert the output to utf-8 to ensure that the JSON library can
    # handle it without problems.
    # TODO(maruel): It is decoding from utf-8 to unicode. This is confused.
    results['output'] = results['output'].decode('utf-8', 'replace')
    response.headers['Content-Type'] = 'application/json; charset=utf-8'
    response.out.write(json.dumps(results))
  else:
    # TODO(maruel): Use 404 if not present.
    response.set_status(204)
    logging.info('Unable to provide runner results [key: %s]', key)


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def CreateApplication():
  urls = [
      ('/', RedirectToMainHandler),
      ('/cleanup_results', CleanupResultsHandler),
      ('/get_matching_test_cases', GetMatchingTestCasesHandler),
      ('/get_result', GetResultHandler),
      ('/get_slave_code', GetSlaveCodeHandler),
      ('/get_slave_code/<version:[0-9a-f]{40}>', GetSlaveCodeHandler),
      ('/poll_for_test', RegisterHandler),
      ('/remote_error', RemoteErrorHandler),
      ('/result', ResultHandler),
      ('/runner_ping', RunnerPingHandler),

      ('/secure/ereporter2/report', Ereporter2ReportHandler),
      ('/secure/ereporter2/request/<request_id:[0-9a-fA-F]+>',
          Ereporter2RequestHandler),
      ('/secure/machine_list', MachineListHandler),
      ('/secure/retry', RetryHandler),
      ('/secure/show_message', ShowMessageHandler),

      ('/server_ping', ServerPingHandler),
      ('/test', TestRequestHandler),
      ('/upload_start_slave', UploadStartSlaveHandler),
      (_DELETE_MACHINE_STATS_URL, DeleteMachineStatsHandler),
      ('/secure/cancel', CancelHandler),
      (_SECURE_CHANGE_WHITELIST_URL, ChangeWhitelistHandler),
      (_SECURE_GET_RESULTS_URL, SecureGetResultHandler),
      (_SECURE_MAIN_URL, MainHandler),
      (_SECURE_USER_PROFILE_URL, UserProfileHandler),
      ('/stats', stats_gviz.StatsSummaryHandler),
      ('/stats/dimensions/<dimensions:.+>', stats_gviz.StatsDimensionsHandler),
      ('/stats/user/<user:.+>', stats_gviz.StatsUserHandler),

      # The new APIs:
      ('/swarming/api/v1/bots', ApiBots),
      ('/swarming/api/v1/bots/dead/count', DeadBotsCountHandler),
      ('/swarming/api/v1/stats/summary/<resolution:[a-z]+>',
        stats_gviz.StatsGvizSummaryHandler),
      ('/swarming/api/v1/stats/dimensions/<dimensions:.+>/<resolution:[a-z]+>',
        stats_gviz.StatsGvizDimensionsHandler),
      ('/swarming/api/v1/stats/user/<user:.+>/<resolution:[a-z]+>',
        stats_gviz.StatsGvizUserHandler),


      # Internal urls.

      # Cron jobs.
      ('/internal/cron/abort_bot_died', CronAbortBotDiedHandler),
      ('/internal/cron/abort_expired_shard_to_run',
          CronAbortExpiredShardToRunHandler),
      ('/internal/cron/sync_all_result_summary',
          CronSyncAllResultSummaryHandler),

      ('/internal/cron/ereporter2/mail', CronSendEreporter2MailHandler),
      ('/internal/cron/stats/update', stats.InternalStatsUpdateHandler),
      ('/internal/cron/trigger_cleanup_data', CronTriggerCleanupDataHandler),

      # Task queues.
      ('/internal/taskqueue/cleanup_data', TaskCleanupDataHandler),

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
      app_version=modules.get_current_version_name(),
      app_revision_url=None)

  # Add routes with Auth REST API and Auth UI.
  routes.extend(auth_ui.get_rest_api_routes())
  routes.extend(auth_ui.get_ui_routes())

  return webapp2.WSGIApplication(routes, debug=True)
