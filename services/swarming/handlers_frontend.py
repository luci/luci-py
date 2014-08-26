# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Main entry point for Swarming service.

This file contains the URL handlers for all the Swarming service URLs,
implemented using the webapp2 framework.
"""

import collections
import datetime
import itertools
import json
import logging
import os
import urllib

import webapp2

from google.appengine import runtime
from google.appengine.api import datastore_errors
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

import handlers_backend
import template
from common import rpc
from common import test_request_message
from components import auth
from components import decorators
from components import ereporter2
from components import utils
from server import acl
from server import bot_management
from server import file_chunks
from server import stats
from server import stats_gviz
from server import task_common
from server import task_result
from server import task_scheduler
from server import task_to_run
from server import user_manager


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


# Helper class for displaying the sort options in html templates.
SortOptions = collections.namedtuple('SortOptions', ['key', 'name'])


###


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
    'tags': [],
  }


def request_work_item(attributes, server_url, remote_addr):
  # TODO(maruel): Split it out a little.
  bot_management.validate_and_fix_attributes(attributes)
  response = bot_management.check_version(attributes, server_url)
  if response:
    return response

  dimensions = attributes['dimensions']
  bot_id = attributes['id'] or dimensions['hostname']
  # Note its existence at two places, one for stats at 1 minute resolution, the
  # other for the list of known bots.
  stats.add_entry(action='bot_active', bot_id=bot_id, dimensions=dimensions)

  # The TaskRunResult will be referenced on the first ping, ensuring that the
  # task was actually taken.
  bot_management.tag_bot_seen(
      bot_id,
      dimensions.get('hostname', bot_id),
      attributes['ip'],
      remote_addr,
      dimensions,
      attributes['version'],
      False)

  request, run_result = task_scheduler.bot_reap_task(dimensions, bot_id)
  if not request:
    try_count = attributes['try_count'] + 1
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
  result_url = '%s/result2?r=%s&id=%s' % (server_url, packed, bot_id)
  ping_url = '%s/runner_ping?r=%s&id=%s' % (server_url, packed, bot_id)
  # Ask the slave to ping every 55 seconds. The main reason is to make sure that
  # the majority of the time, the stats of active shards has a ping in every
  # minute window.
  # TODO(maruel): Have the slave stream the stdout on the fly, so there won't be
  # need for ping unless there's no stdout. The slave will decide this instead
  # of the master.
  ping_delay = 55
  env = request.properties.env.copy()
  env['SWARMING_TASK_ID'] = packed
  test_run = test_request_message.TestRun(
      test_run_name=request.name,
      env_vars=env,
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


def log_unexpected_keys(expected_keys, actual_keys, request, source, name):
  """Logs an error if unexpected keys are present or expected keys are missing.

  This is important to catch typos.
  """
  return log_unexpected_subset_keys(
      expected_keys, expected_keys, actual_keys, request, source, name)


def log_unexpected_subset_keys(
    expected_keys, minimum_keys, actual_keys, request, source, name):
  """Logs an error if expected keys are not present."""
  actual_keys = frozenset(actual_keys)
  superfluous = actual_keys - expected_keys
  missing = minimum_keys - actual_keys
  if superfluous or missing:
    msg_missing = (' missing: %s' % sorted(missing)) if missing else ''
    msg_superfluous = (
        (' superfluous: %s' % sorted(superfluous)) if superfluous else '')
    message = 'Unexpected %s%s%s; did you make a typo?' % (
        name, msg_missing, msg_superfluous)
    ereporter2.log_request(
        request,
        source=source,
        message=message)
    return message


### is_admin pages.

# TODO(maruel): Sort the handlers once they got their final name.


class UploadStartSlaveHandler(auth.AuthenticatingHandler):
  """Accept a new start slave script."""

  @auth.require(acl.is_admin)
  def get(self):
    params = {
      'content': bot_management.get_start_slave(),
      'path': self.request.path,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.out.write(
        template.render('swarming/restricted_uploadstartslave.html', params))

  @auth.require(acl.is_admin)
  def post(self):
    script = self.request.get('script', '')
    if not script:
      self.abort(400, 'No script uploaded')

    bot_management.store_start_slave(script.encode('utf-8', 'replace'))
    self.get()


class UploadBootstrapHandler(auth.AuthenticatingHandler):
  @auth.require(acl.is_admin)
  def get(self):
    params = {
      'content': bot_management.get_bootstrap(self.request.host_url),
      'path': self.request.path,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.out.write(
        template.render('swarming/restricted_uploadbootstrap.html', params))

  @auth.require(acl.is_admin)
  def post(self):
    script = self.request.get('script', '')
    if not script:
      self.abort(400, 'No script uploaded')

    file_chunks.StoreFile('bootstrap.py', script.encode('utf-8'))
    self.get()


class WhitelistIPHandler(auth.AuthenticatingHandler):
  @auth.require(acl.is_admin)
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
        template.render('swarming/restricted_whitelistip.html', params))

  @auth.require(acl.is_admin)
  def post(self):
    ip = self.request.get('i', self.request.remote_addr)
    mask = 32
    if '/' in ip:
      ip, mask = ip.split('/', 1)
      mask = int(mask)
    ips = acl.expand_subnet(ip, mask)

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


### acl.is_privileged_user pages.


class BotsListHandler(auth.AuthenticatingHandler):
  """Presents the list of known bots."""
  ACCEPTABLE_BOTS_SORTS = {
    'last_seen': 'Last Seen',
    'hostname': 'Hostname',
    '__key__': 'ID',
  }
  SORT_OPTIONS = [
    SortOptions(k, v) for k, v in sorted(ACCEPTABLE_BOTS_SORTS.iteritems())
  ]

  @auth.require(acl.is_privileged_user)
  def get(self):
    limit = int(self.request.get('limit', 100))
    cursor = datastore_query.Cursor(urlsafe=self.request.get('cursor'))
    sort_by = self.request.get('sort_by', '__key__')
    if sort_by not in self.ACCEPTABLE_BOTS_SORTS:
      self.abort(400, 'Invalid sort_by query parameter')

    order = datastore_query.PropertyOrder(
        sort_by, datastore_query.PropertyOrder.ASCENDING)

    now = utils.utcnow()
    cutoff = now - bot_management.BOT_DEATH_TIMEOUT

    num_total_bots_future = bot_management.Bot.query().count_async()
    num_dead_bots_future = bot_management.Bot.query(
        bot_management.Bot.last_seen_ts < cutoff).count_async()
    fetch_future = bot_management.Bot.query().order(order).fetch_page_async(
        limit, start_cursor=cursor)

    # TODO(maruel): self.request.host_url should be the default AppEngine url
    # version and not the current one. It is only an issue when
    # version-dot-appid.appspot.com urls are used to access this page.
    version = bot_management.get_slave_version(self.request.host_url)
    bots, cursor, more = fetch_future.get_result()
    # Prefetch the tasks. We don't actually use the value here, it'll be
    # implicitly used by ndb local's cache when refetched by the html template.
    tasks = filter(None, (b.task for b in bots))
    ndb.get_multi(tasks)
    num_total_bots = num_total_bots_future.get_result()
    num_dead_bots = num_dead_bots_future.get_result()
    params = {
      'bots': bots,
      'current_version': version,
      'cursor': cursor.urlsafe() if cursor and more else '',
      'is_admin': acl.is_admin(),
      'is_privileged_user': acl.is_privileged_user(),
      'limit': limit,
      'now': now,
      'num_bots_alive': num_total_bots - num_dead_bots,
      'num_bots_dead': num_dead_bots,
      'sort_by': sort_by,
      'sort_options': self.SORT_OPTIONS,
    }
    self.response.out.write(
        template.render('swarming/restricted_botslist.html', params))


class BotHandler(auth.AuthenticatingHandler):
  @auth.require(acl.is_privileged_user)
  def get(self, bot_id):
    limit = int(self.request.get('limit', 100))
    cursor = datastore_query.Cursor(urlsafe=self.request.get('cursor'))
    bot_future = bot_management.get_bot_key(bot_id).get_async()
    run_results, cursor, more = task_result.TaskRunResult.query(
        task_result.TaskRunResult.bot_id == bot_id).order(
            -task_result.TaskRunResult.started_ts).fetch_page(
                limit, start_cursor=cursor)
    now = utils.utcnow()
    bot = bot_future.get_result()
    # Calculate the time this bot was idle.
    idle_time = datetime.timedelta()
    run_time = datetime.timedelta()
    if run_results:
      run_time = run_results[0].duration_now() or datetime.timedelta()
      if not cursor and run_results[0].state != task_result.State.RUNNING:
        # Add idle time since last task completed. Do not do this when a cursor
        # is used since it's not representative.
        idle_time = now - run_results[0].ended_ts
      for index in xrange(1, len(run_results)):
        idle_time += (
            run_results[index-1].started_ts - run_results[index].ended_ts)
        duration = run_results[index].duration
        if duration:
          run_time += duration

    params = {
      'bot': bot,
      'bot_id': bot_id,
      'current_version':
          bot_management.get_slave_version(self.request.host_url),
      'cursor': cursor.urlsafe() if cursor and more else None,
      'idle_time': idle_time,
      'is_admin': acl.is_admin(),
      'limit': limit,
      'now': now,
      'run_results': run_results,
      'run_time': run_time,
    }
    # TODO(maruel): Make the delete link redirect to /restricted/bots. It would
    # probably be preferable to not use /delete_machine_stats and create a UI
    # specialized endpoint instead.
    self.response.out.write(
        template.render('swarming/restricted_bot.html', params))


### User accessible pages.


class TasksHandler(auth.AuthenticatingHandler):
  """Lists all requests and allows callers to manage them."""
  # Each entry is an item in the Sort column.
  # Each entry is (key, text, hover)
  SORT_CHOICES = [
    ('created_ts', 'Created', 'Most recently created tasks are shown first.'),
    ('modified_ts', 'Active', 'Shows the most recently active tasks first.'),
    ('completed_ts', 'Completed',
      'Shows the most recently completed tasks first.'),
    ('abandoned_ts', 'Abandoned',
      'Shows the most recently abandoned tasks first.'),
  ]

  # Each list is one column in the Task state filtering column.
  # Each sublist is the checkbox item in this column.
  # Each entry is (key, text, hover)
  # TODO(maruel): Evaluate what the categories the users would like for
  # diagnosis, then adapt the DB to enable efficient queries.
  STATE_CHOICES = [
    [
      ('all', 'All', 'All tasks ever requested independent of their state.'),
      ('pending', 'Pending',
        'Tasks that are still ready to be assigned to a bot.'),
      ('running', 'Running', 'Tasks being currently executed by a bot.'),
      ('pending_running', 'Pending|running',
        'Tasks either \'pending\' or \'running\'.'),
    ],
    [
      ('completed', 'Completed',
        'All tasks that are completed, independent if the task itself '
        'succeeded or failed. This excludes tasks that had an infrastructure '
        'failure.'),
      ('completed_success', 'Successes', 'Tasks that completed successfully.'),
      ('completed_failure', 'Failures',
        'Tasks that were executed successfully but failed, e.g. exit code is '
        'non-zero.'),
      # TODO(maruel): This is never set until the new bot API is writen.
      # https://code.google.com/p/swarming/issues/detail?id=117
      #('timed_out', 'Execution timed out'),
    ],
    [
      ('bot_died', 'Bot died',
        'The bot stopped sending updates while running the task, causing the '
        'task execution to time out. This is considered an infrastructure '
        'failure and the usual reason is that the bot BSOD\'ed or '
        'spontaneously rebooted.'),
      ('expired', 'Expired',
        'The task was not assigned a bot until its expiration timeout, causing '
        'the task to never being assigned to a bot. This can happen when the '
        'dimension filter was not available or overloaded with a low priority. '
        'Either fix the priority or bring up more bots with these dimensions.'),
      ('canceled', 'Canceled',
        'The task was explictly canceled by a user before it started '
        'executing.'),
    ],
  ]

  @auth.require(acl.is_user)
  def get(self):
    """Handles both ndb.Query searches and search.Index().search() queries.

    If |task_name| is set or not affects the meaning of |cursor|. When set, the
    cursor is for search.Index, otherwise the cursor is for a ndb.Query.
    """
    cursor_str = self.request.get('cursor')
    limit = int(self.request.get('limit', 100))
    sort = self.request.get('sort', self.SORT_CHOICES[0][0])
    state = self.request.get('state', self.STATE_CHOICES[0][0][0])
    task_name = self.request.get('task_name', '').strip()

    if not any(sort == i[0] for i in self.SORT_CHOICES):
      self.abort(400, 'Invalid sort')
    if not any(any(state == i[0] for i in j) for j in self.STATE_CHOICES):
      self.abort(400, 'Invalid state')

    if sort != 'created_ts':
      # Zap all filters in this case to reduce the number of required indexes.
      # Revisit according to the user requests.
      state = 'all'

    now = utils.utcnow()
    counts_future = self._get_counts_future(now)

    # This call is synchronous.
    tasks, cursor_str, sort, state = self._get_tasks(
        task_name, cursor_str, limit, sort, state)

    # Prefetch the TaskRequest all at once, so that ndb's in-process cache has
    # it instead of fetching them one at a time indirectly when using
    # TaskResultSummary.request_key.get().
    futures = ndb.get_multi_async(t.request_key for t in tasks)

    # Evaluate the counts to print the filtering columns with the associated
    # numbers.
    state_choices = self._get_state_choices(counts_future)

    # Do not let dangling futures linger around.
    ndb.Future.wait_all(futures)
    params = {
      'cursor': cursor_str,
      'is_privileged_user': acl.is_privileged_user(),
      'limit': limit,
      'now': now,
      'sort': sort,
      'sort_choices': self.SORT_CHOICES,
      'state': state,
      'state_choices': state_choices,
      'tasks': tasks,
      'task_name': task_name,
    }
    # TODO(maruel): If admin or if the user is task's .user, show the Cancel
    # button. Do not show otherwise.
    self.response.out.write(template.render('swarming/user_tasks.html', params))

  def _get_tasks(self, task_name, cursor_str, limit, sort, state):
    """Returns all tasks for this query. This function is synchronous."""
    if task_name:
      # Word based search. Override the flags.
      sort = 'created_ts'
      state = 'all'
      tasks, cursor_str = task_scheduler.search_by_name(
          task_name, cursor_str, limit)
    else:
      # Normal listing.
      queries, query = self._get_query(sort, state)
      if queries:
        # When multiple queries are used, we can't use a cursor.
        cursor_str = None

        # Take the first |limit| items for each query. This is not efficient,
        # worst case is fetching N * limit entities.
        futures = [q.fetch_async(limit) for q in queries]
        lists = sum((f.get_result() for f in futures), [])
        tasks = sorted(lists, key=lambda i: i.created_ts, reverse=True)[:limit]
      else:
        # Normal efficient behavior.
        cursor = datastore_query.Cursor(urlsafe=cursor_str)
        tasks, cursor, more = query.fetch_page(limit, start_cursor=cursor)
        cursor_str = cursor.urlsafe() if cursor and more else None

    return tasks, cursor_str, sort, state

  def _get_counts_future(self, now):
    """Returns all the counting futures in parallel."""
    counts_future = {}
    last_24h = now - datetime.timedelta(days=1)
    for state_key, _, _ in itertools.chain.from_iterable(self.STATE_CHOICES):
      _, query = self._get_query(None, state_key)
      if query:
        counts_future[state_key] = query.filter(
            task_result.TaskResultSummary.created_ts >= last_24h).count_async()
    return counts_future

  def _get_state_choices(self, counts_future):
    """Converts STATE_CHOICES with _get_counts_future() into nice text."""
    # Appends the number of tasks for each filter. It gives a sense of how much
    # things are going on.
    counts = {k: v.get_result() for k, v in counts_future.iteritems()}
    state_choices = []
    for choice_list in self.STATE_CHOICES:
      state_choices.append([])
      for state_key, name, title in choice_list:
        if state_key in counts:
          name += ' (%d)' % counts[state_key]
        elif state_key == 'pending_running':
          name += ' (%d)' % (counts['pending'] + counts['running'])
        state_choices[-1].append((state_key, name, title))
    return state_choices

  def _get_query(self, sort, state):
    """Returns one or many TaskResultSummary queries."""
    query = task_result.TaskResultSummary.query()
    if sort:
      order = datastore_query.PropertyOrder(
          sort, datastore_query.PropertyOrder.DESCENDING)
      query = query.order(order)

    if state == 'pending':
      return None, query.filter(
          task_result.TaskResultSummary.state == task_result.State.PENDING)

    if state == 'running':
      return None, query.filter(
          task_result.TaskResultSummary.state == task_result.State.RUNNING)

    if state == 'pending_running':
      # This is a special case that sends two concurrent queries under the hood.
      # ndb.OR() doesn't work when order() is used, it requires __key__ sorting.
      # This is not efficient, so the DB should be updated accordingly to be
      # able to support pagination.
      queries = [
        query.filter(
            task_result.TaskResultSummary.state == task_result.State.PENDING),
        query.filter(
            task_result.TaskResultSummary.state == task_result.State.RUNNING),
      ]
      return queries, None

    if state == 'completed':
      return None, query.filter(
          task_result.TaskResultSummary.state == task_result.State.COMPLETED)

    if state == 'completed_success':
      query = query.filter(
          task_result.TaskResultSummary.state == task_result.State.COMPLETED)
      return None, query.filter(task_result.TaskResultSummary.failure == False)

    if state == 'completed_failure':
      query = query.filter(
          task_result.TaskResultSummary.state == task_result.State.COMPLETED)
      return None, query.filter(task_result.TaskResultSummary.failure == True)

    if state == 'expired':
      return None, query.filter(
          task_result.TaskResultSummary.state == task_result.State.EXPIRED)

    # TODO(maruel): This is never set until the new bot API is writen.
    # https://code.google.com/p/swarming/issues/detail?id=117
    if state == 'timed_out':
      return None, query.filter(
          task_result.TaskResultSummary.state == task_result.State.TIMED_OUT)

    if state == 'bot_died':
      return None, query.filter(
          task_result.TaskResultSummary.state == task_result.State.BOT_DIED)

    if state == 'canceled':
      return None, query.filter(
          task_result.TaskResultSummary.state == task_result.State.CANCELED)

    if state == 'all':
      return None, query

    self.abort(400, 'invalid state')


class TaskHandler(auth.AuthenticatingHandler):
  """Show the full text of a test request."""

  @auth.require(acl.is_user)
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
      except (NotImplementedError, ValueError):
        self.abort(404, 'Invalid key format.')

    # 'result' can be either a TaskRunResult or TaskResultSummary.
    result, request = ndb.get_multi([key, request_key])
    if not result:
      self.abort(404, 'Invalid key.')

    if not acl.is_privileged_user():
      self.abort(403, 'Implement access control based on the user')

    bot_id = result.bot_id
    following_task_future = None
    previous_task_future = None
    if result.started_ts:
      # Use a shortcut name because it becomes unwieldy otherwise.
      cls = task_result.TaskRunResult

      # Note that the links will be to the TaskRunResult, not to
      # TaskResultSummary.
      following_task_future = cls.query(
          cls.bot_id == bot_id,
          cls.started_ts > result.started_ts,
          ).order(cls.started_ts).get_async()
      previous_task_future = cls.query(
          cls.bot_id == bot_id,
          cls.started_ts < result.started_ts,
          ).order(-cls.started_ts).get_async()

    bot_future = (
        bot_management.get_bot_key(bot_id).get_async() if bot_id else None)

    following_task_id = None
    following_task_name = None
    previous_task_id = None
    previous_task_name = None
    if following_task_future:
      following_task = following_task_future.get_result()
      if following_task:
        following_task_id = task_common.pack_run_result_key(following_task.key)
        following_task_name = following_task.name
    if previous_task_future:
      previous_task = previous_task_future.get_result()
      if previous_task:
        previous_task_id = task_common.pack_run_result_key(previous_task.key)
        previous_task_name = previous_task.name

    params = {
      'bot': bot_future.get_result() if bot_future else None,
      'is_privileged_user': acl.is_privileged_user(),
      'following_task_id': following_task_id,
      'following_task_name': following_task_name,
      'now': utils.utcnow(),
      'previous_task_id': previous_task_id,
      'previous_task_name': previous_task_name,
      'request': request,
      'task': result,
    }
    self.response.out.write(template.render('swarming/user_task.html', params))


### New Client APIs.


class ClientHandshakeHandler(auth.ApiHandler):
  """First request to be called to get initial data like XSRF token.

  Request body is a JSON dict:
    {
      # TODO(maruel): Add useful data.
    }

  Response body is a JSON dict:
    {
      "server_version": "138-193f1f3",
      "xsrf_token": "......",
    }
  """

  # This handler is called to get XSRF token, there's nothing to enforce yet.
  xsrf_token_enforce_on = ()

  EXPECTED_KEYS = frozenset()

  @auth.require_xsrf_token_request
  @auth.require(acl.is_bot_or_user)
  def post(self):
    request = self.parse_body()
    log_unexpected_keys(
        self.EXPECTED_KEYS, request, self.request, 'client', 'keys')
    data = {
      # This access token will be used to validate each subsequent request.
      'server_version': utils.get_app_version(),
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.send_response(data)


class ClientTaskResultHandler(auth.ApiHandler):
  @auth.require(acl.is_bot_or_user)
  def get(self, key_id):
    # TODO(maruel): Users can only request their own task. Privileged users can
    # request any task.
    key = None
    try:
      key = task_scheduler.unpack_result_summary_key(key_id)
    except ValueError:
      try:
        key = task_scheduler.unpack_run_result_key(key_id)
      except ValueError:
        self.abort_with_error(400, error='Invalid key')

    result = key.get()
    if not result:
      self.abort_with_error(404, error='Task not found')
    self.send_response(utils.to_json_encodable(result))


class ClientApiBots(auth.ApiHandler):
  """Returns the list of known swarming bots."""

  @auth.require(acl.is_privileged_user)
  def get(self):
    now = utils.utcnow()
    limit = int(self.request.get('limit', 1000))
    cursor = datastore_query.Cursor(urlsafe=self.request.get('cursor'))
    q = bot_management.Bot.query().order(bot_management.Bot.key)
    bots, cursor, more = q.fetch_page(limit, start_cursor=cursor)
    data = {
      'bots': [b.to_dict_with_now(now) for b in bots],
      'death_timeout': bot_management.BOT_DEATH_TIMEOUT.total_seconds(),
      'cursor': cursor.urlsafe() if cursor and more else None,
      'limit': limit,
      'now': now,
    }
    self.send_response(utils.to_json_encodable(data))


### Old Client APIs.


class ApiBots(auth.AuthenticatingHandler):
  """Returns the list of known swarming bots."""

  @auth.require(acl.is_privileged_user)
  def get(self):
    now = utils.utcnow()
    params = {
        'machine_death_timeout':
            int(bot_management.BOT_DEATH_TIMEOUT.total_seconds()),
        'machines': sorted(
            (m.to_dict_with_now(now) for m in bot_management.Bot.query()),
            key=lambda x: x['id']),
        'now': now,
    }
    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.headers['Cache-Control'] = 'no-cache, no-store'
    self.response.write(utils.encode_to_json(params))


class DeleteMachineStatsHandler(auth.AuthenticatingHandler):
  """Handler to delete a bot assignment."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(acl.is_bot_or_admin)
  def post(self):
    # TODO(maruel): Implement:
    # - The bot can only delete itself.
    # - The admin can delete any bot.
    bot_key = bot_management.get_bot_key(self.request.get('r', ''))
    if bot_key.get():
      bot_key.delete()
      self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
      self.response.out.write('Machine Assignment removed.')
    else:
      self.response.set_status(204)


class TestRequestHandler(auth.AuthenticatingHandler):
  """Handles task requests from clients."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(acl.is_bot_or_user)
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

    # If the priority is below 100, make the the user has right to do so.
    if request_properties['priority'] < 100 and not acl.is_bot_or_admin():
      # Silently drop the priority of normal users.
      request_properties['priority'] = 100

    _, result_summary = task_scheduler.make_request(request_properties)
    out = {
      # Return the priority actually used. This enables the client code to print
      # a warning if the priority was dynamically lowered.
      'priority': request_properties['priority'],
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

  @auth.require(acl.is_bot_or_user)
  def get(self):
    """Returns a list of TaskResultSummary ndb.Key."""
    # TODO(maruel): Users can only request their own task. Privileged users can
    # request any task.
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


class GetResultHandler(auth.AuthenticatingHandler):
  """Show the full result string from a test runner."""

  @auth.require(acl.is_bot_or_user)
  def get(self):
    # TODO(maruel): Users can only request their own task. Privileged users can
    # request any task.
    key_id = self.request.get('r', '')
    # TODO(maruel): Formalize returning data for a specific try or the overall
    # results.
    key = None
    try:
      key = task_scheduler.unpack_result_summary_key(key_id)
    except ValueError:
      try:
        key = task_scheduler.unpack_run_result_key(key_id)
      except ValueError:
        self.response.set_status(400)
        self.response.out.write('Invalid key')
        return

    result = key.get()
    if not result:
      # TODO(maruel): Use 404 if not present.
      self.response.set_status(204)
      logging.info('Unable to provide runner results [key: %s]', key_id)
      return

    # The old API does not support stdout streaming, so do not provide any
    # details until the task is completed. This whole handler will be deleted
    # afterward.
    exit_codes = None
    output = None
    if result.state in task_result.State.STATES_NOT_RUNNING:
      exit_codes = ','.join(map(str, result.exit_codes))
      output = u'\n'.join(
          i.decode('utf-8', 'replace') for i in result.get_outputs())
    results = {
      'exit_codes': exit_codes,
      'machine_id': result.bot_id,
      'machine_tag': result.bot_id,
      'config_instance_index': 0,
      'num_config_instances': 1,
      'output': output,
    }

    self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
    self.response.out.write(json.dumps(results))


class CancelHandler(auth.AuthenticatingHandler):
  """Cancel a test runner that is not already running."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(acl.is_admin)
  def post(self):
    """Ensures that the associated TaskToRun is canceled and update the
    TaskResultSummary accordingly.

    TODO(maruel): If a bot is running the task, mark the TaskRunResult as
    canceled and tell the bot on the next ping to reboot itself.
    """
    # TODO(maruel): Users can cancel their own task.
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
    result_summary.abandoned_ts = utils.utcnow()
    result_summary.put()
    self.response.out.write('Runner canceled.')


class RetryHandler(auth.AuthenticatingHandler):
  """Retry a test runner again."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(acl.is_admin)
  def post(self):
    """Duplicates the original request into a new one.

    Only change the ownership to the user that requested the retry.
    TaskProperties is unchanged.
    """
    # TODO(maruel): Users can cancel their own task.
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
      'tags': request.tags,
    }
    # TODO(maruel): The user needs to have a way to get the new task id.
    task_scheduler.make_request(data)

    # TODO(maruel): Return the new TaskRequest key.
    # TODO(maruel): Use json encoded return values for the APIs.
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Runner set for retry.')


### Bot download URLs.


class BootstrapHandler(auth.AuthenticatingHandler):
  """Returns python code to run to bootstrap a swarming bot."""

  @auth.require(acl.is_bot)
  def get(self):
    self.response.headers['Content-Type'] = 'text/x-python'
    self.response.headers['Content-Disposition'] = (
        'attachment; filename="swarming_bot_bootstrap.py"')
    self.response.out.write(bot_management.get_bootstrap(self.request.host_url))


class GetSlaveCodeHandler(auth.AuthenticatingHandler):
  """Returns a zip file with all the files required by a slave.

  Optionally specify the hash version to download. If so, the returned data is
  cacheable.
  """

  @auth.require(acl.is_bot)
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


### New Bot APIs.


class BotHandshakeHandler(auth.ApiHandler):
  """First request to be called to get initial data like XSRF token.

  The bot is server-controled so the server doesn't have to support multiple API
  version. Only the current one and the last one, since the bot are finishing
  their currently running task, they'll be immediately be upgraded after on
  their next poll.

  Request body is a JSON dict:
    {
      "attributes": {
        "dimensions": <dict of properties>,
        "id": <bot id>,
        "ip": <ip address as a string>,
        "version": <sha-1 of swarming_bot.zip uncompressed content>,

        # Optional:

        # If an exception occured:
        "error": <exception as a string>,

        # If the bot decided to quarantine itself.
        "quarantined": True,
      }
      # Differentiate between local_test_runner and slave_machine:
      #"is_local_test_runner", False,
    }

  Response body is a JSON dict:
    {
      "bot_version": <sha-1 of swarming_bot.zip uncompressed content>,
      "server_version": "138-193f1f3",
      "xsrf_token": "......",
    }
  """

  # This handler is called to get XSRF token, there's nothing to enforce yet.
  xsrf_token_enforce_on = ()

  EXPECTED_KEYS = frozenset([u'attributes'])
  ACCEPTED_ATTRIBUTES = frozenset(
      [u'dimensions', u'id', u'ip', u'quarantined', u'version'])
  REQUIRED_ATTRIBUTES = frozenset([u'dimensions', u'id', u'ip', u'version'])

  @auth.require_xsrf_token_request
  @auth.require(acl.is_bot)
  def post(self):
    # In particular, this endpoint does not return commands to the bot, for
    # example to upgrade itself. It'll be told so when it does its first poll.
    request = self.parse_body()
    # If attributes are missing, the bot is severely hosed! We don't refuse it
    # so the bot has a chance to self-heal. Obviously, it won't be able to reap
    # any task.

    log_unexpected_keys(
        self.EXPECTED_KEYS, request, self.request, 'bot', 'keys')
    attributes = request.get('attributes', {})
    log_unexpected_subset_keys(
        self.ACCEPTED_ATTRIBUTES, self.REQUIRED_ATTRIBUTES, attributes,
        self.request, 'bot', 'attributes')

    bot_id = attributes.get('id')
    dimensions = attributes.get('dimensions', {})
    quarantined = attributes.get('quarantined', False)
    if bot_id:
      bot_management.tag_bot_seen(
          bot_id,
          dimensions.get('hostname'),
          attributes.get('ip'),
          self.request.remote_addr,
          dimensions,
          attributes['version'],
          quarantined)
    # Let it live even if bot_id is not set, so the bot has a chance to
    # self-update. It won't be assigned any task anyway.
    data = {
      # This access token will be used to validate each subsequent request.
      'bot_version': bot_management.get_slave_version(self.request.host_url),
      'server_version': utils.get_app_version(),
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.send_response(data)


class BotPollHandler(auth.ApiHandler):
  """The bot polls for a task; returns either a task, update command or sleep.

  The only attributes required are the id. In case of exception on the slave,
  this is enough to get it just far enough to eventually self-update to a
  working version. This is to ensure that coding errors in bot code doesn't kill
  all the fleet at once, they should still be up just enough to be able to
  self-update again even if they don't get task assigned anymore.
  """
  EXPECTED_KEYS = frozenset([u'attributes', u'sleep_streak'])
  ACCEPTED_ATTRIBUTES = frozenset(
      [u'dimensions', u'id', u'ip', u'quarantined', u'version'])
  REQUIRED_ATTRIBUTES = frozenset([u'dimensions', u'id', u'ip', u'version'])

  @auth.require(acl.is_bot)
  def post(self):
    request = self.parse_body()

    log_unexpected_keys(
        self.EXPECTED_KEYS, request, self.request, 'bot', 'keys')
    attributes = request.get('attributes', {})
    log_unexpected_subset_keys(
        self.ACCEPTED_ATTRIBUTES, self.REQUIRED_ATTRIBUTES, attributes,
        self.request, 'bot', 'attributes')

    sleep_streak = request.get('sleep_streak', 0)
    # Be very permissive on missing values. This can happen because of errors on
    # the bot, *we don't want to deny them the capacity to update*, so that the
    # bot code is eventually fixed and the bot self-update to this working code.
    # It makes fleet management much easier.
    dimensions = attributes.get('dimensions', {})
    bot_id = attributes.get('id')
    # The bot may decide to "self-quarantine" itself.
    quarantined = attributes.get('quarantined', False)

    bot_future = None
    if bot_id:
      bot_future = bot_management.get_bot_key(bot_id).get_async()

    # The bot version is host-specific, because the host determines the version
    # being used.
    expected_version = bot_management.get_slave_version(self.request.host_url)
    if attributes.get('version') != expected_version:
      self._cmd_update(expected_version)
      if bot_future:
        bot_future.wait()
      return

    if not bot_id:
      self._cmd_sleep(sleep_streak, True)
      return

    bot = bot_future.get_result()
    # The server may decide to quarantine the bot.
    quarantined = quarantined or (bot.quarantined if bot else True)

    if (not quarantined and
        task_to_run.dimensions_powerset_count(dimensions) >
            task_to_run.MAX_DIMENSIONS):
      # It's a big deal, alert the admins.
      ereporter2.log_request(
          self.request,
          source='bot',
          message='Too many dimensions on bot')
      quarantined = True

    if not quarantined:
      # Note its existence at two places, one for stats at 1 minute resolution,
      # the other for the list of known bots.
      stats.add_entry(action='bot_active', bot_id=bot_id, dimensions=dimensions)

    bot_management.tag_bot_seen(
        bot_id,
        dimensions.get('hostname'),
        attributes.get('ip'),
        self.request.remote_addr,
        dimensions,
        attributes['version'],
        quarantined)

    if quarantined:
      self._cmd_sleep(sleep_streak, quarantined)
      return

    # The bot is in good shape. Try to grab a task.
    try:
      # This is a fairly complex function call, exceptions are expected.
      request, run_result = task_scheduler.bot_reap_task(dimensions, bot_id)
      if not request:
        # No task found, tell it to sleep a bit.
        self._cmd_sleep(sleep_streak, False)
        return

      self._cmd_run(request, run_result.key, bot_id)
    except runtime.DeadlineExceededError:
      # If the timeout happened before a task was assigned there is no problems.
      # If the timeout occurred after a task was assigned, that task will
      # timeout (BOT_DIED) since the bot didn't get the details required to
      # run it) and it will automatically get retried (TODO) when the task times
      # out.
      # TODO(maruel): Note the task if possible and hand it out on next poll.
      # https://code.google.com/p/swarming/issues/detail?id=130
      self.abort(500, 'Deadline')

  def _cmd_run(self, request, run_result_key, bot_id):
    out = {
      'cmd': 'run',
      'manifest': {
        'bot_id': bot_id,
        'commands': request.properties.commands,
        'data': request.properties.data,
        'env': request.properties.env,
        'hard_timeout': request.properties.execution_timeout_secs,
        'io_timeout': request.properties.io_timeout_secs,
        'task_id': task_common.pack_run_result_key(run_result_key),
      },
    }
    self.send_response(out)

  def _cmd_sleep(self, sleep_streak, quarantined):
    out = {
      'cmd': 'sleep',
      'duration': task_scheduler.exponential_backoff(sleep_streak),
      'quarantined': quarantined,
    }
    self.send_response(out)

  def _cmd_update(self, expected_version):
    out = {
      'cmd': 'update',
      'version': expected_version,
    }
    self.send_response(out)


class BotErrorHandler(auth.ApiHandler):
  """Specialized version of ereporter2's /ereporter2/api/v1/on_error.

  This formally quarantines the bot and sends an alert to the admins. It is
  meant to be used by slave_machine.py for non-recoverable issues, for example
  when failing to self update.
  """
  EXPECTED_KEYS = frozenset([u'id', u'message'])

  @auth.require(acl.is_bot)
  def post(self):
    request = self.parse_body()
    log_unexpected_keys(
        self.EXPECTED_KEYS, request, self.request, 'bot', 'keys')

    bot_id = request.get('id', 'unknown')
    # When a bot reports here, it is borked, quarantine the bot.
    bot = bot_management.get_bot_key(bot_id).get()
    bot.quarantined = True
    bot.put()

    ereporter2.log(source='bot', message=request.get('message', 'unknown'))
    self.send_response({})


class BotTaskUpdateHandler(auth.ApiHandler):
  """Receives updates from a Bot for a task.

  The handler verifies packets are processed in order and will refuse
  out-of-order packets.
  """
  ACCEPTED_KEYS = frozenset(
      [
        u'command_index', u'duration', u'exit_code', u'hard_timeout', u'id',
        u'io_timeout', u'output', u'packet_number', u'task_id',
      ])
  REQUIRED_KEYS = frozenset([u'command_index', u'id', u'task_id'])

  @auth.require(acl.is_bot)
  def post(self):
    # Unlike handshake and poll, we do not accept invalid keys here. This code
    # path is much more strict.
    request = self.parse_body()
    msg = log_unexpected_subset_keys(
        self.ACCEPTED_KEYS, self.REQUIRED_KEYS, request, self.request, 'bot',
        'keys')
    if msg:
      self.abort_with_error(400, error=msg)

    bot_id = request['id']
    command_index = request['command_index']
    task_id = request['task_id']

    duration = request.get('duration')
    exit_code = request.get('exit_code')
    out = request.get('output')
    packet_number = request.get('packet_number')

    # TODO(maruel): Make use of io_timeout and hard_timeout.

    run_result_key = task_scheduler.unpack_run_result_key(task_id)
    # Side effect: zaps out any binary content on stdout.
    if out is not None:
      out = out.encode('utf-8', 'replace')

    with task_scheduler.bot_update_task_new(
        run_result_key, bot_id, command_index, packet_number, out,
        exit_code, duration) as entities:
      ndb.put_multi(entities)

    # TODO(maruel): When a task is canceled, reply with 'DIE' so that the bot
    # reboots itself to abort the task abruptly. It is useful when a task hangs
    # and the timeout was set too long or the task was superseded by a newer
    # task with more recent executable (e.g. a new Try Server job on a newer
    # patchset on Rietveld).
    self.send_response({})


class BotTaskErrorHandler(auth.ApiHandler):
  """It is a specialized version of ereporter2's /ereporter2/api/v1/on_error
  that also attaches a task id to it.

  This formally kills the task, marking it as an internal failure. This can be
  used by slave_machine.py to kill the task when local_test_runner misbehaved.
  """
  EXPECTED_KEYS = frozenset([u'id', u'message', u'task_id'])

  @auth.require(acl.is_bot)
  def post(self):
    request = self.parse_body()
    msg = log_unexpected_keys(
        self.EXPECTED_KEYS, request, self.request, 'bot', 'keys')

    ereporter2.log(source='bot', message=request.get('message', 'unknown'))
    if msg:
      self.abort_with_error(400, error=msg)

    bot_id = request['id']
    task_id = request['task_id']
    run_result = task_scheduler.unpack_run_result_key(task_id).get()
    if run_result.bot_id != bot_id:
      msg = 'Bot %s sent task kill for task %s owned by bot %s' % (
          bot_id, run_result.bot_id, run_result.key)
      logging.error(msg)
      self.abort_with_error(400, error=msg)

    task_scheduler.bot_kill_task(run_result)
    self.send_response({})


### Old Bot APIs (To be deleted).


class ServerPingHandler(webapp2.RequestHandler):
  """Handler to ping when checking if the server is up.

  This handler should be extremely lightweight. It shouldn't do any
  computations, it should just state that the server is up. It's open to
  everyone for simplicity and performance.
  """

  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Server up')


class RegisterHandler(auth.AuthenticatingHandler):
  """Handler for the register_machine of the Swarm server.

  Attempt to find a matching job for the querying bot.
  """

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @decorators.silence(
      datastore_errors.InternalError,
      datastore_errors.Timeout,
      datastore_errors.TransactionFailedError)
  @auth.require(acl.is_bot)
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
      # runner will timeout (since the bot didn't get the details required
      # to run it) and it will automatically get retried when the bot "timeout".
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

  @auth.require(acl.is_bot)
  def post(self):
    # TODO(vadimsh): Any bot can send ping on behalf of any other bot. Ensure
    # 'id' matches credentials used to authenticate the request (i.e.
    # auth.get_current_identity()). In practice this doesn't work since the
    # Swarming bot id may not have any relationship to the host accessing the
    # swarming server.
    packed_run_result_key = self.request.get('r', '')
    bot_id = self.request.get('id', '')
    try:
      run_result_key = task_scheduler.unpack_run_result_key(
          packed_run_result_key)
      task_scheduler.bot_update_task_old(run_result_key, bot_id, None, None)
    except ValueError as e:
      logging.error('Failed to accept value %s: %s', packed_run_result_key, e)
      self.abort(400, 'Runner failed to ping.')

    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


class ResultHandler(auth.AuthenticatingHandler):
  """Handles test results from remote test runners."""

  # TODO(vadimsh): Implement XSRF token support.
  xsrf_token_enforce_on = ()

  @auth.require(acl.is_bot)
  def post(self):
    packed = self.request.get('r', '')
    run_result_key = task_scheduler.unpack_run_result_key(packed)
    bot_id = urllib.unquote_plus(self.request.get('id'))
    run_result = run_result_key.get()
    # TODO(vadimsh): Verify bot_id matches credentials that are used for
    # current request (i.e. auth.get_current_identity()). Or shorter, just use
    # auth.get_current_identity() and have the bot stops passing id=foo at all.
    if bot_id != run_result.bot_id:
      # Check that bot that posts the result is same as bot that claimed the
      # task.
      msg = 'Expected bot id %s, got %s' % (run_result.bot_id, bot_id)
      logging.error(msg)
      self.abort(404, msg)

    exit_codes = urllib.unquote_plus(self.request.get('x'))
    exit_codes = filter(None, (i.strip() for i in exit_codes.split(',')))

    # TODO(maruel): Get rid of this: the result string should probably be in the
    # body of the request.
    result_string = urllib.unquote_plus(self.request.get('result_output'))
    if isinstance(result_string, unicode):
      # Zap out any binary content on stdout.
      result_string = result_string.encode('utf-8', 'replace')

    task_scheduler.bot_update_task_old(
        run_result.key, bot_id, map(int, exit_codes), result_string)

    # TODO(maruel): Return JSON.
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Successfully update the runner results.')


class Result2Handler(auth.AuthenticatingHandler):
  """Handles test results from remote test runners."""

  xsrf_token_enforce_on = ()

  @auth.require(acl.is_bot)
  def post(self):
    packed = self.request.get('r', '')
    run_result_key = task_scheduler.unpack_run_result_key(packed)
    bot_id = urllib.unquote_plus(self.request.get('id'))
    run_result = run_result_key.get()
    if bot_id != run_result.bot_id:
      # Check that bot that posts the result is same as bot that claimed the
      # task.
      msg = 'Expected bot id %s, got %s' % (run_result.bot_id, bot_id)
      logging.error(msg)
      self.abort(404, msg)

    exit_codes = urllib.unquote_plus(self.request.get('x'))
    exit_codes = filter(None, (i.strip() for i in exit_codes.split(',')))

    result_string = self.request.get('o').encode('utf-8', 'replace')
    task_scheduler.bot_update_task_old(
        run_result.key, bot_id, map(int, exit_codes), result_string)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Successfully update the runner results.')


class RemoteErrorHandler(auth.AuthenticatingHandler):
  """Handler to log an error reported by a bot."""

  xsrf_token_enforce_on = ()

  @auth.require(acl.is_bot)
  def post(self):
    ereporter2.log_request(
        request=self.request,
        source='bot',
        category='task_failure',
        message=self.request.get('m', ''))

    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.out.write('Success.')


### Public pages.


class RootHandler(auth.AuthenticatingHandler):
  @auth.public
  def get(self):
    params = {
      'host_url': self.request.host_url,
      'is_admin': acl.is_admin(),
      'is_bot': acl.is_bot(),
      'is_privileged_user': acl.is_privileged_user(),
      'is_user': acl.is_user(),
      'user_type': acl.get_user_type(),
    }
    self.response.out.write(template.render('swarming/root.html', params))


class DeadBotsCountHandler(webapp2.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    cutoff = utils.utcnow() - bot_management.BOT_DEATH_TIMEOUT
    count = bot_management.Bot.query().filter(
        bot_management.Bot.last_seen_ts < cutoff).count()
    self.response.out.write(str(count))


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    bot_management.get_swarming_bot_zip(self.request.host_url)
    utils.get_module_version_list(None, None)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


def create_application(debug):
  template.bootstrap()
  ereporter2.configure()

  routes = [
      # Frontend pages. They return HTML.
      # Public pages.
      ('/', RootHandler),
      ('/stats', stats_gviz.StatsSummaryHandler),
      ('/stats/dimensions/<dimensions:.+>', stats_gviz.StatsDimensionsHandler),
      ('/stats/user/<user:.+>', stats_gviz.StatsUserHandler),

      # User pages.
      ('/user/tasks', TasksHandler),
      ('/user/task/<key_id:[0-9a-fA-F]+>', TaskHandler),

      # Privileged user pages.
      ('/restricted/bots', BotsListHandler),
      ('/restricted/bot/<bot_id:.+>', BotHandler),

      # Admin pages.
      ('/restricted/whitelist_ip', WhitelistIPHandler),
      ('/restricted/upload_start_slave', UploadStartSlaveHandler),
      ('/restricted/upload_bootstrap', UploadBootstrapHandler),

      # Eventually accessible for client.
      ('/restricted/cancel', CancelHandler),
      ('/restricted/retry', RetryHandler),

      # Client API, in some cases also indirectly used by the frontend.
      ('/get_matching_test_cases', GetMatchingTestCasesHandler),
      ('/get_result', GetResultHandler),
      ('/test', TestRequestHandler),

      # Bot API.
      ('/bootstrap', BootstrapHandler),
      ('/get_slave_code', GetSlaveCodeHandler),
      ('/get_slave_code/<version:[0-9a-f]{40}>', GetSlaveCodeHandler),
      # Old bot API to be removed:
      ('/poll_for_test', RegisterHandler),
      ('/remote_error', RemoteErrorHandler),
      ('/result', ResultHandler),
      ('/result2', Result2Handler),
      ('/runner_ping', RunnerPingHandler),
      ('/server_ping', ServerPingHandler),

      # Both Client and Bot API. To be split in two and deleted.
      ('/delete_machine_stats', DeleteMachineStatsHandler),

      # The new APIs:
      ('/swarming/api/v1/bots', ApiBots),
      ('/swarming/api/v1/bots/dead/count', DeadBotsCountHandler),
      ('/swarming/api/v1/stats/summary/<resolution:[a-z]+>',
        stats_gviz.StatsGvizSummaryHandler),
      ('/swarming/api/v1/stats/dimensions/<dimensions:.+>/<resolution:[a-z]+>',
        stats_gviz.StatsGvizDimensionsHandler),
      ('/swarming/api/v1/stats/user/<user:.+>/<resolution:[a-z]+>',
        stats_gviz.StatsGvizUserHandler),

      # New Client API:
      ('/swarming/api/v1/client/bots', ClientApiBots),
      ('/swarming/api/v1/client/handshake', ClientHandshakeHandler),
      ('/swarming/api/v1/client/task/<key_id:[0-9a-fA-F]+>',
          ClientTaskResultHandler),

      # New Bot API:
      ('/swarming/api/v1/bot/error', BotErrorHandler),
      ('/swarming/api/v1/bot/handshake', BotHandshakeHandler),
      ('/swarming/api/v1/bot/poll', BotPollHandler),
      ('/swarming/api/v1/bot/task_update', BotTaskUpdateHandler),
      ('/swarming/api/v1/bot/task_error', BotTaskErrorHandler),

      ('/_ah/warmup', WarmupHandler),
  ]
  routes = [webapp2.Route(*i) for i in routes]

  # If running on a local dev server, allow bots to connect without prior
  # groups configuration. Useful when running smoke test.
  if utils.is_local_dev_server():
    acl.bootstrap_dev_server_acls()

  # TODO(maruel): Split backend into a separate module. For now add routes here.
  routes.extend(handlers_backend.get_routes())
  routes.extend(ereporter2.get_frontend_routes())

  return webapp2.WSGIApplication(routes, debug=debug)
