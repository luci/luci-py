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
import os
import re

import webapp2

from google.appengine import runtime
from google.appengine.api import search
from google.appengine.api import users
from google.appengine.datastore import datastore_query
from google.appengine.ext import ndb

import handlers_api
import handlers_bot
import handlers_backend
import mapreduce_jobs
import template
from components import auth
from components import utils
from server import acl
from server import bot_code
from server import bot_management
from server import config
from server import stats_gviz
from server import task_pack
from server import task_request
from server import task_result
from server import task_scheduler


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


# Helper class for displaying the sort options in html templates.
SortOptions = collections.namedtuple('SortOptions', ['key', 'name'])


### is_admin pages.

# TODO(maruel): Sort the handlers once they got their final name.


class RestrictedConfigHandler(auth.AuthenticatingHandler):
  @auth.require(acl.is_admin)
  def get(self):
    self.common(None)

  @auth.require(acl.is_admin)
  def post(self):
    # Convert MultiDict into a dict.
    params = {
      k: self.request.params.getone(k) for k in self.request.params
      if k not in ('keyid', 'xsrf_token')
    }
    params['bot_death_timeout_secs'] = int(params['bot_death_timeout_secs'])
    params['reusable_task_age_secs'] = int(params['reusable_task_age_secs'])
    cfg = config.settings(fresh=True)
    keyid = int(self.request.get('keyid', '0'))
    if cfg.key.integer_id() != keyid:
      self.common('Update conflict %s != %s' % (cfg.key.integer_id(), keyid))
      return
    cfg.populate(**params)
    cfg.store()
    self.common('Settings updated')

  def common(self, note):
    params = {
      'cfg': config.settings(fresh=True),
      'note': note,
      'path': self.request.path,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.write(
        template.render('swarming/restricted_config.html', params))


class UploadBotConfigHandler(auth.AuthenticatingHandler):
  """Stores a new bot_config.py script."""

  @auth.require(acl.is_admin)
  def get(self):
    bot_config = bot_code.get_bot_config()
    params = {
      'content': bot_config.content,
      'path': self.request.path,
      'when': bot_config.when,
      'who': bot_config.who,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.write(
        template.render('swarming/restricted_upload_bot_config.html', params))

  @auth.require(acl.is_admin)
  def post(self):
    script = self.request.get('script', '')
    if not script:
      self.abort(400, 'No script uploaded')

    bot_code.store_bot_config(script.encode('utf-8', 'replace'))
    self.get()


class UploadBootstrapHandler(auth.AuthenticatingHandler):
  """Stores a new bootstrap.py script."""

  @auth.require(acl.is_admin)
  def get(self):
    bootstrap = bot_code.get_bootstrap(self.request.host_url)
    params = {
      'content': bootstrap.content,
      'path': self.request.path,
      'when': bootstrap.when,
      'who': bootstrap.who,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.write(
        template.render('swarming/restricted_upload_bootstrap.html', params))

  @auth.require(acl.is_admin)
  def post(self):
    script = self.request.get('script', '')
    if not script:
      self.abort(400, 'No script uploaded')

    bot_code.store_bootstrap(script.encode('utf-8'))
    self.get()


### Mapreduce related handlers


class RestrictedLaunchMapReduceJob(auth.AuthenticatingHandler):
  """Enqueues a task to start a map reduce job on the backend module.

  A tree of map reduce jobs inherits module and version of a handler that
  launched it. All UI handlers are executes by 'default' module. So to run a
  map reduce on a backend module one needs to pass a request to a task running
  on backend module.
  """

  @auth.require(acl.is_admin)
  def post(self):
    job_id = self.request.get('job_id')
    assert job_id in mapreduce_jobs.MAPREDUCE_JOBS
    success = utils.enqueue_task(
        url='/internal/taskqueue/mapreduce/launch/%s' % job_id,
        queue_name=mapreduce_jobs.MAPREDUCE_TASK_QUEUE,
        use_dedicated_module=False)
    # New tasks should show up on the status page.
    if success:
      self.redirect('/restricted/mapreduce/status')
    else:
      self.abort(500, 'Failed to launch the job')


### acl.is_privileged_user pages.


class BotsListHandler(auth.AuthenticatingHandler):
  """Presents the list of known bots."""
  ACCEPTABLE_BOTS_SORTS = {
    'last_seen_ts': 'Last Seen',
    '-quarantined': 'Quarantined',
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

    if sort_by[0] == '-':
      order = datastore_query.PropertyOrder(
          sort_by[1:], datastore_query.PropertyOrder.DESCENDING)
    else:
      order = datastore_query.PropertyOrder(
          sort_by, datastore_query.PropertyOrder.ASCENDING)

    now = utils.utcnow()
    cutoff = now - datetime.timedelta(
        seconds=config.settings().bot_death_timeout_secs)

    num_bots_busy_future = bot_management.BotInfo.query(
        bot_management.BotInfo.is_busy == True).count_async()
    num_bots_dead_future = bot_management.BotInfo.query(
        bot_management.BotInfo.last_seen_ts < cutoff).count_async()
    num_bots_quarantined_future = bot_management.BotInfo.query(
        bot_management.BotInfo.quarantined == True).count_async()
    num_bots_total_future = bot_management.BotInfo.query().count_async()
    fetch_future = bot_management.BotInfo.query().order(order).fetch_page_async(
        limit, start_cursor=cursor)

    # TODO(maruel): self.request.host_url should be the default AppEngine url
    # version and not the current one. It is only an issue when
    # version-dot-appid.appspot.com urls are used to access this page.
    version = bot_code.get_bot_version(self.request.host_url)
    bots, cursor, more = fetch_future.get_result()
    # Prefetch the tasks. We don't actually use the value here, it'll be
    # implicitly used by ndb local's cache when refetched by the html template.
    tasks = filter(None, (b.task for b in bots))
    ndb.get_multi(tasks)
    num_bots_busy = num_bots_busy_future.get_result()
    num_bots_dead = num_bots_dead_future.get_result()
    num_bots_quarantined = num_bots_quarantined_future.get_result()
    num_bots_total = num_bots_total_future.get_result()
    params = {
      'bots': bots,
      'current_version': version,
      'cursor': cursor.urlsafe() if cursor and more else '',
      'is_admin': acl.is_admin(),
      'is_privileged_user': acl.is_privileged_user(),
      'limit': limit,
      'now': now,
      'num_bots_alive': num_bots_total - num_bots_dead,
      'num_bots_busy': num_bots_busy,
      'num_bots_dead': num_bots_dead,
      'num_bots_quarantined': num_bots_quarantined,
      'sort_by': sort_by,
      'sort_options': self.SORT_OPTIONS,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.write(
        template.render('swarming/restricted_botslist.html', params))


class BotHandler(auth.AuthenticatingHandler):
  """Returns data about the bot, including last tasks and events."""

  @auth.require(acl.is_privileged_user)
  def get(self, bot_id):
    # pagination is currently for tasks, not events.
    limit = int(self.request.get('limit', 100))
    cursor = datastore_query.Cursor(urlsafe=self.request.get('cursor'))
    bot_future = bot_management.get_info_key(bot_id).get_async()
    run_results, cursor, more = task_result.TaskRunResult.query(
        task_result.TaskRunResult.bot_id == bot_id).order(
            -task_result.TaskRunResult.started_ts).fetch_page(
                limit, start_cursor=cursor)

    events_future = bot_management.get_events_query(bot_id).fetch_async(100)

    now = utils.utcnow()
    bot = bot_future.get_result()
    # Calculate the time this bot was idle.
    idle_time = datetime.timedelta()
    run_time = datetime.timedelta()
    if run_results:
      run_time = run_results[0].duration_now(now) or datetime.timedelta()
      if not cursor and run_results[0].state != task_result.State.RUNNING:
        # Add idle time since last task completed. Do not do this when a cursor
        # is used since it's not representative.
        idle_time = now - run_results[0].ended_ts
      for index in xrange(1, len(run_results)):
        # .started_ts will always be set by definition but .ended_ts may be None
        # if the task was abandoned. We can't count idle time since the bot may
        # have been busy running *another task*.
        # TODO(maruel): One option is to add a third value "broken_time".
        # Looking at timestamps specifically could help too, e.g. comparing
        # ended_ts of this task vs the next one to see if the bot was assigned
        # two tasks simultaneously.
        if run_results[index].ended_ts:
          idle_time += (
              run_results[index-1].started_ts - run_results[index].ended_ts)
          duration = run_results[index].duration
          if duration:
            run_time += duration

    params = {
      'bot': bot,
      'bot_id': bot_id,
      'current_version': bot_code.get_bot_version(self.request.host_url),
      'cursor': cursor.urlsafe() if cursor and more else None,
      'events': events_future.get_result(),
      'idle_time': idle_time,
      'is_admin': acl.is_admin(),
      'limit': limit,
      'now': now,
      'run_results': run_results,
      'run_time': run_time,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.write(
        template.render('swarming/restricted_bot.html', params))


class BotDeleteHandler(auth.AuthenticatingHandler):
  """Deletes a known bot.

  This only deletes the BotInfo, not BotRoot, BotEvent's nor BotSettings.

  This is sufficient so the bot doesn't show up on the Bots page while keeping
  historical data.
  """

  @auth.require(acl.is_admin)
  def post(self, bot_id):
    bot_key = bot_management.get_info_key(bot_id)
    if bot_key.get():
      bot_key.delete()
    self.redirect('/restricted/bots')


### User accessible pages.


class TasksHandler(auth.AuthenticatingHandler):
  """Lists all requests and allows callers to manage them."""
  # Each entry is an item in the Sort column.
  # Each entry is (key, text, hover)
  SORT_CHOICES = [
    ('created_ts', 'Created', 'Most recently created tasks are shown first.'),
    ('modified_ts', 'Active',
      'Shows the most recently active tasks first. Using this order resets '
      'state to \'All\'.'),
    ('completed_ts', 'Completed',
      'Shows the most recently completed tasks first. Using this order resets '
      'state to \'All\'.'),
    ('abandoned_ts', 'Abandoned',
      'Shows the most recently abandoned tasks first. Using this order resets '
      'state to \'All\'.'),
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
        'Tasks that are still ready to be assigned to a bot. Using this order '
        'resets order to \'Created\'.'),
      ('running', 'Running',
        'Tasks being currently executed by a bot. Using this order resets '
        'order to \'Created\'.'),
      ('pending_running', 'Pending|running',
        'Tasks either \'pending\' or \'running\'. Using this order resets '
        'order to \'Created\'.'),
    ],
    [
      ('completed', 'Completed',
        'All tasks that are completed, independent if the task itself '
        'succeeded or failed. This excludes tasks that had an infrastructure '
        'failure. Using this order resets order to \'Created\'.'),
      ('completed_success', 'Successes',
        'Tasks that completed successfully. Using this order resets order to '
        '\'Created\'.'),
      ('completed_failure', 'Failures',
        'Tasks that were executed successfully but failed, e.g. exit code is '
        'non-zero. Using this order resets order to \'Created\'.'),
      ('timed_out', 'Timed out',
        'The execution timed out, so it was forcibly killed.'),
    ],
    [
      ('bot_died', 'Bot died',
        'The bot stopped sending updates while running the task, causing the '
        'task execution to time out. This is considered an infrastructure '
        'failure and the usual reason is that the bot BSOD\'ed or '
        'spontaneously rebooted. Using this order resets order to '
        '\'Created\'.'),
      ('expired', 'Expired',
        'The task was not assigned a bot until its expiration timeout, causing '
        'the task to never being assigned to a bot. This can happen when the '
        'dimension filter was not available or overloaded with a low priority. '
        'Either fix the priority or bring up more bots with these dimensions. '
        'Using this order resets order to \'Created\'.'),
      ('canceled', 'Canceled',
        'The task was explictly canceled by a user before it started '
        'executing. Using this order resets order to \'Created\'.'),
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
    task_tags = [
      line for line in self.request.get('task_tag', '').splitlines() if line
    ]

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
    try:
      tasks, cursor_str, sort, state = task_result.get_tasks(
          task_name, task_tags, cursor_str, limit, sort, state)

      # Prefetch the TaskRequest all at once, so that ndb's in-process cache has
      # it instead of fetching them one at a time indirectly when using
      # TaskResultSummary.request_key.get().
      futures = ndb.get_multi_async(t.request_key for t in tasks)

      # Evaluate the counts to print the filtering columns with the associated
      # numbers.
      state_choices = self._get_state_choices(counts_future)
    except (search.QueryError, ValueError) as e:
      self.abort(400, str(e))

    def safe_sum(items):
      return sum(items, datetime.timedelta())

    def avg(items):
      if not items:
        return 0.
      return safe_sum(items) / len(items)

    def median(items):
      if not items:
        return 0.
      middle = len(items) / 2
      if len(items) % 2:
        return items[middle]
      return (items[middle-1]+items[middle]) / 2

    gen = (t.duration_now(now) for t in tasks)
    durations = sorted(t for t in gen if t is not None)
    gen = (t.pending_now(now) for t in tasks)
    pendings = sorted(t for t in gen if t is not None)
    total_cost_usd = sum(t.cost_usd for t in tasks)
    total_cost_saved_usd = sum(
        t.cost_saved_usd for t in tasks if t.cost_saved_usd)
    total_saved = safe_sum(t.duration for t in tasks if t.deduped_from)
    duration_sum = safe_sum(durations)
    total_saved_percent = (
        (100. * total_saved.total_seconds() / duration_sum.total_seconds())
        if duration_sum else 0.)
    params = {
      'cursor': cursor_str,
      'duration_average': avg(durations),
      'duration_median': median(durations),
      'duration_sum': duration_sum,
      'has_pending': any(t.is_pending for t in tasks),
      'has_running': any(t.is_running for t in tasks),
      'is_admin': acl.is_admin(),
      'is_privileged_user': acl.is_privileged_user(),
      'limit': limit,
      'now': now,
      'pending_average': avg(pendings),
      'pending_median': median(pendings),
      'pending_sum': safe_sum(pendings),
      'show_footer': bool(pendings or durations),
      'sort': sort,
      'sort_choices': self.SORT_CHOICES,
      'state': state,
      'state_choices': state_choices,
      'task_name': task_name,
      'task_tag': '\n'.join(task_tags),
      'tasks': tasks,
      'total_cost_usd': total_cost_usd,
      'total_cost_saved_usd': total_cost_saved_usd,
      'total_saved': total_saved,
      'total_saved_percent': total_saved_percent,
      'xsrf_token': self.generate_xsrf_token(),
    }
    # TODO(maruel): If admin or if the user is task's .user, show the Cancel
    # button. Do not show otherwise.
    self.response.write(template.render('swarming/user_tasks.html', params))

    # Do not let dangling futures linger around.
    ndb.Future.wait_all(futures)

  def _get_counts_future(self, now):
    """Returns all the counting futures in parallel."""
    counts_future = {}
    last_24h = now - datetime.timedelta(days=1)
    request_id = task_request.datetime_to_request_base_id(last_24h)
    request_key = task_request.request_id_to_key(request_id)
    for state_key, _, _ in itertools.chain.from_iterable(self.STATE_CHOICES):
      query = task_result.get_result_summary_query(None, state_key, None)
      # It is counter intuitive but the equality has to be reversed, since the
      # value in the db is binary negated.
      counts_future[state_key] = query.filter(
          task_result.TaskResultSummary.key <= request_key).count_async()
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
        name += ' (%d)' % counts[state_key]
        state_choices[-1].append((state_key, name, title))
    return state_choices


class TaskHandler(auth.AuthenticatingHandler):
  """Show the full text of a task request.

  This handler supports both TaskResultSummary (ends with 0) or TaskRunResult
  (ends with 1 or 2).
  """

  @auth.require(acl.is_user)
  def get(self, task_id):
    try:
      key = task_pack.unpack_result_summary_key(task_id)
      request_key = task_pack.result_summary_key_to_request_key(key)
    except ValueError:
      try:
        key = task_pack.unpack_run_result_key(task_id)
        request_key = task_pack.result_summary_key_to_request_key(
            task_pack.run_result_key_to_result_summary_key(key))
      except (NotImplementedError, ValueError):
        self.abort(404, 'Invalid key format.')

    # 'result' can be either a TaskRunResult or TaskResultSummary.
    result_future = key.get_async()
    request_future = request_key.get_async()
    result = result_future.get_result()
    if not result:
      self.abort(404, 'Invalid key.')

    if not acl.is_privileged_user():
      self.abort(403, 'Implement access control based on the user')

    request = request_future.get_result()
    parent_task_future = None
    if request.parent_task_id:
      parent_key = task_pack.unpack_run_result_key(request.parent_task_id)
      parent_task_future = parent_key.get_async()
    children_tasks_futures = [
      task_pack.unpack_result_summary_key(c).get_async()
      for c in result.children_task_ids
    ]

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
        bot_management.get_info_key(bot_id).get_async() if bot_id else None)

    following_task = None
    if following_task_future:
      following_task = following_task_future.get_result()

    previous_task = None
    if previous_task_future:
      previous_task = previous_task_future.get_result()

    parent_task = None
    if parent_task_future:
      parent_task = parent_task_future.get_result()
    children_tasks = [c.get_result() for c in children_tasks_futures]

    params = {
      'bot': bot_future.get_result() if bot_future else None,
      'children_tasks': children_tasks,
      'is_admin': acl.is_admin(),
      'is_gae_admin': users.is_current_user_admin(),
      'is_privileged_user': acl.is_privileged_user(),
      'following_task': following_task,
      'full_appid': os.environ['APPLICATION_ID'],
      'host_url': self.request.host_url,
      'is_running': result.state == task_result.State.RUNNING,
      'now': utils.utcnow(),
      'parent_task': parent_task,
      'previous_task': previous_task,
      'request': request,
      'task': result,
      'xsrf_token': self.generate_xsrf_token(),
    }
    self.response.write(template.render('swarming/user_task.html', params))


class TaskCancelHandler(auth.AuthenticatingHandler):
  """Cancel a task.

  Ensures that the associated TaskToRun is canceled and update the
  TaskResultSummary accordingly.
  """

  @auth.require(acl.is_admin)
  def post(self):
    key_id = self.request.get('task_id', '')
    try:
      key = task_pack.unpack_result_summary_key(key_id)
    except ValueError:
      self.abort_with_error(400, error='Invalid key')
    redirect_to = self.request.get('redirect_to', '')

    task_scheduler.cancel_task(key)
    if redirect_to == 'listing':
      self.redirect('/user/tasks')
    else:
      self.redirect('/user/task/%s' % key_id)


class TaskRetryHandler(auth.AuthenticatingHandler):
  """Retries the same task but with new metadata.

  Retrying a task forcibly make it not idempotent so the task is inconditionally
  scheduled.

  This handler supports both TaskResultSummary (ends with 0) or TaskRunResult
  (ends with 1 or 2).
  """

  @auth.require(acl.is_privileged_user)
  def post(self, task_id):
    try:
      key = task_pack.unpack_result_summary_key(task_id)
      request_key = task_pack.result_summary_key_to_request_key(key)
    except ValueError:
      try:
        key = task_pack.unpack_run_result_key(task_id)
        request_key = task_pack.result_summary_key_to_request_key(
            task_pack.run_result_key_to_result_summary_key(key))
      except (NotImplementedError, ValueError):
        self.abort(404, 'Invalid key format.')

    # Retrying a task is essentially reusing the same task request as the
    # original one, but with new parameters.
    original_request = request_key.get()
    if not original_request:
      self.abort(404, 'Invalid request key.')
    new_request = task_request.make_request_clone(original_request)
    result_summary = task_scheduler.schedule_request(new_request)
    self.redirect('/user/task/%s' % result_summary.key_packed)


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
      'mapreduce_jobs': [],
      'user_type': acl.get_user_type(),
    }
    if acl.is_admin():
      params['mapreduce_jobs'] = [
        {'id': job_id, 'name': job_def['name']}
        for job_id, job_def in mapreduce_jobs.MAPREDUCE_JOBS.iteritems()
      ]
      params['xsrf_token'] = self.generate_xsrf_token()
    self.response.write(template.render('swarming/root.html', params))


class WarmupHandler(webapp2.RequestHandler):
  def get(self):
    auth.warmup()
    bot_code.get_swarming_bot_zip(self.request.host_url)
    utils.get_module_version_list(None, None)
    self.response.headers['Content-Type'] = 'text/plain; charset=utf-8'
    self.response.write('ok')


class EmailHandler(webapp2.RequestHandler):
  """Blackhole any email sent."""
  def post(self, to):
    pass


def create_application(debug):
  template.bootstrap()

  routes = [
      # Frontend pages. They return HTML.
      # Public pages.
      ('/', RootHandler),
      ('/stats', stats_gviz.StatsSummaryHandler),

      # User pages.
      ('/user/tasks', TasksHandler),
      ('/user/task/<task_id:[0-9a-fA-F]+>', TaskHandler),
      ('/user/task/<task_id:[0-9a-fA-F]+>/retry', TaskRetryHandler),
      ('/user/tasks/cancel', TaskCancelHandler),

      # Privileged user pages.
      ('/restricted/bots', BotsListHandler),
      ('/restricted/bot/<bot_id:[^/]+>', BotHandler),
      ('/restricted/bot/<bot_id:[^/]+>/delete', BotDeleteHandler),

      # Admin pages.
      ('/restricted/config', RestrictedConfigHandler),
      ('/restricted/upload/bot_config', UploadBotConfigHandler),
      ('/restricted/upload/bootstrap', UploadBootstrapHandler),

      # Mapreduce related urls.
      (r'/restricted/launch_mapreduce', RestrictedLaunchMapReduceJob),

      # The new APIs:
      ('/swarming/api/v1/stats/summary/<resolution:[a-z]+>',
        stats_gviz.StatsGvizSummaryHandler),
      ('/swarming/api/v1/stats/dimensions/<dimensions:.+>/<resolution:[a-z]+>',
        stats_gviz.StatsGvizDimensionsHandler),

      ('/_ah/mail/<to:.+>', EmailHandler),
      ('/_ah/warmup', WarmupHandler),
  ]
  routes = [webapp2.Route(*i) for i in routes]

  # If running on a local dev server, allow bots to connect without prior
  # groups configuration. Useful when running smoke test.
  if utils.is_local_dev_server():
    acl.bootstrap_dev_server_acls()

  # TODO(maruel): Split backend into a separate module. For now add routes here.
  routes.extend(handlers_backend.get_routes())
  routes.extend(handlers_api.get_routes())
  routes.extend(handlers_bot.get_routes())

  return webapp2.WSGIApplication(routes, debug=debug)
