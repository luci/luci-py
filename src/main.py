#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.
#

"""Main entry point for TRS service.

This file contains the URL handlers for all the TRS service URLs, implemented
using the appengine webapp framework.
"""

import datetime
import json
import logging
import os.path
import urllib

from google.appengine import runtime
from google.appengine.api import app_identity
from google.appengine.api import datastore_errors
from google.appengine.api import files
from google.appengine.api import mail
from google.appengine.api import mail_errors
from google.appengine.api import taskqueue
from google.appengine.api import users
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext import ereporter
from google.appengine.ext.ereporter.report_generator import ReportGenerator
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import util
from google.appengine.ext import ndb

from common import blobstore_helper
from common import test_request_message
from common import url_helper
from server import admin_user
from server import dimension_mapping
from server import test_management
from server import test_request
from server import test_runner
from server import user_manager
from stats import daily_stats
from stats import machine_stats
from stats import runner_stats
from stats import runner_summary
# pylint: enable=g-import-not-at-top

import webapp2  # pylint: disable=g-bad-import-order


_NUM_USER_TEST_RUNNERS_PER_PAGE = 50
_NUM_RECENT_ERRORS_TO_DISPLAY = 10

_HOME_LINK = '<a href=/secure/main>Home</a>'
_MACHINE_LIST_LINK = '<a href=/secure/machine_list>Machine List</a>'
_PROFILE_LINK = '<a href=/secure/user_profile>Profile</a>'
_STATS_LINK = '<a href=/stats>Stats</a>'

_SECURE_CANCEL_URL = '/secure/cancel'
_SECURE_CHANGE_WHITELIST_URL = '/secure/change_whitelist'
_SECURE_DELETE_MACHINE_STATS_URL = '/secure/delete_machine_stats'
_SECURE_GET_RESULTS_URL = '/secure/get_result'
_SECURE_MAIN_URL = '/secure/main'
_SECURE_USER_PROFILE_URL = '/secure/user_profile'

# Allow GET requests to be passed through as POST requests.
ALLOW_POST_AS_GET = False

# The domains that are allowed to access this application.
ALLOW_ACCESS_FROM_DOMAINS = ()


def GenerateTopbar():
  """Generate the topbar to display on all server pages.

  Returns:
    The topbar to display.
  """
  if users.get_current_user():
    topbar = ('%s |  <a href="%s">Sign out</a><br/>' %
              (users.get_current_user().nickname(),
               users.create_logout_url('/')))

    if users.is_current_user_admin():
      topbar += (' %s | %s | %s | %s' %
                 (_HOME_LINK,
                  _PROFILE_LINK,
                  _MACHINE_LIST_LINK,
                  _STATS_LINK))
  else:
    topbar = '<a href="%s">Sign in</a>' % users.create_login_url('/')

  return topbar


def GenerateStatLinks():
  """Generate the stat links to display on all stats pages.

  Returns:
    The stat links to display.
  """
  stat_links = (
      '<a href="/graphs/daily_stats">Graph of Daily Stats</a><br/>'
      '<a href="/runner_summary"">Pending and Waiting Summary</a>')

  return stat_links


def GenerateButtonWithHiddenForm(button_text, url, form_id):
  """Generate a button that when used will post to the given url.

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


def AuthenticateMachine(method):
  """Decorator for 'get' or 'post' methods that require machine authentication.

  Decorated method verifies that a remote machine is IP whitelisted. Methods
  marked by this decorator are indented to be called only by swarm slaves.

  Args:
    method: 'get' or 'post' method of RequestHandler subclass.

  Returns:
    Decorated method that verifies that machine is IP whitelisted.
  """
  def Wrapper(handler, *args, **kwargs):
    if not IsAuthenticatedMachine(handler.request):
      SendAuthenticationFailure(handler.request, handler.response)
    else:
      return method(handler, *args, **kwargs)
  return Wrapper


def AuthenticateMachineOrUser(method):
  """Decorator for 'get' or 'post' methods that require authentication.

  Decorated method verifies that a remote machine is IP whitelisted or
  the request is issued by a known user account. Methods marked by this
  decorator are indented to be called by swarm slaves or by users submitting
  tests to the swarm.

  Args:
    method: 'get' or 'post' method of RequestHandler subclass.

  Returns:
    Decorated method.
  """
  def Wrapper(handler, *args, **kwargs):
    # Check user account first, it's fast.
    user = users.get_current_user()
    if user and (users.is_current_user_admin() or IsAuthenticatedUser(user)):
      return method(handler, *args, **kwargs)

    # Check IP whitelist, it's slower.
    if IsAuthenticatedMachine(handler.request):
      return method(handler, *args, **kwargs)

    # Both checks failed, respond with error,
    SendAuthenticationFailure(handler.request, handler.response)
  return Wrapper


def IsAuthenticatedUser(user):
  """Check to see if the user is allowed to execute a request.

  Args:
    user: api.users.User with info about user that issued the request.

  Returns:
    True if the user is allowed to execute a request.
  """
  assert user
  domain = user.email().partition('@')[2]
  return domain in ALLOW_ACCESS_FROM_DOMAINS


def IsAuthenticatedMachine(request):
  """Check to see if the request is from a whitelisted machine.

  Will use the remote machine's IP and provided password (if any).

  Args:
    request: WebAPP request sent by remote machine.

  Returns:
    True if the request is from a whitelisted machine.
  """
  return user_manager.IsWhitelistedMachine(
      request.remote_addr, request.get('password', None))


def DaysToShow(request):
  """Find the number of days to show, according to the request.

  Args:
    request: A dictionary that might contain the days value, otherwise return
        the default days_to_show value.

  Returns:
    The number of days to show.
  """
  days_to_show = 7
  try:
    days_to_show = int(request.get('days', days_to_show))
  except ValueError:
    # Stick with the default value.
    pass

  return days_to_show


class SortOptions(object):
  """A basic helper class for displaying the sort options."""

  def __init__(self, key, name):
    self.key = key
    self.name = name


class MainHandler(webapp2.RequestHandler):
  """Handler for the main page of the web server.

  This handler lists all pending requests and allows callers to manage them.
  """

  def GetTimeString(self, dt):
    """Convert the datetime object to a user-friendly string.

    Arguments:
      dt: a datetime.datetime object.

    Returns:
      A string representing the datetime object to show in the web UI.
    """
    s = '--'

    if dt:
      midnight_today = datetime.datetime.now().replace(hour=0, minute=0,
                                                       second=0, microsecond=0)
      midnight_yesterday = midnight_today - datetime.timedelta(days=1)
      if dt > midnight_today:
        s = dt.strftime('Today at %H:%M')
      elif dt > midnight_yesterday:
        s = dt.strftime('Yesterday at %H:%M')
      else:
        s = dt.strftime('%d %b at %H:%M')

    return s

  def GeneratePageUrl(self, current_page=None, sort_by=None,
                      include_filters=False):
    """Generate a URL that points to the current page (it has the same options).

    If an option is listed as None, don't include it to allow the html page to
    ensure it can add its desired option.

    Args:
      current_page: The current page to display.
      sort_by: The value to sort the runners by.
      include_filters: True if the filters should be included in the url.

    Returns:
      The user for the described page.
    """
    url = '?'

    if current_page:
      url += 'page=' + str(current_page) + '&'

    if sort_by:
      url += 'sort_by=' + sort_by + '&'

    if include_filters:
      if self.status is not None:
        url += 'status=' + self.status + '&'

      if self.show_successfully_completed is not None:
        url += ('show_successfully_completed=' +
                str(self.show_successfully_completed) +'&')

      if self.test_name_filter is not None:
        url += 'test_name_filter=' + self.test_name_filter + '&'

      if self.machine_id_filter is not None:
        url += 'machine_id_filter=' + self.machine_id_filter + '&'

    # Remove the trailing &.
    url = url[:-1]

    return url

  def ParseFilters(self):
    """Parse the filters from the request."""
    self.status = self.request.get('status', '')

    # Compare to 'False' so that the default value for invalid user input
    # is True.
    self.show_successfully_completed = (
        self.request.get('show_successfully_completed', '') != 'False')

    self.test_name_filter = self.request.get('test_name_filter', '')
    self.machine_id_filter = self.request.get('machine_id_filter', '')

  def GetTestRunners(self, sort_by=None, ascending=None, limit=None,
                     offset=None):
    """Get a query with the given parameters, also applying the filters.

    Args:
      sort_by: The value to sort the runners by.
      ascending: True if the runners should be sorted in ascending order.
      limit: The maximum number of runners the query should return.
      offset: Number of queries to skip.

    Returns:
      The TestRunner query that is properly adjusted and filtered.
    """
    # If we are filtering for running runners, then we will test for inequality
    # on the started field. App engine requires any fields used in an
    # inequality be the first sorts that are applied to the query.
    sort_by_first = 'started' if self.status == 'running' else None

    query = test_runner.GetTestRunners(sort_by, ascending, limit, offset,
                                       sort_by_first)

    return test_runner.ApplyFilters(query, self.status,
                                    self.show_successfully_completed,
                                    self.test_name_filter,
                                    self.machine_id_filter)

  def FilterSelectsHTML(self):
    """Generates the HTML filter select values, with the proper defaults set.

    Returns:
      The HTML representing the filter select options.
    """
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

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    # Build info for test requests table.
    sort_by = self.request.get('sort_by')
    sort_split = sort_by.split('_', 1)
    ascending = not sort_split[0]
    sort_key = sort_split[1] if len(sort_split) > 1 else ''

    # Make sure that the sort key is a valid option.
    if sort_key not in test_runner.ACCEPTABLE_SORTS:
      sort_key = 'created'
      ascending = False

    sorted_by_message = '<p>Currently sorted by: '
    if not ascending:
      sorted_by_message += 'Reverse '
    sorted_by_message += test_runner.ACCEPTABLE_SORTS[sort_key] + '</p>'

    # Prase and load the filters
    self.ParseFilters()

    runner_count_async = self.GetTestRunners().count_async()

    try:
      page = int(self.request.get('page', 1))
    except ValueError:
      page = 1

    runners = []
    for runner in self.GetTestRunners(
        sort_key,
        ascending=ascending,
        limit=_NUM_USER_TEST_RUNNERS_PER_PAGE,
        offset=_NUM_USER_TEST_RUNNERS_PER_PAGE * (page - 1)):
      self._GetDisplayableRunnerTemplate(runner)
      runners.append(runner)

    errors = []
    query = test_management.SwarmError.query(
        default_options=ndb.QueryOptions(
            limit=_NUM_RECENT_ERRORS_TO_DISPLAY)).order(
                -test_management.SwarmError.created)
    for error in query:
      error.log_time = self.GetTimeString(error.created)
      errors.append(error)

    sort_options = []
    for key, value in test_runner.ACCEPTABLE_SORTS.iteritems():
      # Add the leading _ to the non-reversed key to show it is not to be
      # reversed.
      sort_options.append(SortOptions('_' + key, value))
      sort_options.append(SortOptions('reverse_' + key, 'Reverse ' + value))

    selected_sort = '_' if ascending else 'reverse_'
    selected_sort += sort_key

    total_pages = (runner_count_async.get_result() /
                   _NUM_USER_TEST_RUNNERS_PER_PAGE)

    # Ensure the shown page is capped to a page with content.
    page = min(page, total_pages + 1)

    params = {
        'topbar': GenerateTopbar(),
        'runners': runners,
        'errors': errors,
        'sorted_by_message': sorted_by_message,
        'sort_options': sort_options,
        'selected_sort': selected_sort,
        'current_page': page,
        # Add 1 so the pages are 1-indexed.
        'total_pages': map(str, range(1, total_pages + 1, 1)),
        'url_no_page': self.GeneratePageUrl(sort_by=sort_by,
                                            include_filters=True),
        'url_no_sort_by_or_filters': self.GeneratePageUrl(
            page, include_filters=False),
        'url_no_filters': self.GeneratePageUrl(page, sort_by),
        'filter_selects': self.FilterSelectsHTML(),
        'machine_id_filter': self.machine_id_filter,
        'test_name_filter': self.test_name_filter,
    }

    path = os.path.join(os.path.dirname(__file__), 'index.html')
    self.response.out.write(template.render(path, params))

  def _GetDisplayableRunnerTemplate(self, runner):
    """Puts different aspects of the runner in a displayable format.

    Args:
      runner: TestRunner object which will be displayed in Swarm server webpage.
    """
    runner.name_string = runner.name
    runner.key_string = str(runner.key.urlsafe())
    runner.status_string = '&nbsp;'
    runner.requested_on_string = self.GetTimeString(runner.created)
    runner.started_string = '--'
    runner.ended_string = '--'
    runner.machine_id_used = '&nbsp'
    runner.command_string = '&nbsp;'
    runner.failed_test_class_string = ''

    if runner.done:
      runner.started_string = self.GetTimeString(runner.started)
      runner.ended_string = self.GetTimeString(runner.ended)

      runner.machine_id_used = runner.machine_id

      if runner.ran_successfully:
        runner.status_string = (
            '<a title="Click to see results" href="%s?r=%s">Succeeded</a>' %
            (_SECURE_GET_RESULTS_URL, runner.key_string))
      else:
        runner.failed_test_class_string = 'failed_test'
        runner.command_string = GenerateButtonWithHiddenForm(
            'Retry', '/secure/retry?r=%s' % runner.key_string,
            runner.key_string)
        runner.status_string = (
            '<a title="Click to see results" href="%s?r=%s">Failed</a>' %
            (_SECURE_GET_RESULTS_URL, runner.key_string))
    elif runner.started:
      runner.status_string = 'Running on machine %s' % runner.machine_id
      runner.started_string = self.GetTimeString(runner.started)
      runner.machine_id_used = runner.machine_id
    else:
      runner.status_string = 'Pending'
      runner.command_string = GenerateButtonWithHiddenForm(
          'Cancel', '%s?r=%s' % (_SECURE_CANCEL_URL, runner.key_string),
          runner.key_string)


class RedirectToMainHandler(webapp2.RequestHandler):
  """Handler to redirect requests to base page secured main page."""

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    self.redirect(_SECURE_MAIN_URL)


class MachineListHandler(webapp2.RequestHandler):
  """Handler for the machine list page of the web server.

  This handler lists all the machines that have ever polled the server and
  some basic information about them.
  """

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    sort_by = self.request.get('sort_by', '')
    if sort_by != 'status' and sort_by not in machine_stats.ACCEPTABLE_SORTS:
      sort_by = 'machine_id'

    machines = machine_stats.GetAllMachines(sort_by)

    # Add a delete option for each machine assignment.
    machines_displayable = []
    for machine in machines:
      # TODO(user): Actually set the machine status.
      machine.status = '-'

      machine.command_string = GenerateButtonWithHiddenForm(
          'Delete',
          '%s?r=%s' % (_SECURE_DELETE_MACHINE_STATS_URL,
                       machine.key.string_id()),
          machine.key.string_id())
      machines_displayable.append(machine)

    sort_options = [SortOptions(key, value) for key, value in
                    machine_stats.ACCEPTABLE_SORTS.iteritems()]
    # Add the special status sort option.
    sort_options.append(SortOptions('status', 'Status'))

    params = {
        'topbar': GenerateTopbar(),
        'machines': machines_displayable,
        'machine_update_time': machine_stats.MACHINE_UPDATE_TIME,
        'selected_sort': sort_by,
        'sort_options': sort_options,
    }

    path = os.path.join(os.path.dirname(__file__), 'machine_list.html')
    self.response.out.write(template.render(path, params))


class DeleteMachineStatsHandler(webapp2.RequestHandler):
  """Handler to delete a machine assignment."""

  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    key = self.request.get('r')

    if key and machine_stats.DeleteMachineStats(key):
      self.response.out.write('Machine Assignment removed.')
    else:
      self.response.set_status(204)


class TestRequestHandler(webapp2.RequestHandler):
  """Handles test requests from clients."""

  def get(self):  # pylint: disable=g-bad-name
    if ALLOW_POST_AS_GET:
      return self.post()
    else:
      self.response.set_status(405)

  @AuthenticateMachineOrUser
  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    # Validate the request.
    if not self.request.get('request'):
      self.response.set_status(402)
      self.response.out.write('No request parameter found')
      return

    try:
      response = json.dumps(test_management.ExecuteTestRequest(
          self.request.get('request')))
    except test_request_message.Error as ex:
      message = str(ex)
      logging.error(message)
      response = 'Error: %s' % message
    self.response.out.write(response)


class ResultHandler(webapp2.RequestHandler):
  """Handles test results from remote test runners."""

  @AuthenticateMachine
  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    # TODO(user): Share this code between all the request handlers so we
    # can always see how often a request is being sent.
    connection_attempt = self.request.get(url_helper.COUNT_KEY)
    if connection_attempt:
      logging.info('This is the %s connection attempt from this machine to '
                   'POST these results', connection_attempt)

    logging.debug('Received Result: %s', self.request.url)

    runner_key = self.request.get('r', '')
    runner = test_runner.GetRunnerFromUrlSafeKey(runner_key)

    if not runner:
      # If the runner is gone, it probably already received results from
      # a different machine and was naturally deleted. We can't do anything
      # with the results now, so just ignore them.
      msg = ('The runner, with key %s, has already been deleted, results lost.'
             % runner_key)
      logging.info(msg)
      self.response.out.write(msg)
      return

    # Find the high level success/failure from the URL. We assume failure if
    # we can't find the success parameter in the request.
    success = self.request.get('s', 'False') == 'True'
    exit_codes = urllib.unquote_plus(self.request.get('x'))
    overwrite = self.request.get('o', 'False') == 'True'
    machine_id = urllib.unquote_plus(self.request.get('id'))

    # TODO(user): The result string should probably be in the body of the
    # request.
    result_string = urllib.unquote_plus(self.request.get(
        url_helper.RESULT_STRING_KEY))

    # Mark the runner as pinging now to prevent it from timing out while
    # the results are getting stored in the blobstore.
    test_runner.PingRunner(runner.key.urlsafe(), machine_id)

    result_blob_key = blobstore_helper.CreateBlobstore(result_string)
    if not result_blob_key:
      self.response.out.write('The server was unable to save the results to '
                              'the blobstore')
      self.response.set_status(500)
      return

    if runner.UpdateTestResult(machine_id,
                               success=success,
                               exit_codes=exit_codes,
                               result_blob_key=result_blob_key,
                               overwrite=overwrite):
      self.response.out.write('Successfully update the runner results.')
    else:
      self.response.set_status(400)
      self.response.out.write('Failed to update the runner results.')


class CleanupDataHandler(webapp2.RequestHandler):
  """Handles tasks to delete orphaned blobs."""

  def post(self):  # pylint: disable=g-bad-name
    try:
      test_management.DeleteOldErrors()

      dimension_mapping.DeleteOldDimensionMapping()

      test_runner.DeleteOldRunners()
      test_runner.DeleteOrphanedBlobs()

      daily_stats.DeleteOldDailyStats()

      runner_stats.DeleteOldRunnerStats()

      runner_summary.DeleteOldWaitSummaries()

      self.response.out.write('Successfully cleaned up old data.')
    except datastore_errors.Timeout:
      logging.info('Ran out of time while cleaning up data. Triggering '
                   'another cleanup.')
      taskqueue.add(method='POST', url='/task_queues/cleanup_data')


class CronJobHandler(webapp2.RequestHandler):
  """A helper class to handle redirecting GET cron jobs to POSTs."""

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    # Only an app engine cron job is allowed to poll via get (it currently
    # has no way to make its request a post).
    if self.request.headers.get('X-AppEngine-Cron') != 'true':
      self.response.out.write('Only internal cron jobs can do this')
      self.response.set_status(405)
      return

    self.post()


class AbortStaleRunnersHandler(CronJobHandler):
  """Handles cron jobs to abort stale runners."""

  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""

    logging.debug('Polling')
    test_management.AbortStaleRunners()
    self.response.out.write("""
    <html>
    <head>
    <title>Poll Done</title>
    </head>
    <body>Poll Done</body>
    </html>""")


class TriggerCleanupDataHandler(CronJobHandler):
  """Handles cron jobs to delete orphaned blobs."""

  def post(self):  # pylint: disable=g-bad-name
    taskqueue.add(method='POST', url='/task_queues/cleanup_data')
    self.response.out.write('Successfully triggered task to clean up old data.')


class TriggerGenerateDailyStats(CronJobHandler):
  """Handles cron jobs to generate daily stats."""

  def post(self):  # pylint: disable-msg=g-bad-name
    taskqueue.add(method='POST', url='/task_queues/generate_daily_stats')
    self.response.out.write('Successfully triggered task to generate daily '
                            'stats.')


class TriggerGenerateRecentStats(CronJobHandler):
  """Handles cron jobs to generate recent stats."""

  def post(self):   # pylint: disable-msg=g-bad-name
    taskqueue.add(method='POST', url='/task_queues/generate_recent_stats')
    self.response.out.write('Successfully triggered task to generate recent '
                            'stats.')


class DetectDeadMachinesHandler(CronJobHandler):
  """Handles cron jobs to detect dead machines."""

  def post(self):  # pylint: disable=g-bad-name
    dead_machines = machine_stats.FindDeadMachines()
    if not dead_machines:
      msg = 'No dead machines found'
      logging.info(msg)
      self.response.out.write(msg)
      return

    logging.warning('Dead machines were detected, emailing admins')
    machine_stats.NotifyAdminsOfDeadMachines(dead_machines)

    msg = 'Successfully detected dead machines and emailed admins.'
    logging.info(msg)
    self.response.out.write(msg)


class DetectHangingRunnersHandler(CronJobHandler):
  """Handles cron jobs to detect runners that have been waiting too long."""

  def get(self):   # pylint: disable=g-bad-name
    hanging_runners = test_runner.GetHangingRunners()
    if hanging_runners:
      subject = 'Hanging Runners on %s' % app_identity.get_application_id()

      hanging_dimensions = set(runner.dimensions for runner in hanging_runners)
      body = ('The following dimensions have hanging runners (runners that '
              'have been waiting more than %d minutes to run).\n' % (
                  test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS) +
              '\n'.join(hanging_dimensions) +
              '\n\nHere are the hanging runner names:\n' +
              '\n'.join(runner.name for runner in hanging_runners)
             )

      admin_user.EmailAdmins(subject, body)


class GenerateDailyStatsHandler(CronJobHandler):
  """Handles cron jobs to generate new daily stats."""

  def post(self):  # pylint: disable=g-bad-name
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    daily_stats.GenerateDailyStats(yesterday)


class GenerateRecentStatsHandler(CronJobHandler):
  """Handles cron jobs to generate new recent stats."""

  def post(self):  # pylint: disable=g-bad-name
    runner_summary.GenerateSnapshotSummary()
    runner_summary.GenerateWaitSummary()


class SendEReporterHandler(ReportGenerator):
  """Handles calling EReporter with the correct parameters."""

  def get(self):  # pylint: disable=g-bad-name
    # grab the mailing admin
    admin = admin_user.AdminUser.all().get()
    if not admin:
      # Create a dummy value so it can be edited from the datastore admin.
      admin = admin_user.AdminUser(email='')
      admin.put()

    try:
      if mail.is_email_valid(admin.email):
        self.request.GET['sender'] = admin.email
        exception_count = ereporter.ExceptionRecord.all(keys_only=True).count()

        while exception_count:
          try:
            self.request.GET['max_results'] = exception_count
            super(SendEReporterHandler, self).get()
            exception_count = min(
                exception_count,
                ereporter.ExceptionRecord.all(keys_only=True).count())
          except mail_errors.BadRequestError:
            if exception_count == 1:
              # This is bad, it means we can't send any exceptions, so just
              # clear all exceptions.
              db.delete_async(ereporter.ExceptionRecord.all(keys_only=True))
              break

            # ereporter doesn't handle the email being too big, so manually
            # decrease the size and try again.
            exception_count = max(exception_count / 2, 1)
      else:
        self.response.out.write('Invalid admin email, \'%s\'. Must be a valid '
                                'email.' % admin.email)
        self.response.set_status(400)
    except runtime.DeadlineExceededError as e:
      # Hitting the deadline here isn't a big deal if it is rare.
      logging.warning(e)


class ShowMessageHandler(webapp2.RequestHandler):
  """Show the full text of a test request."""

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'

    runner_key = self.request.get('r', '')
    runner = test_runner.GetRunnerFromUrlSafeKey(runner_key)

    if runner:
      self.response.out.write(runner.GetMessage())
    else:
      self.response.out.write('Cannot find message for: %s' % runner_key)


class StatsHandler(webapp2.RequestHandler):
  """Show all the collected swarm stats."""

  def get(self):  # pylint: disable=g-bad-name
    days_to_show = DaysToShow(self.request)

    weeks_daily_stats = daily_stats.GetDailyStats(
        datetime.date.today() - datetime.timedelta(days=days_to_show))
    # Reverse the daily stats so that the newest data is listed first, which
    # makes more sense when listing these values in a table.
    weeks_daily_stats.reverse()

    max_days_to_show = min(daily_stats.DAILY_STATS_LIFE_IN_DAYS,
                           runner_summary.WAIT_SUMMARY_LIFE_IN_DAYS)
    params = {
        'topbar': GenerateTopbar(),
        'stat_links': GenerateStatLinks(),
        'daily_stats': weeks_daily_stats,
        'days_to_show': days_to_show,
        'max_days_to_show': range(1, max_days_to_show),
        'runner_wait_stats': runner_summary.GetRunnerWaitStats(days_to_show),
    }

    path = os.path.join(os.path.dirname(__file__), 'stats.html')
    self.response.out.write(template.render(path, params))


class GetMatchingTestCasesHandler(webapp2.RequestHandler):
  """Get all the keys for any test runner that match a given test case name."""

  @AuthenticateMachineOrUser
  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'

    test_case_name = self.request.get('name', '')

    matches = test_request.GetAllMatchingTestRequests(test_case_name)
    keys = []
    for match in matches:
      keys.extend([key.urlsafe() for key in match.GetAllKeys()])

    logging.info('Found %d keys in %d TestRequests', len(keys), len(matches))
    if keys:
      self.response.out.write(json.dumps(keys))
    else:
      self.response.set_status(404)


class SecureGetResultHandler(webapp2.RequestHandler):
  """Show the full result string from a test runner."""

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    SendRunnerResults(self.response, self.request.get('r', ''))


class GetResultHandler(webapp2.RequestHandler):
  """Show the full result string from a test runner."""

  @AuthenticateMachineOrUser
  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    SendRunnerResults(self.response, self.request.get('r', ''))


class GetSlaveCodeHandler(webapp2.RequestHandler):
  """Returns a zip file with all the files required by a slave."""

  @AuthenticateMachine
  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'application/octet-stream'
    self.response.out.write(test_management.SlaveCodeZipped())


class GetTokenHandler(webapp2.RequestHandler):
  """Returns an authentication token."""

  @AuthenticateMachineOrUser
  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.out.write('dummy_token')


class CleanupResultsHandler(webapp2.RequestHandler):
  """Delete the Test Runner with the given key."""

  @AuthenticateMachine
  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'test/plain'

    key = self.request.get('r', '')
    if test_runner.DeleteRunnerFromKey(key):
      self.response.out.write('Key deleted.')
    else:
      self.response.out.write('Key deletion failed.')
      logging.warning('Unable to delete runner [key: %s]', str(key))


class CancelHandler(webapp2.RequestHandler):
  """Cancel a test runner that is not already running."""

  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'

    runner_key = self.request.get('r', '')
    runner = test_runner.GetRunnerFromUrlSafeKey(runner_key)

    # Make sure found runner is not yet running.
    if runner and not runner.started:
      test_management.AbortRunner(runner, reason='Runner cancelled by user.')
      self.response.out.write('Runner canceled.')
    else:
      self.response.out.write('Cannot find runner or too late to cancel: %s' %
                              runner_key)


class RetryHandler(webapp2.RequestHandler):
  """Retry a test runner again."""

  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'

    runner_key = self.request.get('r', '')
    runner = test_runner.GetRunnerFromUrlSafeKey(runner_key)

    if runner:
      runner.ClearRunnerRun()

      # Update the created time to make sure that retrying the runner does not
      # make it jump the queue and get executed before other runners for
      # requests added before the user pressed the retry button.
      runner.created = datetime.datetime.now()

      runner.put()

      self.response.out.write('Runner set for retry.')
    else:
      self.response.set_status(204)


class RegisterHandler(webapp2.RequestHandler):
  """Handler for the register_machine of the Swarm server.

     Attempt to find a matching job for the querying machine.
  """

  def get(self):  # pylint: disable=g-bad-name
    if ALLOW_POST_AS_GET:
      return self.post()
    else:
      self.response.set_status(405)

  @AuthenticateMachine
  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    # Validate the request.
    if not self.request.body:
      self.response.set_status(402)
      self.response.out.write('Request must have a body')
      return

    attributes_str = self.request.get('attributes')
    try:
      attributes = json.loads(attributes_str)
    except (TypeError, ValueError) as e:
      message = 'Invalid attributes: %s: %s' % (attributes_str, e)
      logging.error(message)
      response = 'Error: %s' % message
      self.response.out.write(response)
      return

    try:
      response = json.dumps(
          test_management.ExecuteRegisterRequest(attributes,
                                                 self.request.host_url))
    except runtime.DeadlineExceededError as e:
      # If the timeout happened before a runner was assigned there are no
      # problems. If the timeout occurred after a runner was assigned, that
      # runner will timeout (since the machine didn't get the details required
      # to run it) and it will automatically get retried when the machine
      # "timeout".
      message = str(e)
      logging.warning(message)
      response = 'Error: %s' % message
    except test_request_message.Error as e:
      message = str(e)
      logging.error(message)
      response = 'Error: %s' % message

    self.response.out.write(response)


class RunnerPingHandler(webapp2.RequestHandler):
  """Handler for runner pings to the server.

     The runner pings are used to let the server know a runner is still working,
     so it won't consider it stale.
  """

  @AuthenticateMachine
  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    key = self.request.get('r', '')
    machine_id = self.request.get('id', '')

    if test_runner.PingRunner(key, machine_id):
      self.response.out.write('Runner successfully pinged.')
    else:
      self.response.set_status(402)
      self.response.out.write('Runner failed to ping.')


class RunnerSummary(object):
  """A basic helper class for holding the runner summary for a dimension."""

  def __init__(self, dimensions, num_pending, num_running):
    self.dimensions = dimensions
    self.pending_runners = num_pending
    self.running_runners = num_running


class RunnerSummaryHandler(webapp2.RequestHandler):
  """Handler for displaying a summary of the current runners."""

  def GenerateHistoryHoursSelect(self):
    """Returns the HTML to generate a select box for the hours to show.

    Returns:
      The required HTML.
    """
    html = ('<select id="hours_to_show" onchange="document.location.href=\''
            '?hours=\' + this.value">')
    html += '<option>-</option>'

    # Allow up to a day, by the hour.
    for i in range(1, 25):
      html += '<option value=\'%d\'>%d hours</option>' % (i, i)

    # Allow multiple days, up to 4 weeks
    for i in range(2, 29):
      html += '<option value=\'%d\'>%d days</option>' % (i * 24, i)

    html += '</select>'

    return html

  def get(self):  # pylint: disable=g-bad-name
    try:
      hours = int(self.request.get('hours', '24'))
    except ValueError:
      hours = 24

    if hours <= 24:
      time_frame = '%d hours' % hours
    else:
      time_frame = '%d days' % (hours/24)

    # Start querying all the summaries for the graph.
    summary_cutoff_time = (datetime.datetime.now() -
                           datetime.timedelta(hours=hours))
    summaries_async = runner_summary.RunnerSummary.query(
        runner_summary.RunnerSummary.time > summary_cutoff_time).fetch_async()

    # Get a snapshot of the current state of pending and running runners.
    total_pending = 0
    total_running = 0
    runner_summaries = runner_summary.GetRunnerSummaryByDimension().iteritems()
    snapshot_summary = []
    for dimensions, summary in runner_summaries:
      total_pending += summary[0]
      total_running += summary[1]
      snapshot_summary.append(
          RunnerSummary(test_request_message.Stringize(dimensions),
                        summary[0],
                        summary[1]))

    # Start the graph dict for each dimension.
    runner_summary_graphs = {}
    for i, summary in enumerate(snapshot_summary):
      runner_summary_graphs[summary.dimensions] = {
          'id': i,
          'title': summary.dimensions,
          'data': []}

    # Convert the runner summaries to the graph array.
    for summary in summaries_async.get_result():
      runner_summary_graphs[summary.dimensions]['data'].append(
          [summary.time.isoformat(), summary.pending, summary.running])

    params = {
        'topbar': GenerateTopbar(),
        'stat_links': GenerateStatLinks(),
        'total_pending_runners': total_pending,
        'total_running_runners': total_running,
        'snapshot_summary': snapshot_summary,
        'hours_select': self.GenerateHistoryHoursSelect(),
        'time_frame': time_frame,
        'runner_summary_graphs': runner_summary_graphs.values(),
    }

    path = os.path.join(os.path.dirname(__file__), 'runner_summary.html')
    self.response.out.write(template.render(path, params))


class DailyStatsGraphHandler(webapp2.RequestHandler):
  """Handler for generating the html page to display the daily stats."""

  def _GetGraphableDailyStats(self, stats):
    """Convert the daily_stats into a list of graphs objects for visualization.

    Args:
      stats: A list of daily stats to split into components.

    Returns:
      A list of dictionaries with the graph title and an array that can be
      passed to the data visualization tool.
    """
    # A mapping of the element's variable name, and the name that should be
    # displayed to the user.
    elements_to_graph = [
        ('shards_finished', 'Shards Finished'),
        ('shards_failed', 'Shards Failed'),
        ('shards_timed_out', 'Shards Timed Out'),
        ('total_running_time', 'Total Running Time'),
        ('total_wait_time', 'Total Wait Time'),
        ]

    graphs_to_show = []
    for element_name, title_name in elements_to_graph:
      graphs_to_show.append(
          {'element_id': element_name,
           'title': title_name,
           'data_array': [['Date', title_name]]})

    for stat in stats:
      date_str = stat.date.isoformat()
      for i, element in enumerate(elements_to_graph):
        graphs_to_show[i]['data_array'].append(
            [date_str, int(getattr(stat, element[0]))])

    return graphs_to_show

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    days_to_show = DaysToShow(self.request)

    params = {
        'graphs': self._GetGraphableDailyStats(daily_stats.GetDailyStats(
            datetime.date.today() - datetime.timedelta(days=days_to_show))),
        'max_days_to_show': range(1, daily_stats.DAILY_STATS_LIFE_IN_DAYS),
        'stat_links': GenerateStatLinks(),
        'topbar': GenerateTopbar()
    }

    path = os.path.join(os.path.dirname(__file__), 'graph.html')
    self.response.out.write(template.render(path, params))


class UserProfileHandler(webapp2.RequestHandler):
  """Handler for the user profile page of the web server.

  This handler lists user info, such as their IP whitelist and settings.
  """

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    topbar = GenerateTopbar()

    display_whitelists = []

    for stored_whitelist in user_manager.MachineWhitelist().all():
      whitelist = {}
      whitelist['ip'] = stored_whitelist.ip
      whitelist['password'] = stored_whitelist.password
      whitelist['key'] = stored_whitelist.key()
      whitelist['url'] = _SECURE_CHANGE_WHITELIST_URL
      display_whitelists.append(whitelist)

    params = {
        'topbar': topbar,
        'whitelists': display_whitelists,
        'change_whitelist_url': _SECURE_CHANGE_WHITELIST_URL
    }

    path = os.path.join(os.path.dirname(__file__), 'user_profile.html')
    self.response.out.write(template.render(path, params))


class ChangeWhitelistHandler(webapp2.RequestHandler):
  """Handler for making changes to a user whitelist."""

  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    ip = self.request.get('i', self.request.remote_addr)

    password = self.request.get('p', None)
    # Make sure a password '' sent by the form is stored as None.
    if not password:
      password = None

    add = self.request.get('a')
    if add == 'True':
      user_manager.AddWhitelist(ip, password)
    elif add == 'False':
      user_manager.DeleteWhitelist(ip)

    self.redirect(_SECURE_USER_PROFILE_URL, permanent=True)


class RemoteErrorHandler(webapp2.RequestHandler):
  """Handler to log an error reported by remote machine."""

  @AuthenticateMachine
  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    error_message = self.request.get('m', '')
    error = test_management.SwarmError(
        name='Remote Error Report', message=error_message,
        info='Remote machine address: %s' % self.request.remote_addr)
    error.put()

    self.response.out.write('Error logged')


class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
  def post(self):  # pylint: disable=g-bad-name
    """Handles HTTP POST requests for this handler's URL."""
    upload_result = self.get_uploads('result')

    if len(upload_result) != 1:
      self.response.set_status(403)
      self.response.out.write('Expected 1 upload but received %d',
                              len(upload_result))

      blobstore.delete_async((b.key() for b in upload_result))
      return

    blob_info = upload_result[0]
    self.response.out.write(blob_info.key())


class WaitsByMinuteHandler(webapp2.RequestHandler):
  """Handler for displaying the wait times, broken by minute, per dimensions."""

  def get(self):  # pylint: disable=g-bad-name
    days_to_show = DaysToShow(self.request)

    params = {
        'topbar': GenerateTopbar(),
        'stat_links': GenerateStatLinks(),
        'days_to_show': days_to_show,
        'max_days_to_show': range(1, runner_summary.WAIT_SUMMARY_LIFE_IN_DAYS),
        'wait_breakdown': runner_summary.GetRunnerWaitStatsBreakdown(
            days_to_show)
    }

    path = os.path.join(os.path.dirname(__file__), 'waits_by_minute.html')
    self.response.out.write(template.render(path, params))


def SendAuthenticationFailure(request, response):
  """Writes an authentication failure error message to response with status.

  Args:
    request: The original request that failed to authenticate.
    response: Response to be sent to remote machine.
  """
  # Log the error.
  error = test_management.SwarmError(
      name='Authentication Failure', message='Handler: %s' % request.url,
      info='Remote machine address: %s' % request.remote_addr)
  error.put()

  response.set_status(403)
  response.out.write('Remote machine not whitelisted for operation')


def SendRunnerResults(response, key):
  """Sends the results of the runner specified by key.

  Args:
    response: Response to be sent to remote machine.
    key: Key identifying the runner.
  """
  response.headers['Content-Type'] = 'text/plain'
  results = test_runner.GetRunnerResults(key)

  if results:
    response.out.write(json.dumps(results))
  else:
    response.set_status(204)
    logging.info('Unable to provide runner results [key: %s]', str(key))


def CreateApplication():
  return webapp2.WSGIApplication([('/', RedirectToMainHandler),
                                  ('/cleanup_results',
                                   CleanupResultsHandler),
                                  ('/get_matching_test_cases',
                                   GetMatchingTestCasesHandler),
                                  ('/get_result', GetResultHandler),
                                  ('/get_slave_code', GetSlaveCodeHandler),
                                  ('/get_token', GetTokenHandler),
                                  ('/graphs/daily_stats',
                                   DailyStatsGraphHandler),
                                  ('/poll_for_test', RegisterHandler),
                                  ('/remote_error', RemoteErrorHandler),
                                  ('/result', ResultHandler),
                                  ('/runner_ping', RunnerPingHandler),
                                  ('/runner_summary', RunnerSummaryHandler),
                                  ('/secure/machine_list', MachineListHandler),
                                  ('/secure/retry', RetryHandler),
                                  ('/secure/show_message',
                                   ShowMessageHandler),
                                  ('/stats', StatsHandler),
                                  ('/task_queues/cleanup_data',
                                   CleanupDataHandler),
                                  ('/task_queues/generate_daily_stats',
                                   GenerateDailyStatsHandler),
                                  ('/task_queues/generate_recent_stats',
                                   GenerateRecentStatsHandler),
                                  ('/tasks/abort_stale_runners',
                                   AbortStaleRunnersHandler),
                                  ('/tasks/detect_dead_machines',
                                   DetectDeadMachinesHandler),
                                  ('/tasks/detect_hanging_runners',
                                   DetectHangingRunnersHandler),
                                  ('/tasks/sendereporter',
                                   SendEReporterHandler),
                                  ('/tasks/trigger_cleanup_data',
                                   TriggerCleanupDataHandler),
                                  ('/tasks/trigger_generate_daily_stats',
                                   TriggerGenerateDailyStats),
                                  ('/tasks/trigger_generate_recent_stats',
                                   TriggerGenerateRecentStats),
                                  ('/test', TestRequestHandler),
                                  ('/upload', UploadHandler),
                                  ('/waits_by_minute', WaitsByMinuteHandler),
                                  (_SECURE_CANCEL_URL, CancelHandler),
                                  (_SECURE_CHANGE_WHITELIST_URL,
                                   ChangeWhitelistHandler),
                                  (_SECURE_DELETE_MACHINE_STATS_URL,
                                   DeleteMachineStatsHandler),
                                  (_SECURE_GET_RESULTS_URL,
                                   SecureGetResultHandler),
                                  (_SECURE_MAIN_URL, MainHandler),
                                  (_SECURE_USER_PROFILE_URL,
                                   UserProfileHandler)],
                                 debug=True)

ereporter.register_logger()
# Use ndb.toplevel to ensure that any async operations started in any handler
# are finished before the handler returns.
app = ndb.toplevel(CreateApplication())
