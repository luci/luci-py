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
from server import test_manager
from server import test_request
from server import test_runner
from server import user_manager
from stats import daily_stats
from stats import machine_stats
from stats import runner_stats
# pylint: enable=g-import-not-at-top

import webapp2  # pylint: disable=g-bad-import-order


_NUM_USER_TEST_RUNNERS_PER_PAGE = 50
_NUM_RECENT_ERRORS_TO_DISPLAY = 10

_HOME_LINK = '<a href=/secure/main>Home</a>'
_MACHINE_LIST_LINK = '<a href=/secure/machine_list>Machine List</a>'
_PROFILE_LINK = '<a href=/secure/user_profile>Profile</a>'
_STATS_LINK = '<a href=/secure/stats>Stats</a>'

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
    # TODO(user): These links should only be visible if the user is able to
    # access them (i.e. the user is an admin or has been given permission to
    # access these pages).
    topbar = ('%s |  <a href="%s">Sign out</a><br/> %s | %s | %s | %s' %
              (users.get_current_user().nickname(),
               users.create_logout_url('/'),
               _HOME_LINK,
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
      '<a href="/secure/graphs/daily_stats">Graph of Daily Stats</a><br/>'
      '<a href="/secure/runner_summary"">Pending and Waiting Summary</a>')

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

  def get(self):  # pylint: disable=g-bad-name
    """Handles HTTP GET requests for this handler's URL."""
    # Build info for test requests table.
    show_success = self.request.get('s', 'False') != 'False'
    sort_by = self.request.get('sort_by', 'reverse_chronological')
    page = int(self.request.get('page', 1))

    sorted_by_message = '<p>Currently sorted by: '
    ascending = True
    if sort_by == 'start':
      sorted_by_message += 'Start Time'
      sorted_by_query = 'created'
    elif sort_by == 'machine_id':
      sorted_by_message += 'Machine ID'
      sorted_by_query = 'machine_id'
    else:
      # The default sort.
      sorted_by_message += 'Reverse Start Time'
      sorted_by_query = 'created'
      ascending = False
    sorted_by_message += '</p>'

    runners = []
    for runner in test_runner.GetTestRunners(
        sorted_by_query,
        ascending=ascending,
        limit=_NUM_USER_TEST_RUNNERS_PER_PAGE,
        offset=_NUM_USER_TEST_RUNNERS_PER_PAGE * (page - 1)):
      # If this runner successfully completed, and we are not showing them,
      # just ignore it.
      if runner.done and runner.ran_successfully and not show_success:
        continue

      self._GetDisplayableRunnerTemplate(runner, detailed_output=True)
      runners.append(runner)

    errors = []
    query = test_manager.SwarmError.query(
        default_options=ndb.QueryOptions(
            limit=_NUM_RECENT_ERRORS_TO_DISPLAY)).order(
                -test_manager.SwarmError.created)
    for error in query:
      error.log_time = self.GetTimeString(error.created)
      errors.append(error)

    if show_success:
      enable_success_message = """
        <a href="?s=False">Hide successfully completed tests</a>
      """
    else:
      enable_success_message = """
        <a href="?s=True">Show successfully completed tests too</a>
      """

    total_pages = (test_runner.TestRunner.query().count() /
                   _NUM_USER_TEST_RUNNERS_PER_PAGE)

    params = {
        'topbar': GenerateTopbar(),
        'runners': runners,
        'errors': errors,
        'enable_success_message': enable_success_message,
        'sorted_by_message': sorted_by_message,
        'current_page': page,
        # Add 1 so the pages are 1-indexed.
        'total_pages': map(str, range(1, total_pages + 1, 1))
    }

    path = os.path.join(os.path.dirname(__file__), 'index.html')
    self.response.out.write(template.render(path, params))

  def _GetDisplayableRunnerTemplate(self, runner, detailed_output=False):
    """Puts different aspects of the runner in a displayable format.

    Args:
      runner: TestRunner object which will be displayed in Swarm server webpage.
      detailed_output: Flag specifying how detailed the output should be.
    """
    runner.name_string = runner.GetName()
    runner.key_string = str(runner.key.urlsafe())
    runner.status_string = '&nbsp;'
    runner.requested_on_string = self.GetTimeString(runner.created)
    runner.started_string = '--'
    runner.ended_string = '--'
    runner.machine_id_used = '&nbsp'
    runner.command_string = '&nbsp;'
    runner.failed_test_class_string = ''

    if not runner.started:
      runner.status_string = 'Pending'
      runner.command_string = GenerateButtonWithHiddenForm(
          'Cancel', '%s?r=%s' % (_SECURE_CANCEL_URL, runner.key_string),
          runner.key_string)

    elif not runner.done:
      if detailed_output:
        runner.status_string = 'Running on machine %s' % runner.machine_id
      else:
        runner.status_string = 'Running'

      runner.started_string = self.GetTimeString(runner.started)
      runner.machine_id_used = runner.machine_id
    else:
      runner.started_string = self.GetTimeString(runner.started)
      runner.ended_string = self.GetTimeString(runner.ended)

      runner.machine_id_used = runner.machine_id

      if runner.ran_successfully:
        if detailed_output:
          runner.status_string = (
              '<a title="Click to see results" href="%s?r=%s">Succeeded</a>' %
              (_SECURE_GET_RESULTS_URL, runner.key_string))
        else:
          runner.status_string = 'Succeeded'
      else:
        runner.failed_test_class_string = 'failed_test'
        runner.command_string = GenerateButtonWithHiddenForm(
            'Retry', '/secure/retry?r=%s' % runner.key_string,
            runner.key_string)
        if detailed_output:
          runner.status_string = (
              '<a title="Click to see results" href="%s?r=%s">Failed</a>' %
              (_SECURE_GET_RESULTS_URL, runner.key_string))
        else:
          runner.status_string = 'Failed'


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
    sort_by = self.request.get('sort_by', 'machine_id')
    machines = machine_stats.GetAllMachines(sort_by)

    # Add a delete option for each machine assignment.
    machines_displayable = []
    for machine in machines:
      machine.machine_id = machine.MachineID()
      machine.command_string = GenerateButtonWithHiddenForm(
          'Delete',
          '%s?r=%s' % (_SECURE_DELETE_MACHINE_STATS_URL, machine.key()),
          machine.key())
      machines_displayable.append(machine)

    params = {
        'topbar': GenerateTopbar(),
        'machines': machines_displayable
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

    test_request_manager = CreateTestManager()
    try:
      response = json.dumps(test_request_manager.ExecuteTestRequest(
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

    test_request_manager = CreateTestManager()
    if test_request_manager.UpdateTestResult(runner, machine_id,
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
    test_manager.DeleteOldErrors()

    dimension_mapping.DeleteOldDimensionMapping()

    test_runner.DeleteOldRunners()
    test_runner.DeleteOrphanedBlobs()

    daily_stats.DeleteOldDailyStats()

    runner_stats.DeleteOldRunnerStats()
    runner_stats.DeleteOldWaitSummaries()

    self.response.out.write('Successfully cleaned up old data.')


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
    test_request_manager = CreateTestManager()

    logging.debug('Polling')
    test_request_manager.AbortStaleRunners()
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

      hanging_dimensions = set(runner.GetDimensionsString()
                               for runner in hanging_runners)
      body = ('The following dimensions have hanging runners (runners that '
              'have been waiting more than %d minutes to run).\n' % (
                  test_runner.TIME_BEFORE_RUNNER_HANGING_IN_MINS) +
              '\n'.join(hanging_dimensions) +
              '\n\nHere are the hanging runner names:\n' +
              '\n'.join(runner.GetName() for runner in hanging_runners)
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
    runner_stats.GenerateStats()


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
    weeks_daily_stats = daily_stats.GetDailyStats(
        datetime.date.today() - datetime.timedelta(days=7))
    # Reverse the daily stats so that the newest data is listed first, which
    # makes more sense when listing these values in a table.
    weeks_daily_stats.reverse()

    params = {
        'topbar': GenerateTopbar(),
        'stat_links': GenerateStatLinks(),
        'daily_stats': weeks_daily_stats,
        'runner_wait_stats': runner_stats.GetRunnerWaitStats(),
        'runner_cutoff': runner_stats.RUNNER_STATS_EVALUATION_CUTOFF_DAYS
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
    self.response.out.write(test_manager.SlaveCodeZipped())


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
      test_request_manager = CreateTestManager()
      test_request_manager.AbortRunner(
          runner, reason='Runner cancelled by user.')
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
      runner.started = None
      # Update the created time to make sure that retrying the runner does not
      # make it jump the queue and get executed before other runners for
      # requests added before the user pressed the retry button.
      runner.machine_id = None
      runner.done = False
      runner.created = datetime.datetime.now()
      runner.ran_successfully = False
      runner.automatic_retry_count = 0
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

    test_request_manager = CreateTestManager()
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
          test_request_manager.ExecuteRegisterRequest(attributes,
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


class RunnerSummaryHandler(webapp2.RequestHandler):
  """Handler for displaying a summary of the current runners."""

  def get(self):  # pylint: disable=g-bad-name
    params = {
        'topbar': GenerateTopbar(),
        'stats_links': GenerateStatLinks(),
        # TODO(user): Generate the actual stats.
        'total_running_runners': 0,
        'total_pending_runners': 0,
        'dimension_summary': [],
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
    params = {
        # TODO(user): Let the user choose the number of days to display.
        'graphs': self._GetGraphableDailyStats(daily_stats.GetDailyStats(
            datetime.date.today() - datetime.timedelta(days=7))),
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
    error = test_manager.SwarmError(
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


def SendAuthenticationFailure(request, response):
  """Writes an authentication failure error message to response with status.

  Args:
    request: The original request that failed to authenticate.
    response: Response to be sent to remote machine.
  """
  # Log the error.
  error = test_manager.SwarmError(
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


def CreateTestManager():
  """Creates and returns a test manager instance.

  Returns:
    A TestManager instance.
  """
  return test_manager.TestRequestManager()


def CreateApplication():
  return webapp2.WSGIApplication([('/', RedirectToMainHandler),
                                  ('/cleanup_results',
                                   CleanupResultsHandler),
                                  ('/get_matching_test_cases',
                                   GetMatchingTestCasesHandler),
                                  ('/get_result', GetResultHandler),
                                  ('/get_slave_code', GetSlaveCodeHandler),
                                  ('/get_token', GetTokenHandler),
                                  ('/poll_for_test', RegisterHandler),
                                  ('/remote_error', RemoteErrorHandler),
                                  ('/result', ResultHandler),
                                  ('/runner_ping', RunnerPingHandler),
                                  ('/secure/graphs/daily_stats',
                                   DailyStatsGraphHandler),
                                  ('/secure/machine_list', MachineListHandler),
                                  ('/secure/retry', RetryHandler),
                                  ('/secure/runner_summary',
                                   RunnerSummaryHandler),
                                  ('/secure/show_message',
                                   ShowMessageHandler),
                                  ('/secure/stats', StatsHandler),
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
app = CreateApplication()
