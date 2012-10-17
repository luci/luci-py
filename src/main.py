#!/usr/bin/python2.7
#
# Copyright 2012 Google Inc. All Rights Reserved.
#


# pylint: disable-msg=C6204
import datetime
import logging
import os.path
try:
  import simplejson as json
except ImportError:
  import json
# pylint: enable-msg=C6204

from google.appengine.api import users
from google.appengine.ext import db
import webapp2
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import util
from common import test_request_message
from server import test_manager
from server import user_manager
# pylint: enable-msg=C6204

_NUM_USER_TEST_RUNNERS_PER_PAGE = 50
_NUM_RECENT_ERRORS_TO_DISPLAY = 10

_HOME_LINK = '<a href=/secure/main>Home</a>'
_PROFILE_LINK = '<a href=/secure/user_profile>Profile</a>'
_MACHINE_LIST_LINK = '<a href=/secure/machine_list>Machine List</a>'


_SECURE_CANCEL_URL = '/secure/cancel'
_SECURE_CHANGE_WHITELIST_URL = '/secure/change_whitelist'
_SECURE_DELETE_MACHINE_ASSIGNMENT_URL = '/secure/delete_machine_assignment'
_SECURE_GET_RESULTS_URL = '/secure/get_result'
_SECURE_MAIN_URL = '/secure/main'
_SECURE_USER_PROFILE_URL = '/secure/user_profile'


def GenerateTopbar():
  """Generate the topbar to display on all server pages.

  Returns:
    The topbar to display.
  """
  if users.get_current_user():
    topbar = ('%s |  <a href="%s">Sign out</a><br/> %s | %s | % s' %
              (users.get_current_user().nickname(),
               users.create_logout_url('/'),
               _HOME_LINK,
               _PROFILE_LINK,
               _MACHINE_LIST_LINK))
  else:
    topbar = '<a href="%s">Sign in</a>' % users.create_login_url('/')

  return topbar


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

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    # Build info for test requests table.
    # TODO(user): eventually we will want to show only runner that are
    # either pending or running.  The number of ended runners will grow
    # unbounded with time.
    show_success = self.request.get('s', 'False') != 'False'
    sort_by = self.request.get('sort_by', 'reverse_chronological')
    page = int(self.request.get('page', 1))

    sorted_by_message = '<p>Currently sorted by: '
    if sort_by == 'start':
      sorted_by_message += 'Start Time'
      sorted_by_query = 'created'
    elif sort_by == 'machine_id':
      sorted_by_message += 'Machine ID'
      sorted_by_query = 'machine_id'
    else:
      # The default sort.
      sorted_by_message += 'Reverse Start Time'
      sorted_by_query = 'created DESC'
    sorted_by_message += '</p>'

    query = test_manager.TestRunner.gql(
        'WHERE user = :1 ORDER BY %s' % sorted_by_query,
        users.get_current_user())

    runners = []
    for runner in query.run(
        limit=_NUM_USER_TEST_RUNNERS_PER_PAGE,
        offset=_NUM_USER_TEST_RUNNERS_PER_PAGE * (page - 1)):
      # If this runner successfully completed, and we are not showing them,
      # just ignore it.
      if runner.done and runner.ran_successfully and not show_success:
        continue

      self._GetDisplayableRunnerTemplate(runner, detailed_output=True)
      runners.append(runner)

    errors = []
    query = test_manager.SwarmError.all().order('-created')
    for error in query.run(limit=_NUM_RECENT_ERRORS_TO_DISPLAY):
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

    total_pages = (
        test_manager.TestRunner.all().count() / _NUM_USER_TEST_RUNNERS_PER_PAGE)

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
    runner.key_string = str(runner.key())
    runner.status_string = '&nbsp;'
    runner.requested_on_string = self.GetTimeString(runner.created)
    runner.started_string = '--'
    runner.ended_string = '--'
    runner.machine_id_used = '&nbsp'
    runner.command_string = '&nbsp;'
    runner.failed_test_class_string = ''
    runner.user_email = runner.user.email()

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

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    self.redirect(_SECURE_MAIN_URL)


class MachineListHandler(webapp2.RequestHandler):
  """Handler for the machine list page of the web server.

  This handler lists all the machines that have ever polled the server and
  some basic information about them.
  """

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    sort_by = self.request.get('sort_by', 'machine_id')
    machines = test_manager.GetAllUserMachines(sort_by)

    # Add a delete option for each machine assignment.
    machines_displayable = []
    for machine in machines:
      machine.command_string = GenerateButtonWithHiddenForm(
          'Delete',
          '%s?r=%s' % (_SECURE_DELETE_MACHINE_ASSIGNMENT_URL, machine.key()),
          machine.key())
      machines_displayable.append(machine)

    params = {
        'topbar': GenerateTopbar(),
        'machines': machines_displayable
    }

    path = os.path.join(os.path.dirname(__file__), 'machine_list.html')
    self.response.out.write(template.render(path, params))


class DeleteMachineAssignmentHandler(webapp2.RequestHandler):
  """Handler to delete a machine assignment."""

  def post(self):  # pylint:disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    key = self.request.get('r')

    if key and test_manager.DeleteMachineAssignment(key):
      self.response.out.write('Machine Assignment removed.')
    else:
      self.response.set_status(204)


class TestRequestHandler(webapp2.RequestHandler):
  """Handles test requests from clients."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    # TODO(user): This handler uses the un-encoded request body to
    # get the Swarm test request. A any call to self.request.get will
    # change the body and encode it. Thus, for now we simply make a copy
    # of the original body and use it later. The correct way would be to
    # have this handler work with properly encoded data in the first place.
    body = self.request.body
    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    # Validate the request.
    if not self.request.body:
      self.response.set_status(402)
      self.response.out.write('Request must have a body')
      return

    test_request_manager = CreateTestManager()
    try:
      test_request_manager.UpdateCacheServerURL(self.request.host_url)
      response = str(test_request_manager.ExecuteTestRequest(body))
      # This enables our callers to use the response string as a JSON string.
      response = response.replace("'", '"')
    except test_request_message.Error as ex:
      message = str(ex)
      logging.exception(message)
      response = 'Error: %s' % message
    self.response.out.write(response)


class ResultHandler(webapp2.RequestHandler):
  """Handles test results from remote test runners."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    logging.debug('Received Result: %s', self.request.url)
    test_request_manager = CreateTestManager()
    test_request_manager.UpdateCacheServerURL(self.request.host_url)
    test_request_manager.HandleTestResults(self.request)


class PollHandler(webapp2.RequestHandler):
  """Handles cron job to poll Machine Provider to execute pending requests."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    test_request_manager = CreateTestManager()

    logging.debug('Polling')
    test_request_manager.UpdateCacheServerURL(self.request.host_url)
    test_request_manager.AbortStaleRunners()
    test_manager.DeleteOldRunners()
    test_manager.DeleteOldErrors()
    self.response.out.write("""
    <html>
    <head>
    <title>Poll Done</title>
    </head>
    <body>Poll Done</body>
    </html>
    """)

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    # Only an app engine cron job is allowed to poll via get (it currently
    # has no way to make its request a post).
    if self.request.headers.get('X-AppEngine-Cron') != 'true':
      self.response.out.write('Only internal cron jobs can do this')
      self.response.set_status(405)
      return

    self.post()


class ShowMessageHandler(webapp2.RequestHandler):
  """Show the full text of a test request."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'

    key = self.request.get('r', '')
    if key:
      runner = test_manager.TestRunner.get(key)

    if runner:
      self.response.out.write(runner.GetMessage())
    else:
      self.response.out.write('Cannot find message for: %s' % key)


class GetMatchingTestCasesHandler(webapp2.RequestHandler):
  """Get all the keys for any test runner that match a given test case name."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    self.response.headers['Content-Type'] = 'text/plain'

    test_case_name = self.request.get('name', '')

    matches = test_manager.GetAllMatchingTestRequests(test_case_name)
    keys = []
    for match in matches:
      keys.extend(map(str, match.GetAllKeys()))

    if keys:
      self.response.out.write('\n'.join(keys))
    else:
      self.response.out.write('No matching Test Cases')


class SecureGetResultHandler(webapp2.RequestHandler):
  """Show the full result string from a test runner."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    SendRunnerResults(self.response, self.request.get('r', ''))


class GetResultHandler(webapp2.RequestHandler):
  """Show the full result string from a test runner."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    key = self.request.get('r', '')
    SendRunnerResults(self.response, key)


class CleanupResultsHandler(webapp2.RequestHandler):
  """Delete the Test Runner with the given key."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    test_request_manager = CreateTestManager()

    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    self.response.headers['Content-Type'] = 'test/plain'

    key = self.request.get('r', '')
    if test_request_manager.DeleteRunner(key):
      self.response.out.write('Key deleted.')
    else:
      self.response.out.write('Key deletion failed.')
      logging.warning('Unable to delete runner [key: %s]', str(key))


class CancelHandler(webapp2.RequestHandler):
  """Cancel a test runner that is not already running."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'

    key = self.request.get('r', '')
    if key:
      runner = test_manager.TestRunner.get(key)

    # Make sure found runner is not yet running.
    if runner and not runner.started:
      test_request_manager = CreateTestManager()
      test_request_manager.UpdateCacheServerURL(self.request.host_url)
      test_request_manager.AbortRunner(
          runner, reason='Runner is not already running.')
      self.response.out.write('Runner canceled.')
    else:
      self.response.out.write('Cannot find runner or too late to cancel: %s' %
                              key)


class RetryHandler(webapp2.RequestHandler):
  """Retry a test runner again."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    self.response.headers['Content-Type'] = 'text/plain'

    key = self.request.get('r', '')
    runner = None
    if key:
      try:
        runner = test_manager.TestRunner.get(key)
      except db.BadKeyError:
        pass

    if runner:
      runner.started = None
      # Update the created time to make sure that retrying the runner does not
      # make it jump the queue and get executed before other runners for
      # requests added before the user pressed the retry button.
      runner.created = datetime.datetime.now()
      runner.ran_successfully = False
      runner.put()

      self.response.out.write('Runner set for retry.')
    else:
      self.response.set_status(204)


class RegisterHandler(webapp2.RequestHandler):
  """Handler for the register_machine of the Swarm server.

     Attempt to find a matching job for the querying machine.
  """

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    # Validate the request.
    if not self.request.body:
      self.response.set_status(402)
      self.response.out.write('Request must have a body')
      return

    test_request_manager = CreateTestManager()
    test_request_manager.UpdateCacheServerURL(self.request.host_url)
    attributes_str = self.request.get('attributes')
    try:
      attributes = json.loads(attributes_str)
    except (json.decoder.JSONDecodeError, TypeError, ValueError):
      message = 'Invalid attributes: ' + attributes_str
      logging.exception(message)
      response = 'Error: %s' % message
      self.response.out.write(response)
      return

    try:
      response = json.dumps(
          test_request_manager.ExecuteRegisterRequest(attributes))
    except test_request_message.Error as ex:
      message = str(ex)
      logging.exception(message)
      response = 'Error: %s' % message

    self.response.out.write(response)


class RunnerPingHandler(webapp2.RequestHandler):
  """Handler for runner pings to the server.

     The runner pings are used to let the server know a runner is still working,
     so it won't consider it stale.
  """

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    key = self.request.get('r', '')

    if test_manager.PingRunner(key):
      self.response.out.write('Runner successfully pinged.')
    else:
      self.response.set_status(402)
      self.response.out.write('Runner failed to ping.')


class UserProfileHandler(webapp2.RequestHandler):
  """Handler for the user profile page of the web server.

  This handler lists user info, such as their IP whitelist and settings.
  """

  def get(self):  # pylint: disable-msg=C6409
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

  def post(self):  # pylint: disable-msg=C6409
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

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    if not AuthenticateRemoteMachine(self.request):
      SendAuthenticationFailure(self.request, self.response)
      return

    error_message = self.request.get('m', '')
    error = test_manager.SwarmError(
        name='Remote Error Report', message=error_message,
        info='Remote machine address: %s' % self.request.remote_addr)
    error.put()

    self.response.out.write('Error logged')


class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    if self.request.host_url != self.request.remote_addr:
      logging.error('A machine attempted to upload a blobstore directly')
      logging.error('Server URL: %s', self.request_host_url)
      logging.error('Machine URL: %s', self.request.remote_addr)

      self.response.out.write('Only the server should attempt to upload '
                              'blobstore data directly.')
      self.response.set_status(403)

    upload_result = self.get_uploads('result')
    blob_info = upload_result[0]
    self.response.out.write(blob_info.key())
    self.redirect('/')


def AuthenticateRemoteMachine(request):
  """Check to see if the request is from a whitelisted machine.

  Will use the remote machine's IP and provided password (if any).

  Args:
    request: WebAPP request sent by remote machine.

  Returns:
    True if the request is from a whitelisted machine.
  """
  return user_manager.IsWhitelistedMachine(
      request.remote_addr, request.get('password', None))


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
  test_request_manager = CreateTestManager()
  results = test_request_manager.GetRunnerResults(key)

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
                                  ('/poll_for_test', RegisterHandler),
                                  ('/remote_error', RemoteErrorHandler),
                                  ('/result', ResultHandler),
                                  ('/runner_ping', RunnerPingHandler),
                                  ('/secure/machine_list', MachineListHandler),
                                  ('/secure/retry', RetryHandler),
                                  ('/secure/show_message',
                                   ShowMessageHandler),
                                  ('/tasks/poll', PollHandler),
                                  ('/test', TestRequestHandler),
                                  ('/upload', UploadHandler),
                                  (_SECURE_CANCEL_URL, CancelHandler),
                                  (_SECURE_CHANGE_WHITELIST_URL,
                                   ChangeWhitelistHandler),
                                  (_SECURE_DELETE_MACHINE_ASSIGNMENT_URL,
                                   DeleteMachineAssignmentHandler),
                                  (_SECURE_GET_RESULTS_URL,
                                   SecureGetResultHandler),
                                  (_SECURE_MAIN_URL, MainHandler),
                                  (_SECURE_USER_PROFILE_URL,
                                   UserProfileHandler)],
                                 debug=True)

app = CreateApplication()
