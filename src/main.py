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
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import util
from common import test_request_message
from server import base_machine_provider
from server import machine_manager
from server import machine_provider
from server import test_manager
# pylint: enable-msg=C6204


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
    #
    # Once we have labels for usernames, we can also show a second table
    # specific to a user to show him his terminated jobs, so allow him to
    # retry from there.
    show_success = self.request.get('s', 'False') != 'False'
    sort_by = self.request.get('sort_by', 'reverse_chronological')

    runners = []
    query = test_manager.TestRunner.all()

    sorted_by_message = '<p>Currently sorted by: '
    if sort_by == 'start':
      sorted_by_message += 'Start Time'
      query.order('created')
    elif sort_by == 'host_name':
      sorted_by_message += 'Hostname'
      query.order('hostname')
    else:
      # The default sort.
      sorted_by_message += 'Reverse Start Time'
      query.order('-created')
    sorted_by_message += '</p>'

    for runner in query:
      runner.name_string = runner.GetName()
      runner.key_string = str(runner.key())
      runner.status_string = '&nbsp;'
      runner.requested_on_string = self.GetTimeString(runner.created)
      runner.started_string = '--'
      runner.ended_string = '--'
      runner.host_used = '&nbsp'
      runner.command_string = '&nbsp;'
      runner.failed_test_class_string = ''

      if runner.machine_id == test_manager.NO_MACHINE_ID:
        runner.status_string = 'Pending'
        runner.command_string = (
            '<a href="/secure/cancel?r=%s">Cancel</a>' % runner.key_string)
      elif runner.machine_id != test_manager.DONE_MACHINE_ID:
        runner.status_string = ('<a title="On machine %s, click for details" '
                                'href="#machine_%s">Running</a>' %
                                (runner.machine_id, runner.machine_id))
        runner.started_string = self.GetTimeString(runner.started)
        runner.host_used = runner.hostname
      else:
        # If this runner successfully completed, and we are not showing them,
        # just ignore it.
        if runner.ran_successfully and not show_success:
          continue

        runner.started_string = self.GetTimeString(runner.started)
        runner.ended_string = self.GetTimeString(runner.ended)

        runner.host_used = runner.hostname

        if runner.ran_successfully:
          runner.status_string = (
              '<a title="Click to see results" href="/secure/get_result?r=%s">'
              'Succeeded</a>' % runner.key_string)
        else:
          runner.failed_test_class_string = 'failed_test'
          runner.command_string = (
              '<a href="/secure/retry?r=%s">Retry</a>' % runner.key_string)
          runner.status_string = (
              '<a title="Click to see results" href="/secure/get_result?r=%s">'
              'Failed</a>' % runner.key_string)

      runners.append(runner)

    # Build info for acquired machines table.
    machines = []
    for machine in machine_manager.Machine.all():
      machine.status_name = 'Unknown'
      if machine.status == base_machine_provider.MachineStatus.WAITING:
        machine.status_name = 'Waiting'
      elif machine.status == base_machine_provider.MachineStatus.ACQUIRED:
        machine.status_name = 'Acquired'
      elif machine.status == base_machine_provider.MachineStatus.STOPPED:
        machine.status_name = 'Stopped'
      elif machine.status == base_machine_provider.MachineStatus.AVAILABLE:
        machine.status_name = 'Available'

      # See if the machine is idle or not.
      idle = test_manager.IdleMachine.gql('WHERE id = :1', machine.id).get()
      if idle:
        machine.action_name = 'Idle'
      else:
        runner = (test_manager.TestRunner.gql('WHERE machine_id = :1',
                                              machine.id).get())
        if runner:
          if runner.started:
            machine.action_name = ('Running <a href="#runner_%s">%s</a>' %
                                   (str(runner.key()), runner.GetName()))
          else:
            machine.action_name = ('Assigned <a href="#runner_%s">%s</a>' %
                                   (str(runner.key()), runner.GetName()))
        else:
          machine.action_name = 'No runner?'

      machines.append(machine)

    if users.get_current_user():
      profile_url = '<a href=/secure/user_profile>Profile</a>'
      topbar = ('%s | %s | <a href="%s">Sign out</a>' %
                (users.get_current_user().nickname(), profile_url,
                 users.create_logout_url('/')))
    else:
      topbar = '<a href="%s">Sign in</a>' % users.create_login_url('/')

    if show_success:
      enable_success_message = """
        <a href="?s=False">Hide successfully completed tests</a>
      """
    else:
      enable_success_message = """
        <a href="?s=True">Show successfully completed tests too</a>
      """

    params = {
        'topbar': topbar,
        'runners': runners,
        'machines': machines,
        'enable_success_message': enable_success_message,
        'sorted_by_message': sorted_by_message
    }

    path = os.path.join(os.path.dirname(__file__), 'index.html')
    self.response.out.write(template.render(path, params))


class RedirectToMainHandler(webapp2.RequestHandler):
  """Handler to redirect requests to base page secured main page."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    self.redirect('secure/main')


class TestRequestHandler(webapp2.RequestHandler):
  """Handles test requests from clients."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    test_request_manager = CreateTestManager()

    # Validate the request.
    if not self.request.body:
      self.response.set_status(402)
      self.response.out.write('Request must have a body')
      return

    try:
      test_request_manager.UpdateCacheServerURL(self.request.host_url)
      response = str(test_request_manager.ExecuteTestRequest(self.request.body))
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
    test_request_manager = CreateTestManager()

    logging.debug('Received Result: %s', self.request.url)
    test_request_manager.UpdateCacheServerURL(self.request.host_url)
    test_request_manager.HandleTestResults(self.request)


class PollHandler(webapp2.RequestHandler):
  """Handles cron job to poll Machine Provider to execute pending requests."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    test_request_manager, the_machine_manager = CreateTestAndMachineManagers()

    logging.debug('Polling')
    the_machine_manager.ValidateMachines()
    test_request_manager.UpdateCacheServerURL(self.request.host_url)
    test_request_manager.AssignPendingRequests()
    test_request_manager.AbortStaleRunners()
    self.response.out.write("""
    <html>
    <head>
    <title>Poll Done</title>
    </head>
    <body>Poll Done</body>
    </html>
    """)


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
    test_request_manager = CreateTestManager()

    self.response.headers['Content-Type'] = 'text/plain'

    test_case_name = self.request.get('name', '')

    matches = test_request_manager.GetAllMatchingTestRequests(test_case_name)
    keys = []
    for match in matches:
      keys.extend(map(str, match.GetAllKeys()))

    if keys:
      self.response.out.write('\n'.join(keys))
    else:
      self.response.out.write('No matching Test Cases')


class GetResultHandler(webapp2.RequestHandler):
  """Show the full result string from a test runner."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    test_request_manager = CreateTestManager()

    self.response.headers['Content-Type'] = 'text/plain'

    runner = None
    key = self.request.get('r', '')
    runner = None
    if key:
      try:
        runner = test_manager.TestRunner.get(key)
      except db.BadKeyError:
        pass

    if runner:
      results = test_request_manager.GetResults(runner)
      self.response.out.write(json.dumps(results))
    else:
      self.response.set_status(204)


class CleanupResultsHandler(webapp2.RequestHandler):
  """Delete the Test Runner with the given key."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    test_request_manager = CreateTestManager()

    self.response.headers['Content-Type'] = 'test/plain'

    key = self.request.get('r', '')
    key_deleted = False
    if key:
      key_deleted = test_request_manager.DeleteRunner(key)

    if key_deleted:
      self.response.out.write('Key deleted.')
    else:
      self.response.out.write('Deletion failed. Key not found.')


class CancelHandler(webapp2.RequestHandler):
  """Cancel a test runner that is not already running."""

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    test_request_manager = CreateTestManager()

    self.response.headers['Content-Type'] = 'text/plain'

    key = self.request.get('r', '')
    if key:
      runner = test_manager.TestRunner.get(key)

    # Make sure found runner is not yet running.
    if runner and runner.machine_id == test_manager.NO_MACHINE_ID:
      test_request_manager.UpdateCacheServerURL(self.request.host_url)
      test_request_manager.AbortRunner(
          runner, reason='Runner is not already running.')
      self.response.out.write('Runner canceled.')
    else:
      self.response.out.write('Cannot find runner or too late to cancel: %s' %
                              key)


class RetryHandler(webapp2.RequestHandler):
  """Retry a test runner again."""

  def get(self):  # pylint: disable-msg=C6409
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
      runner.machine_id = test_manager.NO_MACHINE_ID
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
    test_request_manager = CreateTestManager()

    # Validate the request.
    if not self.request.body:
      self.response.set_status(402)
      self.response.out.write('Request must have a body')
      return

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


class UserProfileHandler(webapp2.RequestHandler):
  """Handler for the user profile page of the web server.

  This handler lists user info, such as their IP whitelist and settings.
  """

  def get(self):  # pylint: disable-msg=C6409
    """Handles HTTP GET requests for this handler's URL."""
    user = users.get_current_user()
    if user:
      main_url = '<a href=/secure/main>Home</a>'
      topbar = ('%s | %s | <a href="%s">Sign out</a>' %
                (user.nickname(), main_url, users.create_logout_url('/')))
    else:
      topbar = ('<a href="%s">Sign in</a>' % users.create_login_url('/'))

    display_whitelists = []

    user_profile = test_manager.UserProfile.all().filter('user =', user).get()
    if user_profile:
      for stored_whitelist in user_profile.whitelist:
        whitelist = {}
        whitelist['ip'] = stored_whitelist.ip
        whitelist['username'] = user_profile.user.email()
        whitelist['password'] = stored_whitelist.password
        whitelist['key'] = stored_whitelist.key()
        whitelist['url'] = '/secure/change_whitelist'
        display_whitelists.append(whitelist)

    params = {
        'topbar': topbar,
        'whitelists': display_whitelists,
    }

    path = os.path.join(os.path.dirname(__file__), 'user_profile.html')
    self.response.out.write(template.render(path, params))


class ChangeWhitelistHandler(webapp2.RequestHandler):
  """Handler for making changes to a user whitelist."""

  def post(self):  # pylint: disable-msg=C6409
    """Handles HTTP POST requests for this handler's URL."""
    test_request_manager = CreateTestManager()

    add = self.request.get('a')
    ip = self.request.get('i')
    password = self.request.get('p', None)

    if (add == 'True' or add == 'False') and ip:
      test_request_manager.ModifyUserProfileWhitelist(
          ip, add == 'True', password)

    self.redirect('/secure/user_profile', permanent=True)


def CreateTestManager():
  """Creates and returns a test manager instance.

  Returns:
    A TestManager instance.
  """
  the_machine_manager = machine_manager.MachineManager(
      machine_provider.MachineProvider())
  test_request_manager = test_manager.TestRequestManager(the_machine_manager)

  return test_request_manager


# Temporary function to keep backward compatibility until we totally
# eliminate machine_manager.
def CreateTestAndMachineManagers():
  """Creates and returns a test manager and machine manager instance.

  Returns:
    A tuple of (TestManager, MachineManager).
  """
  the_machine_manager = machine_manager.MachineManager(
      machine_provider.MachineProvider())
  test_request_manager = test_manager.TestRequestManager(the_machine_manager)

  return test_request_manager, the_machine_manager


def CreateApplication():
  return webapp2.WSGIApplication([('/', RedirectToMainHandler),
                                  ('/cleanup_results',
                                   CleanupResultsHandler),
                                  ('/get_matching_test_cases',
                                   GetMatchingTestCasesHandler),
                                  ('/poll_for_test', RegisterHandler),
                                  ('/result', ResultHandler),
                                  ('/secure/cancel', CancelHandler),
                                  ('/secure/change_whitelist',
                                   ChangeWhitelistHandler),
                                  ('/secure/get_result',
                                   GetResultHandler),
                                  ('/secure/main', MainHandler),
                                  ('/secure/retry', RetryHandler),
                                  ('/secure/show_message',
                                   ShowMessageHandler),
                                  ('/secure/user_profile', UserProfileHandler),
                                  ('/tasks/poll', PollHandler),
                                  ('/test', TestRequestHandler)],
                                 debug=True)

app = CreateApplication()
