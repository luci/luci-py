# Copyright 2013 Google Inc. All Rights Reserved.

"""Test Request.

Test Request objects represent one test request from one client.  The client
can be a build machine requesting a test after a build or it could be a
developer requesting a test from their own build.
"""



from google.appengine.ext import ndb

from common import dimensions_utils
from common import test_request_message
from server import test_runner

# The key for the model that is the parent of every TestRequest. This parent
# allows transaction queries on TestRequest.
TEST_REQUEST_PARENT_KEY = 'test_request_parent_key'


def GetTestCase(request_message):
  """Returns a TestCase object representing this Test Request message.

  Args:
    request_message: The request message to convert.

  Returns:
    A TestCase object representing this Test Request.

  Raises:
    test_request_message.Error: If the request's message isn't valid.
  """
  request_object = test_request_message.TestCase()
  errors = []
  if not request_object.ParseTestRequestMessageText(request_message, errors):
    raise test_request_message.Error('\n'.join(errors))

  return request_object


def GetTestRequestParent():
  """Gets the parent model for all TestRequests.

  Returns:
    The parent model for all TestRequests.
  """
  try:
    return ndb.Model.get_or_insert(TEST_REQUEST_PARENT_KEY)
  except ndb.KindError:
    # This exception is thrown when first trying to find the model on a app
    # engine server (dev or non-dev). This error doesn't occur in the test
    # framework though.
    parent_model = ndb.Model(id=TEST_REQUEST_PARENT_KEY)
    parent_model.put()
    return parent_model


class TestRequest(ndb.Model):
  # The message received from the caller, formatted as a Test Case as
  # specified in
  # http://code.google.com/p/swarming/wiki/SwarmFileFormat.
  message = ndb.TextProperty()

  # The time at which this request was received.
  requested_time = ndb.DateTimeProperty(auto_now_add=True)

  # The name for this test request.
  name = ndb.StringProperty()

  @property
  def runners(self):
    return test_runner.TestRunner.query(
        test_runner.TestRunner.request == self.key)

  def __init__(self, *args, **kwargs):
    # 'parent' can be the first arg or a keyword, only add a parent if there
    # isn't one.
    if not args and 'parent' not in kwargs:
      parent_model = GetTestRequestParent()
      kwargs['parent'] = parent_model.key

    super(TestRequest, self).__init__(*args, **kwargs)

  def GetTestCase(self):
    """Returns a TestCase object representing this Test Request.

    Returns:
      A TestCase object representing this Test Request.

    Raises:
      test_request_message.Error: If the request's message isn't valid.
    """
    # NOTE: because _request_object is not declared with db.Property, it will
    # not be persisted to the data store.  This is used as a transient cache of
    # the test request message to keep from evaluating it all the time
    request_object = getattr(self, '_request_object', None)
    if not request_object:
      request_object = GetTestCase(self.message)
      self._request_object = request_object

    return request_object

  def GetConfiguration(self, config_name):
    """Gets the named configuration.

    Args:
      config_name: The name of the configuration to get.

    Returns:
      A configuration dictionary for the named configuration, or None if the
      name is not found.
    """
    for configuration in self.GetTestCase().configurations:
      if configuration.config_name == config_name:
        return configuration

    return None

  def GetConfigurationDimensionHash(self, config_name):
    """Gets the hash of the named configuration.

    Args:
      config_name: The name of the configuration to get the hash for.

    Returns:
      The hash of the configuration.
    """
    return dimensions_utils.GenerateDimensionHash(
        self.GetConfiguration(config_name).dimensions)

  def GetAllKeys(self):
    """Get all the keys representing the TestRunners owned by this instance.

    Returns:
      A list of all the keys.
    """
    return [runner.key for runner in self.runners]

  def DeleteIfNoMoreRunners(self):
    # Delete this request if we have deleted all the runners that were created
    # because of it.
    if self.runners.count() == 0:
      self.key.delete()


def GetAllMatchingTestRequests(test_case_name):
  """Returns a list of all Test Request that match the given test_case_name.

  Args:
    test_case_name: The test case name to search for.

  Returns:
    A list of all Test Requests that have |test_case_name| as their name.
  """
  parent_model = GetTestRequestParent()

  # Perform the query in a transaction to ensure that it gets the most recent
  # data, otherwise it is possible for one machine to add tests, and then be
  # unable to find them through this function after.
  def GetMatches():
    return TestRequest.gql('WHERE name = :1 AND ANCESTOR IS :2',
                           test_case_name, parent_model.key)
  query = ndb.transaction(GetMatches)

  return [request for request in query]
