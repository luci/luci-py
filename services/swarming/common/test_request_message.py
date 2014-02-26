# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""A function to manage a Test Request Message to/from text.

Using test request format as described in more details here:
http://goto/gforce/test-request-format, this class converts and validate a
string into a Test Request Message.

There are classes for all types of messages so that you can validate that
a text message is properly formatted for that specific test request message
type. An API is also available on each of these classes to allow creation
of new test request messages and convert them to text.

Note that the data members of the classes must use the exact same names as
the dictionary keys in the Test Request Format so that we can interact with
them using the __dict__ of the class.

Classes:
  Error: A simple error exception properly scoped to this module.
  TestRequestMessageBase: Base class with methods common to all messages.
  TestObject: For the simple Test Object to be used in other messages.
  TestConfiguration: For the Configuration object to be used in other messages.
  TestCase: For the Test Case messages.
  TestRun: For the Test Run messages.
"""


import json
import logging
import urllib
import urlparse


# All the accepted url schemes.
VALID_URL_SCHEMES = ['http', 'https', 'file', 'mailto']

# The default encoding to assume for the test output.
DEFAULT_ENCODING = 'ascii'

# The default working directory.
# TODO(user): Change this value if the system isn't windows.
DEFAULT_WORKING_DIR = r'c:\swarm_tests'

# The maximum priority value that a runner can have.
MAX_PRIORITY_VALUE = 1000

# The time (in seconds) to wait after receiving a runner before aborting it.
# This is intended to delete runners that will never run because they will
# never find a matching machine.
SWARM_RUNNER_MAX_WAIT_SECS = 24 * 60 * 60


class Error(Exception):
  """Simple error exception properly scoped here."""
  pass


def Stringize(value, json_readable=False):
  """Properly convert value to a string.

  This is useful for objects deriving from TestRequestMessageBase so that
  we can explicitly convert them to strings instead of getting the
  usual <__main__.XXX object at 0x...>.

  Args:
    value: The value to Stringize.
    json_readable: If true, the string output will be valid to load with
        json.loads().

  Returns:
    The stringized value.
  """
  if isinstance(value, (list, tuple)):
    value = '[%s]' % ', '.join([Stringize(i, json_readable) for i in value])
  elif isinstance(value, dict):
    value = '{%s}' % ', '.join([('%s: %s' % (Stringize(i, json_readable),
                                             Stringize(value[i],
                                                       json_readable)))
                                for i in sorted(value)])
  elif isinstance(value, TestRequestMessageBase):
    value = value.__str__(json_readable)
  elif isinstance(value, basestring):
    if json_readable:
      value = value.replace('\\', '\\\\')
    value = u'\"%s\"' % value if json_readable else u'\'%s\'' % value
  elif json_readable and value is None:
    value = u'null'
  elif json_readable and isinstance(value, bool):
    value = u'true' if value else u'false'
  else:
    value = unicode(value)
  return value


def ExpandVariable(value, variables):
  """Expands the given value with outer variables dictionary.

  Args:
    value: The value to be expanded
  Returns:
    The resulting expanded value.
  """
  if isinstance(value, basestring):
    # Because it is possible for some url paths to contain '%' without referring
    # to variables that should be expanded, we unescape them before expanding
    # the variables and then escape them again before returning.
    unquoted = urllib.unquote(value)
    was_quoted = (value != unquoted)
    if was_quoted:
      value = unquoted
    value %= variables

    # Since the contents of the expanded variables aren't guaranteed to get
    # escaped, they should not require escaping.
    if was_quoted:
      # Don't escape ':' or '/' as doing so will break the format of url
      # strings.
      value = urllib.quote(value, ':/')
  elif isinstance(value, list):
    value = [ExpandVariable(v, variables) for v in value]
  elif isinstance(value, tuple):
    value = tuple(ExpandVariable(v, variables) for v in value)
  elif isinstance(value, TestRequestMessageBase):
    value.ExpandVariables(variables)
  elif isinstance(value, dict):
    for name, val in value.iteritems():
      value[name] = ExpandVariable(val, variables)
  # We must passthru for all non string types, since they can't be expanded.
  return value


class TestRequestMessageBase(object):
  """A Test Request Message base class to provide generic methods.

  It uses the __dict__ of the class to reset it using a new text message
  or to dump it to text when going the other way. So the objects deriving from
  it should have no other instance data members then the ones that are part of
  the Test Request Format.
  """

  def __str__(self, json_readable=False):
    """Returns the request text after validating it.

    If the request isn't valid, an empty string is returned.

    Args:
      json_readable: If true, the string output will be valid to load with
        json.loads().

    Returns:
      The text string representing the request.
    """
    request_text_entries = ['{']
    # We sort the dictionary to ensure the string is always printed the same.
    for item in sorted(self.__dict__):
      request_text_entries.extend([
          Stringize(item, json_readable), ': ',
          Stringize(self.__dict__[item], json_readable), ','])

    # The json format doesn't allow trailing commas.
    if json_readable and request_text_entries[-1] == ',':
      request_text_entries.pop()

    request_text_entries.append('}')
    return ''.join(request_text_entries)

  def __eq__(self, other):
    """Returns a deep compare for equal.

    The default implementation for == does a shallow pointer compare only.
    If the pointers are not the same, it looks for __eq__ for a more specific
    compare, which we want to use to identify identical requests.

    Args:
      other: The other object we must compare too.

    Returns:
      True if other contains the same data as self does.
    """
    return type(self) == type(other) and self.__dict__ == other.__dict__

  def AreValidValues(self, value_keys, value_type, required=False):
    """Raises if any of the values at the given keys are not of the right type.

    Args:
      value_keys: The key names of the values to validate.
      value_type: The type that all the values should be.
      required: An optional flag identifying if the value is required to be
          non-empty. Defaults to False.
    """
    for value_key in value_keys:
      if value_key not in self.__dict__:
        raise Error(
            '%s must have a value for %s' %
            (self.__class__.__name__, value_key))

      value = self.__dict__[value_key]
      # Since 0 is an acceptable required value, but (not 0 == True), we
      # explicity check against 0.
      if required and (not value and value != 0):
        raise Error('%s must have a non-empty value' % value_key)
      # If the value is not required, it could be None, which would
      # not likely be of the value_type. If it is required and None, we would
      # have returned False above.
      if value is not None and not isinstance(value, value_type):
        raise Error('Invalid %s: %s' % (value_key, self.__dict__[value_key]))

  def AreValidLists(self, list_keys, value_type, required=False):
    """Raises if any of the values at the given list keys are not of the right
    type.

    Args:
      list_keys: The key names of the value lists to validate.
      value_type: The type that all the values in the lists should be.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
    """
    self.AreValidValues(list_keys, list, required)

    for value_key in list_keys:
      if self.__dict__[value_key]:
        for value in self.__dict__[value_key]:
          if not isinstance(value, value_type):
            raise Error('Invalid entry in list %s: %s' % (value_key, value))
      elif required:
        raise Error('Missing required %s' % value_key)

  def AreValidDicts(self, list_keys, key_type, value_type, required=False):
    """Raises if any of the values at the given list keys are not of the right
    type.

    Args:
      list_keys: The key names of the value lists to validate.
      key_type: The type that all the keys in the dict should be.
      value_type: The type that all the values in the dict should be.
      required: An optional flag identifying if the dict is required to be
          non-empty. Defaults to False.
    """
    self.AreValidValues(list_keys, dict, required)

    for value_key in list_keys:
      if self.__dict__[value_key]:
        for key, value in self.__dict__[value_key].iteritems():
          if (not isinstance(key, key_type) or
              not isinstance(value, value_type)):
            raise Error('Invalid entry in dict %s: %s' % (value_key, value))
      elif required:
        raise Error('Missing required %s' % value_key)

  def AreValidObjectLists(self, list_keys, object_type, required=False,
                          unique_value_keys=None):
    """Raises if any of the objects of the given lists are not valid.

    Args:
      list_keys: The key names of the value lists to validate.
      object_type: The type of object to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
      unique_value_keys: An optional list of keys to values that must be unique.
    """
    self.AreValidLists(list_keys, object_type, required)

    # We use this dictionary of sets to make sure some values are unique.
    unique_values = {}
    if unique_value_keys:
      for unique_key in unique_value_keys:
        unique_values[unique_key] = set()

    for list_key in list_keys:
      for object_value in self.__dict__[list_key]:
        # Checks all the key name of values that should be unique to see if we
        # have a duplicate, otherwise add this one to the set to make sure we
        # don't see it again later
        if unique_value_keys:
          for unique_key in unique_value_keys:
            if unique_key in object_value.__dict__:
              unique_value = object_value.__dict__[unique_key]
              if unique_value in unique_values[unique_key]:
                raise Error(
                    'Duplicate entry with same value %s: %s in %s' %
                    (unique_key, unique_value, list_key))
              unique_values[unique_key].add(unique_value)
        # Now we validate the whole object.
        object_value.IsValid()

  def IsValidUrl(self, url):
    """Raises if the given value is not a valid URL."""
    if not isinstance(url, basestring):
      raise Error('Unsupported url type, %s, must be a string' % url)

    url_parts = urlparse.urlsplit(url)
    if url_parts[0] not in VALID_URL_SCHEMES:
      raise Error('Unsupported url scheme, %s' % url_parts[0])

  def AreValidUrls(self, url_keys):
    """Raises if any of the value at value_key are not a valid URL."""
    for url_key in url_keys:
      self.IsValidUrl(self.__dict__[url_key])

  def AreValidUrlLists(self, list_keys, required=False):
    """Raises if any of the values in the given lists is not a valid url.

    Args:
      list_keys: The key names of the value lists to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
    """
    self.AreValidValues(list_keys, list, required)

    for list_key in list_keys:
      if self.__dict__[list_key]:
        for value in self.__dict__[list_key]:
          self.IsValidUrl(value)
      elif required:
        raise Error('Missing list %s' % list_key)

  def AreValidDataLists(self, list_keys, required=False):
    """Raises if any of the values in the given lists are not valid 'data'.

    Valid data is either a tuple/list of (valid url, local file name)
    or just a valid url.

    Args:
      list_keys: The key names of the value lists to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
    """
    self.AreValidValues(list_keys, list, required)

    for list_key in list_keys:
      if self.__dict__[list_key]:
        for value in self.__dict__[list_key]:
          if not isinstance(value, (basestring, list, tuple)):
            raise Error(
                'Data list wrong type, must be tuple or basestring, got %s' %
                type(value))

          if isinstance(value, basestring):
            self.IsValidUrl(value)
          else:
            if len(value) != 2:
              raise Error(
                  'Incorrect length, should be 2 but is %d' % len(value))
            self.IsValidUrl(value[0])
            if not isinstance(value[1], basestring):
              raise Error(
                  'Local path should be of type basestring, got %s' %
                  type(value[1]))
      elif required:
        raise Error('Missing list %s' % list_key)

  def IsValidInteger(self, value):
    """Raises if the given value is not castable to a valid integer.

    The value must not only be castable to a valid integer, but it must also be
    a whole number (i.e. 8.0 is valid but 8.5 is not).
    """
    try:
      long(value)
    except ValueError:
      raise Error('Invalid value for size, %s, must be long castable' % value)

    if isinstance(value, float) and int(value) != value:
      raise Error(
          'Size in output destination must be a whole number, was given %s' %
          value)

  def AreValidOutputDestinations(self, value_keys):
    """Raises if any of the values at value_keys are not valid a
    output_destinations.

    Args:
      value_keys: The key names of the values to validate.
    """
    for value_key in value_keys:
      output_destination = self.__dict__[value_key]
      if output_destination is None:
        continue
      if not isinstance(output_destination, dict):
        raise Error(
            'Output destination must be a dictionary, was given: %s' %
            output_destination)

      for key, value in output_destination.iteritems():
        if key == 'size':
          self.IsValidInteger(value)
          if isinstance(value, basestring):
            # If we reach here then value is a valid integer, just in string
            # form, so we convert it to an long to prevent problems with later
            # code not using it correctly. int may be too short for large files
            # on 32 bits platforms.
            output_destination[key] = long(value)
        elif key == 'url':
          self.IsValidUrl(value)
        else:
          raise Error('Invalid key, %s, in output destination' % key)

  def IsValidEncoding(self, encoding):
    """Raises if the given encoding is not valid."""
    try:
      unicode('0', encoding)
    except LookupError:
      raise Error('Invalid encoding %s' % encoding)

  def IsValid(self):
    """Raises if the current content is not valid."""
    raise NotImplementedError()

  @staticmethod
  def ConvertDictionaryToObjectType(dictionary, object_type):
    """Convert a dictionary to an object instance.

    Args:
      dictionary: The dictionary to convert to objects.
      object_type: The type of objects the list entries must be converted to.
          This type of object must expose a ParseDictionary() method.

    Returns:
      An object of the specified type.

    Raises:
      Error: If the dictionary has type errors. The text of the Error exception
          will be set with the type error text message.
    """
    new_object = object_type()
    new_object.ParseDictionary(dictionary)
    return new_object

  @staticmethod
  def ConvertDictionariesToObjectType(dict_list, object_type):
    """Convert all dictionaries in the given list to an object instance.

    Args:
      dict_list: The list of dictionaries to convert to objects.
          The list is updated in place where the dictionaries in the list are
          replaced by object instances.
      object_type: The type of objects the list entries must be converted to.
          This type of object must expose a ParseDictionary() method.

    Raises:
      Error: If dict_list has type errors. The text of the Error exception will
          be set with the type error text message.
    """
    for index in range(len(dict_list)):
      dictionary = dict_list[index]
      dict_list[index] = TestRequestMessageBase.ConvertDictionaryToObjectType(
          dictionary, object_type)
      if dict_list[index] is None:
        raise Error(
            'Invalid dictionary for: %s\n%s' % (object_type, dictionary))

  def ExpandVariables(self, variables):
    """Expands the provided variables in all our text fields.

    Warning: This modifies the instance in-place.

    Args:
      variables: A dictionary containing the variable values.
    """
    # TODO(maruel): This is very weird.
    ExpandVariable(self.__dict__, variables)

  def ParseDictionary(self, dictionary):
    """Parses the given dictionary and merge it into our __dict__.

    Raises:
      Error: If the dictionary has type errors. The text of the Error exception
          will be set with the type error text message.
    """
    # We only want to get the values that are meaningful for us.
    if not isinstance(dictionary, dict):
      raise Error('Invalid dictionary not a dict: %s' % dictionary)
    for item in self.__dict__:
      if item in dictionary:
        self.__dict__[item] = dictionary[item]

  def ParseTestRequestMessageText(self, message_text):
    """Parses the given text, convert it to a test request and validate it.

    Args:
      message_text: The text to be parsed as a Test Request Message.

    Raises:
      Error: If the text has syntax or type errors. The text of the Error
          exception will be set with the syntax/type error text message.
    """
    try:
      test_request = json.loads(message_text)
    except (TypeError, ValueError), e:
      message = ('Failed to evaluate text:\n-----\n%s\n-----\n'
                 'Exception: %s' % (message_text, e))
      logging.exception(message)
      raise Error(message)
    self.ParseDictionary(test_request)
    self.IsValid()


class TestObject(TestRequestMessageBase):
  """The object to hold on and validate attributes for a test.

  Attributes:
    test_name: The name of this test object.
    env_vars: An optional dictionary for environment variables.
    action: The action list of this test object.
    decorate_output: The output decoration flag of this test object.
    hard_time_out: The maximum time this test can take.
    io_time_out: The maximum time this test can take (resetting anytime the
        test writes to stdout).
  """

  def __init__(self, test_name=None, env_vars=None, action=None,
               decorate_output=True, hard_time_out=3600.0, io_time_out=1200.0):
    super(TestObject, self).__init__()
    self.test_name = test_name
    if env_vars:
      self.env_vars = env_vars.copy()
    else:
      self.env_vars = None
    if action:
      self.action = action
    else:
      self.action = []
    self.decorate_output = decorate_output
    self.hard_time_out = hard_time_out
    self.io_time_out = io_time_out

  def IsValid(self):
    """Raises if the current content is not valid."""
    self.AreValidValues(['test_name'], basestring, required=True)
    self.AreValidDicts(['env_vars'], basestring, basestring)
    self.AreValidLists(['action'], basestring, required=True)
    self.AreValidValues(['hard_time_out', 'io_time_out'], (int, long, float))

    # self.decorate_output doesn't need to be validated since we only need
    # to evaluate it to True/False which can be done with any type.
    logging.debug('Successfully validated request: %s', self.__dict__)


class TestConfiguration(TestRequestMessageBase):
  """The object to hold on and validate attributes for a configuration.

  Attributes:
    config_name: The name of this configuration.
    env_vars: An optional dictionary for environment variables.
    data: An optional data list for this configuration. The strings must be
        valid urls.
    tests: An optional tests list for this configuration.
    min_instances: An optional integer specifying the minimum number of
        instances of this configuration we want. Defaults to 1.
        Must be greater than 0.
    additional_instances: An optional integer specifying the maximum number of
        additional instances of this configuration we want. Defaults to 0.
        Must be greater than 0.
    deadline_to_run: An optional value that specifies how long the test can
        wait before it is aborted (in seconds). Defaults to
        SWARM_RUNNER_MAX_WAIT_SECS if no value is given.
    priority: The priority of this configuartion, used to determine execute
        order (a lower number is higher priority). Defaults to 10, the
        acceptable values are [0, MAX_PRIORITY_VALUE].
    dimensions: A dictionary of strings or list of strings for dimensions.
  """

  def __init__(self, config_name=None, env_vars=None, data=None, tests=None,
               min_instances=1, additional_instances=0,
               deadline_to_run=SWARM_RUNNER_MAX_WAIT_SECS,
               priority=100, **dimensions):
    super(TestConfiguration, self).__init__()
    self.config_name = config_name
    if env_vars:
      self.env_vars = env_vars.copy()
    else:
      self.env_vars = None
    if data:
      self.data = data[:]
    else:
      self.data = []
    if tests:
      self.tests = tests[:]
    else:
      self.tests = []
    self.min_instances = min_instances
    self.additional_instances = additional_instances
    self.deadline_to_run = deadline_to_run
    self.priority = priority

    # Dimensions are kept dynamic so that we don't have to update this code
    # when the list of configuration dimensions changes.
    self.dimensions = dimensions

  def IsValid(self):
    """Raises if the current content is not valid."""
    self.AreValidValues(['config_name'], basestring, required=True)
    self.AreValidDicts(['env_vars'], basestring, basestring)
    self.AreValidDataLists(['data'])
    self.AreValidObjectLists(
        ['tests'], TestObject, unique_value_keys=['test_name'])
    # required=True to make sure the caller doesn't set it to None.
    self.AreValidValues(
        ['min_instances', 'additional_instances', 'deadline_to_run',
          'priority'],
        (int, long), required=True)

    if (self.min_instances < 1 or self.additional_instances < 0 or
        self.deadline_to_run < 0 or self.priority < 0 or
        self.priority > MAX_PRIORITY_VALUE):
      raise Error('Invalid TestConfiguration: %s' % self.__dict__)

    if not isinstance(self.dimensions, dict):
      raise Error(
          'Invalid TestConfiguration dimension type: %s' %
          type(self.dimensions))
    for values in self.dimensions.values():
      if not isinstance(values, (list, tuple)):
        values = [values]
      for value in values:
        if not value or not isinstance(value, basestring):
          raise Error('Invalid TestConfiguration dimension value: %s' % value)

  def ParseDictionary(self, dictionary):
    """Parses the given dictionary and merge it into our __dict__.

    We override the base class behavior to create instances of TestOjbects.

    Args:
      dictionary: The dictionary to be parsed and merged into __dict__.

    Raises:
      Error: If the dictionary has type errors. The text of the Error exception
          will be set with the type error text message.
    """
    super(TestConfiguration, self).ParseDictionary(dictionary)
    self.ConvertDictionariesToObjectType(self.tests, TestObject)


class TestCase(TestRequestMessageBase):
  """The object to hold on and validate attributes for a test case.

  Attributes:
    test_case_name: The name of this test case.
    requestor: The id of the user requesting this test (generally an email
        address).
    env_vars: An optional dictionary for environment variables.
    configurations: A list of configurations for this test case.
    data: An optional data list for this test case. The strings must be
        valid urls.
    working_dir: An optional path string for where to download/run tests.
        This must be an absolute path though we don't validate it since this
        script may run on a different platform than the one that will use the
        path. Defaults to c:\\swarm_tests.
        TODO(user): Also support other platforms.
    admin: An optional boolean value that specifies if the tests should be run
        with admin privilege or not.
    tests: An optional tests list for this test case.
    result_url: An optional URL where to post the results of this test case.
    store_result: The key to access the test run's storage string.
    restart_on_failure: An optional value indicating if the machine running the
        tests should restart if any of the tests fail.
    output_destination: An optional dictionary with a URL where to post the
        output of this test case as well as the size of the chunks to use.
        The key for the URL is 'url' and the value must be a valid URL string.
        The key for the chunk size is 'size'. It must be a whole number.
    encoding: The encoding of the tests output.
    cleanup: The key to access the test run's cleanup string.
    failure_email: An optional email where to broadcast failures for this test
        case.
    label: An optional string that can be used to label this test case.
    verbose: An optional boolean value that specifies if logging should be
        verbose or not.
  """
  VALID_STORE_RESULT_VALUES = [None, '', 'all', 'fail', 'none']

  def __init__(self, test_case_name=None, requestor=None, env_vars=None,
               configurations=None, data=None, working_dir=DEFAULT_WORKING_DIR,
               admin=False, tests=None, result_url=None, store_result=None,
               restart_on_failure=None, output_destination=None,
               encoding=DEFAULT_ENCODING, cleanup=None, failure_email=None,
               label=None, verbose=False):
    super(TestCase, self).__init__()
    self.test_case_name = test_case_name
    # TODO(csharp): Stop using a default so test requests that don't give a
    # requestor are rejected.
    self.requestor = requestor or 'unknown'
    if env_vars:
      self.env_vars = env_vars.copy()
    else:
      self.env_vars = None
    if configurations:
      self.configurations = configurations[:]
    else:
      self.configurations = []
    if data:
      self.data = data[:]
    else:
      self.data = []
    self.working_dir = working_dir
    self.admin = admin
    if tests:
      self.tests = tests[:]
    else:
      self.tests = []
    self.result_url = result_url
    self.store_result = store_result
    self.restart_on_failure = restart_on_failure
    if output_destination:
      self.output_destination = output_destination.copy()
    else:
      self.output_destination = None
    self.encoding = encoding
    self.cleanup = cleanup
    self.failure_email = failure_email
    self.label = label
    self.verbose = verbose

  def IsValid(self):
    """Raises if the current content is not valid."""
    self.AreValidValues(['test_case_name'], basestring, required=True)
    self.AreValidValues(
        ['requestor', 'working_dir', 'failure_email', 'result_url', 'label'],
        basestring)
    self.AreValidDicts(['env_vars'], basestring, basestring)
    self.AreValidObjectLists(
        ['configurations'], TestConfiguration, required=True,
        unique_value_keys=['config_name'])
    self.AreValidDataLists(['data'])
    self.AreValidObjectLists(
        ['tests'], TestObject, unique_value_keys=['test_name'])
    self.AreValidOutputDestinations(['output_destination'])

    if self.encoding:
      self.IsValidEncoding(self.encoding)
    if self.result_url:
      self.AreValidUrls(['result_url'])

    if (self.cleanup not in TestRun.VALID_CLEANUP_VALUES or
        self.store_result not in TestCase.VALID_STORE_RESULT_VALUES):
      raise Error('Invalid TestCase: %s' % self.__dict__)

    # self.verbose and self.admin don't need to be validated since we only need
    # to evaluate them to True/False which can be done with any type.

  def ParseDictionary(self, dictionary):
    """Parses the given dictionary and merge it into our __dict__.

    We override the base class behavior to create instances of TestOjbects and
    TestConfiguration.

    Args:
      dictionary: The dictionary to be parsed and merged into __dict__.

    Raises:
      Error: If the dictionary has type errors. The text of the Error exception
          will be set with the type error text message.
    """
    super(TestCase, self).ParseDictionary(dictionary)
    self.ConvertDictionariesToObjectType(self.tests, TestObject)
    self.ConvertDictionariesToObjectType(self.configurations, TestConfiguration)


class TestRun(TestRequestMessageBase):
  """The object to hold on and validate attributes for a test run.

  Attributes:
    test_run_name: The name of the test run.
    env_vars: An optional dictionary for environment variables.
    configuration: An optional configuration object for this test run.
    data: An optional data list for this test run.
    working_dir: An optional path string for where to download/run tests.
        This must be an absolute path though we don't validate it since this
        script may run on a different platform than the one that will use the
        path. Defaults to c:\\swarm_tests.
        TODO(user): Also support other platforms.
    tests: An optional tests list for this test run.
    instance_index: An optional integer specifying the zero based index of this
        test run instance of the given configuration. Defaults to None.
        Must be specified if num_instances is specified.
    num_instances: An optional integer specifying the number of test run
        instances of this configuration that have been shared. Defaults to None.
        Must be greater than instance_index.
        Must be specified if instance_index is specified.
    result_url: An optional URL where to post the results of this test run.
    ping_url: A required URL that tells the test run where to ping to let the
        server know that it is still active.
    ping_delay: The amount of time to wait between pings (in seconds).
    output_destination: An optional dictionary with a URL where to post the
        output of this test case as well as the size of the chunks to use.
        The key for the URL is 'url' and the value must be a valid URL string.
        The key for the chunk size is 'size'. It must be a whole number.
    cleanup: The key to access the test run's cleanup string.
    restart_on_failure: An optional value indicating if the machine running the
        tests should restart if any of the tests fail.
    encoding: The character encoding to use.
  """
  VALID_CLEANUP_VALUES = [None, '', 'zip', 'data', 'root']

  def __init__(self, test_run_name=None, env_vars=None,
               configuration=TestConfiguration(), data=None,
               working_dir=DEFAULT_WORKING_DIR, tests=None,
               instance_index=None, num_instances=None, result_url=None,
               ping_url=None, ping_delay=None, output_destination=None,
               cleanup=None, restart_on_failure=None,
               encoding=DEFAULT_ENCODING):
    super(TestRun, self).__init__()
    self.test_run_name = test_run_name
    if env_vars:
      self.env_vars = env_vars.copy()
    else:
      self.env_vars = env_vars
    self.configuration = configuration
    if data:
      self.data = data[:]
    else:
      self.data = []
    self.working_dir = working_dir
    if tests:
      self.tests = tests[:]
    else:
      self.tests = []
    self.instance_index = instance_index
    self.num_instances = num_instances
    self.result_url = result_url
    self.ping_url = ping_url
    self.ping_delay = ping_delay
    if output_destination:
      self.output_destination = output_destination.copy()
    else:
      self.output_destination = None
    self.cleanup = cleanup
    self.restart_on_failure = restart_on_failure
    self.encoding = encoding

  def IsValid(self):
    """Raises if the current content is not valid."""
    self.AreValidValues(['test_run_name'], basestring, required=True)
    self.AreValidDicts(['env_vars'], basestring, basestring)
    if self.result_url:
      self.IsValidUrl(self.result_url)
    self.IsValidUrl(self.ping_url)
    self.AreValidValues(['ping_delay'], (int, long), required=True)
    self.AreValidDataLists(['data'])
    self.AreValidObjectLists(
        ['tests'], TestObject, unique_value_keys=['test_name'])
    self.AreValidOutputDestinations(['output_destination'])
    self.AreValidValues(
        ['working_dir', 'result_url', 'ping_url'], basestring)
    self.AreValidValues(['instance_index', 'num_instances'], (int, long))
    self.IsValidEncoding(self.encoding)

    if (not self.configuration or
        not isinstance(self.configuration, TestConfiguration) or
        self.ping_delay < 0 or
        self.cleanup not in TestRun.VALID_CLEANUP_VALUES or
        (self.instance_index is not None and self.num_instances is None) or
        (self.num_instances is not None and self.instance_index is None) or
        (self.num_instances is not None and
         self.instance_index >= self.num_instances)):  # zero based index.
      raise Error('Invalid TestRun: %s' % self.__dict__)

    self.configuration.IsValid()

  def ParseDictionary(self, dictionary):
    """Parses the given dictionary and merge it into our __dict__.

    We override the base class behavior to create instances of TestOjbects and
    TestConfiguration.

    Args:
      dictionary: The dictionary to be parsed and merged into __dict__.

    Raises:
      Error: If the dictionary has type errors. The text of the Error exception
          will be set with the type error text message.
    """
    super(TestRun, self).ParseDictionary(dictionary)
    self.ConvertDictionariesToObjectType(self.tests, TestObject)
    self.configuration = self.ConvertDictionaryToObjectType(
        self.configuration, TestConfiguration)
    if self.configuration is None:
      raise Error('Missing configuration')
