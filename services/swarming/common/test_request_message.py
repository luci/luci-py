# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Defines all the objects used by the Swarming API and its serialization."""

import json
import logging
import urllib
import urlparse


# All the accepted url schemes.
VALID_URL_SCHEMES = ['http', 'https', 'file', 'mailto']

# The default encoding to assume for the test output.
DEFAULT_ENCODING = 'ascii'

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
  def __init__(self, **kwargs):
    if kwargs:
      # Cheezy but this will go away with SerializableModelMixin.
      logging.warning('Ignored arguments: %r', kwargs)

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

  def ValidateValues(self, value_keys, value_type, required=False):
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

  def ValidateLists(self, list_keys, value_type, required=False):
    """Raises if any of the values at the given list keys are not of the right
    type.

    Args:
      list_keys: The key names of the value lists to validate.
      value_type: The type that all the values in the lists should be.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
    """
    self.ValidateValues(list_keys, list, required)

    for value_key in list_keys:
      if self.__dict__[value_key]:
        for value in self.__dict__[value_key]:
          if not isinstance(value, value_type):
            raise Error('Invalid entry in list %s: %s' % (value_key, value))
      elif required:
        raise Error('Missing required %s' % value_key)

  def ValidateDicts(self, list_keys, key_type, value_type, required=False):
    """Raises if any of the values at the given list keys are not of the right
    type.

    Args:
      list_keys: The key names of the value lists to validate.
      key_type: The type that all the keys in the dict should be.
      value_type: The type that all the values in the dict should be.
      required: An optional flag identifying if the dict is required to be
          non-empty. Defaults to False.
    """
    self.ValidateValues(list_keys, dict, required)

    for value_key in list_keys:
      if self.__dict__[value_key]:
        for key, value in self.__dict__[value_key].iteritems():
          if (not isinstance(key, key_type) or
              not isinstance(value, value_type)):
            raise Error('Invalid entry in dict %s: %s' % (value_key, value))
      elif required:
        raise Error('Missing required %s' % value_key)

  def ValidateObjectLists(self, list_keys, object_type, required=False,
                          unique_value_keys=None):
    """Raises if any of the objects of the given lists are not valid.

    Args:
      list_keys: The key names of the value lists to validate.
      object_type: The type of object to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
      unique_value_keys: An optional list of keys to values that must be unique.
    """
    self.ValidateLists(list_keys, object_type, required)

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
        object_value.Validate()

  def ValidateUrl(self, url):
    """Raises if the given value is not a valid URL."""
    if not isinstance(url, basestring):
      raise Error('Unsupported url type, %s, must be a string' % url)

    url_parts = urlparse.urlsplit(url)
    if url_parts[0] not in VALID_URL_SCHEMES:
      raise Error('Unsupported url scheme, %s' % url_parts[0])

  def ValidateUrls(self, url_keys):
    """Raises if any of the value at value_key are not a valid URL."""
    for url_key in url_keys:
      self.ValidateUrl(self.__dict__[url_key])

  def ValidateUrlLists(self, list_keys, required=False):
    """Raises if any of the values in the given lists is not a valid url.

    Args:
      list_keys: The key names of the value lists to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
    """
    self.ValidateValues(list_keys, list, required)

    for list_key in list_keys:
      if self.__dict__[list_key]:
        for value in self.__dict__[list_key]:
          self.ValidateUrl(value)
      elif required:
        raise Error('Missing list %s' % list_key)

  def ValidateDataLists(self, list_keys, required=False):
    """Raises if any of the values in the given lists are not valid 'data'.

    Valid data is either a tuple/list of (valid url, local file name)
    or just a valid url.

    Args:
      list_keys: The key names of the value lists to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
    """
    self.ValidateValues(list_keys, list, required)

    for list_key in list_keys:
      if self.__dict__[list_key]:
        for value in self.__dict__[list_key]:
          if not isinstance(value, (basestring, list, tuple)):
            raise Error(
                'Data list wrong type, must be tuple or basestring, got %s' %
                type(value))

          if isinstance(value, basestring):
            self.ValidateUrl(value)
          else:
            if len(value) != 2:
              raise Error(
                  'Incorrect length, should be 2 but is %d' % len(value))
            self.ValidateUrl(value[0])
            if not isinstance(value[1], basestring):
              raise Error(
                  'Local path should be of type basestring, got %s' %
                  type(value[1]))
      elif required:
        raise Error('Missing list %s' % list_key)

  def ValidateInteger(self, value):
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

  def ValidateOutputDestinations(self, value_keys):
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
          self.ValidateInteger(value)
          if isinstance(value, basestring):
            # If we reach here then value is a valid integer, just in string
            # form, so we convert it to an long to prevent problems with later
            # code not using it correctly. int may be too short for large files
            # on 32 bits platforms.
            output_destination[key] = long(value)
        elif key == 'url':
          self.ValidateUrl(value)
        else:
          raise Error('Invalid key, %s, in output destination' % key)

  def ValidateEncoding(self, encoding):
    """Raises if the given encoding is not valid."""
    try:
      unicode('0', encoding)
    except LookupError:
      raise Error('Invalid encoding %s' % encoding)

  def Validate(self):
    """Raises if the current content is not valid."""
    raise NotImplementedError()

  @classmethod
  def FromJSON(cls, data):
    """Parses the given JSON encoded data into an object instance.

    Raises:
      Error: If the data has syntax or type errors.
    """
    try:
      data = json.loads(data)
    except (TypeError, ValueError) as e:
      raise Error('Invalid json: %s' % e)
    return cls.FromDict(data)

  @classmethod
  def FromDict(cls, dictionary):
    """Converts a dictionary to an object instance.

    Args:
      dictionary: The dictionary to convert to objects.

    Returns:
      An object of the specified type.

    Raises:
      Error: If the dictionary has type errors. The text of the Error exception
          will be set with the type error text message.
    """
    try:
      out = cls(**dictionary)
      out.Validate()
      return out
    except (TypeError, ValueError) as e:
      raise Error('Failed to create %s: %s' % (cls.__name__, e))

  @classmethod
  def FromDictList(cls, dict_list):
    """Convert all dictionaries in the given list to an object instance.

    Args:
      dict_list: The list of dictionaries to convert to objects.
      cls: The type of objects the list entries must be converted to. This type
          of object must expose a FromDict() method.
    """
    return [cls.FromDict(d) for d in dict_list]

  def ExpandVariables(self, variables):
    """Expands the provided variables in all our text fields.

    Warning: This modifies the instance in-place.

    Args:
      variables: A dictionary containing the variable values.
    """
    # TODO(maruel): This is very weird.
    ExpandVariable(self.__dict__, variables)


class TestObject(TestRequestMessageBase):
  """Describes a command to run, including the command line.

  A 'task' as described by TestCase can include multiple commands to run. The
  user provides an instance of this class inside a TestCase.

  Attributes:
    test_name: The name of this test object.
    env_vars: An optional dictionary for environment variables.
    action: The command line to run.
    decorate_output: The output decoration flag of this test object.
    hard_time_out: The maximum time this test can take.
    io_time_out: The maximum time this test can take (resetting anytime the test
        writes to stdout).
  """

  def __init__(self, test_name=None, env_vars=None, action=None,
               decorate_output=True, hard_time_out=3600.0, io_time_out=1200.0,
               **kwargs):
    super(TestObject, self).__init__(**kwargs)
    self.test_name = test_name
    self.env_vars = env_vars.copy() if env_vars else {}
    self.action = action[:] if action else []
    self.decorate_output = decorate_output
    self.hard_time_out = hard_time_out
    self.io_time_out = io_time_out

  def Validate(self):
    """Raises if the current content is not valid."""
    self.ValidateValues(['test_name'], basestring, required=True)
    self.ValidateDicts(['env_vars'], basestring, basestring)
    self.ValidateLists(['action'], basestring, required=True)
    self.ValidateValues(['hard_time_out', 'io_time_out'], (int, long, float))

    # self.decorate_output doesn't need to be validated since we only need
    # to evaluate it to True/False which can be done with any type.
    logging.debug('Successfully validated request: %s', self.__dict__)


class TestConfiguration(TestRequestMessageBase):
  """Describes how to choose swarming bot to execute a requests.

  It defines the dimensions that are required and the number of swarming bot
  instances that are going to be used to run this list of 'tests', which is
  actually a task. The user provides an instance of this class inside a
  TestCase.

  Attributes:
    config_name: The name of this configuration.
    env_vars: An optional dictionary for environment variables.
    data: An optional 'data list' for this configuration.
    tests: An optional tests list for this configuration.
    min_instances: An optional integer specifying the minimum number of
        instances of this configuration we want. Defaults to 1. Must be greater
        than 0.
    additional_instances: An optional integer specifying the maximum number of
        additional instances of this configuration we want. Defaults to 0. Must
        be greater than 0. Eh.
        https://code.google.com/p/swarming/issues/detail?id=88
    deadline_to_run: An optional value that specifies how long the test can wait
        before it is aborted (in seconds). Defaults to
        SWARM_RUNNER_MAX_WAIT_SECS.
    priority: The priority of this configuartion, used to determine execute
        order (a lower number is higher priority). Defaults to 10, the
        acceptable values are [0, MAX_PRIORITY_VALUE].
    dimensions: A dictionary of strings or list of strings for dimensions.
  """
  def __init__(self, config_name=None, env_vars=None, data=None, tests=None,
               min_instances=1, additional_instances=0,
               deadline_to_run=SWARM_RUNNER_MAX_WAIT_SECS,
               priority=100, dimensions=None, **kwargs):
    super(TestConfiguration, self).__init__(**kwargs)
    self.config_name = config_name
    self.env_vars = env_vars.copy() if env_vars else {}
    self.data = data[:] if data else []
    self.tests = tests[:] if tests else []
    self.min_instances = min_instances
    self.additional_instances = additional_instances
    self.deadline_to_run = deadline_to_run
    self.priority = priority
    self.dimensions = dimensions.copy() if dimensions else {}

  def Validate(self):
    """Raises if the current content is not valid."""
    self.ValidateValues(['config_name'], basestring, required=True)
    self.ValidateDicts(['env_vars'], basestring, basestring)
    self.ValidateDataLists(['data'])
    self.ValidateObjectLists(
        ['tests'], TestObject, unique_value_keys=['test_name'])
    # required=True to make sure the caller doesn't set it to None.
    self.ValidateValues(
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

  @classmethod
  def FromDict(cls, dictionary):
    """Converts a dictionary to an object instance.

    We override the base class behavior to create instances of TestOjbects.
    """
    dictionary = dictionary.copy()
    dictionary['tests'] = TestObject.FromDictList(dictionary.get('tests', []))
    return super(TestConfiguration, cls).FromDict(dictionary)


class TestCase(TestRequestMessageBase):
  """Describes a task to run.

  It defines the inputs and outputs to run a task on a single swarming bot. The
  task is a list of TestObject commands to run in order. The user provides an
  instance of this class to trigger a Swarming task.

  Attributes:
    test_case_name: The name of this test case.
    requestor: The id of the user requesting this test (generally an email
        address).
    env_vars: An optional dictionary for environment variables.
    configurations: A list of configurations for this test case.
    data: An optional 'data list' for this configuration.
    working_dir: An optional path string for where to download/run tests. This
        must be an absolute path though we don't validate it since this script
        may run on a different platform than the one that will use the path.
    admin: An optional boolean value that specifies if the tests should be run
        with admin privilege or not. TODO(maruel): Remove me.
    tests: An list of TestObject to run for this task.
    result_url: An optional URL where to post the results of this test case.
    store_result: The key to access the test run's storage string.
    restart_on_failure: An optional value indicating if the machine running the
        tests should restart if any of the tests fail.
    output_destination: An optional dictionary with a URL where to post the
        output of this test case as well as the size of the chunks to use. The
        key for the URL is 'url' and the value must be a valid URL string. The
        key for the chunk size is 'size'. It must be a whole number.
    encoding: The encoding of the tests output.
    cleanup: The key to access the test run's cleanup string.
    failure_email: An optional email where to broadcast failures for this test
        case. TODO(maruel): Remove me.
    label: An optional string that can be used to label this test case.
    verbose: An optional boolean value that specifies if logging should be
        verbose.
  """
  VALID_STORE_RESULT_VALUES = (None, '', 'all', 'fail', 'none')

  def __init__(self, test_case_name=None, requestor=None, env_vars=None,
               configurations=None, data=None, working_dir=None,
               admin=False, tests=None, result_url=None, store_result=None,
               restart_on_failure=None, output_destination=None,
               encoding=DEFAULT_ENCODING, cleanup=None, failure_email=None,
               label=None, verbose=False, **kwargs):
    super(TestCase, self).__init__(**kwargs)
    self.test_case_name = test_case_name
    # TODO(csharp): Stop using a default so test requests that don't give a
    # requestor are rejected.
    self.requestor = requestor or 'unknown'
    self.env_vars = env_vars.copy() if env_vars else {}
    self.configurations = configurations[:] if configurations else []
    self.data = data[:] if data else []
    self.working_dir = working_dir
    self.admin = admin
    self.tests = tests[:] if tests else []
    self.result_url = result_url
    self.store_result = store_result
    self.restart_on_failure = restart_on_failure
    self.output_destination = (
        output_destination.copy() if output_destination else {})
    self.encoding = encoding
    self.cleanup = cleanup
    self.failure_email = failure_email
    self.label = label
    self.verbose = verbose

  def Validate(self):
    """Raises if the current content is not valid."""
    self.ValidateValues(['test_case_name'], basestring, required=True)
    self.ValidateValues(
        ['requestor', 'working_dir', 'failure_email', 'result_url', 'label'],
        basestring)
    self.ValidateDicts(['env_vars'], basestring, basestring)
    self.ValidateObjectLists(
        ['configurations'], TestConfiguration, required=True,
        unique_value_keys=['config_name'])
    self.ValidateDataLists(['data'])
    self.ValidateObjectLists(
        ['tests'], TestObject, unique_value_keys=['test_name'])
    self.ValidateOutputDestinations(['output_destination'])

    if self.encoding:
      self.ValidateEncoding(self.encoding)
    if self.result_url:
      self.ValidateUrls(['result_url'])

    if (self.cleanup not in TestRun.VALID_CLEANUP_VALUES or
        self.store_result not in TestCase.VALID_STORE_RESULT_VALUES):
      raise Error('Invalid TestCase: %s' % self.__dict__)

    # self.verbose and self.admin don't need to be validated since we only need
    # to evaluate them to True/False which can be done with any type.

  @classmethod
  def FromDict(cls, dictionary):
    """Converts a dictionary to an object instance.

    We override the base class behavior to create instances of TestOjbects and
    TestConfiguration.
    """
    dictionary = dictionary.copy()
    dictionary['tests'] = TestObject.FromDictList(dictionary.get('tests', []))
    dictionary['configurations'] = TestConfiguration.FromDictList(
        dictionary.get('configurations', []))
    return super(TestCase, cls).FromDict(dictionary)

  def Equivalent(self, test_case):
    """Checks to see if the given test case is equivalent.

    Equivalent in this case just means they would produce the same output,
    not that they are completely equal.

    Args:
      test_case: The new test case being considered.

    Returns:
      True if the given test case could use the results from this request.
    """
    # All of the following values must be the same for the test output to be
    # equal.
    equal_keys = [
        'admin',
        'data',
        'encoding',
        'env_vars',
        'store_result',
        'tests',
        'verbose',
    ]

    for equal_key in equal_keys:
      if getattr(self, equal_key) != getattr(test_case, equal_key):
        return False

    # The following keys must not be set for a TestRequest to be equivalent,
    # because they all have side effects.
    side_effect_keys = [
      'failure_email',
      'output_destination',
      'result_url',
    ]

    for side_effect_key in side_effect_keys:
      if (getattr(self, side_effect_key) !=
          getattr(test_case, side_effect_key)):
        return False

    # The configs must be in the same order before we can begin to compare them.
    def sort_configs(x, y):
      return cmp(x.config_name, y.config_name)

    sorted_configs = sorted(test_case.configurations, sort_configs)
    self_sorted_configs = sorted(self.configurations, sort_configs)

    if (self_sorted_configs != sorted_configs):
      return False

    # Keys that have no impact on the test output and can be safely ignored.
    ignored_keys = [
        'cleanup',
        'label',
        'restart_on_failure',
        'requestor',
        'test_case_name',
        'working_dir',
    ]

    # A sanity check to ensure all values were considered. If a new value is
    # added to TestRequest and not added here, the TestRequest tests should
    # fail here.
    examined_keys = set(['configurations'] + equal_keys + side_effect_keys +
                          ignored_keys)
    missing_keys = examined_keys.symmetric_difference(
        set(test_case.__dict__))
    if missing_keys:
      raise Error(missing_keys)

    return True


class TestRun(TestRequestMessageBase):
  """Contains results of a task execution.

  TODO(maruel): Contains a lot of duplicated fields from TestCase, even if it
  reference them via 'tests'. Should be refactored.

  The Swarming server generates instance of this class for consumption by
  local_test_runner.py. The user does not interact with this API.

  Attributes:
    test_run_name: The name of the test run.
    env_vars: An optional dictionary for environment variables.
    configuration: An optional configuration object for this test run.
    data: An optional 'data list' for this test run.
    working_dir: An optional path string for where to download/run tests. This
        must NOT be an absolute path.
    tests: optional(!?) list of TestObject for this test run.
    instance_index: An optional integer specifying the zero based index of this
        test run instance of the given configuration. Defaults to None. Must be
        specified if num_instances is specified.
    num_instances: An optional integer specifying the number of test run
        instances of this configuration that have been shared. Defaults to None.
        Must be greater than instance_index. Must be specified if instance_index
        is specified.
    result_url: An optional URL where to post the results of this test run.
    ping_url: An URL that tells the test run where to ping to let the server
        know that it is still active.
    ping_delay: The amount of time to wait between pings (in seconds).
    output_destination: An optional dictionary with a URL where to post the
        output of this test case as well as the size of the chunks to use. The
        key for the URL is 'url' and the value must be a valid URL string.  The
        key for the chunk size is 'size'. It must be a whole number.
    cleanup: The key to access the test run's cleanup string.
    restart_on_failure: An optional value indicating if the machine running the
        tests should restart if any of the tests fail.
    encoding: The character encoding to use. 'utf-8' is recommended.
  """
  VALID_CLEANUP_VALUES = (None, '', 'zip', 'data', 'root')

  def __init__(self, test_run_name=None, env_vars=None,
               configuration=None, data=None,
               working_dir=None, tests=None,
               instance_index=None, num_instances=None, result_url=None,
               ping_url=None, ping_delay=None, output_destination=None,
               cleanup=None, restart_on_failure=None,
               encoding=DEFAULT_ENCODING, **kwargs):
    super(TestRun, self).__init__(**kwargs)
    self.test_run_name = test_run_name
    self.env_vars = env_vars.copy() if env_vars else {}
    self.configuration = configuration
    self.data = data[:] if data else []
    self.working_dir = working_dir
    self.tests = tests[:] if tests else []
    self.instance_index = instance_index
    self.num_instances = num_instances
    self.result_url = result_url
    self.ping_url = ping_url
    self.ping_delay = ping_delay
    self.output_destination = (
        output_destination.copy() if output_destination else {})
    self.cleanup = cleanup
    self.restart_on_failure = restart_on_failure
    self.encoding = encoding

  def Validate(self):
    """Raises if the current content is not valid."""
    self.ValidateValues(['test_run_name'], basestring, required=True)
    self.ValidateDicts(['env_vars'], basestring, basestring)
    if self.result_url:
      self.ValidateUrl(self.result_url)
    self.ValidateUrl(self.ping_url)
    self.ValidateValues(['ping_delay'], (int, long), required=True)
    self.ValidateDataLists(['data'])
    self.ValidateObjectLists(
        ['tests'], TestObject, unique_value_keys=['test_name'])
    self.ValidateOutputDestinations(['output_destination'])
    self.ValidateValues(
        ['working_dir', 'result_url', 'ping_url'], basestring)
    self.ValidateValues(['instance_index', 'num_instances'], (int, long))
    self.ValidateEncoding(self.encoding)

    if (not self.configuration or
        not isinstance(self.configuration, TestConfiguration) or
        self.ping_delay < 0 or
        self.cleanup not in TestRun.VALID_CLEANUP_VALUES or
        (self.instance_index is not None and self.num_instances is None) or
        (self.num_instances is not None and self.instance_index is None) or
        (self.num_instances is not None and
         self.instance_index >= self.num_instances)):  # zero based index.
      raise Error('Invalid TestRun: %s' % self.__dict__)

    self.configuration.Validate()

  @classmethod
  def FromDict(cls, dictionary):
    """Converts a dictionary to an object instance.

    We override the base class behavior to create instances of TestOjbects and
    TestConfiguration.
    """
    dictionary = dictionary.copy()
    dictionary['tests'] = TestObject.FromDictList(dictionary.get('tests', []))
    dictionary['configuration'] = TestConfiguration.FromDict(
        dictionary.get('configuration', {}))
    return super(TestRun, cls).FromDict(dictionary)
