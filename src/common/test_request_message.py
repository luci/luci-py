#!/usr/bin/python2.4
#
# Copyright 2010 Google Inc. All Rights Reserved.

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





import logging
import urllib
import urlparse


class Error(Exception):
  """Simple error exception properly scoped here."""
  pass


class TestRequestMessageBase(object):
  """A Test Request Message base class to provide generic methods.

  It uses the __dict__ of the class to reset it using a new text message
  or to dump it to text when going the other way. So the objects deriving from
  it should have no other instance data members then the ones that are part of
  the Test Request Format.
  """
  VALID_URL_SCHEMES = ['http', 'https', 'file', 'mailto']
  DEFAULT_WORKING_DIR = r'c:\swarm_tests'

  @staticmethod
  def LogError(error_text, error_list):
    """Logs an error message and adds it to the given list.

    Will only log when error_list is not None.

    Args:
      error_text: The text of the error.
      error_list: The list where to append the error after we logged it.
    """
    if error_list is not None:  # Explicit compare to None in case it is [].
      logging.error(error_text)
      error_list.append(error_text)

  def __str__(self):
    """Returns the request text after validating it.

    If the request isn't valid, an empty string is returned.

    Returns:
      The text string representing the request, or an empty string on errors.
    """
    if not self.IsValid(errors=None):
      return ''

    def Stringize(value):
      """Properly convert value to a string.

      This is useful for objects deriving from TestRequestMessageBase so that
      we can explicitly convert them to strings instead of getting the
      usual <__main__.XXX object at 0x...>.

      Args:
        value: The value to Stringize.

      Returns:
        The stringized value.
      """
      if isinstance(value, list):
        value = '[%s]' % ', '.join([Stringize(i) for i in value])
      elif isinstance(value, dict):
        value = '{%s}' % ', '.join([('%s: %s' % (Stringize(i),
                                                 Stringize(value[i])))
                                    for i in sorted(value)])
      elif isinstance(value, TestRequestMessageBase):
        value = str(value)
      elif isinstance(value, str):
        value = '\'%s\'' % value
      else:
        value = str(value)
      return value

    request_text_entries = ['{']
    # We sort the dictionary to ensure the string is always printed the same.
    for item in sorted(self.__dict__):
      request_text_entries.extend([Stringize(item), ': ',
                                   Stringize(self.__dict__[item]), ','])
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

  def AreValidValues(self, value_keys, value_type, required=False, errors=None):
    """Checks if all the values at the given keys are of the right type.

    This method also takes care of logging the error and append it to errors
    if errors is not left to None.

    Args:
      value_keys: The key names of the values to validate.
      value_type: The type that all the values should be.
      required: An optional flag identifying if the value is required to be
          non-empty. Defaults to False.
      errors: An array where we can append error messages.

    Returns:
      True if all values are of the right type, False othewise.
    """
    for value_key in value_keys:
      if value_key not in self.__dict__:
        self.LogError('%s must have a value for %s' %
                      (self.__class__.__name__, value_key), errors)
        return False

      value = self.__dict__[value_key]
      # Since 0 is an acceptable required value, but (not 0 == True), we
      # explicity check against 0.
      if required and (not value and value != 0):
        self.LogError('%s must have a non-empty value' % value_key, errors)
        return False
      # If the value is not required, it could be None, which would
      # not likely be of the value_type. If it is required and None, we would
      # have returned False above.
      if value is not None and not isinstance(value, value_type):
        self.LogError('Invalid %s: %s' % (value_key,
                                          self.__dict__[value_key]), errors)
        return False
    return True

  def AreValidLists(self, list_keys, value_type, required=False, errors=None):
    """Checks if all the values at the given list keys are of the right type.

    This method also takes care of logging the error and append it to errors
    if errors is not left to None.

    Args:
      list_keys: The key names of the value lists to validate.
      value_type: The type that all the values in the lists should be.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
      errors: An array where we can append error messages.

    Returns:
      True if all values in all lists are all of the right type, False othewise.
    """
    if not self.AreValidValues(list_keys, list, required, errors):
      return False

    for value_key in list_keys:
      if self.__dict__[value_key]:
        for value in self.__dict__[value_key]:
          if not isinstance(value, value_type):
            self.LogError('Invalid entry in list %s: %s' % (value_key, value),
                          errors)
            return False
      else:
        assert not required
    return True

  def AreValidDicts(self, list_keys, key_type, value_type, required=False,
                    errors=None):
    """Checks if all the values at the given list keys are of the right type.

    This method also takes care of logging the error and append it to errors
    if errors is not left to None.

    Args:
      list_keys: The key names of the value lists to validate.
      key_type: The type that all the keys in the dict should be.
      value_type: The type that all the values in the dict should be.
      required: An optional flag identifying if the dict is required to be
          non-empty. Defaults to False.
      errors: An array where we can append error messages.

    Returns:
      True if all key, value pairs in all dicts are all of the right type,
      False othewise.
    """
    if not self.AreValidValues(list_keys, dict, required, errors):
      return False

    for value_key in list_keys:
      if self.__dict__[value_key]:
        for key, value in self.__dict__[value_key].iteritems():
          if (not isinstance(key, key_type) or
              not isinstance(value, value_type)):
            self.LogError('Invalid entry in dict %s: %s' % (value_key, value),
                          errors)
            return False
      else:
        assert not required
    return True

  def AreValidObjectLists(self, list_keys, object_type, required=False,
                          unique_value_keys=None, errors=None):
    """Checks if all the objects of the given lists are valid.

    This method also takes care of logging the error and append it to errors
    if errors is not left to None.

    Args:
      list_keys: The key names of the value lists to validate.
      object_type: The type of object to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
      unique_value_keys: An optional list of keys to values that must be unique.
      errors: An array where we can append error messages.

    Returns:
      True if all values in all lists are all of the right type, False othewise.
    """
    if not self.AreValidLists(list_keys, object_type, required, errors):
      return False

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
                self.LogError('Duplicate entry with same value %s: %s in %s'
                              % (unique_key, unique_value, list_key), errors)
                return False
              else:
                unique_values[unique_key].add(unique_value)
        # Now we validate the whole object.
        if not object_value.IsValid(errors):
          self.LogError('Invalid entry in %s' % list_key, errors)
          return False
    return True

  def IsValidUrl(self, value, errors=None):
    """Checks if the given value is a valid URL.

    Args:
      value: The potential URL to validate.
      errors: An array where we can append error messages.

    Returns:
      True if the URL is valid, false otherwise.
    """
    if not isinstance(value, str):
      self.LogError('Unsupported url scheme, %s, must be a string' % value,
                    errors)
      return False

    url_parts = urlparse.urlsplit(value)
    if url_parts[0] not in TestRequestMessageBase.VALID_URL_SCHEMES:
      self.LogError('Unsupported url scheme, %s' % url_parts[0], errors)
      return False

    return True

  def AreValidUrls(self, value_keys, errors=None):
    """Checks if the value at value_key is a valid URL.

    Args:
      value_keys: The key names of the values to validate.
      errors: An array where we can append error messages.

    Returns:
      True if the URL is valid, False othewise.
    """
    for value_key in value_keys:
      if not self.IsValidUrl(self.__dict__[value_key], errors=errors):
        return False
    return True

  def AreValidUrlLists(self, list_keys, required=False, errors=None):
    """Checks if all the values in the given lists are valid urls.

    Args:
      list_keys: The key names of the value lists to validate.
      required: An optional flag identifying if the list is required to be
          non-empty. Defaults to False.
      errors: An array where we can append error messages.

    Returns:
      True if all the values are valid urls, False otherwise.
    """
    if not self.AreValidLists(list_keys, str, required, errors):
      return False

    for list_key in list_keys:
      if self.__dict__[list_key]:
        for value in self.__dict__[list_key]:
          if not self.IsValidUrl(value, errors):
            return False
      elif required:
        self.LogError('Missing list %s' % list_key, errors)
        return False

    return True

  def IsValidInteger(self, value, errors=None):
    """Checks if the given value is castable to a valid integer.

      It value must not only be castable to a valid integer, but it
      must also be a whole number (i.e. 8.0 is valid but 8.5 is not).

    Args:
      value: The potential integer to validate.
      errors: An array where we can append error messages.

    Returns:
      True if the value is castable to valid integer, false otherwise.
    """
    try:
      long(value)
    except ValueError:
      self.LogError('Invalid value for size, %s, must be int castable'
                    % value, errors)
      return False

    if isinstance(value, float) and int(value) != value:
      self.LogError('Size in output destination must be a whole number, '
                    'was given %s'% value, errors)
      return False

    return True

  def AreValidOutputDestinations(self, value_keys, errors=None):
    """Checks if the values at value_keys are valid output_destinations.

    Args:
      value_keys: The key names of the values to validate.
      errors: An array where we can append error messages.

    Returns:
      True if the output_destinations are value, False otherwise.
    """
    for value_key in value_keys:
      output_destination = self.__dict__[value_key]
      if output_destination is None:
        continue
      if not isinstance(output_destination, dict):
        self.LogError('Output destination must be a dictionary, was given: %s'
                      % output_destination, errors)
        return False

      for key, value in output_destination.iteritems():
        if key == 'size':
          if not self.IsValidInteger(value, errors):
            self.LogError('Invalid size in output destination, %s' % value,
                          errors)
            return False
          if isinstance(value, str):
            # If we reach here then value is a valid integer, just in string
            # form, so we convert it to an int to prevent problems with later
            # code not using it correctly.
            output_destination[key] = int(value)
        elif key == 'url':
          if not self.IsValidUrl(value, errors=errors):
            self.LogError('Invalid url in output destination, %s' % value,
                          errors)
            return False
        else:
          self.LogError('Invalid key, %s, in output destination' % key, errors)
          return False
    return True

  def IsValid(self, errors=None):
    """Identifies if the current content is valid.

    Note that the base class always returns False, this is to be implemented
    in all derived classes (we assert that we are called on an instance of the
    base class to make sure).

    Args:
      errors: A list to which we can append error messages if any.

    Returns:
      True if the current content is valid, False otherwise.
    """
    # This must be overriden by the derived classes and they shouldn't call us.
    assert self.__class__.__name__ is 'TestRequestMessageBase'
    self.LogError('TestRequestMessageBase class can\'t be used on its own',
                  errors)
    return False

  @staticmethod
  def ConvertDictionaryToObjectType(dictionary, object_type, errors):
    """Convert a dictionary to an object instance.

    Args:
      dictionary: The dictionary to convert to objects.
      object_type: The type of objects the list entries must be converted to.
          This type of object must expose a ParseDictionary() method.
      errors: A list to which we can append error messages if any.

    Returns:
      An object of the specified type, or None if there was an error while
      parsing the dictionary.
    """
    new_object = object_type()
    if not new_object.ParseDictionary(dictionary, errors):
      TestRequestMessageBase.LogError('Invalid dictionary for: %s\n%s' %
                                      (object_type, dictionary), errors)
      return None
    return new_object

  @staticmethod
  def ConvertDictionariesToObjectType(dict_list, object_type, errors):
    """Convert all dictionaries in the given list to an object instance.

    Args:
      dict_list: The list of dictionaries to convert to objects.
          The list is updated in place where the dictionaries in the list are
          replaced by object instances.
      object_type: The type of objects the list entries must be converted to.
          This type of object must expose a ParseDictionary() method.
      errors: A list to which we can append error messages if any.

    Returns:
      True if all entries could successfully be converted (though not validated
      yet), and False otherwise (e.g., ParseDictionary returned False)
    """
    for index in range(len(dict_list)):
      dictionary = dict_list[index]
      dict_list[index] = TestRequestMessageBase.ConvertDictionaryToObjectType(
          dictionary, object_type, errors)
      if dict_list[index] is None:
        TestRequestMessageBase.LogError('Invalid dictionary for: %s\n%s' %
                                        (object_type, dictionary), errors)
        return False
    return True

  def ExpandVariables(self, variables):
    """Expand the provided variables in all our text fields.

    Args:
      variables: A dictionary containing the variable values.
    """

    def ExpandVariable(value):
      """Expand the given value with outer variables dictionary.

      Args:
        value: The value to be expanded
      Returns:
        The resulting expanded value.
      """
      if isinstance(value, str):
        # Because it is possible for some url paths to contain '%' without
        # referring to variables that should be expanded, we unescape them
        # before expanding the variables and then escape them again
        # before returning.
        unquoted = urllib.unquote(value)
        was_quoted = (value != unquoted)
        if was_quoted:
          value = unquoted
        value %= variables

        # Since the contents of the expanded variables aren't guaranteed
        # to get escaped, they should not require escaping.
        if was_quoted:
          # Don't escape ':' or '/' as doing so will break the format of
          # url strings.
          value = urllib.quote(value, ':/')
      elif isinstance(value, list):
        value = map(ExpandVariable, value)
      elif isinstance(value, tuple):
        value = tuple(map(ExpandVariable, value))
      elif isinstance(value, TestRequestMessageBase):
        value.ExpandVariables(variables)
      elif isinstance(value, dict):
        for name, val in value.iteritems():
          value[name] = ExpandVariable(val)
      # We must passthru for all non string types, since they can't be expanded.
      return value

    ExpandVariable(self.__dict__)

  def ParseDictionary(self, dictionary, errors=None):
    """Parses the given dictionary and merge it into our __dict__.

    Args:
      dictionary: The dictionary to be parsed and merged into __dict__.
      errors: A list to which we can append error messages if any.

    Returns:
      False if dictionary is not a dict. True otherwise.
    """
    # We only want to get the values that are meaningful for us.
    if not isinstance(dictionary, dict):
      self.LogError('Invalid dictionary not a dict: %s' % dictionary, errors)
      return False
    for item in self.__dict__:
      if item in dictionary:
        self.__dict__[item] = dictionary[item]
    return True

  def ParseTestRequestMessageText(self, message_text, errors=None):
    """Parses the given text, convert it to a test request and validate it.

    Args:
      message_text: The text to be parsed as a Test Request Message.
      errors: A list to which we can append error messages if any.

    Returns:
      True if the text is valid. And False otherwise.

    Raises:
      Error: If the text has syntax or type errors. The text of the Error
          exception will be set with the syntax/type error text message.
    """
    try:
      # Unfortunately, the escaping of the \ escape character gets lost in the
      # evaluation, so we must double escape it.
      test_request = eval(message_text.replace('\\', '\\\\'),
                          {'__builtins__': None,
                           # True/False needed for booleans like verbose.
                           'False': False, 'True': True})
    except (SyntaxError, TypeError, NameError), e:
      logging.exception('Failed to evaluate text:\n-----\n%s.\n-----\n'
                        'Exception: %s', message_text, e)
      raise Error(e)
    if (not self.ParseDictionary(test_request, errors) or
        not self.IsValid(errors)):
      self.LogError('Invalid request not a dict: %s' % test_request, errors)
      return False
    return True


class TestObject(TestRequestMessageBase):
  """The object to hold on and validate attributes for a test.

  Attributes:
    test_name: The name of this test object.
    env_vars: An optional dictionary for environment variables.
    action: The action list of this test object.
    decorate_output: The output decoration flag of this test object.
    time_out: The time out value of this test object.
  """

  def __init__(self, test_name=None, env_vars=None, action=None,
               decorate_output=True, time_out=1200.0):
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
    self.time_out = time_out

  def IsValid(self, errors=None):
    """Identifies if the current content is valid.

    Args:
      errors: A list to which we can append error messages if any.
          Can be left None if caller is not interested in errors.

    Returns:
      True if the current content is valid, False otherwise.
    """
    if (not self.AreValidValues(['test_name'], str,
                                required=True, errors=errors) or
        not self.AreValidDicts(['env_vars'], str, str, errors=errors) or
        not self.AreValidLists(['action'], str, required=True, errors=errors) or
        not self.AreValidValues(['time_out'], (int, long, float),
                                errors=errors)):
      self.LogError('Invalid TestObject: %s' % self.__dict__, errors)
      return False

    # self.decorate_output doesn't need to be validated since we only need
    # to evaluate it to True/False which can be done with any type.

    logging.debug('Successfully validated request: %s', self.__dict__)
    return True


class TestConfiguration(TestRequestMessageBase):
  """The object to hold on and validate attributes for a configuration.

  Attributes:
    config_name: The name of this configuration.
    env_vars: An optional dictionary for environment variables.
    data: An optional data list for this configuration. The strings must be
        valid urls.
    binaries: An optional binaries list for this configuration.
    tests: An optional tests list for this configuration.
    min_instances: An optional integer specifying the minimum number of
        instances of this configuration we want. Defaults to 1.
        Must be greater than 0.
    additional_instances: An optional integer specifying the maximum number of
        additional instances of this configuration we want. Defaults to 0.
        Must be greater than 0.
    dimensions: A dictionary of strings or list of strings for dimensions.
  """

  def __init__(self, config_name=None, env_vars=None, data=None, binaries=None,
               tests=None, min_instances=1, additional_instances=0,
               **dimensions):
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
    if binaries:
      self.binaries = binaries[:]
    else:
      self.binaries = []
    if tests:
      self.tests = tests[:]
    else:
      self.tests = []
    self.min_instances = min_instances
    self.additional_instances = additional_instances

    # Dimensions are kept dynamic so that we don't have to update this code
    # when the list of configuration dimensions changes.
    self.dimensions = dimensions

  def IsValid(self, errors=None):
    """Identifies if the current content is valid.

    Args:
      errors: A list to which we can append error messages if any.

    Returns:
      True if the current content is valid, False otherwise.
    """
    if (not self.AreValidValues(['config_name'], str,
                                required=True, errors=errors) or
        not self.AreValidDicts(['env_vars'], str, str, errors=errors) or
        not self.AreValidUrlLists(['data'], errors=errors) or
        not self.AreValidLists(['binaries'], str, errors=errors) or
        not self.AreValidObjectLists(['tests'], TestObject,
                                     unique_value_keys=['test_name'],
                                     errors=errors) or
        # required=True to make sure the caller doesn't set it to None.
        not self.AreValidValues(['min_instances', 'additional_instances'],
                                (int, long), required=True, errors=errors) or
        self.min_instances < 1 or self.additional_instances < 0):
      self.LogError('Invalid TestConfiguration: %s' % self.__dict__, errors)
      return False

    if not isinstance(self.dimensions, dict):
      self.LogError('Invalid TestConfiguration dimension type: %s' %
                    type(self.dimensions), errors)
      return False
    for values in self.dimensions.values():
      if not isinstance(values, (list, tuple)):
        values = [values]
      for value in values:
        if not value or not isinstance(value, str):
          self.LogError('Invalid TestConfiguration dimension value: %s' % value,
                        errors)
          return False

    logging.debug('Successfully validated request: %s', self.__dict__)
    return True

  def ParseDictionary(self, dictionary, errors=None):
    """Parses the given dictionary and merge it into our __dict__.

    We override the base class behavior to create instances of TestOjbects.

    Args:
      dictionary: The dictionary to be parsed and merged into __dict__.
      errors: A list to which we can append error messages if any.

    Returns:
      False if dictionary is not a dict. True otherwise.
    """
    if not super(TestConfiguration, self).ParseDictionary(dictionary, errors):
      return False
    if not self.ConvertDictionariesToObjectType(self.tests, TestObject, errors):
      return False
    return True


class TestCase(TestRequestMessageBase):
  """The object to hold on and validate attributes for a test case.

  Attributes:
    test_case_name: The name of this test case.
    env_vars: An optional dictionary for environment variables.
    configurations: A list of configurations for this test case.
    data: An optional data list for this test case. The strings must be
        valid urls.
    binaries: An optional binaries list for this test case.
    working_dir: An optional path string for where to download/run tests.
        This must be an absolute path though we don't validate it since this
        script may run on a different platform than the one that will use the
        path. Defaults to c:\swarm_tests.
        TODO(user): Also support other platforms.
    admin: An optional boolean value that specifies if the tests should be run
        with admin privilege or not.
    virgin: An optional boolean value that specifies if the tests must be run on
        virgin machines or not.
    tests: An optional tests list for this test case.
    result_url: An optional URL where to post the results of this test case.
    store_result: The key to access the test run's storage string.
    output_destination: An optional dictionary with a URL where to post the
        output of this test case as well as the size of the chunks to use.
        The key for the URL is 'url' and the value must be a valid URL string.
        The key for the chunk size is 'size'. It must be a whole number.
    failure_email: An optional email where to broadcast failures for this test
        case.
    verbose: An optional boolean value that specifies if logging should be
        verbose or not.
  """
  VALID_STORE_RESULT_VALUES = [None, '', 'all', 'fail', 'none']

  def __init__(self, test_case_name=None, env_vars=None, configurations=None,
               data=None, binaries=None, working_dir=None, admin=False,
               virgin=False, tests=None, result_url=None, store_result=None,
               output_destination=None, failure_email=None, verbose=False):
    super(TestCase, self).__init__()
    self.test_case_name = test_case_name
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
    if binaries:
      self.binaries = binaries[:]
    else:
      self.binaries = []
    if working_dir:
      self.working_dir = working_dir
    else:
      self.working_dir = self.DEFAULT_WORKING_DIR
    self.admin = admin
    self.virgin = virgin
    if tests:
      self.tests = tests[:]
    else:
      self.tests = []
    self.result_url = result_url
    self.store_result = store_result
    if output_destination:
      self.output_destination = output_destination.copy()
    else:
      self.output_destination = None
    self.failure_email = failure_email
    self.verbose = verbose

  def IsValid(self, errors=None):
    """Identifies if the current content is valid.

    Args:
      errors: A list to which we can append error messages if any.

    Returns:
      True if the current content is valid, False otherwise.
    """
    if (not self.AreValidValues(['test_case_name'], str,
                                required=True, errors=errors) or
        not self.AreValidDicts(['env_vars'], str, str, errors=errors) or
        not self.AreValidObjectLists(['configurations'], TestConfiguration,
                                     required=True,
                                     unique_value_keys=['config_name'],
                                     errors=errors) or
        not self.AreValidUrlLists(['data'], errors=errors) or
        not self.AreValidLists(['binaries'], str, errors=errors) or
        not self.AreValidObjectLists(['tests'], TestObject,
                                     unique_value_keys=['test_name'],
                                     errors=errors) or
        not self.AreValidOutputDestinations(['output_destination'],
                                            errors=errors) or
        not self.AreValidValues(['working_dir', 'failure_email', 'result_url'],
                                str, errors=errors) or
        (self.result_url and not self.AreValidUrls(['result_url'], errors)) or
        self.store_result not in TestCase.VALID_STORE_RESULT_VALUES):
      self.LogError('Invalid TestCase: %s' % self.__dict__, errors)
      return False

    # self.verbose, self.admin and self.virgin don't need to be validated since
    # we only need to evaluate them to True/False which can be done with any
    # type.

    logging.debug('Successfully validated request: %s', self.__dict__)
    return True

  def ParseDictionary(self, dictionary, errors=None):
    """Parses the given dictionary and merge it into our __dict__.

    We override the base class behavior to create instances of TestOjbects and
    TestConfiguration.

    Args:
      dictionary: The dictionary to be parsed and merged into __dict__.
      errors: A list to which we can append error messages if any.

    Returns:
      False if dictionary is not a dict. True otherwise.
    """
    if not super(TestCase, self).ParseDictionary(dictionary, errors):
      return False
    if not self.ConvertDictionariesToObjectType(self.tests, TestObject, errors):
      return False
    if not self.ConvertDictionariesToObjectType(self.configurations,
                                                TestConfiguration, errors):
      return False
    return True


class TestRun(TestRequestMessageBase):
  """The object to hold on and validate attributes for a test run.

  Attributes:
    test_run_name: The name of the test run.
    env_vars: An optional dictionary for environment variables.
    configuration: An optional configuration object for this test run.
    data: An optional data list for this test run. The strings must be valid
        urls.
    working_dir: An optional path string for where to download/run tests.
        This must be an absolute path though we don't validate it since this
        script may run on a different platform than the one that will use the
        path. Defaults to c:\swarm_tests.
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
    output_destination: An optional dictionary with a URL where to post the
        output of this test case as well as the size of the chunks to use.
        The key for the URL is 'url' and the value must be a valid URL string.
        The key for the chunk size is 'size'. It must be a whole number.
    cleanup: The key to access the test run's cleanup string.
  """
  VALID_CLEANUP_VALUES = [None, '', 'zip', 'data', 'root']

  def __init__(self, test_run_name=None, env_vars=None,
               configuration=TestConfiguration(), data=None, working_dir=None,
               tests=None, instance_index=None, num_instances=None,
               result_url=None, ping_url=None, output_destination=None,
               cleanup=None):
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
    if working_dir:
      self.working_dir = working_dir
    else:
      self.working_dir = self.DEFAULT_WORKING_DIR
    if tests:
      self.tests = tests[:]
    else:
      self.tests = []
    self.instance_index = instance_index
    self.num_instances = num_instances
    self.result_url = result_url
    self.ping_url = ping_url
    if output_destination:
      self.output_destination = output_destination.copy()
    else:
      self.output_destination = None
    self.cleanup = cleanup

  def IsValid(self, errors=None):
    """Identifies if the current content is valid.

    Args:
      errors: A list to which we can append error messages if any.

    Returns:
      True if the current content is valid, False otherwise.
    """
    if (not self.AreValidValues(['test_run_name'], str,
                                required=True, errors=errors) or
        not self.AreValidDicts(['env_vars'], str, str, errors=errors) or
        not self.configuration or
        not isinstance(self.configuration, TestConfiguration) or
        not self.configuration.IsValid(errors) or
        not self.AreValidUrlLists(['data'], errors=errors) or
        not self.AreValidObjectLists(['tests'], TestObject,
                                     unique_value_keys=['test_name'],
                                     errors=errors) or
        not self.AreValidOutputDestinations(['output_destination'],
                                            errors=errors) or
        not self.AreValidValues(['working_dir', 'result_url', 'ping_url'], str,
                                errors=errors) or
        (self.result_url and not self.IsValidUrl(self.result_url, errors)) or
        not self.IsValidUrl(self.ping_url, errors) or
        self.cleanup not in TestRun.VALID_CLEANUP_VALUES or
        not self.AreValidValues(['instance_index', 'num_instances'],
                                (int, long), errors=errors) or
        (self.instance_index is not None and self.num_instances is None) or
        (self.num_instances is not None and self.instance_index is None) or
        (self.num_instances is not None and
         self.instance_index >= self.num_instances)):  # zero based index.
      self.LogError('Invalid TestRun: %s' % self.__dict__, errors)
      return False

    logging.debug('Successfully validated request: %s', self.__dict__)
    return True

  def ParseDictionary(self, dictionary, errors=None):
    """Parses the given dictionary and merge it into our __dict__.

    We override the base class behavior to create instances of TestOjbects and
    TestConfiguration.

    Args:
      dictionary: The dictionary to be parsed and merged into __dict__.
      errors: A list to which we can append error messages if any.

    Returns:
      False if dictionary is not a dict. True otherwise.
    """
    if not super(TestRun, self).ParseDictionary(dictionary, errors):
      return False
    if not self.ConvertDictionariesToObjectType(self.tests, TestObject, errors):
      return False
    self.configuration = self.ConvertDictionaryToObjectType(
        self.configuration, TestConfiguration, errors)
    if self.configuration is None:
      return False
    return True
