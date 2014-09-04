#!/usr/bin/env python
# coding=utf-8
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for classes implemented in test_request_message.py."""


import json
import logging
import os.path
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from common import test_request_message


class ParseResults(test_request_message.TestRequestMessageBase):
  def __init__(
      self,
      str_value='a',
      int_value=1,
      int_array_value=None,
      str_array_value=None,
      dict_value=None,
      **kwargs):
    super(ParseResults, self).__init__(**kwargs)
    self.str_value = str_value
    self.int_value = int_value
    self.int_array_value = (
        [1, 2] if int_array_value is None else int_array_value)
    self.str_array_value = (
        ['a', 'b', r'a\b', r'\a\t']
        if str_array_value is None else str_array_value)
    self.dict_value = (
        {'a': 1, 'b': 2} if dict_value is None else dict_value)

  def Validate(self):
    if not (
        isinstance(self.str_value, basestring) and
        isinstance(self.int_value, int) and
        isinstance(self.int_array_value, list) and
        not sum([not isinstance(i, int) for i in self.int_array_value]) and
        isinstance(self.str_array_value, list) and
        not sum([not isinstance(i, basestring)
                  for i in self.str_array_value]) and
        isinstance(self.dict_value, dict)):
      raise test_request_message.Error('Oops')


class OuterParseResults(test_request_message.TestRequestMessageBase):
  def __init__(
      self, str_value='a', parsed_result=None, results=None, dict_value=None,
      **kwargs):
    super(OuterParseResults, self).__init__(**kwargs)
    self.str_value = str_value
    self.parsed_result = (
        ParseResults() if parsed_result is None else parsed_result)
    self.results = (
        [ParseResults(), ParseResults()] if results is None else results)
    self.dict_value = (
        {1: 'a', 'b': 2} if dict_value is None else dict_value)

  def Validate(self):
    self.parsed_result.Validate()
    if (not isinstance(self.str_value, basestring) or
        not isinstance(self.dict_value, dict)):
      raise test_request_message.Error('Oops')


class Validatable(test_request_message.TestRequestMessageBase):
  def __init__(self, is_valid=True):
    super(Validatable, self).__init__()
    self.is_valid = is_valid

  def Validate(self):
    if not self.is_valid:
      raise test_request_message.Error('Oops')


class TestObj(test_request_message.TestRequestMessageBase):
  def __init__(self, **kwargs):
    super(TestObj, self).__init__()
    for k, v in kwargs.iteritems():
      setattr(self, k, v)

  def Validate(self):
    pass


class TestRequestMessageBaseTest(unittest.TestCase):
  def testValidateValues(self):
    trm = TestObj(a=1, b='')
    with self.assertRaises(test_request_message.Error):
      trm.ValidateValues(['a'], str)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateValues(['b'], str, required=True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateValues(['c'], str, required=True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateValues(['a', 'b'], int, required=True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateValues(['b', 'a'], str)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateValues(['b', 'c'], str)

    # pylint: disable=W0201
    trm.c = 3
    trm.d = 'a'
    trm.ValidateValues(['a', 'c'], int, required=True)
    trm.ValidateValues(['a', 'c'], int)
    trm.ValidateValues(['b', 'd'], str)

  def testValidateLists(self):
    trm = TestObj(a=1, b='', c=[], d=[1], e=['a'])
    with self.assertRaises(test_request_message.Error):
      trm.ValidateLists(['a'], int, required=True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateLists(['b'], str)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateLists(['c'], str, required=True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateLists(['d'], str)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateLists(['e'], int, required=True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateLists(['d', 'e'], int)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateLists(['e', 'd'], str, required=True)

    trm.ValidateLists(['c'], int)
    trm.ValidateLists(['c', 'd'], int)
    trm.ValidateLists(['e', 'c'], str)
    trm.ValidateLists(['d'], int, required=True)
    trm.ValidateLists(['e'], str, required=True)

  def testValidateObjectLists(self):
    trm = TestObj(
        a=Validatable(),
        b=[Validatable()],
        c=[Validatable(is_valid=False)],
        d=[Validatable(), Validatable(is_valid=False)],
        e=[Validatable(), Validatable(False)],
        f=[],
        g=[Validatable(), Validatable()])

    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['a'], Validatable, False)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['b'], TestObj, True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['c'], Validatable, False)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['d'], Validatable, True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['e'], Validatable, False)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['f'], Validatable, True)

    # Success!
    trm.ValidateObjectLists(['b'], Validatable, True)
    trm.ValidateObjectLists(['f'], Validatable, False)
    trm.ValidateObjectLists(['g'], Validatable, False)

    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['a'], Validatable, False)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['b'], TestObj, True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateObjectLists(['c'], Validatable, False)

  def testValidateUrlLists(self):
    trm = TestObj(
        a=[],
        b=['http://localhost:9001'],
        c=['http://www.google.com'],
        d=[1],
        e=['http://www.google.com', 'a'],
        f=1)

    trm.ValidateUrlLists(['a'])
    trm.ValidateUrlLists(['b'])
    trm.ValidateUrlLists(['c'])
    trm.ValidateUrlLists(['a', 'b'])
    trm.ValidateUrlLists(['b', 'c'])

    with self.assertRaises(test_request_message.Error):
      trm.ValidateUrlLists(['a'], required=True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateUrlLists(['d'])
    with self.assertRaises(test_request_message.Error):
      trm.ValidateUrlLists(['e'])
    with self.assertRaises(test_request_message.Error):
      trm.ValidateUrlLists(['f'])
    with self.assertRaises(test_request_message.Error):
      trm.ValidateUrlLists(['d'], True)
    with self.assertRaises(test_request_message.Error):
      trm.ValidateUrlLists(['d', 'e'], False)

  def testFromJSON(self):
    # Start with the exception raising tests.
    with self.assertRaises(test_request_message.Error):
      test_request_message.TestRequestMessageBase.FromJSON('name error')
    with self.assertRaises(test_request_message.Error):
      test_request_message.TestRequestMessageBase.FromJSON('syntax_error =')
    with self.assertRaises(test_request_message.Error):
      test_request_message.TestRequestMessageBase.FromJSON(
          'type_error = 1 + "2"')

    # Success stories.
    naked_results = ParseResults()
    parse_results = ParseResults.FromJSON('{}')
    # Make sure no members were added or lost.
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))

    parse_results = ParseResults.FromJSON(json.dumps({'ignored_member': 1}))
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))

    parse_results = ParseResults.FromJSON(
        json.dumps({'str_value': 'new value'}))
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))
    self.assertEqual(parse_results.str_value, 'new value')

    text_request = json.dumps({
        'str_value': 'newer value',
        'int_value': 2,
        'int_array_value': [3, 4],
        'str_array_value': ['cc', 'dd', 'mm\nn'],
        'dict_value': {'cc': 3, 'dd': 4},
      })
    parse_results = ParseResults.FromJSON(text_request)
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))
    self.assertEqual(parse_results.str_value, 'newer value')
    self.assertEqual(parse_results.int_value, 2)
    self.assertEqual(parse_results.int_array_value, [3, 4])
    self.assertEqual(parse_results.str_array_value, [u'cc', u'dd', u'mm\nn'])
    self.assertEqual(parse_results.dict_value, {u'cc': 3, u'dd': 4})

    # Now try a few invalid types.
    with self.assertRaises(test_request_message.Error):
      ParseResults.FromJSON(json.dumps({'int_value': 'new value'}))

    with self.assertRaises(test_request_message.Error):
      ParseResults.FromJSON(json.dumps({'str_value': 42}))

  def testRequestText(self):
    # Success stories.
    parse_results = ParseResults()
    expected_text = ("""{'dict_value': {'a': 1, 'b': 2},"""
                     """'int_array_value': [1, 2],'int_value': 1,"""
                     """'str_array_value': ['a', 'b', 'a\\b', '\\a\\t'],"""
                     """'str_value': 'a',}""")
    self.assertEqual(expected_text, str(parse_results))
    parse_results.int_value = 'invalid'
    expected_text = (
        "{'dict_value': {'a': 1, 'b': 2},'int_array_value': [1, "
        "2],'int_value': 'invalid','str_array_value': ['a', 'b', 'a\\b', "
        "'\\a\\t'],'str_value': 'a',}")
    self.assertEqual(expected_text, str(parse_results))

    # Now test embedded cases.
    outer_parse_result = OuterParseResults()
    outer_expected_text = ("""{'dict_value': {1: 'a', 'b': 2},"""
                           """'parsed_result': %s,"""
                           """'results': [%s, %s],'str_value': 'a',}""" %
                           (str(outer_parse_result.parsed_result),
                            str(outer_parse_result.parsed_result),
                            str(outer_parse_result.parsed_result)))
    self.assertEqual(str(outer_parse_result), outer_expected_text)


class TestHelper(unittest.TestCase):
  EXTRA_OPTIONAL_STRING_VALUES = ['', None]

  INVALID_STRING_VALUES = [42, [], {}]
  INVALID_REQUIRED_STRING_VALUES = (INVALID_STRING_VALUES +
                                    EXTRA_OPTIONAL_STRING_VALUES)
  VALID_STRING_VALUES = ['a', '42', '[]', os.path.join('a', 'b', 'c'),
                         r'a\b\c', u'\xe2\x99\x88', u'âââ']
  VALID_OPTIONAL_STRING_VALUES = (VALID_STRING_VALUES +
                                  EXTRA_OPTIONAL_STRING_VALUES)

  VALID_URL_VALUES = ['http://a.com', 'https://safe.a.com']
  VALID_OPTIONAL_URL_VALUES = VALID_URL_VALUES + EXTRA_OPTIONAL_STRING_VALUES
  INVALID_URL_VALUES = ['file://a.com', 'mailto:foo@localhost',
                        ] + INVALID_STRING_VALUES

  VALID_URL_LIST_VALUES = [['http://a.com'], ['https://safe.a.com']]
  VALID_OPTIONAL_URL_LIST_VALUES = VALID_URL_LIST_VALUES + [None, []]
  INVALID_URL_LIST_VALUES = [['httpx://a.com'], ['shttps://safe.a.com'], [55]]

  VALID_URL_LOCAL_PATH_TUPLES_LISTS = [
      [('http://a.com', 'a')], [['https://safe.a.com', 'b']],
  ]
  INVALID_URL_LOCAL_PATH_TUPLES_LISTS = [
    '',
    1,
    ('hello'),
    [('asd', 'a')],
    [['hps://safe.a.com', 'b'], ('local/state',)],
    [('http://www.google.com', 5)],
    [('http://a.com', 'b', 'c')],
  ]

  INVALID_STRING_LIST_VALUES = [[1], ['str', 1], 1, {}]
  INVALID_REQUIRED_STRING_LIST_VALUES = (INVALID_STRING_LIST_VALUES +
                                         [[]])
  VALID_REQUIRED_STRING_LIST_VALUES = [['1'], ['str', '1'], ['1', '[]']]
  VALID_OPTIONAL_STRING_LIST_VALUES = (VALID_REQUIRED_STRING_LIST_VALUES +
                                       [[]])

  VALID_BOOLEAN_VALUES = [True, False, 1.1, None, '0', [], '']

  NON_ZERO_VALID_INT_VALUES = [1, 2L, 4, 8, 13, 42, 1234567890L]
  VALID_INT_VALUES = NON_ZERO_VALID_INT_VALUES + [0L, 0]
  INVALID_INT_VALUES = [None, '', 3.14159, [], (1,)]
  INVALID_POSITIVE_INT_VALUES = INVALID_INT_VALUES + [-1, -10L]
  NON_ZERO_INVALID_INT_VALUES = INVALID_INT_VALUES + [0, 0L]
  NON_ZERO_INVALID_POSITIVE_INT_VALUES = INVALID_INT_VALUES + [-1, -10L]

  VALID_ENV_VARS = [{'a': 'b'}, {'a': 'b', '1': 'b'}, {}, None]
  INVALID_ENV_VARS = [{1: 2}, {'a': 'b', 1: 'b'}, {'a': 1},
                      {'a': None}]

  def AssertValidValues(self, value_key, values):
    for value in values:
      setattr(self.test_request, value_key, value)
      logging.info('Validating %s with %s', value_key, value)
      self.test_request.Validate()

  def AssertInvalidValues(self, value_key, values):
    # First check that the test request is valid, otherwise we are failing
    # due to errors other than we intended.
    self.test_request.Validate()

    for value in values:
      setattr(self.test_request, value_key, value)
      logging.info('Validating %s with %s', value_key, value)
      with self.assertRaises(test_request_message.Error):
        self.test_request.Validate()


class TestObjectTest(TestHelper):
  def setUp(self):
    super(TestObjectTest, self).setUp()
    # Always start with a valid case, and make it explicitly invalid as needed.
    self.test_request = test_request_message.TestObject(
        action=TestHelper.VALID_REQUIRED_STRING_LIST_VALUES[0])

  @staticmethod
  def GetFullObject():
    return test_request_message.TestObject(
        action=TestHelper.VALID_REQUIRED_STRING_LIST_VALUES[-1],
        decorate_output=TestHelper.VALID_BOOLEAN_VALUES[-1],
        hard_time_out=42,
        io_time_out=42)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()

    # And then a few more valid values
    self.AssertValidValues('action',
                           TestHelper.VALID_REQUIRED_STRING_LIST_VALUES)
    self.AssertValidValues('hard_time_out', [1.1, 3, 0, .00003])
    self.AssertValidValues('io_time_out', [1.1, 3, 0, .00003])
    self.AssertValidValues('decorate_output', TestHelper.VALID_BOOLEAN_VALUES)
    self.AssertValidValues('env_vars', TestHelper.VALID_ENV_VARS)

    # Now try invalid values.
    self.AssertInvalidValues('action',
                             TestHelper.INVALID_REQUIRED_STRING_LIST_VALUES)
    # Put the value back to a valid value, to test invalidity of other values.
    self.test_request.action = TestHelper.VALID_REQUIRED_STRING_LIST_VALUES[0]
    self.AssertInvalidValues('hard_time_out', ['never', '', {}, [], (1, 2)])
    self.test_request.hard_time_out = None
    self.AssertInvalidValues('io_time_out', ['never', '', {}, [], (1, 2)])
    self.test_request.io_time_out = None

  def testStringize(self):
    # Vanilla object.
    new_object = test_request_message.TestObject.FromJSON(
        test_request_message.Stringize(self.test_request, json_readable=True))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestObjectTest.GetFullObject()
    full_object.Validate()
    new_object = test_request_message.TestObject.FromJSON(
        test_request_message.Stringize(full_object, json_readable=True))
    self.assertEqual(new_object, full_object)


class TestConfigurationTest(TestHelper):
  def setUp(self):
    super(TestConfigurationTest, self).setUp()
    # Always start with a valid case, and make it explicitly invalid as needed.
    dimensions = dict(os='a', browser='a', cpu='a')
    self.test_request = test_request_message.TestConfiguration(
        dimensions=dimensions)

  @staticmethod
  def GetFullObject():
    dimensions = dict(os='a', browser='a', cpu='a')
    return test_request_message.TestConfiguration(
        dimensions=dimensions,
        deadline_to_run=1,
        priority=1)

  def testNoReferences(self):
    # Ensure that Test Configuration makes copies of its input, not references.
    dimensions = {'a': ['b']}

    test_configurations = test_request_message.TestConfiguration(
        dimensions=dimensions)

    dimensions['c'] = 'd'
    self.assertNotEqual(dimensions, test_configurations.dimensions)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()

    # And then a few more valid values
    self.AssertValidValues('deadline_to_run',
                           TestHelper.VALID_INT_VALUES)

    self.AssertValidValues('priority',
                           [0, 10, 33, test_request_message.MAX_PRIORITY_VALUE])

    for value in TestHelper.VALID_STRING_VALUES:
      self.test_request.dimensions[value] = value
    self.test_request.Validate()

    # Now try invalid values.
    self.AssertInvalidValues('deadline_to_run',
                             TestHelper.INVALID_POSITIVE_INT_VALUES)
    # Put the value back to a valid value, to test invalidity of other values.
    self.test_request.deadline_to_run = 0

    self.AssertInvalidValues('priority',
                             TestHelper.INVALID_POSITIVE_INT_VALUES +
                             [test_request_message.MAX_PRIORITY_VALUE + 1])
    self.test_request.priority = 10

    for value in TestHelper.INVALID_REQUIRED_STRING_VALUES:
      self.test_request.dimensions[str(value)] = value
    with self.assertRaises(test_request_message.Error):
      self.test_request.Validate()

  def testStringize(self):
    # Vanilla object.
    new_object = test_request_message.TestConfiguration.FromJSON(
        test_request_message.Stringize(self.test_request, json_readable=True))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestConfigurationTest.GetFullObject()
    full_object.Validate()
    new_object = test_request_message.TestConfiguration.FromJSON(
        test_request_message.Stringize(full_object, json_readable=True))
    self.assertEqual(new_object, full_object)


class TestCaseTest(TestHelper):
  def setUp(self):
    super(TestCaseTest, self).setUp()
    # Always start with a valid case, and make it explicitly invalid as needed.
    dimensions = dict(os='a', browser='a', cpu='a')
    self.test_request = test_request_message.TestCase(
        test_case_name='a',
        configurations=[
          test_request_message.TestConfiguration(dimensions=dimensions),
        ])

  @staticmethod
  def GetFullObject():
    return test_request_message.TestCase(
        test_case_name='a',
        requestor='user@swarm.com',
        env_vars=TestHelper.VALID_ENV_VARS[-1],
        data=TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS[-1],
        tests=[TestObjectTest.GetFullObject()],
        verbose=TestHelper.VALID_BOOLEAN_VALUES[-1],
        configurations=[TestConfigurationTest.GetFullObject()])

  def testNoReferences(self):
    # Ensure that Test Case makes copies of its input, not references.
    env_vars = {'a': '1'}
    configurations = [TestConfigurationTest.GetFullObject()]
    data = [('http://localhost/foo', 'foo.zip')]
    tests = [TestObjectTest.GetFullObject()]

    test_case = test_request_message.TestCase(
        test_case_name='foo', env_vars=env_vars, configurations=configurations,
        data=data, tests=tests)

    env_vars['a'] = '2'
    self.assertNotEqual(env_vars, test_case.env_vars)

    configurations.append(TestConfigurationTest.GetFullObject())
    self.assertNotEqual(configurations, test_case.configurations)

    data.append('data2')
    self.assertNotEqual(data, test_case.data)

    tests.append('tests2')
    self.assertNotEqual(tests, test_case.tests)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()
    # And then a few more valid values
    self.AssertValidValues('test_case_name',
                           TestHelper.VALID_STRING_VALUES)
    # TODO(csharp): requestor should change to be required, not optional.
    self.AssertValidValues('requestor',
                           TestHelper.VALID_OPTIONAL_STRING_VALUES)
    self.AssertValidValues('data', TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)

    test_object1 = test_request_message.TestObject(action=['a'])
    test_object1.Validate()
    test_object2 = test_request_message.TestObject(
        action=['b', 'c'], decorate_output=False)
    test_object2.Validate()
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = [TestObjectTest.GetFullObject()]

    test_config1 = test_request_message.TestConfiguration(
        dimensions=dict(os='a', browser='a', cpu='a'))
    test_config1.Validate()
    test_config2 = test_request_message.TestConfiguration(
        dimensions=dict(os='b', browser='b', cpu='b'),
        data=[('http://a.com/foo', 'foo.zip')])
    test_config2.Validate()
    self.AssertValidValues('configurations', [[test_config1]])
    self.AssertInvalidValues('configurations', [[test_config1, test_config2]])
    self.test_request.configurations = [TestConfigurationTest.GetFullObject()]

    self.AssertValidValues('verbose', TestHelper.VALID_BOOLEAN_VALUES)
    self.AssertValidValues('env_vars', TestHelper.VALID_ENV_VARS)

    # Now try invalid values.
    self.AssertInvalidValues('test_case_name',
                             TestHelper.INVALID_REQUIRED_STRING_VALUES)
    # Put the value back to a valid value, to test invalidity of other values.
    self.test_request.test_case_name = TestHelper.VALID_STRING_VALUES[0]
    self.AssertInvalidValues('requestor',
                             TestHelper.INVALID_STRING_VALUES)
    self.test_request.requestor = 'user@swarm.com'
    self.AssertInvalidValues(
        'data', TestHelper.INVALID_URL_LOCAL_PATH_TUPLES_LISTS)
    self.test_request.data = TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS[-1]
    self.test_request.Validate()

    test_object1.io_time_out = 'never'
    with self.assertRaises(test_request_message.Error):
      test_object1.Validate()
    test_object2.action = 'None'
    with self.assertRaises(test_request_message.Error):
      test_object2.Validate()
    self.AssertInvalidValues('tests', [[test_object1, test_object2],
                                       [test_object1]])
    self.test_request.tests = []

    test_config1.dimensions = [42]
    with self.assertRaises(test_request_message.Error):
      test_config1.Validate()
    self.AssertInvalidValues('configurations', [[test_config1, test_config2],
                                                [test_config1]])
    # Put the value back to a valid value, to test invalidity of other values.
    dimensions = dict(os='a', browser='a', cpu='a')
    valid_config = test_request_message.TestConfiguration(
        dimensions=dimensions)
    self.test_request.configurations = [valid_config]

    self.AssertInvalidValues('env_vars', TestHelper.INVALID_ENV_VARS)
    self.test_request.env_vars = None

  def testStringize(self):
    # Vanilla object.
    new_object = test_request_message.TestCase.FromJSON(
        test_request_message.Stringize(self.test_request, json_readable=True))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestCaseTest.GetFullObject()
    full_object.Validate()
    new_object = test_request_message.TestCase.FromJSON(
        test_request_message.Stringize(full_object, json_readable=True))
    self.assertEqual(new_object, full_object)


class TestRunTest(TestHelper):
  def setUp(self):
    super(TestRunTest, self).setUp()
    # Always start with a valid case, and make it explicitly invalid as needed.
    self.test_request = test_request_message.TestRun(
        configuration=test_request_message.TestConfiguration(
            dimensions=dict(os='a', browser='a', cpu='a')),
        result_url='http://localhost:1',
        ping_url='http://localhost:2',
        ping_delay=1)

  @staticmethod
  def GetFullObject():
    return test_request_message.TestRun(
        configuration=TestConfigurationTest.GetFullObject(),
        env_vars=TestHelper.VALID_ENV_VARS[-1],
        data=TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS[-1],
        tests=[TestObjectTest.GetFullObject()],
        result_url='http://localhost:1',
        ping_url='http://localhost:2',
        ping_delay=TestHelper.VALID_INT_VALUES[-1])

  def testNoReferences(self):
    # Ensure that Test Run makes copies of its input, not references.
    env_vars = {'a': '1'}
    data = [('http://localhost/foo', 'foo.zip')]
    tests = [TestObjectTest.GetFullObject()]

    test_run = test_request_message.TestRun(
        env_vars=env_vars,
        data=data,
        tests=tests,
        result_url='http://localhost:1',
        ping_url='http://localhost:2',
        ping_delay=10,
        configuration=test_request_message.TestConfiguration(
            dimensions=dict(os='a')))

    env_vars['a'] = 2
    self.assertNotEqual(env_vars, test_run.env_vars)

    data.append('data2')
    self.assertNotEqual(data, test_run.data)

    tests.append('tests2')
    self.assertNotEqual(tests, test_run.tests)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()

    # And then a few more valid values
    self.AssertValidValues('data', TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)

    test_object1 = test_request_message.TestObject(action=['a'])
    test_object1.Validate()
    test_object2 = test_request_message.TestObject(
        action=['b', 'c'], decorate_output=False)
    test_object2.Validate()
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = [TestObjectTest.GetFullObject()]

    dimensions = dict(os='a', browser='a', cpu='a')
    test_config = test_request_message.TestConfiguration(
        dimensions=dimensions)
    test_config.Validate()
    self.AssertValidValues('configuration', [test_config])
    self.test_request.configuration = (
        TestConfigurationTest.GetFullObject())

    self.AssertValidValues('ping_url', TestHelper.VALID_URL_VALUES)
    self.AssertValidValues('ping_delay', TestHelper.VALID_INT_VALUES)

    self.AssertValidValues('env_vars', TestHelper.VALID_ENV_VARS)

    # Now try invalid values.
    self.AssertInvalidValues(
        'data', TestHelper.INVALID_URL_LOCAL_PATH_TUPLES_LISTS)
    # Put the value back to a valid value, to test invalidity of other values.
    self.test_request.data = TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS[-1]

    test_object1.io_time_out = 'never'
    with self.assertRaises(test_request_message.Error):
      test_object1.Validate()
    test_object2.action = 'None'
    with self.assertRaises(test_request_message.Error):
      test_object2.Validate()
    self.AssertInvalidValues('tests', [[test_object1, test_object2],
                                       [test_object1]])
    self.test_request.tests = []

    test_config.dimensions = [42]
    with self.assertRaises(test_request_message.Error):
      test_config.Validate()
    test_config.dimensions = {'a': 42}
    self.AssertInvalidValues('configuration', [test_config])
    self.test_request.configuration = TestConfigurationTest.GetFullObject()

    self.AssertInvalidValues('result_url', TestHelper.INVALID_URL_VALUES)
    self.test_request.result_url = TestHelper.VALID_URL_VALUES[0]

    self.AssertInvalidValues('ping_url', TestHelper.INVALID_URL_VALUES)
    self.test_request.ping_url = TestHelper.VALID_URL_VALUES[0]

    self.AssertInvalidValues('ping_delay',
                             TestHelper.INVALID_POSITIVE_INT_VALUES)
    self.test_request.ping_delay = TestHelper.VALID_INT_VALUES[0]

    self.AssertInvalidValues('env_vars', TestHelper.INVALID_ENV_VARS)
    self.test_request.env_vars = None

  def testStringize(self):
    # Vanilla object.
    new_object = test_request_message.TestRun.FromJSON(
        test_request_message.Stringize(self.test_request, json_readable=True))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestRunTest.GetFullObject()
    full_object.Validate()

    new_object = test_request_message.TestRun.FromJSON(
        test_request_message.Stringize(full_object, json_readable=True))
    self.assertEqual(new_object, full_object)


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
