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


class TestRequestMessageBaseTest(unittest.TestCase):
  def setUp(self):
    self.trm = test_request_message.TestRequestMessageBase()

  def testValidateValues(self):
    self.trm.a = 1
    self.trm.b = ''
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateValues(['a'], str)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateValues(['b'], str, required=True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateValues(['c'], str, required=True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateValues(['a', 'b'], int, required=True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateValues(['b', 'a'], str)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateValues(['b', 'c'], str)

    self.trm.c = 3
    self.trm.d = 'a'
    self.trm.ValidateValues(['a', 'c'], int, required=True)
    self.trm.ValidateValues(['a', 'c'], int)
    self.trm.ValidateValues(['b', 'd'], str)

  def testValidateLists(self):
    self.trm.a = 1
    self.trm.b = ''
    self.trm.c = []
    self.trm.d = [1]
    self.trm.e = ['a']
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateLists(['a'], int, required=True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateLists(['b'], str)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateLists(['c'], str, required=True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateLists(['d'], str)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateLists(['e'], int, required=True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateLists(['d', 'e'], int)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateLists(['e', 'd'], str, required=True)

    self.trm.ValidateLists(['c'], int)
    self.trm.ValidateLists(['c', 'd'], int)
    self.trm.ValidateLists(['e', 'c'], str)
    self.trm.ValidateLists(['d'], int, required=True)
    self.trm.ValidateLists(['e'], str, required=True)

  def testValidateObjectLists(self):
    class Validatable(test_request_message.TestRequestMessageBase):
      def __init__(self, is_valid=True):
        self.is_valid = is_valid

      def Validate(self):
        if not self.is_valid:
          raise test_request_message.Error('Oops')

    class InvalidObject(test_request_message.TestRequestMessageBase):
      def Validate(self):
        raise test_request_message.Error('Oops')

    self.trm.a = Validatable()
    self.trm.b = [Validatable()]
    self.trm.c = [InvalidObject()]
    self.trm.d = [Validatable(), InvalidObject()]
    self.trm.e = [Validatable(), Validatable(False)]
    self.trm.f = []
    self.trm.g = [Validatable(), Validatable()]

    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['a'], Validatable, False)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['b'], InvalidObject, True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['c'], InvalidObject, False)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['d'], Validatable, True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['e'], Validatable, False)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['f'], Validatable, True)

    # Success!
    self.trm.ValidateObjectLists(['b'], Validatable, True)
    self.trm.ValidateObjectLists(['f'], Validatable, False)
    self.trm.ValidateObjectLists(['g'], Validatable, False)

    # Not unique names.
    self.trm.g[0].a = 'a'
    self.trm.g[1].a = 'a'
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['g'], Validatable, False, ['a'])
    self.trm.g[1].a = 'b'
    self.trm.ValidateObjectLists(['g'], Validatable, False, ['a'])

    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['a'], Validatable, False, [])
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['b'], InvalidObject, True, [])
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateObjectLists(['c'], InvalidObject, False, [])

  def testValidateUrlLists(self):
    self.trm.a = []
    self.trm.b = ['http://localhost:9001']
    self.trm.c = ['http://www.google.com']
    self.trm.d = [1]
    self.trm.e = ['http://www.google.com', 'a']
    self.trm.f = 1

    self.trm.ValidateUrlLists(['a'])
    self.trm.ValidateUrlLists(['b'])
    self.trm.ValidateUrlLists(['c'])
    self.trm.ValidateUrlLists(['a', 'b'])
    self.trm.ValidateUrlLists(['b', 'c'])

    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateUrlLists(['a'], required=True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateUrlLists(['d'])
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateUrlLists(['e'])
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateUrlLists(['f'])
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateUrlLists(['d'], True)
    with self.assertRaises(test_request_message.Error):
      self.trm.ValidateUrlLists(['d', 'e'], False)

  def testExpandVariables(self):
    self.trm.a = '%(var1)s'
    self.trm.b = ['%(var1)s', '%(var2)d']
    self.trm.c = test_request_message.TestRequestMessageBase()
    self.trm.c.a = '%(var3)d'
    self.trm.c.b = [('%(var4)s',), ('%(var1)s', '%(var2)d', '%(var3)d')]
    self.trm.c.c = 42
    self.trm.d = None
    self.trm.e = {'%(var4)s': '%(var1)s', '%(var2)d': '%(var3)d'}
    url = 'http://www.google.com/hi%20world'
    self.trm.f = url
    self.trm.g = url + '%(var1)s'

    self.trm.ExpandVariables({'var1': 'one', 'var2': 22, 'var3': 333,
                              'var4': 'four'})
    self.assertEqual('one', self.trm.a)
    self.assertEqual(['one', '22'], self.trm.b)
    self.assertEqual('333', self.trm.c.a)
    self.assertEqual([('four',), ('one', '22', '333')], self.trm.c.b)
    self.assertEqual(42, self.trm.c.c)
    self.assertEqual(None, self.trm.d)
    # We intentionnaly don't expand key names, just values!
    self.assertEqual({'%(var4)s': 'one', '%(var2)d': '333'}, self.trm.e)
    self.assertEqual(url, self.trm.f)
    self.assertEqual(url + 'one', self.trm.g)

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

  VALID_OPTIONAL_OUTPUT_DESTINATION_VALUES = [
    {},
    None,
    {'size': 10.0},
    {'size': 5},
    {'size': '12'},
    {'size': -5},
    {'size': '0'},
    {'size': '-5'},
    {'url': 'http://a.b.com', 'size': 1024},
  ]
  INVALID_OUTPUT_DESTINATION_VALUES = ['', 1, 'a', [], {'a': ['b']},
                                       {'a': {1: 2}}, {1: 2}, {'size': 0.5},
                                       {'size': 'size'},
                                       {'url': 'svn://c/test'},
                                       {'url': 5}, {'url': 'invalid'}]

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

  INVALID_CLEANUP_VALUES = (INVALID_STRING_VALUES +
                            ['mad', '7zip', 'binaries', 'tests'])

  VALID_ENCODING_VALUES = ['ascii', 'utf_8', 'latin_1']
  INVALID_ENCODING_VALUES = ['mad', 'my_encoding', 'None', 'unicode']

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
    # Always start with a valid case, and make it explicitly invalid as needed.
    self.test_request = test_request_message.TestObject(
        test_name=TestHelper.VALID_STRING_VALUES[0],
        action=TestHelper.VALID_REQUIRED_STRING_LIST_VALUES[0])

  @staticmethod
  def GetFullObject():
    return test_request_message.TestObject(
        test_name=TestHelper.VALID_STRING_VALUES[-1],
        action=TestHelper.VALID_REQUIRED_STRING_LIST_VALUES[-1],
        env_vars=TestHelper.VALID_ENV_VARS[-1],
        decorate_output=TestHelper.VALID_BOOLEAN_VALUES[-1],
        hard_time_out=42,
        io_time_out=42)

  def testNoReferences(self):
    # Ensure that Test Object makes copies of its input, not references.
    env_vars = {'a': 1}

    test_object = test_request_message.TestObject(env_vars=env_vars)

    env_vars['a'] = 2
    self.assertNotEqual(env_vars, test_object.env_vars)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()

    # And then a few more valid values
    self.AssertValidValues('test_name', TestHelper.VALID_STRING_VALUES)
    self.AssertValidValues('action',
                           TestHelper.VALID_REQUIRED_STRING_LIST_VALUES)
    self.AssertValidValues('hard_time_out', [1.1, 3, 0, .00003])
    self.AssertValidValues('io_time_out', [1.1, 3, 0, .00003])
    self.AssertValidValues('decorate_output', TestHelper.VALID_BOOLEAN_VALUES)
    self.AssertValidValues('env_vars', TestHelper.VALID_ENV_VARS)

    # Now try invalid values.
    self.AssertInvalidValues('test_name',
                             TestHelper.INVALID_REQUIRED_STRING_VALUES)
    # Put the value back to a valid value, to test invalidity of other values.
    self.test_request.test_name = TestHelper.VALID_STRING_VALUES[0]
    self.AssertInvalidValues('action',
                             TestHelper.INVALID_REQUIRED_STRING_LIST_VALUES)
    self.test_request.action = TestHelper.VALID_REQUIRED_STRING_LIST_VALUES[0]
    self.AssertInvalidValues('hard_time_out', ['never', '', {}, [], (1, 2)])
    self.test_request.hard_time_out = None
    self.AssertInvalidValues('io_time_out', ['never', '', {}, [], (1, 2)])
    self.test_request.io_time_out = None
    self.AssertInvalidValues('env_vars', TestHelper.INVALID_ENV_VARS)
    self.test_request.env_vars = None

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
    # Always start with a valid case, and make it explicitly invalid as needed.
    dimensions = dict(os='a', browser='a', cpu='a')
    self.test_request = test_request_message.TestConfiguration(
        config_name='a', dimensions=dimensions)

  @staticmethod
  def GetFullObject():
    dimensions = dict(os='a', browser='a', cpu='a')
    return test_request_message.TestConfiguration(
        config_name='a',
        dimensions=dimensions,
        env_vars=TestHelper.VALID_ENV_VARS[-1],
        data=TestHelper.VALID_URL_LIST_VALUES[-1],
        tests=[TestObjectTest.GetFullObject()],
        min_instances=1,
        additional_instances=1,
        deadline_to_run=1,
        priority=1)

  def testNoReferences(self):
    # Ensure that Test Configuration makes copies of its input, not references.
    env_vars = {'a': 1}
    data = ['data']
    tests = ['test']
    dimensions = {'a': ['b']}

    test_configurations = test_request_message.TestConfiguration(
        env_vars=env_vars, data=data, tests=tests, dimensions=dimensions)

    env_vars['a'] = 2
    self.assertNotEqual(env_vars, test_configurations.env_vars)

    data.append('data2')
    self.assertNotEqual(data, test_configurations.data)

    tests.append('tests2')
    self.assertNotEqual(tests, test_configurations.tests)

    dimensions['c'] = 'd'
    self.assertNotEqual(dimensions, test_configurations.dimensions)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()

    # And then a few more valid values
    self.AssertValidValues('config_name',
                           TestHelper.VALID_STRING_VALUES)
    self.AssertValidValues('data', TestHelper.VALID_OPTIONAL_URL_LIST_VALUES +
                           TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)

    test_object1 = test_request_message.TestObject(test_name='a', action=['a'])
    test_object1.Validate()
    test_object2 = test_request_message.TestObject(
        test_name='b', action=['b', 'c'], decorate_output=False)
    test_object2.Validate()
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = []

    self.AssertValidValues('min_instances',
                           TestHelper.NON_ZERO_VALID_INT_VALUES)
    self.AssertValidValues('additional_instances',
                           TestHelper.VALID_INT_VALUES)

    self.AssertValidValues('deadline_to_run',
                           TestHelper.VALID_INT_VALUES)

    self.AssertValidValues('priority',
                           [0, 10, 33, test_request_message.MAX_PRIORITY_VALUE])

    self.AssertValidValues('env_vars', TestHelper.VALID_ENV_VARS)

    for value in TestHelper.VALID_STRING_VALUES:
      self.test_request.dimensions[value] = value
    self.test_request.Validate()

    # Now try invalid values.
    self.AssertInvalidValues('config_name',
                             TestHelper.INVALID_REQUIRED_STRING_VALUES)
    # Put the value back to a valid value, to test invalidity of other values.
    self.test_request.config_name = TestHelper.VALID_STRING_VALUES[0]
    self.AssertInvalidValues('data', TestHelper.INVALID_URL_LIST_VALUES +
                             TestHelper.INVALID_URL_LOCAL_PATH_TUPLES_LISTS)
    self.test_request.data = TestHelper.VALID_URL_LIST_VALUES[-1]

    # Test names must be unique.
    test_object2.test_name = test_object1.test_name
    self.AssertInvalidValues('tests', [[test_object1, test_object2]])
    self.test_request.tests = [TestObjectTest.GetFullObject()]

    # Make the names different again so we can test other failures
    test_object2.test_name = '%s2' % test_object1.test_name

    test_object1.io_time_out = 'never'
    with self.assertRaises(test_request_message.Error):
      test_object1.Validate()
    test_object2.action = 'None'
    with self.assertRaises(test_request_message.Error):
      test_object2.Validate()
    self.AssertInvalidValues('tests', [[test_object1, test_object2],
                                       [test_object1]])
    self.test_request.tests = []

    self.AssertInvalidValues('min_instances',
                             TestHelper.NON_ZERO_INVALID_POSITIVE_INT_VALUES)
    self.test_request.min_instances = 1
    self.AssertInvalidValues('additional_instances',
                             TestHelper.INVALID_POSITIVE_INT_VALUES)
    self.test_request.additional_instances = 0

    self.AssertInvalidValues('deadline_to_run',
                             TestHelper.INVALID_POSITIVE_INT_VALUES)
    self.test_request.deadline_to_run = 0

    self.AssertInvalidValues('priority',
                             TestHelper.INVALID_POSITIVE_INT_VALUES +
                             [test_request_message.MAX_PRIORITY_VALUE + 1])
    self.test_request.priority = 10

    self.AssertInvalidValues('env_vars', TestHelper.INVALID_ENV_VARS)
    self.test_request.env_vars = None

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
    # Always start with a valid case, and make it explicitly invalid as needed.
    dimensions = dict(os='a', browser='a', cpu='a')
    self.test_request = test_request_message.TestCase(
        test_case_name='a',
        configurations=[
          test_request_message.TestConfiguration(
              config_name='a', dimensions=dimensions),
        ])

  @staticmethod
  def GetFullObject():
    return test_request_message.TestCase(
        test_case_name='a',
        requestor='user@swarm.com',
        env_vars=TestHelper.VALID_ENV_VARS[-1],
        data=TestHelper.VALID_URL_LIST_VALUES[-1],
        working_dir=TestHelper.VALID_STRING_VALUES[-1],
        admin=TestHelper.VALID_BOOLEAN_VALUES[-1],
        tests=[TestObjectTest.GetFullObject()],
        result_url=TestHelper.VALID_URL_VALUES[-1],
        store_result=
        test_request_message.TestCase.VALID_STORE_RESULT_VALUES[-1],
        restart_on_failure=TestHelper.VALID_BOOLEAN_VALUES[-1],
        output_destination=
        TestHelper.VALID_OPTIONAL_OUTPUT_DESTINATION_VALUES[-1],
        encoding=TestHelper.VALID_ENCODING_VALUES[-1],
        cleanup=test_request_message.TestRun.VALID_CLEANUP_VALUES[-1],
        label=TestHelper.VALID_OPTIONAL_STRING_VALUES[-1],
        verbose=TestHelper.VALID_BOOLEAN_VALUES[-1],
        configurations=[TestConfigurationTest.GetFullObject()])

  def testNoReferences(self):
    # Ensure that Test Case makes copies of its input, not references.
    env_vars = {'a': 1}
    configurations = [TestConfigurationTest.GetFullObject()]
    data = ['data']
    tests = ['test']
    output_destination = {'url': 'http://www.google.com', 'size': 1}

    test_case = test_request_message.TestCase(
        env_vars=env_vars, configurations=configurations, data=data,
        tests=tests, output_destination=output_destination)

    env_vars['a'] = 2
    self.assertNotEqual(env_vars, test_case.env_vars)

    configurations.append(TestConfigurationTest.GetFullObject())
    self.assertNotEqual(configurations, test_case.configurations)

    data.append('data2')
    self.assertNotEqual(data, test_case.data)

    tests.append('tests2')
    self.assertNotEqual(tests, test_case.tests)

    output_destination['size'] = 2
    self.assertNotEqual(output_destination, test_case.output_destination)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()
    # And then a few more valid values
    self.AssertValidValues('test_case_name',
                           TestHelper.VALID_STRING_VALUES)
    # TODO(csharp): requestor should change to be required, not optional.
    self.AssertValidValues('requestor',
                           TestHelper.VALID_OPTIONAL_STRING_VALUES)
    self.AssertValidValues('data', TestHelper.VALID_OPTIONAL_URL_LIST_VALUES +
                           TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)
    self.AssertValidValues('admin', TestHelper.VALID_BOOLEAN_VALUES)

    test_object1 = test_request_message.TestObject(test_name='a', action=['a'])
    test_object1.Validate()
    test_object2 = test_request_message.TestObject(
        test_name='b', action=['b', 'c'], decorate_output=False)
    test_object2.Validate()
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = [TestObjectTest.GetFullObject()]

    test_config1 = test_request_message.TestConfiguration(
        config_name='a', dimensions=dict(os='a', browser='a', cpu='a'))
    test_config1.Validate()
    test_config2 = test_request_message.TestConfiguration(
        config_name='b', dimensions=dict(os='b', browser='b', cpu='b'),
        data=['http://a.com'])
    test_config2.Validate()
    self.AssertValidValues('configurations', [[test_config1, test_config2],
                                              [test_config1]])
    self.test_request.configurations = [TestConfigurationTest.GetFullObject()]

    self.AssertValidValues('result_url', TestHelper.VALID_OPTIONAL_URL_VALUES)
    valid_store_result = test_request_message.TestCase.VALID_STORE_RESULT_VALUES
    self.AssertValidValues('store_result', valid_store_result)

    self.AssertValidValues('restart_on_failure',
                           TestHelper.VALID_BOOLEAN_VALUES)
    self.AssertValidValues('output_destination',
                           TestHelper.VALID_OPTIONAL_OUTPUT_DESTINATION_VALUES)
    self.AssertValidValues('encoding',
                           TestHelper.VALID_ENCODING_VALUES)

    self.AssertValidValues('cleanup',
                           test_request_message.TestRun.VALID_CLEANUP_VALUES)
    self.AssertValidValues('label',
                           TestHelper.VALID_OPTIONAL_STRING_VALUES)
    self.AssertValidValues('working_dir',
                           TestHelper.VALID_OPTIONAL_STRING_VALUES)
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
    self.AssertInvalidValues('data', TestHelper.INVALID_URL_LIST_VALUES +
                             TestHelper.INVALID_URL_LOCAL_PATH_TUPLES_LISTS)
    self.test_request.data = TestHelper.VALID_URL_LIST_VALUES[-1]
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
    test_config2.tests = 'None'
    with self.assertRaises(test_request_message.Error):
      test_config2.Validate()
    self.AssertInvalidValues('configurations', [[test_config1, test_config2],
                                                [test_config1]])
    # Put the value back to a valid value, to test invalidity of other values.
    dimensions = dict(os='a', browser='a', cpu='a')
    valid_config = test_request_message.TestConfiguration(
        config_name='a', dimensions=dimensions)
    self.test_request.configurations = [valid_config]

    self.AssertInvalidValues('result_url', TestHelper.INVALID_URL_VALUES)
    self.test_request.result_url = TestHelper.VALID_URL_VALUES[0]

    invalid_store_result = (TestHelper.INVALID_STRING_VALUES +
                            ['all_results', 'some', 'mine'])
    self.assertFalse(any(i in valid_store_result for i in invalid_store_result))
    self.AssertInvalidValues('store_result', invalid_store_result)
    self.test_request.store_result = None

    self.AssertInvalidValues('output_destination',
                             TestHelper.INVALID_OUTPUT_DESTINATION_VALUES)
    self.test_request.output_destination = None

    self.AssertInvalidValues('encoding', TestHelper.INVALID_ENCODING_VALUES)
    self.test_request.encoding = TestHelper.VALID_ENCODING_VALUES[-1]

    self.AssertInvalidValues('label', TestHelper.INVALID_STRING_VALUES)
    self.test_request.label = None

    self.AssertInvalidValues('working_dir', TestHelper.INVALID_STRING_VALUES)
    self.test_request.working_dir = None

    self.AssertInvalidValues('cleanup', TestHelper.INVALID_CLEANUP_VALUES)
    self.test_request.cleanup = None

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

  def testEquivalent(self):
    test_case = test_request_message.TestCase()
    self.assertTrue(test_case.Equivalent(test_case))

    # Test that certain values don't affect equivalence
    equivalent_test_case = test_request_message.TestCase()
    self.assertTrue(test_case.Equivalent(equivalent_test_case))

    equivalent_test_case.cleanup = 'root'
    self.assertTrue(test_case.Equivalent(equivalent_test_case))

    equivalent_test_case.label = 'important'
    self.assertTrue(test_case.Equivalent(equivalent_test_case))

    equivalent_test_case.requestor = 'chris'
    self.assertTrue(test_case.Equivalent(equivalent_test_case))

    equivalent_test_case.working_dir = 'new_folder'
    self.assertTrue(test_case.Equivalent(equivalent_test_case))

    # Test that we can get failures.
    equivalent_test_case.admin = not equivalent_test_case.admin
    self.assertFalse(test_case.Equivalent(equivalent_test_case))

  def testEquivalentsDifferentConfigs(self):
    test_case = test_request_message.TestCase(
      configurations=[test_request_message.TestConfiguration()])
    different_test_case = test_request_message.TestCase(
      configurations=[test_request_message.TestConfiguration()])

    self.assertTrue(test_case.Equivalent(different_test_case))

    # Ensure that a request with the same name, but a different config will
    # fail.
    different_test_case.configurations[0].config_name = 'different_name'
    self.assertFalse(test_case.Equivalent(different_test_case))
    different_test_case.configurations[0].config_name = (
        test_case.configurations[0].config_name)

    # Ensure that a request with a matching config, but additional configs as
    # well, fails.
    different_test_case.configurations.append(
        different_test_case.configurations[0])
    self.assertFalse(test_case.Equivalent(different_test_case))

  def testEquivalentMultipleConfigs(self):
    num_configs = 2
    test_case = test_request_message.TestCase()
    for i in range(num_configs):
      test_case.configurations.append(
          test_request_message.TestConfiguration(config_name=str(i)))

    self.assertTrue(test_case.Equivalent(test_case))
    # Change the position of the position of the configs and ensure they are
    # still equivalent.
    equivalent_test_case = test_request_message.TestCase()
    for i in range(num_configs):
      equivalent_test_case.configurations.append(
          test_request_message.TestConfiguration(config_name=str(i)))

    equivalent_test_case.configurations = list(reversed(
        equivalent_test_case.configurations))

    self.assertTrue(test_case.Equivalent(equivalent_test_case))


class TestRunTest(TestHelper):
  def setUp(self):
    # Always start with a valid case, and make it explicitly invalid as needed.
    dimensions = dict(os='a', browser='a', cpu='a')
    self.test_request = test_request_message.TestRun(
        test_run_name='a',
        configuration=test_request_message.TestConfiguration(
            config_name='a', dimensions=dimensions),
        result_url=TestHelper.VALID_URL_VALUES[0],
        ping_url=TestHelper.VALID_URL_VALUES[0],
        ping_delay=TestHelper.VALID_INT_VALUES[0],
        encoding=TestHelper.VALID_ENCODING_VALUES[0])

  @staticmethod
  def GetFullObject():
    return test_request_message.TestRun(
        test_run_name=TestHelper.VALID_STRING_VALUES[-1],
        configuration=TestConfigurationTest.GetFullObject(),
        env_vars=TestHelper.VALID_ENV_VARS[-1],
        data=TestHelper.VALID_URL_LIST_VALUES[-1],
        working_dir=TestHelper.VALID_STRING_VALUES[-1],
        tests=[TestObjectTest.GetFullObject()],
        instance_index=1,
        num_instances=2,
        result_url=TestHelper.VALID_URL_VALUES[-1],
        ping_url=TestHelper.VALID_URL_VALUES[-1],
        ping_delay=TestHelper.VALID_INT_VALUES[-1],
        output_destination=
        TestHelper.VALID_OPTIONAL_OUTPUT_DESTINATION_VALUES[-1],
        cleanup=test_request_message.TestRun.VALID_CLEANUP_VALUES[-1],
        restart_on_failure=TestHelper.VALID_BOOLEAN_VALUES[-1],
        encoding=TestHelper.VALID_ENCODING_VALUES[-1])

  def testNoReferences(self):
    # Ensure that Test Run makes copies of its input, not references.
    env_vars = {'a': 1}
    data = ['data']
    tests = ['test']
    output_destination = {'url': 'http://www.google.com', 'size': 1}

    test_run = test_request_message.TestRun(
        env_vars=env_vars, data=data, tests=tests,
        output_destination=output_destination)

    env_vars['a'] = 2
    self.assertNotEqual(env_vars, test_run.env_vars)

    data.append('data2')
    self.assertNotEqual(data, test_run.data)

    tests.append('tests2')
    self.assertNotEqual(tests, test_run.tests)

    output_destination['size'] = 2
    self.assertNotEqual(output_destination, test_run.output_destination)

  def testValidate(self):
    # Start with default success.
    self.test_request.Validate()

    # And then a few more valid values
    self.AssertValidValues('test_run_name',
                           TestHelper.VALID_STRING_VALUES)
    self.AssertValidValues('data', TestHelper.VALID_OPTIONAL_URL_LIST_VALUES +
                           TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)

    test_object1 = test_request_message.TestObject(test_name='a', action=['a'])
    test_object1.Validate()
    test_object2 = test_request_message.TestObject(
        test_name='b', action=['b', 'c'], decorate_output=False)
    test_object2.Validate()
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = [TestObjectTest.GetFullObject()]

    self.test_request.num_instances = max(TestHelper.VALID_INT_VALUES) + 1
    self.AssertValidValues('instance_index', TestHelper.VALID_INT_VALUES)
    self.test_request.instance_index = 0
    self.AssertValidValues('num_instances',
                           TestHelper.NON_ZERO_VALID_INT_VALUES)

    dimensions = dict(os='a', browser='a', cpu='a')
    test_config = test_request_message.TestConfiguration(
        config_name='a', dimensions=dimensions)
    test_config.Validate()
    self.AssertValidValues('configuration', [test_config])
    self.test_request.configuration = (
        TestConfigurationTest.GetFullObject())

    self.AssertValidValues('result_url', TestHelper.VALID_OPTIONAL_URL_VALUES)
    self.AssertValidValues('ping_url', TestHelper.VALID_URL_VALUES)
    self.AssertValidValues('ping_delay', TestHelper.VALID_INT_VALUES)
    self.AssertValidValues('output_destination',
                           TestHelper.VALID_OPTIONAL_OUTPUT_DESTINATION_VALUES)
    self.AssertValidValues('working_dir',
                           TestHelper.VALID_OPTIONAL_STRING_VALUES)

    self.AssertValidValues('cleanup',
                           test_request_message.TestRun.VALID_CLEANUP_VALUES)
    self.AssertValidValues('env_vars', TestHelper.VALID_ENV_VARS)
    self.AssertValidValues('restart_on_failure',
                           TestHelper.VALID_BOOLEAN_VALUES)
    self.AssertValidValues('encoding', TestHelper.VALID_ENCODING_VALUES)

    # Now try invalid values.
    self.AssertInvalidValues('test_run_name',
                             TestHelper.INVALID_REQUIRED_STRING_VALUES)
    # Put the value back to a valid value, to test invalidity of other values.
    self.test_request.test_run_name = TestHelper.VALID_STRING_VALUES[0]
    self.AssertInvalidValues('data', TestHelper.INVALID_URL_LIST_VALUES +
                             TestHelper.INVALID_URL_LOCAL_PATH_TUPLES_LISTS)
    self.test_request.data = TestHelper.VALID_URL_LIST_VALUES[-1]

    test_object1.io_time_out = 'never'
    with self.assertRaises(test_request_message.Error):
      test_object1.Validate()
    test_object2.action = 'None'
    with self.assertRaises(test_request_message.Error):
      test_object2.Validate()
    self.AssertInvalidValues('tests', [[test_object1, test_object2],
                                       [test_object1]])
    self.test_request.tests = []

    self.AssertInvalidValues('instance_index', TestHelper.INVALID_INT_VALUES)
    self.test_request.instance_index = 0
    self.AssertInvalidValues('num_instances',
                             TestHelper.NON_ZERO_INVALID_INT_VALUES)

    self.test_request.instance_index = 2
    self.test_request.num_instances = 1
    with self.assertRaises(test_request_message.Error):
      self.test_request.Validate()

    self.test_request.instance_index = 2
    self.test_request.num_instances = 2
    with self.assertRaises(test_request_message.Error):
      self.test_request.Validate()

    self.test_request.instance_index = 1
    self.test_request.num_instances = 2

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

    self.AssertInvalidValues('output_destination',
                             TestHelper.INVALID_OUTPUT_DESTINATION_VALUES)
    self.test_request.output_destination = (
        TestHelper.VALID_OPTIONAL_OUTPUT_DESTINATION_VALUES[-1])

    self.AssertInvalidValues('working_dir', TestHelper.INVALID_STRING_VALUES)
    self.test_request.working_dir = None

    self.AssertInvalidValues('cleanup', TestHelper.INVALID_CLEANUP_VALUES)
    self.test_request.cleanup = None

    self.AssertInvalidValues('env_vars', TestHelper.INVALID_ENV_VARS)
    self.test_request.env_vars = None

    self.AssertInvalidValues('encoding', TestHelper.INVALID_ENCODING_VALUES)
    self.test_request.encoding = TestHelper.VALID_ENCODING_VALUES[-1]

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
