#!/usr/bin/python2.7
# coding=utf-8
# Copyright 2011 Google Inc. All Rights Reserved.

"""Unit tests for classes implemented in test_request_message.py."""





import json
import logging
import os.path
import unittest

from common import test_request_message


class TestRequestMessageBaseTest(unittest.TestCase):
  def setUp(self):
    self.trm = test_request_message.TestRequestMessageBase()

  def testAreValidValues(self):
    self.trm.a = 1
    self.trm.b = ''
    self.assertFalse(self.trm.AreValidValues(['a'], str))
    self.assertFalse(self.trm.AreValidValues(['b'], str, required=True))
    self.assertFalse(self.trm.AreValidValues(['c'], str, required=True))
    self.assertFalse(self.trm.AreValidValues(['a', 'b'], int, required=True))
    self.assertFalse(self.trm.AreValidValues(['b', 'a'], str))
    self.assertFalse(self.trm.AreValidValues(['b', 'c'], str))

    self.trm.c = 3
    self.trm.d = 'a'
    self.assertTrue(self.trm.AreValidValues(['a', 'c'], int, required=True))
    self.assertTrue(self.trm.AreValidValues(['a', 'c'], int))
    self.assertTrue(self.trm.AreValidValues(['b', 'd'], str))

    # Returning errors.
    errors = []
    self.assertFalse(self.trm.AreValidValues(['a'], str, False, errors))
    self.assertTrue(errors, 'There must be at least one error!')
    self.assertFalse(self.trm.AreValidValues(['b'], str, True, errors))
    self.assertTrue(len(errors) > 1, 'There must be at least two errors!')
    self.assertFalse(self.trm.AreValidValues(['c'], str, True, errors))
    self.assertTrue(len(errors) > 2, 'There must be at least three errors!')

  def testAreValidLists(self):
    self.trm.a = 1
    self.trm.b = ''
    self.trm.c = []
    self.trm.d = [1]
    self.trm.e = ['a']
    self.assertFalse(self.trm.AreValidLists(['a'], int, required=True))
    self.assertFalse(self.trm.AreValidLists(['b'], str))
    self.assertFalse(self.trm.AreValidLists(['c'], str, required=True))
    self.assertFalse(self.trm.AreValidLists(['d'], str))
    self.assertFalse(self.trm.AreValidLists(['e'], int, required=True))
    self.assertFalse(self.trm.AreValidLists(['d', 'e'], int))
    self.assertFalse(self.trm.AreValidLists(['e', 'd'], str, required=True))

    self.assertTrue(self.trm.AreValidLists(['c'], int))
    self.assertTrue(self.trm.AreValidLists(['c', 'd'], int))
    self.assertTrue(self.trm.AreValidLists(['e', 'c'], str))
    self.assertTrue(self.trm.AreValidLists(['d'], int, required=True))
    self.assertTrue(self.trm.AreValidLists(['e'], str, required=True))

    # Returning errors.
    errors = []
    self.assertFalse(self.trm.AreValidLists(['c'], str, True, errors))
    self.assertTrue(errors, 'There must be at least one error!')
    self.assertFalse(self.trm.AreValidLists(['d'], str, False, errors))
    self.assertTrue(len(errors) > 1, 'There must be at least two errors!')
    self.assertFalse(self.trm.AreValidLists(['e'], int, True, errors))
    self.assertTrue(len(errors) > 2, 'There must be at least three errors!')
    errors = []
    self.assertFalse(self.trm.AreValidLists(['d', 'e'], int, False, errors))
    self.assertTrue(errors, 'There must be at least one error!')
    self.assertFalse(self.trm.AreValidLists(['e', 'd'], str, True, errors))
    self.assertTrue(len(errors) > 1, 'There must be at least two errors!')
    self.assertFalse(self.trm.AreValidLists('b', str, True, errors))
    self.assertTrue(len(errors) > 2, 'There must be at least three errors!')
    self.assertFalse(self.trm.AreValidLists('c', str, True, errors))
    self.assertTrue(len(errors) > 3, 'There must be at least four errors!')

  def testAreValidObjectLists(self):
    class Validatable(test_request_message.TestRequestMessageBase):
      def __init__(self, is_valid=True):
        self.is_valid = is_valid

      def IsValid(self, errors=None):
        errors = errors
        return self.is_valid

    class InvalidObject(test_request_message.TestRequestMessageBase):
      def IsValid(self, errors=None):
        errors = errors
        return False

    self.trm.a = Validatable()
    self.trm.b = [Validatable()]
    self.trm.c = [InvalidObject()]
    self.trm.d = [Validatable(), InvalidObject()]
    self.trm.e = [Validatable(), Validatable(False)]
    self.trm.f = []
    self.trm.g = [Validatable(), Validatable()]

    self.assertFalse(self.trm.AreValidObjectLists(['a'], Validatable, False))
    self.assertFalse(self.trm.AreValidObjectLists(['b'], InvalidObject, True))
    self.assertFalse(self.trm.AreValidObjectLists(['c'], InvalidObject, False))
    self.assertFalse(self.trm.AreValidObjectLists(['d'], Validatable, True))
    self.assertFalse(self.trm.AreValidObjectLists(['e'], Validatable, False))
    self.assertFalse(self.trm.AreValidObjectLists(['f'], Validatable, True))

    # Success!
    self.assertTrue(self.trm.AreValidObjectLists(['b'], Validatable, True))
    self.assertTrue(self.trm.AreValidObjectLists(['f'], Validatable, False))
    self.assertTrue(self.trm.AreValidObjectLists(['g'], Validatable, False))

    # Not unique names.
    self.trm.g[0].a = 'a'
    self.trm.g[1].a = 'a'
    self.assertFalse(self.trm.AreValidObjectLists(['g'], Validatable, False,
                                                  ['a']))
    self.trm.g[1].a = 'b'
    self.assertTrue(self.trm.AreValidObjectLists(['g'], Validatable, False,
                                                 ['a']))

    # Returning errors.
    errors = []
    self.assertFalse(self.trm.AreValidObjectLists(['a'], Validatable, False,
                                                  [], errors))
    self.assertTrue(errors, 'There must be at least one error!')
    self.assertFalse(self.trm.AreValidObjectLists(['b'], InvalidObject, True,
                                                  [], errors))
    self.assertTrue(len(errors) > 1, 'There must be at least two errors!')
    self.assertFalse(self.trm.AreValidObjectLists(['c'], InvalidObject, False,
                                                  [], errors))
    self.assertTrue(len(errors) > 2, 'There must be at least three errors!')

  def testAreValidUrlLists(self):
    self.trm.a = []
    self.trm.b = ['http://localhost:9001']
    self.trm.c = ['http://www.google.com']
    self.trm.d = [1]
    self.trm.e = ['http://www.google.com', 'a']
    self.trm.f = 1

    self.assertTrue(self.trm.AreValidUrlLists(['a']))
    self.assertTrue(self.trm.AreValidUrlLists(['b']))
    self.assertTrue(self.trm.AreValidUrlLists(['c']))
    self.assertTrue(self.trm.AreValidUrlLists(['a', 'b']))
    self.assertTrue(self.trm.AreValidUrlLists(['b', 'c']))

    self.assertFalse(self.trm.AreValidUrlLists(['a'], required=True))
    self.assertFalse(self.trm.AreValidUrlLists(['d']))
    self.assertFalse(self.trm.AreValidUrlLists(['e']))
    self.assertFalse(self.trm.AreValidUrlLists(['f']))

    # Returning errors.
    errors = []
    self.assertFalse(self.trm.AreValidUrlLists(['d'], True, errors))
    self.assertTrue(len(errors) == 1, 'There must be one error!')
    self.assertFalse(self.trm.AreValidUrlLists(['d', 'e'], False, errors))
    self.assertTrue(len(errors) == 2, 'There must be two errors!')

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

  class ParseResults(test_request_message.TestRequestMessageBase):
    def __init__(self):
      self.str_value = 'a'
      self.int_value = 1
      self.int_array_value = [1, 2]
      self.str_array_value = ['a', 'b', r'a\b', r'\a\t']
      self.dict_value = {'a': 1, 'b': 2}

    def IsValid(self, errors=None):
      errors = errors
      return (
          isinstance(self.str_value, basestring) and
          isinstance(self.int_value, int) and
          isinstance(self.int_array_value, list) and
          not sum([not isinstance(i, int) for i in self.int_array_value]) and
          isinstance(self.str_array_value, list) and
          not sum([not isinstance(i, basestring)
                   for i in self.str_array_value]) and
          isinstance(self.dict_value, dict))

  def testParseTestRequestMessageText(self):
    # Start with the exception raising tests.
    self.assertRaises(test_request_message.Error,
                      self.trm.ParseTestRequestMessageText,
                      'name error')
    self.assertRaises(test_request_message.Error,
                      self.trm.ParseTestRequestMessageText,
                      'syntax_error =')
    self.assertRaises(test_request_message.Error,
                      self.trm.ParseTestRequestMessageText,
                      'type_error = 1 + "2"')

    # Success stories.
    parse_results = TestRequestMessageBaseTest.ParseResults()
    naked_results = TestRequestMessageBaseTest.ParseResults()
    text_request = '{}'
    self.assertTrue(parse_results.ParseTestRequestMessageText(text_request))
    # Make sure no members were added or lost.
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))
    text_request = json.dumps({'ignored_member': 1})
    self.assertTrue(parse_results.ParseTestRequestMessageText(text_request))
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))
    text_request = json.dumps({'str_value': 'new value'})
    self.assertTrue(parse_results.ParseTestRequestMessageText(text_request))
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))
    self.assertEqual(parse_results.str_value, 'new value')
    text_request = json.dumps({
        'str_value': 'newer value',
        'int_value': 2,
        'int_array_value': [3, 4],
        'str_array_value': ['cc', 'dd', 'mm\nn'],
        'dict_value': {'cc': 3, 'dd': 4}})
    self.assertTrue(parse_results.ParseTestRequestMessageText(text_request))
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))
    self.assertEqual(parse_results.str_value, 'newer value')
    self.assertEqual(parse_results.int_value, 2)
    self.assertEqual(parse_results.int_array_value, [3, 4])
    self.assertEqual(parse_results.str_array_value, [u'cc', u'dd', u'mm\nn'])
    self.assertEqual(parse_results.dict_value, {u'cc': 3, u'dd': 4})

    # Now try a few invalid types.
    text_request = json.dumps({'int_value': 'new value'})
    self.assertFalse(parse_results.ParseTestRequestMessageText(text_request))
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))
    text_request = json.dumps({'str_value': 42})
    self.assertFalse(parse_results.ParseTestRequestMessageText(text_request))
    self.assertEqual(len(parse_results.__dict__), len(naked_results.__dict__))

  def testRequestText(self):
    # Success stories.
    parse_results = TestRequestMessageBaseTest.ParseResults()
    expected_text = ("""{'dict_value': {'a': 1, 'b': 2},"""
                     """'int_array_value': [1, 2],'int_value': 1,"""
                     """'str_array_value': ['a', 'b', 'a\\b', '\\a\\t'],"""
                     """'str_value': 'a',}""")
    self.assertEqual(str(parse_results), expected_text)
    parse_results.int_value = 'invalid'
    self.assertEqual(str(parse_results), '')

    # Now test embedded cases.
    class OuterParseResults(test_request_message.TestRequestMessageBase):
      def __init__(self):
        self.str_value = 'a'
        self.parsed_result = TestRequestMessageBaseTest.ParseResults()
        self.results = [TestRequestMessageBaseTest.ParseResults(),
                        TestRequestMessageBaseTest.ParseResults()]
        self.dict_value = {1: 'a', 'b': 2}

      def IsValid(self, errors):
        return (
            isinstance(self.str_value, basestring) and
            self.parsed_result.IsValid(errors) and
            isinstance(self.dict_value, dict))
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

  VALID_URL_VALUES = ['http://a.com', 'https://safe.a.com', 'file://here',
                      'mailto://me@there.com']
  VALID_OPTIONAL_URL_VALUES = VALID_URL_VALUES + EXTRA_OPTIONAL_STRING_VALUES
  INVALID_URL_VALUES = ['httpx://a.com', 'shttps://safe.a.com', 'nfile://here',
                        'mailtoo://me@there.com'] + INVALID_STRING_VALUES

  VALID_URL_LIST_VALUES = [['http://a.com'],
                           ['https://safe.a.com', 'file://here'],
                           ['mailto://me@there.com']]
  VALID_OPTIONAL_URL_LIST_VALUES = VALID_URL_LIST_VALUES + [None, []]
  INVALID_URL_LIST_VALUES = [['httpx://a.com'],
                             ['shttps://safe.a.com', 'nfile://here'],
                             [55]]

  VALID_URL_LOCAL_PATH_TUPLES_LISTS = [[('http://a.com', 'a')],
                                       [['https://safe.a.com', 'b'],
                                        ('file://my_file', 'local/state')]]
  INVALID_URL_LOCAL_PATH_TUPLES_LISTS = ['', 1, ('hello'), [('asd', 'a')],
                                         [['hps://safe.a.com', 'b'],
                                          ('nfile://my_file', 'local/state')],
                                         [('http://www.google.com', 5)],
                                         [('http://a.com', 'b', 'c')]]

  VALID_OPTIONAL_OUTPUT_DESTINATION_VALUES = [{}, None, {'size': 10.0},
                                              {'size': 5}, {'size': '12'},
                                              {'size': -5}, {'size': '0'},
                                              {'size': '-5'},
                                              {'url': 'mailto:joe@doe.com'},
                                              {'url': 'http://a.b.com',
                                               'size': 1024}]
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
      message = 'Validating %s with %s' % (value_key, value)
      setattr(self.test_request, value_key, value)
      self.assertTrue(self.test_request.IsValid(), message)
      errors = []
      self.assertTrue(self.test_request.IsValid(errors), message)
      self.assertEqual(len(errors), 0, message)

  def AssertInvalidValues(self, value_key, values):
    # First check that the test request is valid, otherwise we are failing
    # due to errors other than we intended.
    self.assertTrue(self.test_request.IsValid())

    for value in values:
      message = 'Validating %s with %s' % (value_key, value)
      setattr(self.test_request, value_key, value)
      self.assertFalse(self.test_request.IsValid(), message)
      errors = []
      self.assertFalse(self.test_request.IsValid(errors), message)
      self.assertTrue(errors, message)


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
        time_out=42)

  def testNoReferences(self):
    # Ensure that Test Object makes copies of its input, not references.
    env_vars = {'a': 1}

    test_object = test_request_message.TestObject(env_vars=env_vars)

    env_vars['a'] = 2
    self.assertNotEqual(env_vars, test_object.env_vars)

  def testIsValid(self):
    # Start with default success.
    errors = []
    self.assertTrue(self.test_request.IsValid(errors), errors)
    self.assertFalse(errors, errors)
    self.assertTrue(self.test_request.IsValid())

    # And then a few more valid values
    self.AssertValidValues('test_name', TestHelper.VALID_STRING_VALUES)
    self.AssertValidValues('action',
                           TestHelper.VALID_REQUIRED_STRING_LIST_VALUES)
    self.AssertValidValues('time_out', [1.1, 3, 0, .00003])
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
    self.AssertInvalidValues('time_out', ['never', '', {}, [], (1, 2)])
    self.test_request.time_out = None
    self.AssertInvalidValues('env_vars', TestHelper.INVALID_ENV_VARS)
    self.test_request.env_vars = None

  def testStringize(self):
    # Vanilla object.
    new_object = test_request_message.TestObject()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(self.test_request, json_readable=True)),
                    'Bad string: %s' % unicode(self.test_request))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestObjectTest.GetFullObject()
    errors = []
    self.assertTrue(full_object.IsValid(errors), errors)
    new_object = test_request_message.TestObject()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(full_object, json_readable=True)),
                    'Bad string: %s' % unicode(full_object))
    self.assertEqual(new_object, full_object)


class TestConfigurationTest(TestHelper):
  def setUp(self):
    # Always start with a valid case, and make it explicitly invalid as needed.
    self.test_request = test_request_message.TestConfiguration(
        config_name='a', os='a', browser='a', cpu='a')

  @staticmethod
  def GetFullObject():
    return test_request_message.TestConfiguration(
        config_name='a', os='a', browser='a', cpu='a',
        env_vars=TestHelper.VALID_ENV_VARS[-1],
        data=TestHelper.VALID_URL_LIST_VALUES[-1],
        tests=[TestObjectTest.GetFullObject()],
        min_instances=1,
        additional_instances=1,
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

  def testIsValid(self):
    # Start with default success.
    errors = []
    self.assertTrue(self.test_request.IsValid(errors), errors)
    self.assertFalse(errors, errors)
    self.assertTrue(self.test_request.IsValid())

    # And then a few more valid values
    self.AssertValidValues('config_name',
                           TestHelper.VALID_STRING_VALUES)
    self.AssertValidValues('data', TestHelper.VALID_OPTIONAL_URL_LIST_VALUES +
                           TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)

    test_object1 = test_request_message.TestObject(test_name='a', action=['a'])
    test_object2 = test_request_message.TestObject(
        test_name='b', action=['b', 'c'], decorate_output=False)
    self.assertTrue(test_object1.IsValid())
    self.assertTrue(test_object2.IsValid())
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = []

    self.AssertValidValues('min_instances',
                           TestHelper.NON_ZERO_VALID_INT_VALUES)
    self.AssertValidValues('additional_instances',
                           TestHelper.VALID_INT_VALUES)

    self.AssertValidValues('priority',
                           [0, 10, 33, test_request_message.MAX_PRIORITY_VALUE])

    self.AssertValidValues('env_vars', TestHelper.VALID_ENV_VARS)

    for value in TestHelper.VALID_STRING_VALUES:
      self.test_request.dimensions[value] = value
    message = 'Validating dimensions'
    self.assertTrue(self.test_request.IsValid(), message)
    errors = []
    self.assertTrue(self.test_request.IsValid(errors), message)
    self.assertEqual(len(errors), 0, message)

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

    test_object1.time_out = 'never'
    test_object2.action = 'None'
    self.assertFalse(test_object1.IsValid())
    self.assertFalse(test_object2.IsValid())
    self.AssertInvalidValues('tests', [[test_object1, test_object2],
                                       [test_object1]])
    self.test_request.tests = []

    self.AssertInvalidValues('min_instances',
                             TestHelper.NON_ZERO_INVALID_POSITIVE_INT_VALUES)
    self.test_request.min_instances = 1
    self.AssertInvalidValues('additional_instances',
                             TestHelper.INVALID_POSITIVE_INT_VALUES)
    self.test_request.additional_instances = 0

    self.AssertInvalidValues('priority',
                             TestHelper.INVALID_POSITIVE_INT_VALUES +
                             [test_request_message.MAX_PRIORITY_VALUE + 1])
    self.test_request.priority = 10

    self.AssertInvalidValues('env_vars', TestHelper.INVALID_ENV_VARS)
    self.test_request.env_vars = None

    for value in TestHelper.INVALID_REQUIRED_STRING_VALUES:
      self.test_request.dimensions[str(value)] = value
    message = 'Checking invalid dimensions'
    self.assertFalse(self.test_request.IsValid(), message)
    errors = []
    self.assertFalse(self.test_request.IsValid(errors), message)
    self.assertTrue(errors, message)

  def testStringize(self):
    # Vanilla object.
    new_object = test_request_message.TestConfiguration()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(self.test_request, json_readable=True)),
                    'Bad string: %s' % unicode(self.test_request))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestConfigurationTest.GetFullObject()
    errors = []
    self.assertTrue(full_object.IsValid(errors), errors)
    new_object = test_request_message.TestConfiguration()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(full_object, json_readable=True)),
                    'Bad string: %s' % unicode(full_object))
    self.assertEqual(new_object, full_object)


class TestCaseTest(TestHelper):
  def setUp(self):
    # Always start with a valid case, and make it explicitly invalid as needed.
    self.test_request = test_request_message.TestCase(
        test_case_name='a',
        configurations=[test_request_message.TestConfiguration(
            config_name='a', os='a', browser='a', cpu='a')])

  @staticmethod
  def GetFullObject():
    return test_request_message.TestCase(
        test_case_name='a',
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
        failure_email=TestHelper.VALID_OPTIONAL_STRING_VALUES[-1],
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

  def testIsValid(self):
    # Start with default success.
    errors = []
    self.assertTrue(self.test_request.IsValid(errors), errors)
    self.assertFalse(errors, errors)
    self.assertTrue(self.test_request.IsValid())
    # And then a few more valid values
    self.AssertValidValues('test_case_name',
                           TestHelper.VALID_STRING_VALUES)
    self.AssertValidValues('data', TestHelper.VALID_OPTIONAL_URL_LIST_VALUES +
                           TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)
    self.AssertValidValues('admin', TestHelper.VALID_BOOLEAN_VALUES)

    test_object1 = test_request_message.TestObject(test_name='a', action=['a'])
    test_object2 = test_request_message.TestObject(
        test_name='b', action=['b', 'c'], decorate_output=False)
    self.assertTrue(test_object1.IsValid())
    self.assertTrue(test_object2.IsValid())
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = [TestObjectTest.GetFullObject()]

    test_config1 = test_request_message.TestConfiguration(
        config_name='a', os='a', browser='a', cpu='a')
    test_config2 = test_request_message.TestConfiguration(
        config_name='b', os='b', browser='b', cpu='b',
        data=['http://a.com', 'file://here'])
    self.assertTrue(test_config1.IsValid())
    self.assertTrue(test_config2.IsValid())
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
    self.AssertValidValues('failure_email',
                           TestHelper.VALID_OPTIONAL_STRING_VALUES)
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
    self.AssertInvalidValues('data', TestHelper.INVALID_URL_LIST_VALUES +
                             TestHelper.INVALID_URL_LOCAL_PATH_TUPLES_LISTS)
    self.test_request.data = TestHelper.VALID_URL_LIST_VALUES[-1]
    self.assertTrue(self.test_request.IsValid())

    test_object1.time_out = 'never'
    test_object2.action = 'None'
    self.assertFalse(test_object1.IsValid())
    self.assertFalse(test_object2.IsValid())
    self.AssertInvalidValues('tests', [[test_object1, test_object2],
                                       [test_object1]])
    self.test_request.tests = []

    test_config1.dimensions = [42]
    test_config2.tests = 'None'
    self.assertFalse(test_config1.IsValid())
    self.assertFalse(test_config2.IsValid())
    self.AssertInvalidValues('configurations', [[test_config1, test_config2],
                                                [test_config1]])
    # Put the value back to a valid value, to test invalidity of other values.
    valid_config = test_request_message.TestConfiguration(
        config_name='a', os='a', browser='a', cpu='a')
    self.test_request.configurations = [valid_config]

    self.AssertInvalidValues('result_url', TestHelper.INVALID_URL_VALUES)
    self.test_request.result_url = TestHelper.VALID_URL_VALUES[0]

    invalid_store_result = (TestHelper.INVALID_STRING_VALUES +
                            ['all_results', 'some', 'mine'])
    # pylint: disable=g-long-lambda
    map(lambda i: self.assertFalse(i in valid_store_result),
        invalid_store_result)
    # pylint: enable=g-long-lambda
    self.AssertInvalidValues('store_result', invalid_store_result)
    self.test_request.store_result = None

    self.AssertInvalidValues('output_destination',
                             TestHelper.INVALID_OUTPUT_DESTINATION_VALUES)
    self.test_request.output_destination = None

    self.AssertInvalidValues('encoding', TestHelper.INVALID_ENCODING_VALUES)
    self.test_request.encoding = TestHelper.VALID_ENCODING_VALUES[-1]

    self.AssertInvalidValues('failure_email', TestHelper.INVALID_STRING_VALUES)
    self.test_request.failure_email = None

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
    new_object = test_request_message.TestCase()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(self.test_request, json_readable=True)),
                    'Bad string: %s' % unicode(self.test_request))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestCaseTest.GetFullObject()
    errors = []
    self.assertTrue(full_object.IsValid(errors), errors)
    new_object = test_request_message.TestCase()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(full_object, json_readable=True)),
                    'Bad string: %s' % unicode(full_object))
    self.assertEqual(new_object, full_object)


class TestRunTest(TestHelper):
  def setUp(self):
    # Always start with a valid case, and make it explicitly invalid as needed.
    self.test_request = test_request_message.TestRun(
        test_run_name='a',
        configuration=test_request_message.TestConfiguration(
            config_name='a', os='a', browser='a', cpu='a'),
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

  def testIsValid(self):
    # Start with default success.
    errors = []
    self.assertTrue(self.test_request.IsValid(errors), errors)
    self.assertFalse(errors, errors)
    self.assertTrue(self.test_request.IsValid())

    # And then a few more valid values
    self.AssertValidValues('test_run_name',
                           TestHelper.VALID_STRING_VALUES)
    self.AssertValidValues('data', TestHelper.VALID_OPTIONAL_URL_LIST_VALUES +
                           TestHelper.VALID_URL_LOCAL_PATH_TUPLES_LISTS)

    test_object1 = test_request_message.TestObject(test_name='a', action=['a'])
    test_object2 = test_request_message.TestObject(
        test_name='b', action=['b', 'c'], decorate_output=False)
    self.assertTrue(test_object1.IsValid())
    self.assertTrue(test_object2.IsValid())
    self.AssertValidValues('tests', [[test_object1, test_object2],
                                     [test_object1]])
    self.test_request.tests = [TestObjectTest.GetFullObject()]

    self.test_request.num_instances = max(TestHelper.VALID_INT_VALUES) + 1
    self.AssertValidValues('instance_index', TestHelper.VALID_INT_VALUES)
    self.test_request.instance_index = 0
    self.AssertValidValues('num_instances',
                           TestHelper.NON_ZERO_VALID_INT_VALUES)

    test_config = test_request_message.TestConfiguration(
        config_name='a', os='a', browser='a', cpu='a')
    self.assertTrue(test_config.IsValid())
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

    test_object1.time_out = 'never'
    test_object2.action = 'None'
    self.assertFalse(test_object1.IsValid())
    self.assertFalse(test_object2.IsValid())
    self.AssertInvalidValues('tests', [[test_object1, test_object2],
                                       [test_object1]])
    self.test_request.tests = []

    self.AssertInvalidValues('instance_index', TestHelper.INVALID_INT_VALUES)
    self.test_request.instance_index = 0
    self.AssertInvalidValues('num_instances',
                             TestHelper.NON_ZERO_INVALID_INT_VALUES)

    self.test_request.instance_index = 2
    self.test_request.num_instances = 1
    self.assertFalse(self.test_request.IsValid(), 'Validating num < index')

    self.test_request.instance_index = 2
    self.test_request.num_instances = 2
    self.assertFalse(self.test_request.IsValid(), 'Validating num == index')

    self.test_request.instance_index = 1
    self.test_request.num_instances = 2

    test_config.dimensions = [42]
    self.assertFalse(test_config.IsValid())
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
    new_object = test_request_message.TestRun()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(self.test_request, json_readable=True)),
                    'Bad string: %s' % unicode(self.test_request))
    self.assertEqual(new_object, self.test_request)

    # Full object
    full_object = TestRunTest.GetFullObject()
    errors = []
    self.assertTrue(full_object.IsValid(errors), errors)

    new_object = test_request_message.TestRun()
    self.assertTrue(new_object.ParseTestRequestMessageText(
        test_request_message.Stringize(full_object, json_readable=True)),
                    'Bad string: %s' % unicode(full_object))
    self.assertEqual(new_object, full_object)


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You can comment it out for more information when debugging.
  logging.disable(logging.ERROR)
  unittest.main()
