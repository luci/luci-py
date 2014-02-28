#!/usr/bin/env python
# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Unit tests for the SwarmGenerator class."""

import StringIO
import logging
import os
import shutil
import sys
import tempfile
import unittest
import zipfile

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import test_env

test_env.setup_test_env()

from depot_tools import auto_stub
from tools import dimensions
from tools import swarm_generator


class SwarmGeneratorTest(auto_stub.TestCase):
  """A test case class for the SwarmGenerator class."""

  class DerivedSwarmGeneratorBase(swarm_generator.SwarmGenerator):
    def __init__(self):
      # These are needed in the base class constructor.
      self.default_test_name = 'default_test_name'
      self.default_result_url = 'default_result_url'
      self.default_data_base_url = 'default_data_base_url'
      self.default_data_base_unc_path = '.'

      super(SwarmGeneratorTest.DerivedSwarmGeneratorBase, self).__init__()
      self.data_files_to_zip = []
      self.other_local_data_files = []
      self.other_data_file_urls = []
      self.config_names = dimensions.DIMENSIONS.keys()

      class Options(object):
        pass
      self.options = Options()
      self.options.verbose = None
      self.options.list_configs = False
      self.options.local_root = None
      self.options.single_config = False
      self.options.test_name = self.GetDefaultTestName()
      self.options.config = []
      self.options.target = 'Debug'
      self.options.unc_base_path = self.GetDefaultDataBaseUncPath()
      self.options.http_base_url = self.GetDefaultDataBaseUrl()
      self.options.result_url = self.GetDefaultResultUrl()
      self.options.destination_path = '.'
      self.options.verbose_test = False

    def GetDefaultTestName(self):
      return self.default_test_name

    def GetDefaultResultUrl(self):
      return self.default_result_url

    def GetDefaultDataBaseUrl(self):
      return self.default_data_base_url

    def GetDefaultDataBaseUncPath(self):
      return self.default_data_base_unc_path

    def GetDataFilesToZip(self):
      return self.data_files_to_zip

    def GetOtherLocalDataFiles(self):
      return self.other_local_data_files

    def GetOtherDataFileUrls(self):
      return self.other_data_file_urls

    def CreateParser(self):
      class TestParser(object):
        def __init__(self, options, args):
          self.options = options
          self.args = args

        def parse_args(self):
          return (self.options, self.args)

        def format_help(self):  # pylint:disable=R0201
          return ''
      self.parser = TestParser(self.options, None)

  class DerivedSwarmGenerator(DerivedSwarmGeneratorBase):
    def __init__(self):
      super(SwarmGeneratorTest.DerivedSwarmGenerator, self).__init__()
      self.tests_array = []

    def GetTestsArray(self):
      return self.tests_array

  def setUp(self):
    self.derived_swarm_generator_base = (
        SwarmGeneratorTest.DerivedSwarmGeneratorBase())
    self.derived_swarm_generator = SwarmGeneratorTest.DerivedSwarmGenerator()
    self.valid_local_root = '.'
    self.invalid_path_value = '/I can\'t exist:/\\'
    self.invalid_target = 'not valid'
    self.invalid_config = 'Not a valid config'
    self.valid_test_array = [{'test_name': 'test_name1',
                              'action': ['a1', 'a2']},
                             {'test_name': 'test_name2',
                              'action': ['b1', 'b2']}]
    (temp_file_descriptor, self.temp_file_name) = tempfile.mkstemp()
    os.close(temp_file_descriptor)

    default_swarm_request_file = (
        self.derived_swarm_generator.GetDefaultTestName() + '.swarm')

    self.files_to_remove = [self.temp_file_name,
                            default_swarm_request_file]

    self.trees_to_remove = []

  def tearDown(self):
    if hasattr(self, 'trees_to_remove'):
      for tree_to_remove in self.trees_to_remove:
        shutil.rmtree(tree_to_remove)
    if hasattr(self, 'files_to_remove'):
      for file_to_remove in self.files_to_remove:
        if os.path.exists(file_to_remove):
          os.remove(file_to_remove)

  def testValidateOptions(self):
    self.derived_swarm_generator.options.local_root = self.invalid_path_value
    self.assertFalse(self.derived_swarm_generator.ValidateOptions())

    # Local root can't be a file, must be a folder
    self.derived_swarm_generator.options.local_root = __file__
    self.assertFalse(self.derived_swarm_generator.ValidateOptions())

    self.derived_swarm_generator.options.local_root = self.valid_local_root
    self.assertTrue(self.derived_swarm_generator.ValidateOptions())

    self.derived_swarm_generator.options.target = self.invalid_target
    self.assertFalse(self.derived_swarm_generator.ValidateOptions())

    self.derived_swarm_generator.options.target = 'Debug'
    self.assertTrue(self.derived_swarm_generator.ValidateOptions())

    self.derived_swarm_generator.options.destination_path = (
        self.invalid_path_value)
    self.assertFalse(self.derived_swarm_generator.ValidateOptions())

    self.derived_swarm_generator.options.destination_path = os.path.abspath(
        __file__)
    self.assertTrue(self.derived_swarm_generator.ValidateOptions())

    self.derived_swarm_generator.options.config = self.invalid_config
    self.assertFalse(self.derived_swarm_generator.ValidateOptions())

    self.derived_swarm_generator.options.config = None
    self.assertTrue(self.derived_swarm_generator.ValidateOptions())

  def testCreateAndUploadZipFile(self):
    self.derived_swarm_generator.options.local_root = os.path.dirname(
        self.temp_file_name)
    # If we don't specify any files to zip, this should return None.
    self.assertEqual(None,
                     self.derived_swarm_generator.CreateAndUploadZipFile())

    # Now give it something to chew on...
    temp_file_name = os.path.basename(self.temp_file_name)
    self.derived_swarm_generator.data_files_to_zip = [temp_file_name]

    # An invalid unc path should convert the os.error into an IOError.
    self.derived_swarm_generator.options.unc_base_path = self.invalid_path_value
    self.assertRaises(IOError,
                      self.derived_swarm_generator.CreateAndUploadZipFile)

    # Now, specify a valid folder for the zip destination.
    # We can't use the temp folder since it is used to copy the zip file from.
    unc_base_path = tempfile.mkdtemp()
    self.derived_swarm_generator.options.unc_base_path = unc_base_path

    zip_file_name = self.derived_swarm_generator.CreateAndUploadZipFile()
    self.assertNotEqual(None, zip_file_name)

    zip_file = zipfile.ZipFile(os.path.join(unc_base_path, zip_file_name))
    zipped_files = zip_file.namelist()
    self.assertEqual(1, len(zipped_files))
    self.assertEqual(temp_file_name, zipped_files[0])

    zip_file.close()
    self.trees_to_remove = [unc_base_path]

  def testGetAllDataFileUrls(self):
    # By default, nothing is done.
    self.assertEqual([],
                     self.derived_swarm_generator.GetAllDataFileUrls())

    self.derived_swarm_generator.other_data_file_urls = ['a']
    self.assertEqual(['a'],
                     self.derived_swarm_generator.GetAllDataFileUrls())

    unc_base_path = tempfile.mkdtemp()
    self.derived_swarm_generator.options.unc_base_path = unc_base_path

    self.derived_swarm_generator.options.local_root = os.path.dirname(
        self.temp_file_name)
    temp_file_name = os.path.basename(self.temp_file_name)
    self.derived_swarm_generator.other_local_data_files = [temp_file_name]
    http_base_url = 'http_base_url'
    self.derived_swarm_generator.options.http_base_url = http_base_url
    full_data_file_url = '%s/%s' % (http_base_url, temp_file_name)
    self.assertEqual(['a', full_data_file_url],
                     self.derived_swarm_generator.GetAllDataFileUrls())

    self.derived_swarm_generator.data_files_to_zip = [temp_file_name]
    test_name = 'test_name'
    self.derived_swarm_generator.options.test_name = test_name
    full_zip_file_url = '%s/%s.zip' % (http_base_url, test_name)
    self.assertEqual(['a', full_zip_file_url, full_data_file_url],
                     self.derived_swarm_generator.GetAllDataFileUrls())

    self.trees_to_remove = [unc_base_path]

  def testCreateTestRequest(self):
    # Most basic test_run case.
    self.derived_swarm_generator.options.single_config = True
    config_names = dimensions.DIMENSIONS.keys()
    self.derived_swarm_generator.options.config = [config_names[0]]
    self.assertEqual(
        {'test_run_name': self.derived_swarm_generator.default_test_name,
         'tests': [0],
         'data': [],
         'result_url': self.derived_swarm_generator.GetDefaultResultUrl(),
         'configuration': {
             'config_name': config_names[0],
             'dimensions': dimensions.DIMENSIONS[config_names[0]]},
         'verbose': False},
        self.derived_swarm_generator.CreateTestRequest([0]))

    # Make sure we only use a single config, even if we specify more than one.
    self.derived_swarm_generator.options.config = [config_names[3],
                                                   config_names[2],
                                                   config_names[1]]
    self.assertEqual(
        {'test_run_name': self.derived_swarm_generator.default_test_name,
         'tests': [7],
         'data': [],
         'result_url': self.derived_swarm_generator.GetDefaultResultUrl(),
         'configuration': {
             'config_name': config_names[3],
             'dimensions': dimensions.DIMENSIONS[config_names[3]]},
         'verbose': False},
        self.derived_swarm_generator.CreateTestRequest([7]))

    # More complex test case.
    self.derived_swarm_generator.options.single_config = False
    self.derived_swarm_generator.options.config = config_names[1:3]

    # Specify our own name to avoid the time stamp suffix
    self.derived_swarm_generator.options.test_name = 'test_name'

    self.derived_swarm_generator.tests_array = self.valid_test_array
    self.derived_swarm_generator.other_data_file_urls = ['data1', 'data2']

    self.assertEqual(
        {'test_case_name': 'test_name',
         'tests': self.valid_test_array,
         'data': ['data1', 'data2'],
         'result_url': self.derived_swarm_generator.GetDefaultResultUrl(),
         'configurations': [
             {'config_name': config_names[1],
              'dimensions': dimensions.DIMENSIONS[config_names[1]]},
             {'config_name': config_names[2],
              'dimensions': dimensions.DIMENSIONS[config_names[2]]}],
         'verbose': False},
        self.derived_swarm_generator.CreateTestRequest(self.valid_test_array))

    # Single config test case
    self.derived_swarm_generator.options.config = config_names[:1]
    self.assertEqual(
        {'test_case_name': 'test_name',
         'tests': self.valid_test_array[1:],
         'data': ['data1', 'data2'],
         'result_url': self.derived_swarm_generator.GetDefaultResultUrl(),
         'configurations': [{
             'config_name': config_names[0],
             'dimensions': dimensions.DIMENSIONS[config_names[0]]}],
         'verbose': False},
        self.derived_swarm_generator.CreateTestRequest(
            self.valid_test_array[1:]))

  def testMain(self):
    self.mock(sys, 'stdout', StringIO.StringIO())
    # The base class doesn't implement GetTestsArray and this raises an error.
    self.derived_swarm_generator_base.options.local_root = self.valid_local_root
    self.assertRaises(AttributeError, self.derived_swarm_generator_base.Main)

    self.derived_swarm_generator.options.local_root = self.valid_local_root

    # By default we have an empty test array and it's not valid.
    # Main returns 0 for success and non-0 for failures.
    self.assertNotEqual(0, self.derived_swarm_generator.Main())

    self.derived_swarm_generator.tests_array = self.valid_test_array

    self.derived_swarm_generator.options.list_configs = True
    self.assertEqual(0, self.derived_swarm_generator.Main())


if __name__ == '__main__':
  # We don't want the application logs to interfere with our own messages.
  # You swap the following lines for more information when debugging.
  # logging.getLogger().setLevel(logging.DEBUG)
  logging.disable(logging.ERROR)
  unittest.main()
