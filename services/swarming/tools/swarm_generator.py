# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""A base class for creating Swarm files.

The typical usage is to create a derived class that overrides some of the
methods like GetTestsArray() (which is like a pure virtual not implemented on
the base class) and others like GetDataFilesToZip() and/or
GetOtherDataFileUrls() and/or GetOtherLocalDataFiles().

Then, create a main function that instantiates the derived class and calls
Main() on it.

For example:

import sys

import swarm_generator


class MySwarm(swarm_generator.SwarmGenerator):

  def GetDataFilesToZip(self):
    return ['one_file_i_need.py', 'another_file_i_need.dat']

  def GetTestsArray(self):
    return [
      {
        'test_name': 'My Test',
        'action': [
          'python',
          '-u',
          'one_file_i_need.py',
          '-i',
          'another_file_i_need.dat',
        ],
      },
    ]


if __name__ == '__main__':
  my_swarm = MySwarm()
  sys.exit(my_swarm.Main())
"""


import datetime
import logging
import optparse
import os.path
import shutil
import sys
import tempfile
import urllib
import zipfile

# Add swarm folder to system path.
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from tools import dimensions


class SwarmGenerator(object):
  """A base class for creating Swarm files."""

  def __init__(self):
    self.description = """This script simplifies the creation of a test request
to run tests on one or more configurations. More details on
http://goto/swarm.
"""
    self.all_config_names = dimensions.DIMENSIONS.keys()

    # Create the base option parser so that derived classes can modify it.
    self.parser = optparse.OptionParser(usage='%prog [options]',
                                        description=self.description)
    self.parser.add_option('-v', '--verbose', action='store_true',
                           help='Set logging level to DEBUG. Optional. '
                           'Defaults to ERROR level.')
    self.parser.add_option('-l', action='store_true', dest='list_configs',
                           help='Print the names of the available configs. '
                           'Takes precedence over all other options.')
    self.parser.add_option('-r', '--local_root',
                           help='Local root folder (above src) where the files '
                           r'are to be read from (e.g., d:\src\ceee). '
                           'Optional, but needed if there are local files to '
                           'be read.')
    self.parser.add_option('-s', action='store_true', dest='single_config',
                           help='Specify that a single config test run should '
                           'be created. Optional. Defaults to False for '
                           'multi-config test case.', default=False)
    self.parser.add_option('-n', '--test_name',
                           help='The name of the test. Also used for zip '
                           'file base name and swarm file name. Optional. '
                           'Defaults to %s.' % self.GetDefaultTestName(),
                           default=self.GetDefaultTestName())
    self.parser.add_option('-c', '--config', action='append',
                           help='Specify a configuration to run the tests on. '
                           'Can specify more than once. Optional. Defaults to '
                           'all if a multi-config test case is created, and '
                           'the first config in the list is chosen for a '
                           'single-config test run.')
    self.parser.add_option('-t', '--target',
                           help='Debug|Release target. Optional. Defaults to '
                           'Debug.', default='Debug')
    self.parser.add_option('-p', '--unc_base_path',
                           help='Where to copy the data files.')
    self.parser.add_option('-u', '--http_base_url',
                           help='Where to load the data files from.')
    self.parser.add_option('-m', '--failure_email',
                           help='The email where to send failed results. '
                           'Optional.')
    self.parser.add_option('-o', '--result_url',
                           help='Where to post the results of the tests. '
                           'Optional. Defaults to %s.' %
                           self.GetDefaultResultUrl(),
                           default=self.GetDefaultResultUrl())
    self.parser.add_option('-d', '--destination_path',
                           help='Where to save the swarm file. Optional. '
                           'Defaults to current folder and test name.',
                           default='.')
    self.parser.add_option('-b', '--verbose_test', action='store_true',
                           help='Indicates that the Swarm test case should '
                           'display verbose logging. Optional. Defaults to '
                           'False.',
                           default=False)

    self.options = None

  def GetDefaultTestName(self):  # pylint: disable=R0201
    """Allows derived classes to specify the default test name.

    The default test name is used if none was specified as a command
    line argument. It is used as the test_case_name (or test_run_name) field of
    the test request as well as the name of the generated zip file and the
    name of the output Swarm file.

    Returns:
      A string reprensenting the test name. Base class returns 'Test'.
    """
    return 'Test'

  def GetDefaultResultUrl(self):  # pylint: disable=R0201
    """Allows derived classes to specify the default result URL.

    The default result URL is used if none was specified as a command
    line argument. It is used as the result_url field of the test request.

    Returns:
      A base result URL that will be used to set the result_url field.
      Base class returns 'http://mad-tests.appspot.com/result'.
    """
    return 'http://mad-tests.appspot.com/result'

  def GetDataFilesToZip(self):  # pylint: disable=R0201
    """Allows derived classes to specify other data files to be zipped.

    The resulting zip file will be copied to the base UNC path.

    Returns:
      An array of local data files to be zipped.
      Base class returns an empty array.
    """
    return []

  def GetOtherLocalDataFiles(self):  # pylint: disable=R0201
    """Allows derived classes to specify other local data files.

    Local data files are copied to the data UNC path so that they can be
    accessed via the data URL.

    Returns:
      An array of local data files. Base class returns an empty array.
    """
    return []

  def GetOtherDataFileUrls(self):  # pylint: disable=R0201
    """Allows derived classes to specify other data file URLs.

    Other data file URLs are simply appended to the test request's data field.

    Returns:
      An array of data file URLs. Base class returns an empty array.
    """
    return []

  def LogError(self, error_message):
    """Logs an error message and prints the command line argument details.

    Args:
      error_message: The error message to display.
    """
    if hasattr(self, 'parser'):
      print self.parser.format_help()
    logging.error(error_message)

  def ValidateOptions(self):
    """Validate the values of self.options which are set in __init__.

    Returns:
      True if all values are valid. False otherwise.
    """
    if not hasattr(self, 'options'):
      self.LogError('Options have not been parsed yet.')
      return False

    if (self.options.local_root and
        (not (os.path.exists(self.options.local_root) and
              os.path.isdir(self.options.local_root)))):
      self.LogError('%s is not a valid local root path.' %
                    self.options.local_root)
      return False

    if self.options.target.lower() not in ['debug', 'release']:
      self.LogError('%s is not a valid target. Must be Debug or Release.' %
                    self.options.target)
      return False

    # The destination path must exist if it's a folder or be contained in
    # a valid folder otherwise.
    if os.path.isdir(self.options.destination_path):
      path_to_check = self.options.destination_path
    else:
      path_to_check = os.path.dirname(self.options.destination_path)
    if not os.path.exists(path_to_check):
      self.LogError('Destination path doesn\'t exist: "%s".' % path_to_check)
      return False

    # If no configs were specified, we use all of them.
    if not self.options.config:
      if self.options.single_config:
        self.options.config = self.all_config_names[:1]
      else:
        self.options.config = self.all_config_names[:]
    else:
      # But if some were, they have to be part of the list we know of.
      for config in self.options.config:
        if config.lower() not in self.all_config_names:
          error_message = ('Unknown config: %s\nConfig name must be in:\n%s' %
                           (config, self.all_config_names))
          self.LogError(error_message)
          return False

    return True

  def CreateAndUploadZipFile(self):
    """Creates a zip file based on the list returned by GetDataFilesToZip.

    The file is created in a temp folder, copied to self.options.unc_base_path,
    and then the temp file is removed.

    Returns:
      The name of the zip file or None if none was created.

    Raises:
      IOError: if some files can't be added to the zip file.
    """
    # The derived class can specify the list of files to zip.
    files_to_zip = self.GetDataFilesToZip()[:]
    if not files_to_zip:
      return None

    assert self.options.local_root, ('Zipping local files requires a local '
                                     'root dir to be specified.')

    if not os.path.exists(self.options.unc_base_path):
      try:
        os.makedirs(self.options.unc_base_path)
      except os.error as e:
        logging.exception('Can\'t create inexistent unc_base_path: %s', e)
        raise IOError(e)

    temp_folder = tempfile.gettempdir()
    zip_file_name = self.options.test_name + '.zip'
    zip_file_path = os.path.join(temp_folder, zip_file_name)

    logging.info('Zipping files from %s to %s.',
                 self.options.local_root, zip_file_path)

    def AddFolderToZip(zip_file, folder_path, folder_children):
      """Used by os.walk to add folder content to the zip_file."""
      for folder_child in folder_children:
        child_path = os.path.join(folder_path, folder_child)
        if not os.path.isdir(child_path):
          zip_file.write(child_path, child_path.replace(self.options.local_root,
                                                        ''))

    zip_file = zipfile.ZipFile(zip_file_path, 'w')
    for file_to_zip in files_to_zip:
      full_path_to_zip = os.path.join(self.options.local_root, file_to_zip)
      if not os.path.exists(full_path_to_zip):
        zip_file.close()
        logging.error('Invalid file to zip: %s.', full_path_to_zip)
        os.remove(zip_file_path)
        raise IOError()
      elif os.path.isdir(full_path_to_zip):
        os.path.walk(full_path_to_zip, AddFolderToZip, zip_file)
      else:
        zip_file.write(full_path_to_zip, file_to_zip)
    zip_file.close()

    logging.info('Copying new archive to %s.', self.options.unc_base_path)
    shutil.copy(zip_file_path, self.options.unc_base_path)

    logging.info('Removing new archive local copy %s.', zip_file_path)
    os.remove(zip_file_path)

    return zip_file_name

  def GetAllDataFileUrls(self):
    """Returns the list of data file URLs to be added to the test request.

    We may also generate a new zip file that will be copied to
    self.options.unc_base_path.

    Returns:
      The list of data file URLs.
    """
    zip_file_name = self.CreateAndUploadZipFile()

    # Allow derived classes to specify other data file URLs.
    # Base class returns an empty list, which we can append to.
    data_file_urls = self.GetOtherDataFileUrls()[:]
    if zip_file_name:
      data_file_urls.append('%s/%s' % (self.options.http_base_url,
                                       urllib.quote(zip_file_name)))
    local_data_files = self.GetOtherLocalDataFiles()
    if local_data_files:
      assert self.options.local_root, ('Copying local files requires a local '
                                       'root dir to be specified.')
    for local_data_file in local_data_files:
      local_data_file_path = os.path.join(self.options.local_root,
                                          local_data_file)
      logging.info('Copying %s to %s.', local_data_file_path,
                   self.options.unc_base_path)
      shutil.copy(local_data_file_path, self.options.unc_base_path)
      data_file_urls.append('%s/%s' %
                            (self.options.http_base_url,
                             urllib.quote(os.path.basename(local_data_file))))
    return data_file_urls

  def CreateTestRequest(self, tests_array):
    """Creates a test request based on all options and given tests_array.

    Args:
      tests_array: The validated array of tests to put in the requests.

    Returns:
      A test request dictionary.
    """
    assert isinstance(tests_array, (list, tuple))
    assert tests_array  # Should not be empty.

    name_suffix = datetime.datetime.utcnow().strftime('-%Y-%m%d-%Hh%Mm%Ss')
    test_name_suffix = ''
    # For multi config without a specific test name,
    # we add a test_name_suffix to avoid collisions.
    if (self.options.test_name == self.GetDefaultTestName() and
        not self.options.single_config):
      test_name_suffix = name_suffix

    # When asked to generate a request for a single config we create a test run.
    # Otherwise, it's a test case. See http://goto/swarm for more details.
    if self.options.single_config:
      test_name_key = 'test_run_name'
    else:
      test_name_key = 'test_case_name'

    test_request = {
        test_name_key: '%s%s'% (self.options.test_name, test_name_suffix),
        'data': self.GetAllDataFileUrls(),
        'tests': tests_array,
        'result_url': self.options.result_url,
        'failure_email': self.options.failure_email,  # OK to be None.
        'verbose': self.options.verbose_test,
    }

    if self.options.single_config:
      assert self.options.config
      test_request['configuration'] = {
          'config_name': self.options.config[0],
          'dimensions': dimensions.DIMENSIONS[
              self.options.config[0].lower()]}
    else:
      test_request['configurations'] = []
      for config in self.options.config:
        test_request['configurations'].append({
            'config_name': config,
            'dimensions': dimensions.DIMENSIONS[config.lower()]})
    return test_request

  def SaveTestRequest(self, test_request):
    """Save the given test request to a new file based on options.

    Args:
      test_request: The test request dictionary to save to a file.

    Returns:
      0 if we succeeded, and non-0 if we failed.
    """
    if os.path.isdir(self.options.destination_path):
      self.options.destination_path = os.path.join(
          self.options.destination_path, '%s.swarm' % self.options.test_name)

    logging.info('Saving swarm file: %s.', self.options.destination_path)
    if os.path.exists(self.options.destination_path):
      logging.warning('Will attempt to overwrite existing file.')

    with open(self.options.destination_path, 'w') as f:
      f.write(str(test_request))

    return 0

  def Main(self):
    """Main entry point doing all the work :-).

    Returns:
      0 for success, and non-0 for failures.
    """
    (self.options, args) = self.parser.parse_args()

    if self.options.verbose:
      logging.getLogger().setLevel(logging.DEBUG)
    else:
      logging.getLogger().setLevel(logging.ERROR)

    if args:
      logging.warning('Ignoring extra arguments: %s', args)

    if self.options.list_configs:
      print 'Available configuration names:'
      print self.all_config_names
      return 0

    if not self.ValidateOptions():
      return -1

    # Let derived class specify tests and return early if they don't.
    # Note that the base class doesn't have a default implementation, so
    # derived classes MUST provide one.
    tests_array = self.GetTestsArray()
    if not tests_array:
      self.LogError('There must be an array of test objects!')
      return -1

    if self.options.single_config:
      assert self.options.config
      logging.info('Will create a test run for this config: %s.',
                   self.options.config[0])
      if len(self.options.config) > 1:
        logging.warning('These extra configs will be ignored: %s',
                        self.options.config[1:])
    else:
      logging.info('Will create a test case for these configs:\n[%s].',
                   ' \n'.join(self.options.config))

    try:
      return self.SaveTestRequest(self.CreateTestRequest(tests_array))
    except IOError as e:
      logging.exception('Failed to create and save test request: %s', e)
      return -1
