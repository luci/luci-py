# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Top-level presubmit script for appengine/components/.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gclient.
"""


def CommonChecks(input_api, output_api):
  output = []
  def join(*args):
    return input_api.os_path.join(input_api.PresubmitLocalPath(), *args)

  block_list = list(input_api.DEFAULT_BLOCK_LIST) + [
    r'.*_pb2\.py$',
  ]
  disabled_warnings = [
    # Pylint fails to recognize lazy module loading in components.auth.config,
    # no local disables work, so had to kill it globally.
    'cyclic-import',
    'relative-import',
  ]
  output.extend(input_api.canned_checks.RunPylint(
      input_api, output_api,
      block_list=block_list,
      disabled_warnings=disabled_warnings))

  test_directories = [
    join('components'),
    join('components', 'auth'),
    join('components', 'auth', 'ui'),
    join('components', 'config'),
    join('components', 'datastore_utils'),
    join('components', 'endpoints_webapp2'),
    join('components', 'ereporter2'),
    join('components', 'protoutil'),
    join('components', 'prpc'),
    join('components', 'prpc', 'discovery'),
    join('components', 'stats_framework'),
    join('tests'),
  ]
  tests = []
  for directory in test_directories:
    tests.extend(
        input_api.canned_checks.GetUnitTestsInDirectory(
            input_api, output_api,
            directory,
            whitelist=[r'.+_test\.py$'],
            blacklist=[]))
  output.extend(input_api.RunTests(tests, parallel=True))
  return output


# pylint: disable=unused-argument
def CheckChangeOnUpload(input_api, output_api):
  return []


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
