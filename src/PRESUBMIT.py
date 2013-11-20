# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Top-level presubmit script for swarming-server.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gcl.
"""


def CommonChecks(input_api, output_api):
  output = []
  def join(*args):
    return input_api.os_path.join(input_api.PresubmitLocalPath(), *args)

  disabled_warnings = [
      'E0611', # No name X in module Y
      'F0401', # Unable to import X
      'W0232', # Class has no __init__ method
      #TODO (csharp): Enable the warnings below.
      'E1002',
      'R0201',
      'W0201',
      'W0212',
      'W0222',
      'W0231',
  ]
  output.extend(input_api.canned_checks.RunPylint(
      input_api, output_api, disabled_warnings=disabled_warnings))

  test_directories = [
      input_api.PresubmitLocalPath(),
      join('common'),
      join('server'),
      join('stats'),
      join('swarm_bot'),
      join('tests'),
      join('tools'),
  ]

  # post_test.py isn't a test, it it meant to post a test to the server.
  blacklist = ['post_test.py']

  for directory in test_directories:
    output.extend(
        input_api.canned_checks.RunUnitTestsInDirectory(
            input_api, output_api,
            directory,
            whitelist=[r'.+_test\.py$'],
            blacklist=blacklist))

  return output


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
