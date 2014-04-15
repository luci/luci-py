# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Top-level presubmit script for swarming-server.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gcl.
"""


def FindAppEngineSDK(input_api):
  """Returns an absolute path to AppEngine SDK (or None if not found)."""
  import sys
  old_sys_path = sys.path
  try:
    # Add 'tools' to sys.path to be able to import find_gae_sdk.
    tools_dir = input_api.os_path.join(
        input_api.PresubmitLocalPath(), '..', 'tools')
    sys.path = [tools_dir] + sys.path
    # pylint: disable=F0401
    import find_gae_sdk
    return find_gae_sdk.find_gae_sdk()
  finally:
    sys.path = old_sys_path


def CommonChecks(input_api, output_api):
  output = []
  def join(*args):
    return input_api.os_path.join(input_api.PresubmitLocalPath(), *args)

  gae_sdk_path = FindAppEngineSDK(input_api)
  if not gae_sdk_path:
    output.append(output_api.PresubmitError('Couldn\'t find AppEngine SDK.'))
    return output

  import sys
  old_sys_path = sys.path
  try:
    # Add GAE SDK modules to sys.path.
    sys.path = [gae_sdk_path] + sys.path
    import appcfg
    appcfg.fix_sys_path()
    # Add project specific paths to sys.path
    sys.path = [
      join('components', 'third_party'),
      join('swarm_bot'),
      join('..', 'tools'),
      join('..', 'tools', 'third_party'),
    ] + sys.path
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
  finally:
    sys.path = old_sys_path

  test_directories = [
      input_api.PresubmitLocalPath(),
      join('common'),
      join('server'),
      join('stats'),
      join('swarm_bot'),
  ]

  blacklist = []
  if not input_api.is_committing:
    # Skip smoke tests on upload.
    blacklist.append(r'.+_smoke_test\.py$')
  else:
    # GetUnitTestsInDirectory() will error out if it doesn't find anything to
    # run in a directory and 'tests' only contain smoke tests, so only add the
    # directory if smoke tests are to be run.
    test_directories.append(join('tests'))

  tests = []
  for directory in test_directories:
    tests.extend(
        input_api.canned_checks.GetUnitTestsInDirectory(
            input_api, output_api,
            directory,
            whitelist=[r'.+_test\.py$'],
            blacklist=blacklist))
  output.extend(input_api.RunTests(tests, parallel=True))
  return output


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
