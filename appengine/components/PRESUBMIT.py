# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Top-level presubmit script for appengine/components/.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gcl.
"""


def FindAppEngineSDK(input_api):
  """Returns an absolute path to AppEngine SDK (or None if not found)."""
  import sys
  old_sys_path = sys.path
  try:
    sys.path = [input_api.PresubmitLocalPath()] + sys.path
    from tool_support import gae_sdk_utils
    return gae_sdk_utils.find_gae_sdk()
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
    appcfg.fix_sys_path(appcfg.ENDPOINTSCFG_EXTRA_PATHS)
    # Add project specific paths to sys.path
    sys.path = [
      join('..', 'third_party_local'),
      join('third_party'),
    ] + sys.path
    black_list = list(input_api.DEFAULT_BLACK_LIST) + [
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
        black_list=black_list,
        disabled_warnings=disabled_warnings))
  finally:
    sys.path = old_sys_path

  # Add the commented out directories when adding tests.
  test_directories = [
    join('components'),
    join('components', 'auth'),
    join('components', 'auth', 'ui'),
    join('components', 'datastore_utils'),
    join('components', 'ereporter2'),
  ]
  blacklist = []
  if not input_api.is_committing:
    # Skip smoke tests on upload.
    blacklist.append(r'.+_smoke_test\.py$')
  else:
    # 'tests' contains only smoke tests. Add it only when committing (otherwise
    # depot_tools emits warning "Out of 4 files, found none that matched ...").
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
