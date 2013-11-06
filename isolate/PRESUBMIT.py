# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Top-level presubmit script for isolateserver.

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
      # For tests/test_env.py.
      join('tests'),
      # See tests/test_env.py for more information.
      join('..'),
      join('third_party'),
      join('..', 'common', 'tests'),
      join('tests', 'third_party'),
      join('..', 'common', 'third_party'),
      join('..', 'tools'),
    ] + sys.path
    disabled_warnings = [
        'E1101', # Instance X has no member Y
        'W0232', # Class has no __init__ method
    ]
    output.extend(input_api.canned_checks.RunPylint(
        input_api, output_api, disabled_warnings=disabled_warnings))
  finally:
    sys.path = old_sys_path

  output.extend(
      input_api.canned_checks.RunUnitTestsInDirectory(
          input_api, output_api,
          join('tests'),
          whitelist=[r'.+_test\.py$'],
          blacklist=[r'.+_smoke_test\.py$']))
  return output


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
