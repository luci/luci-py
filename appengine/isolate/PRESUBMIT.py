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
    sys.path = [input_api.PresubmitLocalPath()] + sys.path
    # pylint: disable=relative-import
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
    appcfg.fix_sys_path()
    # Add project specific paths to sys.path
    sys.path = [
      join('components', 'third_party'),
      join('tests'),
      # See tests/test_env.py for more information.
      join('third_party'),
      join('..', 'components'),
      join('..', 'components', 'third_party'),
    ] + sys.path
    black_list = list(input_api.DEFAULT_BLACK_LIST) + [
      r'.*_pb2\.py$',
    ]
    disabled_warnings = [
    ]
    output.extend(input_api.canned_checks.RunPylint(
        input_api, output_api,
        black_list=black_list,
        disabled_warnings=disabled_warnings))
  finally:
    sys.path = old_sys_path

  # TODO(maruel): Create a smoke test that use local servers.
  tests = input_api.canned_checks.GetUnitTestsInDirectory(
      input_api, output_api,
      join('tests'),
      whitelist=[r'.+_test\.py$'],
      blacklist=[r'.+_smoke_test\.py$'])
  output.extend(input_api.RunTests(tests, parallel=True))
  return output


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
