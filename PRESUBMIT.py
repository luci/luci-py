# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""Top-level presubmit script for isolateserver.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gcl.
"""

def CommonChecks(input_api, output_api):
  output = []
  test_directory = input_api.os_path.join(
      input_api.PresubmitLocalPath(), 'tests')

  import sys
  old_sys_path = sys.path
  try:
    sys.path = [test_directory] + sys.path
    disabled_warnings = [
        'E1101',  # Instance X has no member Y
        'W0232', # Class has no __init__ method
    ]
    output.extend(input_api.canned_checks.RunPylint(
        input_api, output_api, disabled_warnings=disabled_warnings))
  finally:
    sys.path = old_sys_path

  output.extend(
      input_api.canned_checks.RunUnitTestsInDirectory(
          input_api, output_api,
          test_directory,
          whitelist=[r'.+_test\.py$'],
          blacklist=[r'.+_smoke_test\.py$']))
  return output


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  output = CommonChecks(input_api, output_api)
  output.extend(input_api.canned_checks.PanProjectChecks(
      input_api, output_api, owners_check=False))
  return output
