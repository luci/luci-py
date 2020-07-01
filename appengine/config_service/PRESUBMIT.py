# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Top-level presubmit script for config_service.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gclient.
"""


def CommonChecks(input_api, output_api):
  output = []

  block_list = list(input_api.DEFAULT_BLOCK_LIST) + [
    r'.*_pb2\.py$',
  ]
  output.extend(input_api.canned_checks.RunPylint(
      input_api, output_api,
      block_list=block_list))

  tests = input_api.canned_checks.GetUnitTestsInDirectory(
      input_api, output_api,
      input_api.PresubmitLocalPath(),
      whitelist=[r'.+_test\.py$'])
  output.extend(input_api.RunTests(tests, parallel=True))
  return output


# pylint: disable=unused-argument
def CheckChangeOnUpload(input_api, output_api):
  return []


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
