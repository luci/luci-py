# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Presubmit for GCE Backend.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into git cl.
"""

def CommonChecks(input_api, output_api):
  tests = input_api.canned_checks.GetUnitTestsInDirectory(
      input_api,
      output_api,
      input_api.PresubmitLocalPath(),
      whitelist=[r'.+_test\.py$'],
  )
  return input_api.RunTests(tests, parallel=True)


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
