# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Top-level presubmit script for swarming-server.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gclient.
"""

USE_PYTHON3 = True


def CommonChecks(input_api, output_api):
  files_to_skip = list(input_api.DEFAULT_FILES_TO_SKIP) + [
    r"ui2/node_modules/.*",
    r"ui2/nodejs/.*",
    r".*_pb2\.py$",
  ]
  disabled_warnings = [
    "bad-plugin-value",
    "consider-using-dict-items",
    "consider-using-f-string",
    "consider-using-from-import",
    "consider-using-generator",
    "consider-using-in",
    "consider-using-max-builtin",
    "consider-using-min-builtin",
    "consider-using-with",
    "contextmanager-generator-missing-cleanup",
    "line-too-long",
    "missing-parentheses-for-call-in-test",
    "missing-timeout",
    "no-value-for-parameter",
    "possibly-used-before-assignment",
    "relative-import",
    "unknown-option-value",
    "unnecessary-lambda-assignment",
    "unrecognized-option",
    "unspecified-encoding",
    "unsupported-assignment-operation",
    "unused-variable",
    "use-dict-literal",
    "use-yield-from",
    "used-before-assignment",
    "useless-object-inheritance",
    "useless-option-value",
  ]
  return input_api.canned_checks.RunPylint(
    input_api,
    output_api,
    files_to_skip=files_to_skip,
    disabled_warnings=disabled_warnings,
    pylintrc=input_api.os_path.join(
      input_api.PresubmitLocalPath(), "../../", "pylintrc"
    ),
    version="3.2",
  )


# pylint: disable=unused-argument
def CheckChangeOnUpload(input_api, output_api):
  return []


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
