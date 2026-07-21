# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Top-level presubmit script for appengine/components/.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gclient.
"""

USE_PYTHON3 = True


def CommonChecks(input_api, output_api):
  files_to_skip = list(input_api.DEFAULT_FILES_TO_SKIP) + [
    r".*_pb2\.py$",
  ]
  disabled_warnings = [
    "arguments-renamed",
    "bad-plugin-value",
    "chained-comparison",
    "consider-using-f-string",
    "consider-using-with",
    "cyclic-import",
    "keyword-arg-before-vararg",
    "line-too-long",
    "possibly-used-before-assignment",
    "relative-import",
    "return-in-finally",
    "unknown-option-value",
    "unnecessary-dunder-call",
    "unnecessary-lambda-assignment",
    "unrecognized-option",
    "unspecified-encoding",
    "unused-argument",
    "use-yield-from",
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
