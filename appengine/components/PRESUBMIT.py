# Copyright 2013 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Top-level presubmit script for appengine/components/.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gclient.
"""


def CommonChecks(input_api, output_api):
  files_to_skip = list(input_api.DEFAULT_FILES_TO_SKIP) + [
      r'.*_pb2\.py$',
      r'.*tools[\\/].*$',  # python3, while pylint is still python2
  ]
  disabled_warnings = [
    # Pylint fails to recognize lazy module loading in components.auth.config,
    # no local disables work, so had to kill it globally.
    'cyclic-import',
    'relative-import',
  ]
  pylintrc = input_api.os_path.join(input_api.PresubmitLocalPath(), '../../',
                                    'pylintrc')
  return input_api.canned_checks.RunPylint(input_api,
                                           output_api,
                                           files_to_skip=files_to_skip,
                                           disabled_warnings=disabled_warnings,
                                           pylintrc=pylintrc,
                                           version='1.5')


# pylint: disable=unused-argument
def CheckChangeOnUpload(input_api, output_api):
  return []


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
