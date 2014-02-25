# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Top-level presubmit script for golang implementations.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into git-cl.
"""


def CommonChecks(input_api, output_api):
  output = []
  cmds = []

  extra = []
  if input_api.verbose:
    extra.append('--verbose')
  if any(f.LocalPath().endswith('.go') for f in input_api.AffectedFiles()):
    base_cmd = [input_api.python_executable, 'run_go_tests.py']
    # This tests the local implementation.
    cmds.append(
        input_api.Command(
            name='run_go_tests.py',
            cmd=base_cmd + extra,
            kwargs={},
            message=output_api.PresubmitError))

    # This tests the AppEngine implementation. It is super slow so only do this
    # on commit.
    if input_api.is_committing:
      cmds.append(
          input_api.Command(
              name='run_go_tests.py --gae',
              cmd=base_cmd + ['--gae'] + extra,
              kwargs={},
              message=output_api.PresubmitError))
  if cmds:
    output.extend(input_api.RunTests(cmds))

  return output


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  return CommonChecks(input_api, output_api)
