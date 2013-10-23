# Copyright 2012 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Top-level presubmit script for isolateserver.

See http://dev.chromium.org/developers/how-tos/depottools/presubmit-scripts for
details on the presubmit API built into gcl.
"""

def CommonChecks(input_api, output_api):
  output = []
  def join(*args):
    return input_api.os_path.join(input_api.PresubmitLocalPath(), *args)

  import sys
  old_sys_path = sys.path
  try:
    sys.path = [
      join('tests'),
      join('third_party'),
      join('tests', 'third_party'),
    ] + sys.path
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
          join('tests'),
          whitelist=[r'.+_test\.py$'],
          blacklist=[r'.+_smoke_test\.py$']))
  return output


def CheckChangeOnUpload(input_api, output_api):
  return CommonChecks(input_api, output_api)


def CheckChangeOnCommit(input_api, output_api):
  output = CommonChecks(input_api, output_api)
  current_year = int(input_api.time.strftime('%Y'))
  allowed_years = (str(s) for s in reversed(xrange(2011, current_year + 1)))
  years_re = '(' + '|'.join(allowed_years) + ')'
  license_header = (
    r'.*? Copyright %(year)s The Swarming Authors\. '
      r'All rights reserved\.\n'
    r'.*? Use of this source code is governed by the Apache v2\.0 license '
      r'that can be\n'
    r'.*? found in the LICENSE file\.(?: \*/)?\n'
  ) % {
    'year': years_re,
  }
  output.extend(input_api.canned_checks.PanProjectChecks(
      input_api, output_api, owners_check=False, license_header=license_header))
  return output
