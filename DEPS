# Copyright 2021 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# Any git deps in this repo should be added as submodules.
git_dependencies = "SUBMODULES"

use_relative_paths = True

deps = {
  'appengine/swarming/ui2/nodejs/': {
    'packages': [
      {
        'package': 'infra/3pp/tools/nodejs/${{platform}}',
        'version': 'version:2@20.10.0',
      }
    ],
    'dep_type': 'cipd',
    'condition': 'checkout_x64',
  },

  # luci-go clients are used for client/run_isolated.py and integration tests.
  'luci-go': {
    'packages': [
      {
        'package': 'infra/tools/luci/cas/${{platform}}',
        'version': 'git_revision:218dc31109b1f821c47053c46c0e066058b4601d',
      },
      {
        'package': 'infra/tools/luci/fakecas/${{platform}}',
        'version': 'git_revision:a9a10995e2889126ace91faf4052949e38c69d33',
      },
      {
        'package': 'infra/tools/luci/isolate/${{platform}}',
        'version': 'git_revision:14c1dfd95e07b23eeaff304bd29a2eb80b06fe0b',
      },
      {
        'package': 'infra/tools/luci/swarming/${{platform}}',
        'version': 'git_revision:218dc31109b1f821c47053c46c0e066058b4601d',
      }
    ],
    'dep_type': 'cipd',
    'condition': 'checkout_x64',
  },

  # Nsjail is used for our unit tests.
  'nsjail': {
    'packages': [
      {
        'package': 'infra/3pp/tools/nsjail/${{platform}}',
        'version': 'version:2@3.0.chromium.1',
      }
    ],
    "condition": "checkout_linux",
    'dep_type': 'cipd',
  },
}
